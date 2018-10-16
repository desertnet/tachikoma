#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Table
# ----------------------------------------------------------------------
#
# $Id: Table.pm 31247 2017-11-06 05:42:49Z chris $
#

package Tachikoma::Nodes::Table;
use strict;
use warnings;
use Tachikoma::Nodes::ConsumerBroker;
use Tachikoma::Nodes::Timer;
use Tachikoma;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_INFO
);
use Digest::MD5 qw( md5 );
use Getopt::Long qw( GetOptionsFromString );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = 'v2.0.197';

my $Default_Num_Partitions = 1;
my $Default_Window_Size    = 900;
my $Default_Num_Buckets    = 4;

sub help {
    my $self = shift;
    return <<'EOF';
make_node Table <node name> --num_partitions=<int> \
                            --window_size=<int>    \
                            --num_buckets=<int>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ( $num_partitions, $window_size, $num_buckets );
        my ( $r, $argv ) = GetOptionsFromString(
            $arguments,
            'num_partitions=i' => \$num_partitions,
            'window_size=i'    => \$window_size,
            'num_buckets=i'    => \$num_buckets,
        );
        die "ERROR: invalid option\n" if ( not $r );
        $self->{arguments}      = $arguments;
        $self->{caches}         = [];
        $self->{num_partitions} = $num_partitions // $Default_Num_Partitions;
        $self->{window_size}    = $window_size // $Default_Window_Size;
        $self->{num_buckets}    = $num_buckets // $Default_Num_Buckets;
        $self->{next_window}    = undef;

        if ( $self->{window_size} ) {
            my $time = time;
            my ( $sec, $min, $hour ) = localtime $time;
            my $delay = $self->{window_size};
            $delay -= $hour * 3600 % $delay if ( $delay > 3600 );
            $delay -= $min * 60 % $delay    if ( $delay > 60 );
            $delay -= $sec % $delay;
            $self->{next_window} = $time + $delay;
            $self->set_timer( $delay * 1000, 'oneshot' );
        }
    }
    return $self->{arguments};
}

sub fill {
    my ( $self, $message ) = @_;
    if ( $message->[TYPE] & TM_INFO ) {
        my ( $cmd, $key ) = split q( ), $message->[PAYLOAD], 2;
        if ( $cmd eq 'GET' ) {
            chomp $key;
            my $value = $self->lookup($key) // q{};
            $self->send_entry( $message->[FROM], $key, $value );
            $self->{counter}++;
        }
        elsif ( $cmd eq 'KEYS' ) {
            my $value = $self->get_keys;
            $self->send_entry( $message->[FROM], $cmd, $value );
        }
        elsif ( $cmd eq 'STATS' ) {
            $self->send_stats( $message->[FROM] );
        }
        else {
            $self->stderr(
                'ERROR: bad request: ', $message->[PAYLOAD],
                ' - from: ',            $message->[FROM]
            );
        }
    }
    else {
        $self->store( $message->[TIMESTAMP], $message->[STREAM],
            $message->payload );
        $self->cancel($message);
    }
    return;
}

sub fire {
    my $self = shift;
    for my $cache ( @{ $self->{caches} } ) {
        unshift @{$cache}, {};
        while ( @{$cache} > $self->{num_buckets} ) {
            pop @{$cache};
        }
    }
    my $delay = $self->{window_size};
    my $time  = time;
    my ( $sec, $min, $hour ) = localtime $time;
    $delay -= $hour * 3600 % $delay if ( $delay > 3600 );
    $delay -= $min * 60 % $delay    if ( $delay > 60 );
    $delay -= $sec % $delay;
    $self->{next_window} = $time + $delay;
    return $self->set_timer( $delay * 1000, 'oneshot' );
}

sub lookup {
    my ( $self, $key ) = @_;
    my $value = undef;
    my $cache = $self->get_cache($key);
    for my $bucket ( @{$cache} ) {
        next if ( not exists $bucket->{$key} );
        $value = $bucket->{$key};
        last;
    }
    return $value;
}

sub store {
    my ( $self, $timestamp, $key, $value ) = @_;
    if ( $self->collect( $timestamp, $key, $value ) ) {
        $value = undef;
        my $cache = $self->get_cache($key);
        for my $bucket ( reverse @{$cache} ) {
            next if ( not exists $bucket->{$key} );
            $value = $bucket->{$key};
            delete $bucket->{$key};
        }
        if ( defined $value and $self->{owner} ) {
            $self->send_entry( $self->{owner}, $key, $value );
        }
    }
    return;
}

sub collect {
    my ( $self, $timestamp, $key, $value ) = @_;
    return 1 if ( not length $value );
    my $cache = $self->get_cache($key);
    my $bucket = $self->get_bucket( $cache, $timestamp );
    $bucket->{$key} = $value;
    return;
}

sub get_cache {
    my ( $self, $key ) = @_;
    my $i = 0;
    if ( $self->{num_partitions} ) {
        $i += $_ for ( unpack 'C*', md5($key) );
        $i %= $self->{num_partitions};
    }
    $self->{caches}->[$i] //= [ {} ];
    return $self->{caches}->[$i];
}

sub get_bucket {
    my ( $self, $cache, $timestamp ) = @_;
    my $i      = 0;
    my $bucket = undef;
    if ( $self->{window_size} ) {
        my $span = $self->{next_window} - $timestamp;
        $i = int $span / $self->{window_size};
    }
    if ( $i < $self->{num_buckets} ) {
        $cache->[$i] //= {};
        $bucket = $cache->[$i];
    }
    return $bucket;
}

sub send_entry {
    my ( $self, $to, $key, $value ) = @_;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = ref($value) ? TM_STORABLE : TM_BYTESTREAM;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $to;
    $response->[STREAM]  = $key;
    $response->[PAYLOAD] = $value;
    $self->{sink}->fill($response);
    return;
}

sub send_stats {
    my ( $self, $to ) = @_;
    my @stats = ();
    for my $i ( 1 .. $self->{num_partitions} ) {
        my $cache       = $self->{caches}->[ $i - 1 ];
        my @cache_stats = ();
        for my $b ( 1 .. $self->{num_buckets} ) {
            my $bucket = $cache->[ $b - 1 ] // {};
            push @cache_stats, sprintf '%6d',
                $bucket ? scalar keys %{$bucket} : 0;
        }
        push @stats, '[ ', ( join q{, }, @cache_stats ), " ]\n";
    }
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $to;
    $response->[PAYLOAD] = join q{}, @stats;
    $self->{sink}->fill($response);
    return;
}

########################
# synchronous interface
########################

sub fetch {
    my ( $self, $key ) = @_;
    die 'ERROR: no key' if ( not defined $key );
    my $field     = $self->{field} or die 'ERROR: no field';
    my $payload   = undef;
    my $rv        = undef;
    my $tachikoma = $self->{connector};
    my $request   = Tachikoma::Message->new;
    $request->type(TM_INFO);
    $request->to($field);
    $request->payload("GET $key\n");

    if ( not $tachikoma ) {
        $tachikoma = Tachikoma->inet_client( $self->{host}, $self->{port} );
        $self->{connector} = $tachikoma;
    }
    $tachikoma->callback(
        sub {
            my $response = shift;
            die 'ERROR: fetch failed'
                if (not $response->[TYPE] & TM_BYTESTREAM
                and not $response->[TYPE] & TM_STORABLE );
            $rv = $response->payload;
            return;
        }
    );
    $tachikoma->fill($request);
    $tachikoma->drain;
    return $rv;
}

sub mget {
    my ( $self, $keys ) = @_;
    die 'ERROR: no key' if ( not defined $keys->[0] );
    my $field     = $self->{field} or die 'ERROR: no field';
    my $payload   = undef;
    my @rv        = ();
    my $tachikoma = $self->{connector};
    my $request   = Tachikoma::Message->new;
    my $expecting = scalar @{$keys};
    $request->[TYPE] = TM_INFO;
    $request->[TO]   = $field;

    if ( not $tachikoma ) {
        $tachikoma = Tachikoma->inet_client( $self->{host}, $self->{port} );
        $self->{connector} = $tachikoma;
    }
    $tachikoma->callback(
        sub {
            my $response = shift;
            die 'ERROR: fetch failed'
                if (not $response->[TYPE] & TM_BYTESTREAM
                and not $response->[TYPE] & TM_STORABLE );
            push @rv, $response->payload;
            return $expecting-- > 1 ? 1 : undef;
        }
    );
    for my $key ( @{$keys} ) {
        $request->[PAYLOAD] = "GET $key\n";
        $tachikoma->fill($request);
    }
    $tachikoma->drain;
    return \@rv;
}

sub fetch_offset {
    my ( $self, $partition, $offset ) = @_;
    my $value = undef;
    my $topic = $self->{topic} or die 'ERROR: no topic';
    my $group = $self->{consumer_broker};
    chomp $offset;
    if ( not $group ) {
        $group = Tachikoma::Nodes::ConsumerBroker->new($topic);
        $self->{consumer_broker} = $group;
    }
    else {
        $group->get_partitions;
    }
    my $consumer = $group->{consumers}->{$partition}
        || $group->make_sync_consumer($partition);
    if ($consumer) {
        my $messages = undef;
        $consumer->next_offset($offset);
        do { $messages = $consumer->fetch }
            while ( not @{$messages} and not $consumer->eos );
        die $consumer->sync_error if ( $consumer->sync_error );
        if ( not @{$messages} ) {
            die "ERROR: fetch_offset failed ($partition:$offset)";
        }
        else {
            my $message = shift @{$messages};
            $value = $message if ( $message->[ID] =~ m{^$offset:} );
        }
    }
    else {
        die "ERROR: consumer lookup failed ($partition:$offset)";
    }
    return $value;
}

# async support
sub caches {
    my $self = shift;
    if (@_) {
        $self->{caches} = shift;
    }
    return $self->{caches};
}

sub num_partitions {
    my $self = shift;
    if (@_) {
        $self->{num_partitions} = shift;
    }
    return $self->{num_partitions};
}

sub window_size {
    my $self = shift;
    if (@_) {
        $self->{window_size} = shift;
    }
    return $self->{window_size};
}

sub num_buckets {
    my $self = shift;
    if (@_) {
        $self->{num_buckets} = shift;
    }
    return $self->{num_buckets};
}

sub next_window {
    my $self = shift;
    if (@_) {
        $self->{next_window} = shift;
    }
    return $self->{next_window};
}

# sync support
sub host {
    my $self = shift;
    if (@_) {
        $self->{host} = shift;
    }
    return $self->{host};
}

sub port {
    my $self = shift;
    if (@_) {
        $self->{port} = shift;
    }
    return $self->{port};
}

sub topic {
    my $self = shift;
    if (@_) {
        $self->{topic} = shift;
    }
    return $self->{topic};
}

sub field {
    my $self = shift;
    if (@_) {
        $self->{field} = shift;
    }
    return $self->{field};
}

sub connector {
    my $self = shift;
    if (@_) {
        $self->{connector} = shift;
    }
    return $self->{connector};
}

sub consumer_broker {
    my $self = shift;
    if (@_) {
        $self->{consumer_broker} = shift;
    }
    return $self->{consumer_broker};
}

sub new_cache {
    return [];
}

1;
