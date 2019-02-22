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
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_REQUEST TM_ERROR TM_EOF
);
use Tachikoma;
use Digest::MD5 qw( md5 );
use Getopt::Long qw( GetOptionsFromString );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.197');

my $Default_Num_Partitions = 1;
my $Default_Num_Buckets    = 2;
my $Default_Window_Size    = 300;

sub help {
    my $self = shift;
    return <<'EOF';
make_node Table <node name> --num_partitions=<int> \
                            --num_buckets=<int>    \
                            --window_size=<int>    \
                            --bucket_size=<int>
EOF
}

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{caches}         = [];
    $self->{on_save_window} = [];
    $self->{num_partitions} = $Default_Num_Partitions;
    $self->{num_buckets}    = $Default_Num_Buckets;
    $self->{window_size}    = undef;
    $self->{bucket_size}    = undef;
    $self->{next_window}    = [];
    $self->{host}           = undef;
    $self->{port}           = undef;
    $self->{field}          = undef;
    $self->{connector}      = undef;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ( $num_partitions, $num_buckets, $window_size, $bucket_size );
        my ( $r, $argv ) = GetOptionsFromString(
            $arguments,
            'num_partitions=i' => \$num_partitions,
            'num_buckets=i'    => \$num_buckets,
            'window_size=i'    => \$window_size,
            'bucket_size=i'    => \$bucket_size,
        );
        die "ERROR: invalid option\n" if ( not $r );
        die "ERROR: num_partitions must be greater than or equal to 1\n"
            if ( defined $num_partitions and $num_partitions < 1 );
        die "ERROR: num_buckets must be greater than or equal to 1\n"
            if ( defined $num_buckets and $num_buckets < 1 );
        die "ERROR: num_buckets must be 1 when window_size is unset\n"
            if (defined $window_size
            and $window_size == 0
            and $num_buckets
            and $num_buckets != 1 );
        die "ERROR: window_size and bucket_size can't be used together\n"
            if ( $window_size and $bucket_size );
        $self->{arguments}      = $arguments;
        $self->{caches}         = [];
        $self->{num_partitions} = $num_partitions // $Default_Num_Partitions;
        $self->{num_buckets}    = $num_buckets // $Default_Num_Buckets;

        if ($bucket_size) {
            $self->{bucket_size} = $bucket_size;
        }
        else {
            $self->{window_size} = $window_size // $Default_Window_Size;
        }
        $self->{next_window} = [];
    }
    return $self->{arguments};
}

sub fill {
    my ( $self, $message ) = @_;
    if ( $message->[TYPE] & TM_REQUEST ) {
        my ( $cmd, $key ) = split q( ), $message->[PAYLOAD], 2;
        if ( $cmd eq 'GET' ) {
            chomp $key;
            my $value = $self->lookup($key) // q();
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
    elsif ( not $message->[TYPE] & TM_ERROR
        and not $message->[TYPE] & TM_EOF )
    {
        $self->store( $message->[TIMESTAMP], $message->[STREAM],
            $message->payload );
        $self->cancel($message);
    }
    return;
}

sub lookup {
    my ( $self, $key ) = @_;
    my $value = undef;
    if ( length $key ) {
        my $i = $self->get_partition_id($key);
        for my $bucket ( @{ $self->{caches}->[$i] } ) {
            next if ( not exists $bucket->{$key} );
            $value = $bucket->{$key};
            last;
        }
    }
    else {
        $value = [];
        for my $cache ( @{ $self->{caches} } ) {
            for my $bucket ( @{$cache} ) {
                push @{$value}, keys %{$bucket};
            }
        }
    }
    return $value;
}

sub lru_lookup {
    my ( $self, $key ) = @_;
    my $value = undef;
    my $i     = $self->get_partition_id($key);
    my $cache = $self->{caches}->[$i];
    for my $j ( 0 .. $#{$cache} ) {
        next if ( not exists $cache->[$j]->{$key} );
        $value = $cache->[$j]->{$key};
        if ($j) {
            delete $cache->[$j]->{$key};
            $cache->[0]->{$key} = $value;
            if ( $self->{bucket_size} ) {
                $self->roll_count( $i, $Tachikoma::Now, 0 )
                    if ( $cache->[0]
                    and scalar
                    keys %{ $cache->[0] } >= $self->{bucket_size} );
            }
        }
        last;
    }
    return $value;
}

sub windowed_lookup {
    my ( $self, $timestamp, $key ) = @_;
    my $value  = undef;
    my $i      = $self->get_partition_id($key);
    my $bucket = $self->get_bucket( $i, $timestamp );
    $value = $bucket->{$key} if ($bucket);
    return $value;
}

sub store {
    my ( $self, $timestamp, $key, $value ) = @_;
    my $i = $self->get_partition_id($key);
    $self->{caches}->[$i] ||= [];
    if ( $self->{window_size} ) {
        my $next_window = $self->{next_window}->[$i] // 0;
        $self->roll_window( $i, $timestamp )
            if ( $timestamp > $next_window );
    }
    elsif ( $self->{bucket_size} ) {
        my $cache = $self->{caches}->[$i];
        $self->roll_count( $i, $timestamp, 0 )
            if ( $cache->[0]
            and scalar keys %{ $cache->[0] } >= $self->{bucket_size} );
    }
    if ( $self->collect( $i, $timestamp, $key, $value ) ) {
        $value = undef;
        for my $bucket ( reverse @{ $self->{caches}->[$i] } ) {
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

sub roll_window {
    my ( $self, $i, $timestamp ) = @_;
    my $next_window = $self->{next_window}->[$i] // 0;
    my $span        = $timestamp - $next_window;
    my $count       = int $span / $self->{window_size};
    $count = $self->{num_buckets} if ( $count > $self->{num_buckets} );
    $self->roll_count( $i, $next_window, $count );
    my $delay = $self->{window_size};
    my ( $sec, $min, $hour ) = localtime $timestamp;
    $delay -= $hour * 3600 % $delay if ( $delay > 3600 );
    $delay -= $min * 60 % $delay    if ( $delay > 60 );
    $delay -= $sec % $delay;
    $self->{next_window}->[$i] = $timestamp + $delay;
    return;
}

sub roll_count {
    my ( $self, $i, $timestamp, $count ) = @_;
    my $cache   = $self->{caches}->[$i];
    my $save_cb = $self->{on_save_window}->[$i];
    if ($timestamp) {
        &{$save_cb}( $timestamp, $cache->[0] ) if ($save_cb);
        $self->{edge}->activate(
            {   partition => $i,
                timestamp => $timestamp,
                bucket    => $cache->[0]
            }
        ) if ( $self->{edge} );
    }
    for ( 0 .. $count ) {
        unshift @{$cache}, {};
    }
    while ( @{$cache} > $self->{num_buckets} ) {
        pop @{$cache};
    }
    return;
}

sub collect {
    my ( $self, $i, $timestamp, $key, $value ) = @_;
    return 1 if ( not length $value );
    my $bucket = $self->get_bucket( $i, $timestamp );
    $bucket->{$key} = $value if ($bucket);
    return;
}

sub get_partition_id {
    my ( $self, $key ) = @_;
    my $i = 0;
    if ( $self->{num_partitions} > 1 ) {
        $i += $_ for ( unpack 'C*', md5($key) );
        $i %= $self->{num_partitions};
    }
    return $i;
}

sub get_bucket {
    my ( $self, $i, $timestamp ) = @_;
    my $cache  = $self->{caches}->[$i];
    my $bucket = undef;
    my $j      = 0;
    if ( $self->{window_size} ) {
        my $span = $self->{next_window}->[$i] - $timestamp;
        $j = int $span / $self->{window_size};
    }
    if ( $j >= 0 and $j < $self->{num_buckets} ) {
        $cache->[$j] ||= {};
        $bucket = $cache->[$j];
    }
    return $bucket;
}

sub send_entry {
    my ( $self, $to, $key, $value ) = @_;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = ref $value ? TM_STORABLE : TM_BYTESTREAM;
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
    for my $i ( 1 .. $self->num_partitions ) {
        my $cache       = $self->caches->[ $i - 1 ];
        my @cache_stats = ();
        for my $b ( 1 .. $self->num_buckets ) {
            my $bucket = $cache->[ $b - 1 ] // {};
            push @cache_stats, sprintf '%6d',
                $bucket ? scalar keys %{$bucket} : 0;
        }
        push @stats, '[ ', ( join q(, ), @cache_stats ), " ]\n";
    }
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[FROM]    = $self->name;
    $response->[TO]      = $to;
    $response->[PAYLOAD] = join q(), @stats;
    $self->sink->fill($response);
    return;
}

sub on_load_window {
    my ( $self, $i, $stored ) = @_;
    my $next_window = $self->{next_window}->[$i] // 0;
    my $timestamp   = $stored->{timestamp}       // 0;
    if ( $timestamp > $next_window ) {
        $self->{caches}->[$i] ||= [];
        my $cache = $self->{caches}->[$i];
        my $span  = $timestamp - $next_window;
        my $count = int $span / $self->{window_size};
        $count = $self->{num_buckets} if ( $count > $self->{num_buckets} );
        if ( $count > 1 ) {
            for ( 2 .. $count ) {
                unshift @{$cache}, {};
            }
        }
        unshift @{$cache}, $stored->{cache};
        $self->{next_window}->[$i] = $timestamp
            if ( $self->{window_size} );
        while ( @{$cache} > $self->{num_buckets} ) {
            pop @{$cache};
        }
    }
    return;
}

sub on_load_window_complete {
    my ( $self, $i ) = @_;
    $self->{caches}->[$i] ||= [];
    return;
}

sub on_save_window {
    my $self = shift;
    if (@_) {
        $self->{on_save_window} = shift;
    }
    return $self->{on_save_window};
}

sub on_load_snapshot {
    my ( $self, $i, $stored ) = @_;
    $self->{caches}->[$i] = $stored->{cache} || [];
    return;
}

sub on_save_snapshot {
    my ( $self, $i, $stored ) = @_;
    $stored->{cache} = $self->{caches}->[$i];
    return;
}

########################
# synchronous interface
########################

sub fetch {
    my $self      = shift;
    my $key       = shift // q();
    my $field     = $self->{field} or die 'ERROR: no field';
    my $rv        = undef;
    my $tachikoma = $self->{connector};
    my $request   = Tachikoma::Message->new;
    $request->type(TM_REQUEST);
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
    my @rv        = ();
    my $tachikoma = $self->{connector};
    my $request   = Tachikoma::Message->new;
    my $expecting = scalar @{$keys};
    $request->[TYPE] = TM_REQUEST;
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

sub num_buckets {
    my $self = shift;
    if (@_) {
        $self->{num_buckets} = shift;
    }
    return $self->{num_buckets};
}

sub window_size {
    my $self = shift;
    if (@_) {
        $self->{window_size} = shift;
    }
    return $self->{window_size};
}

sub bucket_size {
    my $self = shift;
    if (@_) {
        $self->{bucket_size} = shift;
    }
    return $self->{bucket_size};
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

1;
