#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Table
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Table;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_INFO TM_REQUEST TM_ERROR TM_EOF
);
use Digest::MD5 qw( md5 );
use Getopt::Long qw( GetOptionsFromString );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.197');

my $DEFAULT_NUM_PARTITIONS = 1;
my $DEFAULT_NUM_BUCKETS    = 2;
my $DEFAULT_WINDOW_SIZE    = 300;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{caches}         = [];
    $self->{on_save_window} = [];
    $self->{num_partitions} = $DEFAULT_NUM_PARTITIONS;
    $self->{num_buckets}    = $DEFAULT_NUM_BUCKETS;
    $self->{window_size}    = undef;
    $self->{bucket_size}    = undef;
    $self->{next_window}    = [];
    $self->{queue}          = undef;
    $self->{host}           = undef;
    $self->{port}           = undef;
    $self->{field}          = undef;
    $self->{connector}      = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Table <node name> --num_partitions=<int> \
                            --num_buckets=<int>    \
                            --window_size=<int>    \
                            --bucket_size=<int>    \
                            --queue
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ($num_partitions, $num_buckets, $window_size,
            $bucket_size,    $should_queue
        );
        my ( $r, $argv ) = GetOptionsFromString(
            $arguments,
            'num_partitions=i' => \$num_partitions,
            'num_buckets=i'    => \$num_buckets,
            'window_size=i'    => \$window_size,
            'bucket_size=i'    => \$bucket_size,
            'queue'            => \$should_queue,
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
        $self->{num_partitions} = $num_partitions // $DEFAULT_NUM_PARTITIONS;
        $self->{num_buckets}    = $num_buckets // $DEFAULT_NUM_BUCKETS;

        if ($bucket_size) {
            $self->{bucket_size} = $bucket_size;
        }
        else {
            $self->{window_size} = $window_size // $DEFAULT_WINDOW_SIZE;
        }
        $self->{next_window} = [];
        $self->{queue}       = [] if ($should_queue);
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
    elsif ( $message->[TYPE] & TM_INFO ) {
        my $payload = $message->[PAYLOAD];
        chomp $payload;
        $self->set_timer
            if ( $payload eq 'READY' and not $self->{timer_is_active} );
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

sub fire {
    my $self = shift;
    if ( $self->{window_size} ) {
        for my $i ( 0 .. $self->{num_partitions} - 1 ) {
            my $next_window = $self->{next_window}->[$i] // 0;
            $self->roll_window( $i, $Tachikoma::Now, $next_window )
                if ( $Tachikoma::Now >= $next_window );
        }
    }
    if ( $self->{queue} ) {
        my $queue = $self->{queue};
        while ( @{$queue} ) {
            if ( $Tachikoma::Now - $queue->[0]->{timestamp}
                > $queue->[0]->{delay} )
            {
                &{ $queue->[0]->{send_cb} }();
                shift @{$queue};
            }
            else {
                last;
            }
        }
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
        $self->roll_window( $i, $timestamp, $next_window )
            if ( $timestamp >= $next_window );
    }
    elsif ( $self->{bucket_size} ) {
        my $cache = $self->{caches}->[$i];
        $self->roll_count( $i, $timestamp, 0 )
            if ( $cache->[0]
            and scalar keys %{ $cache->[0] } >= $self->{bucket_size} );
    }
    if ( $self->collect( $i, $timestamp, $key, $value ) ) {
        $value = $self->remove_entry( $i, $key );
        if ( defined $value and $self->{owner} ) {
            $self->send_entry( $self->{owner}, $key, $value );
        }
    }
    return;
}

sub roll_window {
    my ( $self, $i, $timestamp, $next_window ) = @_;
    my $span  = $timestamp - $next_window;
    my $count = int $span / $self->{window_size};
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
    my ( $self, $i, $window, $count ) = @_;
    $self->{caches}->[$i] ||= [];
    my $cache = $self->{caches}->[$i];
    for ( 0 .. $count ) {
        $self->roll( $i, $window );
        $window += $self->{window_size}
            if ( $window and $self->{window_size} );
    }
    while ( @{$cache} > $self->{num_buckets} ) {
        pop @{$cache};
    }
    return;
}

sub roll {
    my ( $self, $i, $window ) = @_;
    my $cache = $self->{caches}->[$i];
    if ($window) {
        my $save_cb = $self->{on_save_window}->[$i];
        my $bucket  = $cache->[0];
        $self->queue(
            $window => sub {
                $self->send_bucket( $i, $window, $bucket )
                    if ( $self->{edge} );
                &{$save_cb}( $window, $cache->[0] ) if ($save_cb);
            }
        );
    }
    unshift @{$cache}, {};
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
    my $cache = $self->{caches}->[$i];
    my $j     = 0;
    if ( $timestamp and $self->{window_size} ) {
        my $span = $self->{next_window}->[$i] - $timestamp;
        $j = int $span / $self->{window_size};
        $j = 0 if ( $j < 0 );
        $j = $self->{num_buckets} - 1 if ( $j >= $self->{num_buckets} );
    }
    $cache->[$j] ||= {};
    return $cache->[$j];
}

sub remove_entry {
    my ( $self, $i, $key ) = @_;
    my $value = undef;
    for my $bucket ( reverse @{ $self->{caches}->[$i] } ) {
        next if ( not exists $bucket->{$key} );
        $value = $bucket->{$key};
        delete $bucket->{$key};
    }
    return $value;
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

sub send_bucket {
    my ( $self, $i, $window, $bucket ) = @_;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]      = TM_STORABLE;
    $response->[FROM]      = $self->{name};
    $response->[STREAM]    = $i;
    $response->[TIMESTAMP] = $window;
    $response->[PAYLOAD]   = $bucket;
    $self->{edge}->fill($response);
    return;
}

sub send_stats {
    my ( $self, $to ) = @_;
    my @stats = ();
    my $total = 0;
    for my $i ( 0 .. $self->num_partitions - 1 ) {
        my $cache       = $self->caches->[$i];
        my @cache_stats = ();
        for my $b ( 0 .. $self->num_buckets - 1 ) {
            my $bucket = $cache->[$b] // {};
            my $count  = $bucket ? scalar keys %{$bucket} : 0;
            push @cache_stats, sprintf '%6d', $count;
            $total += $count;
        }
        push @stats, sprintf( '%3d [ ', $i ), join( q(, ), @cache_stats ),
            " ]\n";
    }
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[FROM]    = $self->name;
    $response->[TO]      = $to;
    $response->[PAYLOAD] = join q(), @stats, "total: $total\n";
    $self->sink->fill($response);
    return;
}

sub get_keys {
    my ($self) = @_;
    my %unique = ();
    for my $cache ( @{ $self->{caches} } ) {
        for my $bucket ( @{$cache} ) {
            next if ( not $bucket );
            $unique{$_}++ for ( keys %{$bucket} );
        }
    }
    return \%unique;
}

sub on_load_window {
    my ( $self, $i, $stored ) = @_;
    my $next_window = $self->{next_window}->[$i] // 0;
    my $timestamp   = $stored->{timestamp}       // 0;
    $self->{caches}->[$i] ||= [];
    if ( $timestamp > $next_window ) {
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
        while ( @{$cache} > $self->{num_buckets} ) {
            pop @{$cache};
        }
        $self->{next_window}->[$i] = $timestamp
            if ( $self->{window_size} );
    }
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
    $self->{caches}->[$i]      = $stored->{cache}       || [];
    $self->{next_window}->[$i] = $stored->{next_window} || [];
    return;
}

sub on_save_snapshot {
    my ( $self, $i, $stored ) = @_;
    $stored->{cache}       = $self->{caches}->[$i];
    $stored->{next_window} = $self->{next_window}->[$i];
    return;
}

sub new_cache {
    my ( $self, $i ) = @_;
    if ( defined $i ) {
        if ( $i < $self->{num_partitions} ) {
            $self->{caches}->[$i]      = [];
            $self->{next_window}->[$i] = 0;
        }
    }
    else {
        $self->{caches}      = [];
        $self->{next_window} = [];
    }
    return;
}

sub queue {
    my $self = shift;
    if (@_) {
        my $window  = shift;
        my $send_cb = shift;
        if ( $self->{queue} ) {
            my $queue = $self->{queue};
            my $delay = $Tachikoma::Now - $window;
            $delay = 1 if ( $delay < 1 );
            $delay = $self->{window_size}
                if ( $delay > $self->{window_size} );
            push @{$queue},
                {
                timestamp => $Tachikoma::Now,
                delay     => $delay,
                send_cb   => $send_cb,
                };
            while ( @{$queue} > $self->{num_buckets} ) {
                &{ $queue->[0]->{send_cb} }()
                    if ( $queue->[0] and $queue->[0]->{send_cb} );
                shift @{$queue};
            }
        }
        else {
            &{$send_cb}();
        }
    }
    return $self->{queue};
}

sub remove_node {
    my $self = shift;
    $self->{queue} = undef;
    $self->SUPER::remove_node;
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
