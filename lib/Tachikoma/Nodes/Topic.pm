#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Topic
# ----------------------------------------------------------------------
#
# $Id: Topic.pm 29406 2017-04-29 11:18:09Z chris $
#

package Tachikoma::Nodes::Topic;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD IS_UNTHAWED LAST_MSG_FIELD
    TM_BYTESTREAM TM_BATCH TM_STORABLE TM_INFO
    TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use Tachikoma;
use Digest::MD5 qw( md5 );
use Time::HiRes qw( usleep );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.256');

my $Poll_Interval    = 15;       # delay between polls
my $Response_Timeout = 300;      # timeout before deleting async responses
my $Hub_Timeout      = 60;       # timeout waiting for hub
my $Batch_Threshold  = 64000;    # low water mark before sending batches
my $Offset = LAST_MSG_FIELD + 1;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;

    # async and sync support
    $self->{topic}         = shift;
    $self->{partitions}    = undef;
    $self->{poll_interval} = $Poll_Interval;

    # async support
    $self->{broker_path}    = undef;
    $self->{next_partition} = undef;
    $self->{last_check}     = undef;
    $self->{batch}          = undef;
    $self->{batch_size}     = 0;
    $self->{responses}      = {};

    # sync support
    $self->{hosts}       = { localhost => [ 5501, 5502 ] };
    $self->{broker_ids}  = undef;
    $self->{persist}     = 'cancel';
    $self->{hub_timeout} = $Hub_Timeout;
    $self->{targets}     = {};
    $self->{sync_error}  = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Topic <name> <broker path> [ <topic> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        $self->{arguments} = $arguments;
        my ( $broker, $topic ) = split q( ), $arguments, 2;
        die "ERROR: bad arguments for Topic\n" if ( not $broker );
        $self->{broker_path}    = $broker;
        $self->{topic}          = $topic // $self->{name};
        $self->{next_partition} = 0;
        $self->{last_check}     = 0;
        $self->{batch}          = [];
        $self->{batch_size}     = [];
        $self->set_timer;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_RESPONSE ) {
        my $last_commit_offset = $message->[ID];
        my $broker_id          = ( split m{/}, $message->[FROM], 2 )[0];
        my $responses          = $self->{responses}->{$broker_id} // [];
        while ( $last_commit_offset and @{$responses} ) {
            last if ( $responses->[0]->[$Offset] > $last_commit_offset );
            $self->cancel( shift @{$responses} );
        }
    }
    elsif ( $message->[TYPE] & TM_ERROR ) {
        $self->set_timer if ( defined $self->{timer_interval} );
        my %senders = ();
        for my $broker_id ( keys %{ $self->{responses} } ) {
            for my $response ( @{ $self->{responses}->{$broker_id} } ) {
                $senders{ $response->[FROM] } = 1;
            }
            $self->{responses}->{$broker_id} = [];
        }
        for my $sender ( keys %senders ) {
            my $copy = bless [ @{$message} ], ref $message;
            $copy->[TO]     = $sender;
            $copy->[ID]     = q();
            $copy->[STREAM] = q();
            $self->{sink}->fill($copy);
        }
        $self->{partitions} = undef;
    }
    elsif ( $message->[FROM] ne $self->{broker_path} ) {
        if ( $self->{partitions} ) {
            push @{ $self->{batch} }, $message;
            $self->{batch_size} += length $message->[PAYLOAD];
            if (   $self->{batch_size} > $Batch_Threshold
                || $Tachikoma::Now - $message->[TIMESTAMP] > 1 )
            {
                $self->set_timer(0)
                    if ( not defined $self->{timer_interval} );
            }
            elsif ( defined $self->{timer_interval} ) {
                $self->set_timer;
            }
        }
        elsif ( $message->[TYPE] & TM_PERSIST ) {
            my $response = Tachikoma::Message->new;
            $response->[TYPE]    = TM_ERROR;
            $response->[FROM]    = $self->{name};
            $response->[TO]      = $message->[FROM];
            $response->[ID]      = $message->[ID];
            $response->[STREAM]  = $message->[STREAM];
            $response->[PAYLOAD] = "NOT_AVAILABLE\n";
            $self->{sink}->fill($response);
        }
    }
    elsif ( $message->[TYPE] & TM_STORABLE ) {
        $self->update_partitions($message);
    }
    else {
        $self->stderr( $message->type_as_string, ' from ', $message->from );
    }
    return;
}

sub activate {    ## no critic (RequireArgUnpacking, RequireFinalReturn)
    my $message = Tachikoma::Message->new;
    if ( ref $_[1] ) {
        if ( ref $_[1] eq 'SCALAR' ) {
            $message->[TYPE]    = TM_BYTESTREAM;
            $message->[PAYLOAD] = ${ $_[1] };
        }
        elsif ( ref $_[1] eq 'HASH' ) {
            $message->[TYPE] = TM_STORABLE;
            $message->[TO]   = join q(/), $_[0]->{owner}, $_[1]->{partition};
            $message->[TIMESTAMP] = $_[1]->{timestamp}
                if ( $_[1]->{timestamp} );
            $message->[PAYLOAD] = $_[1]->{bucket} // $_[1];
        }
        else {
            $message->[TYPE]    = TM_STORABLE;
            $message->[PAYLOAD] = $_[1];
        }
    }
    else {
        $message->[TYPE]    = TM_BYTESTREAM;
        $message->[PAYLOAD] = $_[1];
    }
    push @{ $_[0]->{batch} }, $message;
    $_[0]->set_timer(0) if ( not defined $_[0]->{timer_interval} );
}

sub fire {
    my $self       = shift;
    my $partitions = $self->{partitions};
    if ($partitions) {
        my $topic = $self->{topic};
        my %batch = ();
        for my $message ( @{ $self->{batch} } ) {
            my $i = 0;
            if ( length $message->[TO] ) {
                $i = $message->[TO];
            }
            elsif ( $message->[STREAM] ) {
                $i += $_ for ( unpack 'C*', md5( $message->[STREAM] ) );
                $i %= scalar @{$partitions};
            }
            else {
                $i = $self->{next_partition};
                $self->{next_partition} = ( $i + 1 ) % @{$partitions};
            }
            my $broker_id = $partitions->[$i];
            $batch{$i} //= [];
            push @{ $batch{$i} }, ${ $message->packed };
            $message->[$Offset] = $self->{counter}++;
            $message->[PAYLOAD] = q();
            push @{ $self->{responses}->{$broker_id} }, $message
                if ( $message->[TYPE] & TM_PERSIST );
        }
        for my $i ( keys %batch ) {
            my $broker_id = $partitions->[$i];
            my $message   = Tachikoma::Message->new;
            $message->[TYPE]    = TM_BATCH | TM_PERSIST;
            $message->[FROM]    = $self->{name};
            $message->[TO]      = "$topic:partition:$i";
            $message->[ID]      = $self->{counter};
            $message->[STREAM]  = scalar @{ $batch{$i} };
            $message->[PAYLOAD] = join q(), @{ $batch{$i} };
            $Tachikoma::Nodes{$broker_id}->fill($message)
                if ( $Tachikoma::Nodes{$broker_id} );
        }
        $self->{batch}      = [];
        $self->{batch_size} = 0;
    }
    if ($Tachikoma::Right_Now - $self->{last_check} > $self->{poll_interval} )
    {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_INFO;
        $message->[FROM]    = $self->{name};
        $message->[TO]      = $self->{broker_path};
        $message->[PAYLOAD] = "GET_PARTITIONS $self->{topic}\n";
        $self->{sink}->fill($message);
        $self->{last_check} = $Tachikoma::Right_Now;
    }
    $self->set_timer if ( defined $self->{timer_interval} );
    return;
}

sub update_partitions {
    my $self       = shift;
    my $message    = shift;
    my $partitions = $message->payload;
    my $okay       = 1;
    for my $broker_id ( @{$partitions} ) {
        if ( not $broker_id ) {
            $okay = undef;
            last;
        }
        my $node = $Tachikoma::Nodes{$broker_id};
        if ( not $node ) {
            my ( $host, $port ) = split m{:}, $broker_id, 2;
            $node =
                inet_client_async Tachikoma::Nodes::Socket( $host, $port,
                $broker_id );
            $node->on_EOF('reconnect');
            $node->sink( $self->sink );
            $self->{responses}->{$broker_id} = [];
        }
    }
    $self->{partitions} = $partitions if ($okay);
    return;
}

########################
# synchronous interface
########################

sub send_messages {    ## no critic (ProhibitExcessComplexity)
    my $self       = shift;
    my $i          = shift;
    my $payloads   = shift;
    my $topic      = $self->{topic};
    my $partitions = $self->{partitions} || $self->get_partitions or return;
    my $broker_id  = $partitions->[ $i % @{$partitions} ] or return;
    my $target =
        $self->{targets}->{$broker_id} || $self->get_target($broker_id)
        or return;
    my $persist   = $self->{persist};
    my $expecting = $persist ? 1 : 0;
    my $is_ref    = ref $payloads->[0];
    my @buffer    = ();
    my $rv        = 1;
    my $message   = Tachikoma::Message->new;
    $message->[TYPE] = ( $is_ref ? TM_STORABLE : TM_BYTESTREAM );

    for my $payload ( @{$payloads} ) {
        $message->[PAYLOAD]     = $payload;
        $message->[IS_UNTHAWED] = 1;
        push @buffer, $message->packed;
    }
    $self->{sync_error} = undef;
    return if ( not @buffer );
    $message->[TYPE]    = ( $persist ? TM_BATCH | TM_PERSIST : TM_BATCH );
    $message->[STREAM]  = scalar @{$payloads};
    $message->[TO]      = "$topic:partition:$i";
    $message->[PAYLOAD] = join q(), map ${$_}, @buffer;
    $target->callback(
        sub {
            if ( $_[0]->[TYPE] & TM_RESPONSE ) { $expecting = 0; }
            elsif ( $_[0]->[TYPE] & TM_ERROR )    { die $_[0]->[PAYLOAD]; }
            elsif ( $_[0]->[TYPE] & TM_EOF )      { $expecting = -1; }
            elsif ( not $_[0]->[TYPE] & TM_INFO ) { die $_[0]->[PAYLOAD]; }
            return ( $expecting > 0 ? 1 : undef );
        }
    ) if ($persist);
    my $okay = eval {
        $target->fill($message);
        $target->drain if ($persist);
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ || 'unknown error';
        chomp $error;
        $self->sync_error("SEND_MESSAGES: $error\n");
        $expecting = -2;
    }
    elsif ( not $target->{fh} ) {
        $self->sync_error("SEND_MESSAGES: lost connection\n");
        $expecting = -3;
    }
    if ( $expecting != 0 ) {
        $self->remove_target($broker_id);
        $self->partitions(undef);
        if ( not $self->sync_error ) {
            $self->sync_error("SEND_MESSAGES: failed: $expecting\n");
        }
        $rv = undef;
    }
    return $rv;
}

sub send_kv {    ## no critic (ProhibitExcessComplexity)
    my $self       = shift;
    my $i          = shift;
    my $payloads   = shift;
    my $topic      = $self->{topic};
    my $partitions = $self->{partitions} || $self->get_partitions or return;
    my $broker_id  = $partitions->[ $i % @{$partitions} ] or return;
    my $target =
        $self->{targets}->{$broker_id} || $self->get_target($broker_id)
        or return;
    my $persist   = $self->{persist};
    my $expecting = $persist ? 1 : 0;
    my $is_ref    = ref( ( values %{$payloads} )[0]->[0] );
    my @buffer    = ();
    my $count     = 0;
    my $rv        = 1;
    my $message   = Tachikoma::Message->new;
    $message->[TYPE] = ( $is_ref ? TM_STORABLE : TM_BYTESTREAM );

    for my $stream ( keys %{$payloads} ) {
        $message->[STREAM] = $stream;
        for my $payload ( @{ $payloads->{$stream} } ) {
            $message->[PAYLOAD]     = $payload;
            $message->[IS_UNTHAWED] = 1;
            push @buffer, $message->packed;
            $count++;
        }
    }
    $self->{sync_error} = undef;
    return if ( not @buffer );
    $message->[TYPE]    = ( $persist ? TM_BATCH | TM_PERSIST : TM_BATCH );
    $message->[STREAM]  = $count;
    $message->[TO]      = "$topic:partition:$i";
    $message->[PAYLOAD] = join q(), map ${$_}, @buffer;
    $target->callback(
        sub {
            if ( $_[0]->[TYPE] & TM_RESPONSE ) { $expecting = 0; }
            elsif ( $_[0]->[TYPE] & TM_ERROR )    { die $_[0]->[PAYLOAD]; }
            elsif ( $_[0]->[TYPE] & TM_EOF )      { $expecting = -1; }
            elsif ( not $_[0]->[TYPE] & TM_INFO ) { die $_[0]->[PAYLOAD]; }
            return ( $expecting > 0 ? 1 : undef );
        }
    ) if ($persist);
    my $okay = eval {
        $target->fill($message);
        $target->drain if ($persist);
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ || 'unknown error';
        chomp $error;
        $self->sync_error("SEND_STREAM: $error\n");
        $expecting = -1;
    }
    elsif ( not $target->{fh} ) {
        $self->sync_error("SEND_STREAM: lost connection\n");
        $expecting = -1;
    }
    if ( $expecting != 0 ) {
        $self->remove_target($broker_id);
        $self->partitions(undef);
        if ( not $self->sync_error ) {
            $self->sync_error("SEND_STREAM: failed\n");
        }
        $rv = undef;
    }
    return $rv;
}

# for feeding into ConsumerBroker->partitions(),
# ConsumerBroker->make_sync_consumers(), etc
sub get_mapping {
    my $self       = shift;
    my $partitions = $self->get_partitions or return;
    my %mapping    = ();
    my $i          = 0;
    for my $broker_id ( @{$partitions} ) {
        $mapping{ $i++ } = $broker_id;
    }
    return \%mapping;
}

sub get_partitions {
    my $self = shift;
    die "ERROR: no topic\n" if ( not $self->topic );
    my $partitions = undef;
    $self->sync_error(undef);
    for my $broker_id ( keys %{ $self->broker_ids } ) {
        $partitions = $self->request_partitions($broker_id);
        if ($partitions) {
            $self->sync_error(undef);
            last;
        }
    }
    $self->partitions($partitions);
    return $partitions;
}

sub request_partitions {
    my $self            = shift;
    my $broker_id       = shift;
    my $topic           = $self->topic;
    my $target          = $self->get_target($broker_id) or return;
    my $partitions      = undef;
    my $request_payload = "GET_PARTITIONS $topic\n";
    my $request         = Tachikoma::Message->new;
    $request->[TYPE]    = TM_INFO;
    $request->[TO]      = 'broker';
    $request->[PAYLOAD] = $request_payload;
    $target->callback(
        sub {
            my $response = shift;
            if ( $response->[TYPE] & TM_RESPONSE ) {
                return 1;
            }
            elsif ( $response->[TYPE] & TM_STORABLE ) {
                $partitions = $response->payload;
            }
            elsif ( $response->[PAYLOAD] ) {
                die $response->[PAYLOAD];
            }
            else {
                die $response->type_as_string . "\n";
            }
            return;
        }
    );
    my $okay = eval {
        $target->fill($request);
        $target->drain;
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ || 'unknown error';
        die "CANT_FIND_TOPIC\n" if ( $error eq "CANT_FIND_TOPIC\n" );
        $self->remove_target($broker_id);
        chomp $error;
        $self->sync_error("GET_PARTITIONS: $error\n");
    }
    elsif ( not $target->{fh} ) {
        $self->remove_target($broker_id);
        $self->sync_error("GET_PARTITIONS: lost connection\n");
    }
    return $partitions;
}

sub get_controller {
    my $self       = shift;
    my $controller = undef;
    for my $broker_id ( keys %{ $self->broker_ids } ) {
        my $target = $self->get_target($broker_id) or next;
        $self->sync_error(undef);
        my $request = Tachikoma::Message->new;
        $request->[TYPE]    = TM_INFO;
        $request->[TO]      = 'broker';
        $request->[PAYLOAD] = "GET_CONTROLLER\n";
        $target->callback(
            sub {
                my $response = shift;
                if ( $response->[TYPE] & TM_RESPONSE ) {
                    return 1;
                }
                elsif ( $response->[TYPE] & TM_INFO ) {
                    $controller = $response->[PAYLOAD];
                    chomp $controller;
                }
                elsif ( $response->[PAYLOAD] ) {
                    die $response->[PAYLOAD];
                }
                else {
                    die $response->type_as_string . "\n";
                }
                return;
            }
        );
        my $okay = eval {
            $target->fill($request);
            $target->drain;
            return 1;
        };
        if ( not $okay ) {
            my $error = $@ || 'unknown error';
            $self->remove_target($broker_id);
            chomp $error;
            $self->sync_error("GET_CONTROLLER: $error\n");
        }
        elsif ( not $target->{fh} ) {
            $self->remove_target($broker_id);
            $self->sync_error("GET_CONTROLLER: lost connection\n");
        }
        last if ($controller);
    }
    return $controller;
}

sub get_target {
    my $self      = shift;
    my $broker_id = shift;
    my $target    = $self->targets->{$broker_id};
    if ( not $target or not $target->{fh} ) {
        my ( $host, $port ) = split m{:}, $broker_id, 2;
        $target = undef;
        my $okay = eval {
            $target = Tachikoma->inet_client( $host, $port );
            $target->timeout( $self->hub_timeout );
            return 1;
        };
        if ( not $okay ) {
            my $caller = uc( ( split m{::}, ( caller 1 )[3] )[-1] );
            $caller = uc( ( split m{::}, ( caller 2 )[3] )[-1] )
                if ( $caller eq 'GET_TARGET' );
            $self->sync_error("$caller: get_target($broker_id): $@");
            $self->partitions(undef);
        }
        else {
            $self->targets->{$broker_id} = $target;
        }
    }
    return $target;
}

sub remove_targets {
    my $self = shift;
    for my $broker_id ( keys %{ $self->targets } ) {
        $self->remove_target($broker_id);
    }
    return;
}

sub remove_target {
    my $self      = shift;
    my $broker_id = shift;
    if ( $self->targets->{$broker_id} ) {
        $self->targets->{$broker_id}->close_filehandle;
        delete $self->targets->{$broker_id};
    }
    return;
}

# async and sync support
sub topic {
    my $self = shift;
    if (@_) {
        $self->{topic} = shift;
    }
    return $self->{topic};
}

sub partitions {
    my $self = shift;
    if (@_) {
        $self->{partitions} = shift;
    }
    return $self->{partitions};
}

sub poll_interval {
    my $self = shift;
    if (@_) {
        $self->{poll_interval} = shift;
    }
    return $self->{poll_interval};
}

# async support
sub broker_path {
    my $self = shift;
    if (@_) {
        $self->{broker_path} = shift;
    }
    return $self->{broker_path};
}

sub next_partition {
    my $self = shift;
    if (@_) {
        $self->{next_partition} = shift;
    }
    return $self->{next_partition};
}

sub last_check {
    my $self = shift;
    if (@_) {
        $self->{last_check} = shift;
    }
    return $self->{last_check};
}

sub batch {
    my $self = shift;
    if (@_) {
        $self->{batch} = shift;
    }
    return $self->{batch};
}

sub batch_size {
    my $self = shift;
    if (@_) {
        $self->{batch_size} = shift;
    }
    return $self->{batch_size};
}

sub responses {
    my $self = shift;
    if (@_) {
        $self->{responses} = shift;
    }
    return $self->{responses};
}

# sync support
sub hosts {
    my $self = shift;
    if (@_) {
        $self->{hosts} = shift;
    }
    return $self->{hosts};
}

sub broker_ids {
    my $self = shift;
    if (@_) {
        $self->{broker_ids} = shift;
    }
    if ( not defined $self->{broker_ids} ) {
        my %broker_ids = ();
        for my $host ( keys %{ $self->hosts } ) {
            for my $port ( @{ $self->hosts->{$host} } ) {
                my $broker_id = join q(:), $host, $port;
                $broker_ids{$broker_id} = undef;
            }
        }
        $self->{broker_ids} = \%broker_ids;
    }
    return $self->{broker_ids};
}

sub persist {
    my $self = shift;
    if (@_) {
        $self->{persist} = shift;
    }
    return $self->{persist};
}

sub hub_timeout {
    my $self = shift;
    if (@_) {
        $self->{hub_timeout} = shift;
    }
    return $self->{hub_timeout};
}

sub targets {
    my $self = shift;
    if (@_) {
        $self->{targets} = shift;
    }
    return $self->{targets};
}

sub sync_error {
    my $self = shift;
    if (@_) {
        $self->{sync_error} = shift;
    }
    return $self->{sync_error};
}

1;
