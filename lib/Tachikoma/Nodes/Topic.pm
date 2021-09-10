#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Topic
# ----------------------------------------------------------------------
#
#   - Sends messages to Partitions
#
# $Id: Topic.pm 29406 2017-04-29 11:18:09Z chris $
#

package Tachikoma::Nodes::Topic;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD IS_UNTHAWED LAST_MSG_FIELD
    TM_BYTESTREAM TM_BATCH TM_STORABLE TM_REQUEST
    TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use Tachikoma;
use Digest::MD5 qw( md5 );
use Time::HiRes qw( usleep );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.256');

my $Poll_Interval    = 15;       # delay between polls
my $Startup_Delay    = 30;       # wait at least this long on startup
my $Response_Timeout = 300;      # wait before abandoning async responses
my $Hub_Timeout      = 60;       # synchronous timeout waiting for hub
my $Batch_Threshold  = 65536;    # low water mark before sending batches
my $Batch_Interval   = 0.25;     # how long to wait if below threshold

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;

    # async and sync support
    $self->{topic}         = shift;
    $self->{partitions}    = undef;
    $self->{poll_interval} = $Poll_Interval;

    # async support
    $self->{broker_path}            = undef;
    $self->{next_partition}         = undef;
    $self->{last_check}             = undef;
    $self->{batch}                  = undef;
    $self->{batch_offset}           = undef;
    $self->{batch_size}             = undef;
    $self->{batch_timestamp}        = undef;
    $self->{responses}              = undef;
    $self->{batch_responses}        = undef;
    $self->{valid_broker_paths}     = undef;
    $self->{registrations}->{READY} = {};

    # sync support
    $self->{broker_ids}  = [ 'localhost:5501', 'localhost:5502' ];
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
        $self->{broker_path}     = $broker;
        $self->{topic}           = $topic // $self->{name};
        $self->{partitions}      = undef;
        $self->{next_partition}  = 0;
        $self->{last_check}      = $Tachikoma::Now + $Startup_Delay;
        $self->{batch}           = {};
        $self->{batch_offset}    = {};
        $self->{batch_size}      = {};
        $self->{batch_timestamp} = {};
        $self->{responses}       = {};
        $self->{batch_responses} = {};
        $self->set_timer;
    }
    return $self->{arguments};
}

sub fill {
    my ( $self, $message ) = @_;
    if ( $message->[TYPE] & TM_RESPONSE ) {
        $self->handle_response($message);
    }
    elsif ( $self->is_broker_path( $message->[FROM] ) ) {
        if ( $message->[TYPE] & TM_ERROR ) {
            $self->handle_error($message);
        }
        elsif ( $message->[TYPE] & TM_STORABLE ) {
            my $okay = $self->update_partitions($message);
            $self->set_state('READY')
                if ( $okay and not $self->{set_state}->{READY} );
        }
        else {
            $self->stderr( $message->type_as_string, ' from ',
                $message->from );
        }
    }
    elsif ( not $message->[TYPE] & TM_ERROR ) {
        if ( $self->{partitions} ) {
            $self->batch_message($message);
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
    return;
}

sub fire {
    my $self       = shift;
    my $partitions = $self->{partitions};
    my $batch      = $self->{batch};
    if ($partitions) {
        my $topic           = $self->{topic};
        my $batch_offset    = $self->{batch_offset};
        my $batch_size      = $self->{batch_size};
        my $batch_timestamp = $self->{batch_timestamp};
        my $responses       = $self->{responses};
        my $batch_responses = $self->{batch_responses};
        for my $i ( keys %{$batch} ) {
            next
                if ($batch_size->{$i} < $Batch_Threshold
                and $Tachikoma::Right_Now - $batch_timestamp->{$i}
                < $Batch_Interval );
            my $broker_id = $partitions->[$i];
            my $persist   = $responses->{$i} ? TM_PERSIST : 0;
            my $message   = Tachikoma::Message->new;
            $message->[TYPE]    = TM_BATCH | $persist;
            $message->[FROM]    = $self->{name};
            $message->[TO]      = "$topic:partition:$i";
            $message->[ID]      = join q(:), $i, $batch_offset->{$i};
            $message->[PAYLOAD] = join q(), @{ $batch->{$i} };
            $Tachikoma::Nodes{$broker_id}->fill($message)
                if ( $Tachikoma::Nodes{$broker_id} );
            $batch_responses->{$i} //= [];
            push @{ $batch_responses->{$i} },
                {
                last_commit_offset => $batch_offset->{$i},
                batch              => $responses->{$i},
                }
                if ($persist);
            delete $batch->{$i};
            delete $batch_size->{$i};
            delete $batch_timestamp->{$i};
            delete $responses->{$i};
        }
    }
    if ( $Tachikoma::Right_Now - $self->{last_check}
        >= $self->{poll_interval} )
    {
        $self->{valid_broker_paths} = undef;
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_REQUEST;
        $message->[FROM]    = $self->{name};
        $message->[TO]      = $self->{broker_path};
        $message->[PAYLOAD] = "GET_PARTITIONS $self->{topic}\n";
        $self->{sink}->fill($message);
        $self->{last_check} = $Tachikoma::Right_Now;
    }
    if ( keys %{$batch} ) {
        $self->set_timer( $Batch_Interval * 1000 )
            if ( not $self->{timer_interval} );
    }
    elsif ( defined $self->{timer_interval} ) {
        $self->set_timer;
    }
    return;
}

sub handle_response {
    my ( $self, $message ) = @_;
    my ( $i, $last_commit_offset ) = split m{:}, $message->[ID], 2;
    my $batch_responses = $self->{batch_responses}->{$i} // [];
    my $responses = $batch_responses->[0];
    if (    $responses
        and $responses->{last_commit_offset} == $last_commit_offset )
    {
        $self->cancel($_) for ( @{ $responses->{batch} } );
        shift @{$batch_responses};
    }
    else {
        $self->print_less_often(
            'WARNING: missing responses from: ',
            $responses->{last_commit_offset},
            ' to: ', $last_commit_offset
        );
        my $error = Tachikoma::Message->new;
        $error->[TYPE]    = TM_ERROR;
        $error->[FROM]    = $self->{name};
        $error->[PAYLOAD] = "NOT_AVAILABLE\n";
        $self->handle_error($error);
    }
    return;
}

sub handle_error {
    my ( $self, $message ) = @_;
    my %senders = ();
    for my $i ( keys %{ $self->{batch_responses} } ) {
        my $batch_responses = $self->{batch_responses}->{$i};
        for my $responses ( @{$batch_responses} ) {
            $senders{ $_->[FROM] } = 1 for ( @{ $responses->{batch} } );
        }
    }
    for my $i ( keys %{ $self->{responses} } ) {
        for my $response ( @{ $self->{responses}->{$i} } ) {
            $senders{ $response->[FROM] } = 1;
        }
    }
    my $error = $message->[PAYLOAD];

    # $self->stderr("INFO: reset_topic - got $error");
    $self->reset_topic;
    for my $sender ( keys %senders ) {
        my $copy = bless [ @{$message} ], ref $message;
        $copy->[TO]     = $sender;
        $copy->[ID]     = q();
        $copy->[STREAM] = q();
        $self->{sink}->fill($copy);
    }
    return;
}

sub batch_message {
    my ( $self, $message ) = @_;
    my $partitions = $self->{partitions};
    my $i          = 0;
    if ( length $message->[TO] ) {
        $i = $message->[TO];
        $message->[TO] = q();
    }
    elsif ( $message->[STREAM] ) {
        $i += $_ for ( unpack 'C*', md5( $message->[STREAM] ) );
        $i %= scalar @{$partitions};
    }
    else {
        $i = $self->{next_partition};
        $self->{next_partition} = ( $i + 1 ) % @{$partitions};
    }
    my $packed = $message->packed;
    $self->{batch}->{$i}           //= [];
    $self->{batch_offset}->{$i}    //= 0;
    $self->{batch_size}->{$i}      //= 0;
    $self->{batch_timestamp}->{$i} //= $Tachikoma::Right_Now;
    push @{ $self->{batch}->{$i} }, ${$packed};
    $self->{batch_offset}->{$i}++;
    $self->{batch_size}->{$i} += length ${$packed};

    if ( $message->[TYPE] & TM_PERSIST ) {
        $self->{responses}->{$i} //= [];
        $message->[PAYLOAD] = q();
        push @{ $self->{responses}->{$i} }, $message;
    }
    if ( $self->{batch_size}->{$i} > $Batch_Threshold ) {
        $self->set_timer(0)
            if (
            not defined $self->{timer_interval}
            or ( defined $self->{timer_interval}
                and $self->{timer_interval} != 0 )
            );
    }
    elsif ( not defined $self->{timer_interval} ) {
        $self->set_timer( $Batch_Interval * 1000 );
    }
    return;
}

sub update_partitions {
    my ( $self, $message ) = @_;
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
        }
    }
    my $old_partitions =
        $self->{partitions}
        ? join q(|), @{ $self->{partitions} }
        : q();
    my $new_partitions = $partitions ? join q(|), @{$partitions} : q();
    if ( not $okay ) {
        $self->stderr("WARNING: got partial partition map: $new_partitions");
        $self->reset_topic;
    }
    elsif ( $old_partitions ne $new_partitions ) {

        # $self->stderr(
        #     "INFO: partition remap: $old_partitions -> $new_partitions");
        $self->reset_topic;
    }
    $self->{partitions} = $partitions if ($okay);
    return $okay;
}

sub reset_topic {
    my $self = shift;
    $self->{partitions}      = undef;
    $self->{next_partition}  = 0;
    $self->{last_check}      = $Tachikoma::Now + $Startup_Delay;
    $self->{batch}           = {};
    $self->{batch_offset}    = {};
    $self->{batch_size}      = {};
    $self->{batch_timestamp} = {};
    $self->{responses}       = {};
    $self->{batch_responses} = {};
    return;
}

sub is_broker_path {
    my ( $self, $path ) = @_;
    my $paths = $self->{valid_broker_paths};
    if ( not $paths ) {
        $paths = { $self->{broker_path} => 1 };
        my $node = $Tachikoma::Nodes{ $self->{broker_path} };
        if ($node) {
            my $owner = $node->owner;
            if ( ref $owner ) {
                $paths->{$_} = 1 for ( @{$owner} );
            }
            else {
                $paths->{$owner} = 1;
            }
        }
        $self->{valid_broker_paths} = $paths;
    }
    return $paths->{$path};
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
    $message->[TO]      = "$topic:partition:$i";
    $message->[PAYLOAD] = join q(), map ${$_}, @buffer;
    $target->callback(
        sub {
            if ( $_[0]->[TYPE] & TM_RESPONSE ) { $expecting = 0; }
            elsif ( $_[0]->[TYPE] & TM_ERROR )       { die $_[0]->[PAYLOAD]; }
            elsif ( $_[0]->[TYPE] & TM_EOF )         { $expecting = -1; }
            elsif ( not $_[0]->[TYPE] & TM_REQUEST ) { die $_[0]->[PAYLOAD]; }
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
            elsif ( $_[0]->[TYPE] & TM_ERROR )       { die $_[0]->[PAYLOAD]; }
            elsif ( $_[0]->[TYPE] & TM_EOF )         { $expecting = -1; }
            elsif ( not $_[0]->[TYPE] & TM_REQUEST ) { die $_[0]->[PAYLOAD]; }
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
    for my $broker_id ( @{ $self->broker_ids } ) {
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
    $request->[TYPE]    = TM_REQUEST;
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
    for my $broker_id ( @{ $self->broker_ids } ) {
        my $target = $self->get_target($broker_id) or next;
        $self->sync_error(undef);
        my $request = Tachikoma::Message->new;
        $request->[TYPE]    = TM_REQUEST;
        $request->[TO]      = 'broker';
        $request->[PAYLOAD] = "GET_CONTROLLER\n";
        $target->callback(
            sub {
                my $response = shift;
                if ( $response->[TYPE] & TM_RESPONSE ) {
                    return 1;
                }
                elsif ( $response->[TYPE] & TM_REQUEST ) {
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

sub batch_offset {
    my $self = shift;
    if (@_) {
        $self->{batch_offset} = shift;
    }
    return $self->{batch_offset};
}

sub batch_size {
    my $self = shift;
    if (@_) {
        $self->{batch_size} = shift;
    }
    return $self->{batch_size};
}

sub batch_timestamp {
    my $self = shift;
    if (@_) {
        $self->{batch_timestamp} = shift;
    }
    return $self->{batch_timestamp};
}

sub responses {
    my $self = shift;
    if (@_) {
        $self->{responses} = shift;
    }
    return $self->{responses};
}

sub batch_responses {
    my $self = shift;
    if (@_) {
        $self->{batch_responses} = shift;
    }
    return $self->{batch_responses};
}

sub valid_broker_paths {
    my $self = shift;
    if (@_) {
        $self->{valid_broker_paths} = shift;
    }
    return $self->{valid_broker_paths};
}

# sync support
sub broker_ids {
    my $self = shift;
    if (@_) {
        $self->{broker_ids} = shift;
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
