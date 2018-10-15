#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::ConsumerBroker
# ----------------------------------------------------------------------
#
# $Id: ConsumerBroker.pm 29406 2017-04-29 11:18:09Z chris $
#

package Tachikoma::Nodes::ConsumerBroker;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::Topic;
use Tachikoma::Nodes::Consumer;
use Tachikoma::Message qw(
    TYPE FROM TO ID PAYLOAD
    TM_INFO TM_STORABLE TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use Tachikoma;
use Getopt::Long qw( GetOptionsFromString );
use Time::HiRes qw( usleep );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = 'v2.0.256';

my $Poll_Interval       = 1;      # synchronous poll for messages
my $Check_Interval      = 5;      # partition map check
my $Commit_Interval     = 15;     # commit offsets
my $Default_Timeout     = 900;    # default async message timeout
my $Default_Hub_Timeout = 60;     # timeout waiting for hub

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;

    # async and sync support
    $self->{topic}          = shift;
    $self->{group}          = shift;
    $self->{consumers}      = {};
    $self->{default_offset} = 'end';
    $self->{poll_interval}  = $Poll_Interval;
    $self->{cache_dir}      = undef;
    $self->{auto_commit}    = $self->{group} ? $Commit_Interval : undef;
    $self->{auto_offset}    = $self->{auto_commit} ? 1 : undef;

    # async support
    $self->{partition_id}   = undef;
    $self->{broker_path}    = undef;
    $self->{leader_path}    = undef;
    $self->{max_unanswered} = undef;
    $self->{timeout}        = undef;

    # sync support
    $self->{broker}      = undef;
    $self->{hosts}       = { localhost => [ 5501, 5502 ] };
    $self->{broker_ids}  = undef;
    $self->{hub_timeout} = $Default_Hub_Timeout;
    $self->{targets}     = {};
    $self->{persist}     = 'cancel';
    $self->{partitions}  = undef;
    $self->{last_check}  = 0;
    $self->{last_expire} = 0;
    $self->{eos}         = undef;
    $self->{sync_error}  = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node ConsumerBroker <node name> --broker=<path>               \
                                     --topic=<name>                \
                                     --group=<name>                \
                                     --partition_id=<int>          \
                                     --max_unanswered=<int>        \
                                     --timeout=<seconds>           \
                                     --poll_interval=<seconds>     \
                                     --cache_dir=<path>            \
                                     --auto_commit=<seconds>       \
                                     --hub_timeout=<seconds>       \
                                     --default_offset=<int|string>
    # valid offsets: "start" (or "0"), "recent" (or "-2"), "end" (or "-1")
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ($broker,         $topic,       $group,         $partition_id,
            $max_unanswered, $timeout,     $poll_interval, $cache_dir,
            $auto_commit,    $hub_timeout, $default_offset
        );
        my ( $r, $argv ) = GetOptionsFromString(
            $arguments,
            'broker=s'         => \$broker,
            'topic=s'          => \$topic,
            'group=s'          => \$group,
            'partition_id=s'   => \$partition_id,
            'max_unanswered=i' => \$max_unanswered,
            'timeout=i'        => \$timeout,
            'poll_interval=i'  => \$poll_interval,
            'cache_dir=s'      => \$cache_dir,
            'auto_commit=i'    => \$auto_commit,
            'hub_timeout=i'    => \$hub_timeout,
            'default_offset=s' => \$default_offset,
        );
        die "ERROR: invalid option\n" if ( not $r );
        die "ERROR: no topic\n"       if ( not $topic );
        $self->{arguments}      = $arguments;
        $self->{broker_path}    = $broker;
        $self->{topic}          = $topic;
        $self->{group}          = $group;
        $self->{partition_id}   = $partition_id;
        $self->{max_unanswered} = $max_unanswered // 1;
        $self->{timeout}        = $timeout || $Default_Timeout;
        $self->{poll_interval}  = $poll_interval || $Poll_Interval;

        if ($group) {
            $self->{cache_dir}   = $cache_dir;
            $self->{auto_commit} = $auto_commit // $Commit_Interval;
            $self->{auto_offset} = $self->{auto_commit} ? 1 : undef;
        }
        $self->{hub_timeout} = $hub_timeout || $Default_Hub_Timeout;
        $self->{default_offset} = $default_offset // 'end';
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_INFO ) {
        my $topic  = $self->topic;
        my $leader = $message->[PAYLOAD];
        chomp $leader;
        $self->make_broker_connection($leader);
        $self->leader_path( join q{/}, $leader, $self->group );
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_INFO;
        $response->[FROM]    = $self->name;
        $response->[TO]      = $self->leader_path;
        $response->[PAYLOAD] = "GET_PARTITIONS $topic\n";
        $self->sink->fill($response);
    }
    elsif ( $message->[TYPE] & TM_STORABLE ) {
        my $partitions = $message->payload;
        if ( ref $partitions eq 'ARRAY' ) {
            my %mapping = ();
            my $i       = 0;
            for my $broker_id ( @{$partitions} ) {
                $mapping{ $i++ } = $broker_id;
            }
            $partitions = \%mapping;
        }
        if ( defined $self->{partition_id} ) {
            my $partition_id = $self->{partition_id};
            my $broker_id    = $partitions->{$partition_id};
            if ( $broker_id and $self->make_broker_connection($broker_id) ) {
                $self->make_async_consumer( $partitions, $partition_id );
            }
        }
        else {
            for my $partition_id ( sort keys %{$partitions} ) {
                my $broker_id = $partitions->{$partition_id};
                next if ( not $broker_id );
                if ( $self->make_broker_connection($broker_id) ) {
                    $self->make_async_consumer( $partitions, $partition_id );
                }
            }
        }
        for my $partition_id ( keys %{ $self->consumers } ) {
            if ( not $partitions->{$partition_id} ) {
                $self->consumers->{$partition_id}->remove_node;
                delete $self->consumers->{$partition_id};
            }
        }
    }
    elsif ( $message->[TYPE] & TM_ERROR ) {
        for my $partition_id ( keys %{ $self->consumers } ) {
            $self->consumers->{$partition_id}->remove_node;
            delete $self->consumers->{$partition_id};
        }
    }
    else {
        $self->stderr( 'INFO: ', $message->type_as_string, ' from ',
            $message->from );
    }
    return;
}

sub fire {
    my $self    = shift;
    my $message = Tachikoma::Message->new;
    $message->[TYPE] = TM_INFO;
    $message->[FROM] = $self->name;
    $message->[TO]   = $self->broker_path;
    if ( defined $self->{partition_id} or not $self->{group} ) {
        $message->[PAYLOAD] = "GET_PARTITIONS $self->{topic}\n";
    }
    else {
        $message->[PAYLOAD] = "GET_LEADER $self->{group}\n";
    }
    $self->sink->fill($message);
    $self->set_timer( $Check_Interval * 1000 )
        if ( not $self->{timer_interval} );
    return;
}

sub make_broker_connection {
    my $self      = shift;
    my $broker_id = shift;
    my $node      = $Tachikoma::Nodes{$broker_id};
    my $rv        = 1;
    if ( not $node ) {
        my ( $host, $port ) = split m{:}, $broker_id, 2;
        $node = inet_client_async Tachikoma::Nodes::Socket( $host, $port,
            $broker_id );
        $node->on_EOF('reconnect');
        $node->sink( $self->sink );
        $rv = undef;
    }
    return $rv;
}

sub make_async_consumer {
    my $self          = shift;
    my $partitions    = shift;
    my $partition_id  = shift;
    my $broker_id     = $partitions->{$partition_id};
    my $log_name      = join q{:}, $self->topic, 'partition', $partition_id;
    my $log           = join q{/}, $broker_id, $log_name;
    my $consumer_name = undef;
    if ( $self->{group} ) {
        $consumer_name = join q{:}, $log_name, $self->{group};
    }
    else {
        $consumer_name = join q{:}, $self->name, $partition_id;
    }
    my $consumer = $Tachikoma::Nodes{$consumer_name};
    if ( not $consumer ) {
        $consumer = Tachikoma::Nodes::Consumer->new;
        $consumer->name($consumer_name);
        $consumer->arguments("--partition=$log");
        $consumer->broker_id($broker_id);
        $consumer->partition_id($partition_id);
        if ( $self->{group} ) {
            if ( $self->cache_dir ) {
                $consumer->cache_dir( $self->cache_dir );
            }
            elsif ( $self->{auto_offset} ) {
                my $offsets = join q{:}, $log, $self->{group};
                $consumer->offsetlog($offsets);
            }
            $consumer->group( $self->group );
            $consumer->auto_commit( $self->auto_commit );
        }
        $consumer->default_offset( $self->default_offset );
        $consumer->max_unanswered( $self->max_unanswered );
        $consumer->sink( $self->sink );
        $consumer->edge( $self->edge );
        $consumer->set_timer(5000);
        $self->consumers->{$partition_id} = $consumer;
    }
    if (   $consumer->{partition} ne $log
        or $consumer->{broker_id} ne $broker_id )
    {
        $consumer->remove_node;
        delete $self->consumers->{$partition_id};
    }
    else {
        $consumer->{owner} = $self->{owner};
    }
    return;
}

sub owner {
    my $self = shift;
    if (@_) {
        $self->{owner} = shift;
        $self->set_timer(0);
    }
    return $self->{owner};
}

sub edge {
    my $self = shift;
    if (@_) {
        $self->{edge} = shift;
        $self->set_timer(0);
    }
    return $self->{edge};
}

sub remove_node {
    my $self = shift;
    for my $partition_id ( keys %{ $self->consumers } ) {
        $self->consumers->{$partition_id}->remove_node;
    }
    if ( $self->leader_path ) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_INFO;
        $message->[FROM]    = $self->name;
        $message->[TO]      = $self->leader_path;
        $message->[PAYLOAD] = "DISCONNECT\n";
        $self->sink->fill($message)
            if ( $self->sink and not Tachikoma->shutting_down );
    }
    return $self->SUPER::remove_node;
}

########################
# synchronous interface
########################

sub fetch {
    my $self       = shift;
    my $count      = shift;
    my $callback   = shift;
    my $partitions = $self->get_partitions;
    my $messages   = [];
    my $eof        = 1;
    usleep( $self->{poll_interval} * 1000000 )
        if ( $self->{eos} and $self->{poll_interval} );
    for my $partition_id ( keys %{$partitions} ) {
        my $consumer = $self->{consumers}->{$partition_id}
            || $self->make_sync_consumer($partition_id)
            or next;
        push @{$messages}, @{ $consumer->fetch( $count, $callback ) };
        $eof = undef if ( not $consumer->{eos} );
        if ( $consumer->{sync_error} ) {
            $self->{sync_error} = $consumer->{sync_error};
            $self->remove_consumers;
            $eof = 1;
            last;
        }
    }
    $self->{eos} = $eof;
    return $messages;
}

sub get_partitions {
    my $self = shift;
    $self->{sync_error} = undef;
    if ( not $self->{partitions}
        or time - $self->{last_check} > $Check_Interval )
    {
        my $partitions = undef;
        if ( $self->{group} ) {
            $partitions = $self->request_partitions;
        }
        else {
            $partitions = $self->broker->get_mapping;
        }
        for my $partition_id ( keys %{ $self->{consumers} } ) {
            $self->remove_consumer($partition_id)
                if ( not $partitions
                or not $partitions->{$partition_id}
                or $self->{consumers}->{$partition_id}->{broker_id} ne
                $partitions->{$partition_id} );
        }
        $self->{last_check} = time;
        $self->{partitions} = $partitions;
        $self->{sync_error} = undef if ($partitions);
    }
    return $self->{partitions};
}

sub request_partitions {
    my $self            = shift;
    my $leader          = $self->get_leader or return;
    my $target          = $self->get_target($leader) or return;
    my $partitions      = undef;
    my $request_payload = "GET_PARTITIONS $self->{topic}\n";
    my $request         = Tachikoma::Message->new;
    $request->[TYPE]    = TM_INFO;
    $request->[TO]      = $self->{group};
    $request->[PAYLOAD] = $request_payload;
    $target->callback(
        sub {
            my $response = shift;
            if ( $response->[TYPE] & TM_STORABLE ) {
                $partitions = $response->payload;
            }
            elsif ( $response->[PAYLOAD] eq "NOT_LEADER\n" ) {
                $self->{leader} = undef;
            }
            elsif ( $response->[PAYLOAD] ) {
                my $error = $response->[PAYLOAD];
                chomp $error;
                $self->{sync_error} = "REQUEST_PARTITIONS: $error\n";
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
    if ( not $okay or not $target->{fh} ) {
        my $error = $@ || 'lost connection';
        $self->remove_consumers;
        chomp $error;
        $self->{sync_error} = "REQUEST_PARTITIONS: $error\n";
        $partitions = undef;
    }
    return $partitions;
}

sub get_leader {
    my $self = shift;
    if ( not $self->{leader} ) {
        my $broker_ids = $self->broker_ids;
        my $leader     = undef;
        die "ERROR: no group specified\n" if ( not $self->{group} );
        for my $name ( keys %{$broker_ids} ) {
            $leader = $self->request_leader($name);
            last if ($leader);
        }
        $self->{leader} = $leader;
        $self->{sync_error} = undef if ($leader);
    }
    return $self->{leader};
}

sub request_leader {
    my $self            = shift;
    my $name            = shift;
    my $target          = $self->get_target($name) or return;
    my $leader          = undef;
    my $request_payload = "GET_LEADER $self->{group}\n";
    my $request         = Tachikoma::Message->new;
    $request->[TYPE]    = TM_INFO;
    $request->[TO]      = 'broker';
    $request->[PAYLOAD] = $request_payload;
    $target->callback(
        sub {
            my $response = shift;
            if ( $response->[TYPE] & TM_INFO ) {
                $leader = $response->[PAYLOAD];
                chomp $leader;
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
    if ( not $okay or not $target->{fh} ) {
        my $error = $@ || 'lost connection';
        $self->remove_consumers;
        chomp $error;
        $self->{sync_error} = "REQUEST_LEADER: $error\n";
    }
    return $leader;
}

sub get_controller {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::get_controller(@args);
}

sub get_group_cache {
    my $self   = shift;
    my $caches = {};
    die "ERROR: no group\n" if ( not $self->{group} );
    my $mapping = $self->broker->get_mapping( $self->topic );
    $self->make_sync_consumers($mapping)
        if ( not keys %{ $self->consumers } );
    $self->get_offset;
    for my $partition_id ( keys %{ $self->consumers } ) {
        my $consumer = $self->consumers->{$partition_id};
        $caches->{ $consumer->partition } = $consumer->cache;
    }
    return $caches;
}

sub get_cache {
    my $self   = shift;
    my $i      = shift;
    my $caches = {};
    die "ERROR: no group\n" if ( not $self->{group} );
    my $partitions = $self->broker->get_mapping( $self->topic );
    my $mapping    = { $i => $partitions->{$i} };
    my $consumer   = $self->{consumers}->{$i}
        || $self->make_sync_consumer($i);
    die "ERROR: couldn't get consumer for group\n"
        if ( not $consumer );
    $consumer->get_offset;
    $self->{sync_error} ||= $@;
    return $consumer->cache;
}

sub get_offset {
    my $self = shift;
    for my $partition_id ( keys %{ $self->{consumers} } ) {
        my $consumer = $self->{consumers}->{$partition_id};
        $consumer->get_offset;
        if ( $consumer->sync_error ) {
            $self->sync_error( $consumer->sync_error );
            $self->remove_consumers;
            last;
        }
    }
    return;
}

sub commit_offset {
    my $self = shift;
    for my $partition_id ( keys %{ $self->{consumers} } ) {
        my $consumer = $self->{consumers}->{$partition_id};
        $consumer->commit_offset;
        if ( $consumer->sync_error ) {
            $self->sync_error( $consumer->sync_error );
            $self->remove_consumers;
            last;
        }
    }
    return;
}

sub retry_offset {
    my $self = shift;
    for my $partition_id ( keys %{ $self->consumers } ) {
        my $consumer = $self->consumers->{$partition_id};
        $consumer->retry_offset;
    }
    return;
}

sub make_sync_consumers {
    my $self    = shift;
    my $mapping = shift;
    $self->partitions($mapping);
    for my $partition_id ( keys %{$mapping} ) {
        $self->make_sync_consumer($partition_id);
    }
    return;
}

sub make_sync_consumer {
    my $self         = shift;
    my $partition_id = shift;
    die "ERROR: no partition id\n" if ( not defined $partition_id );
    my $partitions = $self->get_partitions;
    my $broker_id  = $partitions->{$partition_id} or return;
    my $log        = join q{:}, $self->{topic}, 'partition', $partition_id;
    my $consumer   = Tachikoma::Nodes::Consumer->new($log);
    $consumer->broker_id($broker_id);
    $consumer->partition_id($partition_id);
    $consumer->target( $self->get_target($broker_id) );

    if ( $self->{group} ) {
        if ( $self->{cache_dir} ) {
            $consumer->cache_dir( $self->{cache_dir} );
        }
        elsif ( $self->{auto_offset} ) {
            my $offsets = join q{:}, $log, $self->{group};
            $consumer->offsetlog($offsets);
        }
        $consumer->group( $self->{group} );
        $consumer->auto_commit( $self->{auto_commit} );
    }
    $consumer->default_offset( $self->{default_offset} );
    $consumer->poll_interval(undef);
    $consumer->hub_timeout( $self->hub_timeout );
    $self->{consumers}->{$partition_id} = $consumer;
    return $consumer;
}

sub disconnect {
    my $self    = shift;
    my $leader  = $self->get_leader or return;
    my $target  = $self->get_target($leader) or return;
    my $request = Tachikoma::Message->new;
    $request->[TYPE]    = TM_INFO;
    $request->[TO]      = $self->{group};
    $request->[PAYLOAD] = "DISCONNECT\n";
    $target->fill($request);
    $self->remove_consumers;
    return;
}

sub get_target {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::get_target(@args);
}

sub remove_consumers {
    my $self = shift;
    $self->{leader}     = undef;
    $self->{partitions} = undef;
    for my $partition_id ( keys %{ $self->consumers } ) {
        $self->remove_consumer($partition_id);
    }
    $self->remove_targets;
    $self->broker->partitions(undef) if ( $self->broker );
    return;
}

sub remove_consumer {
    my $self         = shift;
    my $partition_id = shift;
    my $consumer     = $self->consumers->{$partition_id};
    $consumer->remove_target;
    $consumer->remove_node;
    delete $self->consumers->{$partition_id};
    return;
}

sub remove_targets {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::remove_targets(@args);
}

sub remove_target {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::remove_target(@args);
}

# async and sync support
sub topic {
    my $self = shift;
    if (@_) {
        $self->{topic} = shift;
    }
    return $self->{topic};
}

sub group {
    my $self = shift;
    if (@_) {
        $self->{group} = shift;
    }
    return $self->{group};
}

sub consumers {
    my $self = shift;
    if (@_) {
        $self->{consumers} = shift;
    }
    return $self->{consumers};
}

sub default_offset {
    my $self = shift;
    if (@_) {
        $self->{default_offset} = shift;
        for my $partition_id ( keys %{ $self->{consumers} } ) {
            my $consumer = $self->{consumers}->{$partition_id};
            $consumer->default_offset( $self->{default_offset} );
        }
    }
    return $self->{default_offset};
}

sub poll_interval {
    my $self = shift;
    if (@_) {
        $self->{poll_interval} = shift;
    }
    return $self->{poll_interval};
}

sub cache_dir {
    my $self = shift;
    if (@_) {
        $self->{cache_dir} = shift;
    }
    return $self->{cache_dir};
}

sub auto_commit {
    my $self = shift;
    if (@_) {
        $self->{auto_commit} = shift;
        die "ERROR: couldn't auto_commit without a group\n"
            if ( $self->{auto_commit} and not $self->{group} );
        $self->auto_offset(1) if ( $self->{auto_commit} );
        for my $partition_id ( keys %{ $self->{consumers} } ) {
            my $consumer = $self->{consumers}->{$partition_id};
            $consumer->auto_commit( $self->{auto_commit} );
        }
    }
    return $self->{auto_commit};
}

sub auto_offset {
    my $self = shift;
    if (@_) {
        $self->{auto_offset} = shift;
        die "ERROR: couldn't auto_offset without a group\n"
            if ( $self->{auto_offset} and not $self->{group} );
        $self->auto_commit(undef)
            if ( not $self->{auto_offset} and $self->{auto_commit} );
    }
    return $self->{auto_offset};
}

# async support
sub partition_id {
    my $self = shift;
    if (@_) {
        $self->{partition_id} = shift;
    }
    return $self->{partition_id};
}

sub broker_path {
    my $self = shift;
    if (@_) {
        $self->{broker_path} = shift;
    }
    return $self->{broker_path};
}

sub leader_path {
    my $self = shift;
    if (@_) {
        $self->{leader_path} = shift;
    }
    return $self->{leader_path};
}

sub msg_unanswered {
    my $self           = shift;
    my $msg_unanswered = 0;
    for my $partition_id ( keys %{ $self->{consumers} } ) {
        my $consumer = $self->{consumers}->{$partition_id};
        $msg_unanswered += $consumer->{msg_unanswered};
    }
    return $msg_unanswered;
}

sub max_unanswered {
    my $self = shift;
    if (@_) {
        $self->{max_unanswered} = shift;
    }
    return $self->{max_unanswered};
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

# sync support
sub broker {
    my $self = shift;
    if (@_) {
        $self->{broker} = shift;
    }
    if ( not defined $self->{broker} ) {
        my $broker = Tachikoma::Nodes::Topic->new( $self->topic );
        $broker->hosts( $self->hosts );
        $broker->poll_interval( $self->poll_interval );
        $broker->hub_timeout( $self->hub_timeout );
        $self->{broker} = $broker;
    }
    return $self->{broker};
}

sub hosts {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::hosts(@args);
}

sub broker_ids {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::broker_ids(@args);
}

sub persist {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::persist(@args);
}

sub hub_timeout {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::hub_timeout(@args);
}

sub targets {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::targets(@args);
}

sub partitions {
    my $self = shift;
    if (@_) {
        $self->{partitions} = shift;
    }
    return $self->{partitions};
}

sub last_check {
    my $self = shift;
    if (@_) {
        $self->{last_check} = shift;
    }
    return $self->{last_check};
}

sub last_expire {
    my $self = shift;
    if (@_) {
        $self->{last_expire} = shift;
    }
    return $self->{last_expire};
}

sub eos {
    my $self = shift;
    if (@_) {
        $self->{eos} = shift;
    }
    return $self->{eos};
}

sub sync_error {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::sync_error(@args);
}

1;
