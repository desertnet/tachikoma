#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::ConsumerBroker
# ----------------------------------------------------------------------
#
#   - Gets assignment from ConsumerGroup and creates Consumers
#

package Tachikoma::Nodes::ConsumerBroker;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::Topic;
use Tachikoma::Nodes::Consumer;
use Tachikoma::Message qw(
    TYPE FROM TO ID PAYLOAD
    TM_INFO TM_REQUEST TM_STORABLE TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use Tachikoma;
use Getopt::Long qw( GetOptionsFromString );
use Time::HiRes qw( usleep );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.256');

my $Poll_Interval   = 1;             # poll for new messages this often
my $Startup_Delay   = 30;            # wait at least this long on startup
my $Check_Interval  = 15;            # synchronous partition map check
my $Commit_Interval = 60;            # commit offsets
my $Timeout         = 900;           # default async message timeout
my $Hub_Timeout     = 300;           # timeout waiting for hub
my $Cache_Type      = 'snapshot';    # save complete state

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;

    # async and sync support
    $self->{topic}          = shift;
    $self->{group}          = shift;
    $self->{consumers}      = {};
    $self->{default_offset} = 'end';
    $self->{poll_interval}  = $Poll_Interval;
    $self->{cache_type}     = 'snapshot';
    $self->{cache_dir}      = undef;
    $self->{auto_commit}    = $self->{group} ? $Commit_Interval : undef;
    $self->{auto_offset}    = $self->{auto_commit} ? 1 : undef;
    $self->{last_check}     = 0;

    # async support
    $self->{partition_id}            = undef;
    $self->{broker_path}             = undef;
    $self->{leader_path}             = undef;
    $self->{max_unanswered}          = undef;
    $self->{timeout}                 = undef;
    $self->{registrations}->{ACTIVE} = {};
    $self->{registrations}->{READY}  = {};

    # sync support
    $self->{broker}      = undef;
    $self->{broker_ids}  = [ 'localhost:5501', 'localhost:5502' ];
    $self->{hub_timeout} = $Hub_Timeout;
    $self->{targets}     = {};
    $self->{partitions}  = undef;
    $self->{leader}      = undef;
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
                                     --cache_type=<string>         \
                                     --cache_dir=<path>            \
                                     --auto_commit=<seconds>       \
                                     --hub_timeout=<seconds>       \
                                     --default_offset=<int|string>
    # valid cache types: window, snapshot
    # valid offsets: start (0), recent (-2), end (-1)
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ($broker,        $topic,          $group,
            $partition_id,  $max_unanswered, $timeout,
            $poll_interval, $cache_type,     $cache_dir,
            $auto_commit,   $hub_timeout,    $default_offset
        );
        my ( $r, $argv ) = GetOptionsFromString(
            $arguments,
            'broker=s'         => \$broker,
            'topic=s'          => \$topic,
            'group=s'          => \$group,
            'partition_id=s'   => \$partition_id,
            'max_unanswered=i' => \$max_unanswered,
            'timeout=i'        => \$timeout,
            'poll_interval=f'  => \$poll_interval,
            'cache_type=s'     => \$cache_type,
            'cache_dir=s'      => \$cache_dir,
            'auto_commit=i'    => \$auto_commit,
            'hub_timeout=i'    => \$hub_timeout,
            'default_offset=s' => \$default_offset,
        );
        die "ERROR: bad arguments for ConsumerBroker\n" if ( not $r );
        die "ERROR: no topic for ConsumerBroker\n"      if ( not $topic );
        die "ERROR: can't set auto_commit with window cache_type\n"
            if ( $auto_commit and $cache_type and $cache_type eq 'window' );
        $self->{arguments}      = $arguments;
        $self->{broker_path}    = $broker;
        $self->{topic}          = $topic;
        $self->{group}          = $group;
        $self->{partition_id}   = $partition_id;
        $self->{max_unanswered} = $max_unanswered // 1;
        $self->{timeout}        = $timeout || $Timeout;
        $self->{poll_interval}  = $poll_interval || $Poll_Interval;

        if ($group) {
            $self->{cache_type}  = $cache_type // $Cache_Type;
            $self->{cache_dir}   = $cache_dir;
            $self->{auto_commit} = $auto_commit // $Commit_Interval;
            if ( $self->{cache_type} eq 'window' ) {
                $self->{auto_commit} = undef;
                $self->{auto_offset} = 1;
            }
            elsif ( $self->{auto_commit} ) {
                $self->{auto_offset} = 1;
            }
            else {
                $self->{auto_offset} = undef;
            }
        }
        $self->{hub_timeout}    = $hub_timeout || $Hub_Timeout;
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
        $self->make_broker_connection($leader) or return;
        $self->leader_path( join q(/), $leader, $self->group );
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_REQUEST;
        $response->[FROM]    = $self->name;
        $response->[TO]      = $self->leader_path;
        $response->[PAYLOAD] = "GET_PARTITIONS $topic\n";
        $self->sink->fill($response);
    }
    elsif ( $message->[TYPE] & TM_STORABLE ) {
        $self->update_graph( $message->payload );
    }
    elsif ( $message->[TYPE] & TM_ERROR ) {
        if ( $self->{edge} and $self->{edge}->can('new_cache') ) {
            $self->{edge}->new_cache;
        }
        for my $partition_id ( keys %{ $self->consumers } ) {
            $self->consumers->{$partition_id}->remove_node;
            delete $self->consumers->{$partition_id};
        }
    }
    elsif ( not $message->[TYPE] & TM_EOF ) {
        $self->stderr( 'INFO: ', $message->type_as_string, ' from ',
            $message->from );
    }
    return;
}

sub fire {
    my $self      = shift;
    my $consumers = $self->{consumers};
    if ( $Tachikoma::Now - $self->{last_check} > $Check_Interval ) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE] = TM_REQUEST;
        $message->[FROM] = $self->name;
        $message->[TO]   = $self->broker_path;
        if ( defined $self->{partition_id} or not $self->{group} ) {
            $message->[PAYLOAD] = "GET_PARTITIONS $self->{topic}\n";
        }
        else {
            $message->[PAYLOAD] = "GET_LEADER $self->{group}\n";
        }
        $self->sink->fill($message);
        $self->{last_check} = $Tachikoma::Now;
    }
    for my $partition_id ( keys %{$consumers} ) {
        $consumers->{$partition_id}->fire;
    }
    return;
}

sub update_graph {
    my $self       = shift;
    my $partitions = shift;
    if ( ref $partitions eq 'ARRAY' ) {
        my %mapping = ();
        my $i       = 0;
        for my $broker_id ( @{$partitions} ) {
            $mapping{ $i++ } = $broker_id;
        }
        $partitions = \%mapping;
    }
    for my $partition_id ( keys %{ $self->consumers } ) {
        if ( not $partitions->{$partition_id} ) {
            $self->stderr("WARNING: not assigned to partition $partition_id");
            $self->consumers->{$partition_id}->remove_node;
            delete $self->consumers->{$partition_id};
        }
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
    return;
}

sub make_broker_connection {
    my $self      = shift;
    my $broker_id = shift;
    my $node      = $Tachikoma::Nodes{$broker_id};
    if ( not $node ) {
        my ( $host, $port ) = split m{:}, $broker_id, 2;
        $node = inet_client_async Tachikoma::Nodes::Socket( $host, $port,
            $broker_id );
        $node->on_EOF('reconnect');
        $node->sink( $self->sink );
    }
    return $node->auth_complete;
}

sub make_async_consumer {
    my $self          = shift;
    my $partitions    = shift;
    my $partition_id  = shift;
    my $broker_id     = $partitions->{$partition_id};
    my $log_name      = join q(:), $self->topic, 'partition', $partition_id;
    my $log           = join q(/), $broker_id, $log_name;
    my $consumer_name = undef;
    if ( $self->{group} ) {
        $consumer_name = join q(:), $log_name, $self->{group};
    }
    else {
        $consumer_name = join q(:), $self->name, $partition_id;
    }
    my $consumer = $Tachikoma::Nodes{$consumer_name};
    if ( not $consumer ) {
        $consumer = Tachikoma::Nodes::Consumer->new;
        $consumer->name($consumer_name);
        $consumer->arguments("--partition=$log");
        $consumer->broker_id($broker_id);
        $consumer->partition_id($partition_id);
        if ( $self->{group} ) {
            $consumer->cache_type( $self->cache_type );
            if ( $self->cache_dir ) {
                $consumer->cache_dir( $self->cache_dir );
            }
            elsif ( $self->{auto_offset} ) {
                my $offsetlog = join q(:), $log, $self->{group};
                $consumer->offsetlog($offsetlog);
            }
            $consumer->auto_commit( $self->auto_commit );
        }
        $consumer->default_offset( $self->default_offset );
        $consumer->poll_interval( $self->poll_interval );
        $consumer->hub_timeout( $self->hub_timeout );
        $consumer->max_unanswered( $self->max_unanswered );
        $consumer->timeout( $self->timeout );
        $consumer->sink( $self->sink );
        $consumer->edge( $self->edge );
        $consumer->owner( $self->owner );

        for my $event ( keys %{ $self->{registrations} } ) {
            my $r = $self->{registrations}->{$event};
            $consumer->{registrations}->{$event} =
                { map { $_ => defined $r->{$_} ? 0 : undef } keys %{$r} };
        }
        $self->consumers->{$partition_id} = $consumer;
    }
    return;
}

sub owner {
    my $self = shift;
    if (@_) {
        $self->{owner}      = shift;
        $self->{last_check} = $Tachikoma::Now + $Startup_Delay;
        for my $partition_id ( keys %{ $self->consumers } ) {
            $self->consumers->{$partition_id}->owner( $self->{owner} );
        }
        $self->set_timer( $self->{poll_interval} * 1000 );
    }
    return $self->{owner};
}

sub edge {
    my $self = shift;
    if (@_) {
        $self->{edge}       = shift;
        $self->{last_check} = $Tachikoma::Now + $Startup_Delay;
        if ( $self->{edge} and $self->{edge}->can('new_cache') ) {
            $self->{edge}->new_cache;
        }
        for my $partition_id ( keys %{ $self->consumers } ) {
            $self->consumers->{$partition_id}->edge( $self->{edge} );
        }
        $self->set_timer( $self->{poll_interval} * 1000 );
    }
    return $self->{edge};
}

sub remove_node {
    my $self = shift;
    if ( $self->{edge} and $self->{edge}->can('new_cache') ) {
        $self->{edge}->new_cache;
    }
    for my $partition_id ( keys %{ $self->consumers } ) {
        $self->consumers->{$partition_id}->remove_node;
    }
    if ( $self->leader_path ) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_REQUEST;
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
            $self->sync_error( $consumer->sync_error );
            $self->remove_consumers;
            $eof = 1;
            last;
        }
    }
    $self->{eos} = $eof;
    return $messages;
}

sub fetch_offset {
    my ( $self, $partition, $offset ) = @_;
    my $value = undef;
    my $topic = $self->{topic} or die 'ERROR: no topic';
    chomp $offset;
    $self->get_partitions;
    my $consumer = $self->{consumers}->{$partition}
        || $self->make_sync_consumer($partition);
    if ($consumer) {
        my $messages = undef;
        $consumer->next_offset($offset);
        do { $messages = $consumer->fetch }
            while ( not @{$messages} and not $consumer->eos );
        die $consumer->sync_error if ( $consumer->sync_error );
        if ( not @{$messages} ) {
            die "ERROR: fetch_offset failed at $partition:$offset\n";
        }
        else {
            my $message = shift @{$messages};
            $value = $message if ( $message->[ID] =~ m{^$offset:} );
        }
    }
    else {
        die "ERROR: consumer lookup failed at $partition:$offset\n";
    }
    return $value;
}

sub get_partitions {
    my $self = shift;
    $self->{sync_error} = undef;
    if ( not $self->{partitions}
        or time - $self->{last_check} > $Check_Interval )
    {
        my $partitions = undef;
        if ( $self->group ) {
            $partitions = $self->request_partitions;
        }
        else {
            $partitions = $self->broker->get_mapping;
        }
        for my $partition_id ( keys %{ $self->consumers } ) {
            $self->remove_consumer($partition_id)
                if ( not $partitions
                or not $partitions->{$partition_id}
                or $self->{consumers}->{$partition_id}->{broker_id} ne
                $partitions->{$partition_id} );
        }
        $self->last_check(time);
        $self->partitions($partitions);
        $self->sync_error(undef) if ($partitions);
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
    $request->[TYPE]    = TM_REQUEST;
    $request->[TO]      = $self->group;
    $request->[PAYLOAD] = $request_payload;
    $target->callback(
        sub {
            my $response = shift;
            if ( $response->[TYPE] & TM_STORABLE ) {
                $partitions = $response->payload;
            }
            elsif ( $response->[PAYLOAD] eq "NOT_LEADER\n" ) {
                $self->leader(undef);
            }
            elsif ( $response->[PAYLOAD] ) {
                my $error = $response->[PAYLOAD];
                chomp $error;
                $self->sync_error("REQUEST_PARTITIONS: $error\n");
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
        chomp $error;
        $self->remove_consumers;
        $self->sync_error("REQUEST_PARTITIONS: $error\n");
        $partitions = undef;
    }
    elsif ( not $target->{fh} ) {
        $self->remove_consumers;
        $self->sync_error("REQUEST_PARTITIONS: lost connection\n");
        $partitions = undef;
    }
    return $partitions;
}

sub get_leader {
    my $self = shift;
    if ( not $self->{leader} ) {
        die "ERROR: no group specified\n" if ( not $self->group );
        my $leader = undef;
        for my $name ( @{ $self->broker_ids } ) {
            $leader = $self->request_leader($name);
            last if ($leader);
        }
        $self->leader($leader);
        $self->sync_error(undef) if ($leader);
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
    $request->[TYPE]    = TM_REQUEST;
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
    if ( not $okay ) {
        my $error = $@ || 'unknown error';
        chomp $error;
        $self->remove_consumers;
        $self->sync_error("REQUEST_LEADER: $error\n");
        $leader = undef;
    }
    elsif ( not $target->{fh} ) {
        $self->remove_consumers;
        $self->sync_error("REQUEST_LEADER: lost connection\n");
        $leader = undef;
    }
    return $leader;
}

sub get_controller {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::get_controller(@args);
}

sub get_group_cache {
    my $self    = shift;
    my $caches  = {};
    my $topic   = $self->topic or die "ERROR: no topic\n";
    my $group   = $self->group or die "ERROR: no group\n";
    my $mapping = $self->broker->get_mapping($topic);
    for my $partition_id ( keys %{$mapping} ) {
        my $broker_id = $mapping->{$partition_id};
        my $offsetlog = join q(:), $topic, 'partition', $partition_id, $group;
        my $consumer  = Tachikoma::Nodes::Consumer->new($offsetlog);
        $consumer->next_offset(-2);
        $consumer->broker_id($broker_id);
        $consumer->timeout( $self->timeout );
        $consumer->hub_timeout( $self->hub_timeout );
        while (1) {
            my $messages = $consumer->fetch;
            my $error    = $consumer->sync_error // q();
            chomp $error;
            $self->sync_error("GET_OFFSET: $error\n") if ($error);
            last                                      if ( not @{$messages} );
            $caches->{$partition_id} = $messages->[-1]->payload;
        }
    }
    return $caches;
}

sub commit_offset {
    my $self = shift;
    for my $partition_id ( keys %{ $self->consumers } ) {
        my $consumer = $self->consumers->{$partition_id};
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
    my $log        = join q(:), $self->topic, 'partition', $partition_id;
    my $consumer   = Tachikoma::Nodes::Consumer->new($log);
    $consumer->broker_id($broker_id);
    $consumer->partition_id($partition_id);
    $consumer->target( $self->get_target($broker_id) );

    if ( $self->group ) {
        $consumer->group( $self->group );
        $consumer->cache_type( $self->cache_type );
        if ( $self->cache_dir ) {
            $consumer->cache_dir( $self->cache_dir );
        }
        elsif ( $self->auto_offset ) {
            my $offsetlog = join q(:), $log, $self->group;
            $consumer->offsetlog($offsetlog);
        }
        $consumer->auto_commit( $self->auto_commit );
    }
    $consumer->default_offset( $self->default_offset );
    $consumer->poll_interval(undef);
    $consumer->hub_timeout( $self->hub_timeout );
    $self->consumers->{$partition_id} = $consumer;
    return $consumer;
}

sub disconnect {
    my $self    = shift;
    my $leader  = $self->get_leader or return;
    my $target  = $self->get_target($leader) or return;
    my $request = Tachikoma::Message->new;
    $request->[TYPE]    = TM_REQUEST;
    $request->[TO]      = $self->group;
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
    $self->leader(undef);
    $self->partitions(undef);
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

sub cache_type {
    my $self = shift;
    if (@_) {
        $self->{cache_type} = shift;
    }
    return $self->{cache_type};
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
        die "ERROR: can't auto_commit without a group\n"
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
        die "ERROR: can't auto_offset without a group\n"
            if ( $self->{auto_offset} and not $self->{group} );
        $self->auto_commit(undef)
            if ( not $self->{auto_offset} and $self->{auto_commit} );
    }
    return $self->{auto_offset};
}

sub last_check {
    my $self = shift;
    if (@_) {
        $self->{last_check} = shift;
    }
    return $self->{last_check};
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
        $broker->broker_ids( $self->broker_ids );
        $broker->poll_interval( $self->poll_interval );
        $broker->hub_timeout( $self->hub_timeout );
        $self->{broker} = $broker;
    }
    return $self->{broker};
}

sub broker_ids {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::broker_ids(@args);
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

sub leader {
    my $self = shift;
    if (@_) {
        $self->{leader} = shift;
    }
    return $self->{leader};
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
    return Tachikoma::Nodes::Consumer::sync_error(@args);
}

1;
