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
use Tachikoma::Nodes::Socket qw( TK_SYNC );
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::Topic;
use Tachikoma::Nodes::Consumer;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_INFO TM_REQUEST TM_STORABLE TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use Getopt::Long qw( GetOptionsFromString );
use Time::HiRes qw( usleep );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.256');

my $ASYNC_INTERVAL    = 5;             # check partition map this often
my $POLL_INTERVAL     = 1;             # check for new messages this often
my $CONSUMER_INTERVAL = 15;            # sanity check consumers this often
my $STARTUP_DELAY     = 5;             # wait at least this long on startup
my $COMMIT_INTERVAL   = 60;            # commit offsets
my $DEFAULT_TIMEOUT   = 900;           # default message timeout
my $HUB_TIMEOUT       = 300;           # timeout waiting for hub
my $CACHE_TYPE        = 'snapshot';    # save complete state

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{topic}          = shift;
    $self->{group}          = shift;
    $self->{flags}          = 0;
    $self->{consumers}      = {};
    $self->{async_interval} = $ASYNC_INTERVAL;
    $self->{check_interval} = 0;
    $self->{hub_timeout}    = $HUB_TIMEOUT;
    $self->{cache_type}     = $CACHE_TYPE;
    $self->{auto_commit}    = $self->{group} ? $COMMIT_INTERVAL : undef;
    $self->{auto_offset}    = $self->{auto_commit} ? 1 : undef;
    $self->{default_offset} = 'end';
    $self->{last_check}     = 0;
    $self->{partition_id}   = undef;
    $self->{broker_path}    = undef;
    $self->{leader_path}    = undef;
    $self->{max_unanswered} = undef;
    $self->{timeout}        = $DEFAULT_TIMEOUT;
    $self->{startup_delay}  = 0;
    $self->{registrations}->{READY} = {};

    # sync support
    if ( length $self->{topic} ) {
        $self->{broker}        = undef;
        $self->{broker_ids}    = ['localhost:5501'];
        $self->{targets}       = {};
        $self->{partitions}    = undef;
        $self->{leader}        = undef;
        $self->{poll_interval} = $POLL_INTERVAL;
        $self->{last_expire}   = 0;
        $self->{eos}           = undef;
        $self->{sync_error}    = undef;
    }
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
                                     --hub_timeout=<seconds>       \
                                     --cache_type=<string>         \
                                     --auto_commit=<seconds>       \
                                     --default_offset=<int|string>
    # valid cache types: window, snapshot
    # valid offsets: start (0), recent (-2), end (-1)
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ($broker,       $topic,          $group,
            $partition_id, $max_unanswered, $timeout,
            $hub_timeout,  $startup_delay,  $cache_type,
            $auto_commit,  $default_offset,
        );
        my ( $r, $argv ) = GetOptionsFromString(
            $arguments,
            'broker=s'         => \$broker,
            'topic=s'          => \$topic,
            'group=s'          => \$group,
            'partition_id=s'   => \$partition_id,
            'max_unanswered=i' => \$max_unanswered,
            'timeout=i'        => \$timeout,
            'hub_timeout=i'    => \$hub_timeout,
            'startup_delay=i'  => \$startup_delay,
            'cache_type=s'     => \$cache_type,
            'auto_commit=i'    => \$auto_commit,
            'default_offset=s' => \$default_offset,
        );
        die "ERROR: bad arguments for ConsumerBroker\n" if ( not $r );
        die "ERROR: no topic for ConsumerBroker\n" if ( not length $topic );
        die "ERROR: can't set auto_commit with window cache_type\n"
            if ( $auto_commit and $cache_type and $cache_type eq 'window' );
        $self->{arguments}      = $arguments;
        $self->{broker_path}    = $broker;
        $self->{topic}          = $topic;
        $self->{group}          = $group;
        $self->{partition_id}   = $partition_id;
        $self->{max_unanswered} = $max_unanswered // 1;
        $self->{timeout}        = $timeout || $DEFAULT_TIMEOUT;
        $self->{check_interval} = 0;
        $self->{hub_timeout}    = $hub_timeout || $HUB_TIMEOUT;
        $self->{startup_delay}  = $startup_delay // $STARTUP_DELAY;

        if ($group) {
            $self->{cache_type}  = $cache_type  // $CACHE_TYPE;
            $self->{auto_commit} = $auto_commit // $COMMIT_INTERVAL;
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
        $self->{default_offset} = $default_offset // 'end';
        $self->{last_check}     = 0;
        $self->{set_state}      = {};
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_INFO ) {
        if ( length $message->[ID] ) {
            $self->update_leader($message);
        }
        else {
            $self->update_state($message);
        }
    }
    elsif ( $message->[TYPE] & TM_STORABLE ) {
        $self->update_graph($message);
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
    $self->stderr( 'DEBUG: FIRE ', $self->{timer_interval}, 'ms' )
        if ( $self->{debug_state} and $self->{debug_state} >= 3 );
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
    $self->stderr( 'DEBUG: ' . $message->[PAYLOAD] )
        if ( $self->{debug_state} and $self->{debug_state} >= 2 );
    $self->sink->fill($message);
    if ( not $self->{timer_interval}
        or $self->{timer_interval} != $self->{async_interval} * 1000 )
    {
        $self->set_timer( $self->{async_interval} * 1000 );
    }
    if ( $Tachikoma::Right_Now - $self->{last_check} > $CONSUMER_INTERVAL ) {
        for my $partition_id ( keys %{$consumers} ) {
            next
                if ( $consumers->{$partition_id}->{timer_is_active}
                or not $consumers->{$partition_id}->{name} );
            $consumers->{$partition_id}->fire;
        }
        $self->{last_check} = $Tachikoma::Right_Now;
    }
    return;
}

sub update_leader {
    my $self    = shift;
    my $message = shift;
    my $topic   = $self->topic;
    my $leader  = $message->[PAYLOAD];
    chomp $leader;
    $self->make_broker_connection($leader) or return;
    $self->leader_path( join q(/), $leader, $self->group );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_REQUEST;
    $response->[FROM]    = $self->name;
    $response->[TO]      = $self->leader_path;
    $response->[PAYLOAD] = "GET_PARTITIONS $topic\n";
    $self->stderr( 'DEBUG: ' . $message->[PAYLOAD] )
        if ( $self->{debug_state} and $self->{debug_state} >= 2 );
    $self->sink->fill($response);
    return;
}

sub update_state {
    my $self      = shift;
    my $message   = shift;
    my $event     = $message->[STREAM];
    my $consumers = $self->consumers;
    my $state     = 1;
    for my $i ( keys %{$consumers} ) {
        $state = undef
            if ( not exists $consumers->{$i}->{set_state}->{$event} );
    }
    $self->set_state($event) if ($state);
    return;
}

sub update_graph {
    my $self       = shift;
    my $message    = shift;
    my $partitions = $message->payload;
    my $ready      = $self->{check_interval} != $ASYNC_INTERVAL;
    if ( ref $partitions eq 'ARRAY' ) {
        my %mapping = ();
        my $i       = 0;
        for my $broker_id ( @{$partitions} ) {
            $mapping{ $i++ } = $broker_id;
        }
        $partitions = \%mapping;
    }
    else {
        $ready = undef;
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
            $self->make_consumer( $partitions, $partition_id );
        }
        else {
            $ready = undef;
        }
    }
    else {
        for my $partition_id ( sort keys %{$partitions} ) {
            my $broker_id = $partitions->{$partition_id};
            if ( $broker_id and $self->make_broker_connection($broker_id) ) {
                $self->make_consumer( $partitions, $partition_id );
            }
            else {
                $ready = undef;
            }
        }
    }
    if ($ready) {
        $self->stderr('DEBUG: GRAPH_COMPLETE') if ( $self->debug_state );
        $self->{check_interval} = $ASYNC_INTERVAL;
    }
    return;
}

sub make_broker_connection {
    my $self      = shift;
    my $broker_id = shift;
    my $node      = $Tachikoma::Nodes{$broker_id};
    if ( not $node ) {
        my ( $host, $port ) = split m{:}, $broker_id, 2;
        if ( $self->flags & TK_SYNC ) {
            $node = Tachikoma::Nodes::Socket->inet_client( $host, $port,
                TK_SYNC );
        }
        else {
            $node =
                Tachikoma::Nodes::Socket->inet_client_async( $host, $port );
        }
        $node->name($broker_id);
        $node->debug_state( $self->debug_state );
        $node->on_EOF('reconnect');
        $node->sink( $self->sink );
    }
    return $node->auth_complete;
}

sub make_consumer {
    my $self          = shift;
    my $partitions    = shift;
    my $partition_id  = shift;
    my $broker_id     = $partitions->{$partition_id};
    my $my_name       = join q(:), $self->topic, 'consumer', $partition_id;
    my $log_name      = join q(:), $self->topic, 'partition', $partition_id;
    my $log           = join q(/), $broker_id, $log_name;
    my $consumer_name = undef;

    if ( $self->{group} ) {
        $consumer_name = join q(:), $my_name, $self->{group};
    }
    else {
        $consumer_name = join q(:), $self->name, $partition_id;
    }
    my $consumer = $Tachikoma::Nodes{$consumer_name};
    if ( not $consumer ) {
        $self->stderr("DEBUG: CREATE $consumer_name")
            if ( $self->debug_state );
        $consumer = Tachikoma::Nodes::Consumer->new;
        $consumer->name($consumer_name);
        $consumer->partition($log);
        $consumer->broker_id($broker_id);
        $consumer->partition_id($partition_id);
        if ( $self->{group} ) {
            $consumer->cache_type( $self->cache_type );
            if ( $self->{auto_offset} ) {
                my $offsetlog = join q(:), $log, $self->{group};
                $consumer->offsetlog($offsetlog);
            }
            $consumer->auto_commit( $self->auto_commit );
        }
        $consumer->default_offset( $self->default_offset );
        $consumer->async_interval($CONSUMER_INTERVAL);
        $consumer->hub_timeout( $self->hub_timeout );
        $consumer->max_unanswered( $self->max_unanswered );
        $consumer->timeout( $self->timeout );
        $consumer->startup_delay( $self->startup_delay );
        $consumer->sink( $self->sink );
        $consumer->edge( $self->edge );
        $consumer->owner( $self->owner );
        $consumer->debug_state( $self->debug_state );
        $consumer->register( 'READY', $self->name );
        $self->consumers->{$partition_id} = $consumer;
    }
    return;
}

sub owner {
    my $self = shift;
    if (@_) {
        $self->{owner} = shift;
        for my $partition_id ( keys %{ $self->consumers } ) {
            $self->consumers->{$partition_id}->owner( $self->{owner} );
        }
        $self->set_timer( $self->{startup_delay} * 1000 );
    }
    return $self->{owner};
}

sub edge {
    my $self = shift;
    if (@_) {
        $self->{edge} = shift;
        if ( $self->{edge} and $self->{edge}->can('new_cache') ) {
            $self->{edge}->new_cache;
        }
        for my $partition_id ( keys %{ $self->consumers } ) {
            $self->consumers->{$partition_id}->edge( $self->{edge} );
        }
        $self->set_timer( $self->{startup_delay} * 1000 );
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
        if ( not Tachikoma->shutting_down ) {
            $self->stderr( 'DEBUG: ' . $message->[PAYLOAD] )
                if ( $self->debug_state );
            $self->sink->fill($message) if ( $self->sink );
        }
    }
    $self->SUPER::remove_node;
    return;
}

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

sub flags {
    my $self = shift;
    if (@_) {
        $self->{flags} = shift;
    }
    return $self->{flags};
}

sub consumers {
    my $self = shift;
    if (@_) {
        $self->{consumers} = shift;
    }
    return $self->{consumers};
}

sub async_interval {
    my $self = shift;
    if (@_) {
        $self->{async_interval} = shift;
    }
    return $self->{async_interval};
}

sub check_interval {
    my $self = shift;
    if (@_) {
        $self->{check_interval} = shift;
    }
    return $self->{check_interval};
}

sub hub_timeout {
    my $self = shift;
    if (@_) {
        $self->{hub_timeout} = shift;
    }
    return $self->{hub_timeout};
}

sub cache_type {
    my $self = shift;
    if (@_) {
        $self->{cache_type} = shift;
    }
    return $self->{cache_type};
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

sub last_check {
    my $self = shift;
    if (@_) {
        $self->{last_check} = shift;
    }
    return $self->{last_check};
}

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

sub startup_delay {
    my $self = shift;
    if (@_) {
        $self->{startup_delay} = shift;
    }
    return $self->{startup_delay};
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
        or time - $self->{last_check} > $ASYNC_INTERVAL )
    {
        my $partitions = $self->broker->get_mapping;
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
    $consumer->default_offset( $self->default_offset );
    $consumer->poll_interval(undef);
    $consumer->hub_timeout( $self->hub_timeout );
    $self->consumers->{$partition_id} = $consumer;
    return $consumer;
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

sub broker {
    my $self = shift;
    if (@_) {
        $self->{broker} = shift;
    }
    if ( not defined $self->{broker} ) {
        my $broker = Tachikoma::Nodes::Topic->new( $self->topic );
        $broker->broker_ids( $self->broker_ids );
        $broker->hub_timeout( $self->hub_timeout );
        $self->{broker} = $broker;
    }
    return $self->{broker};
}

sub broker_ids {
    my (@args) = @_;
    return Tachikoma::Nodes::Topic::broker_ids(@args);
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

sub poll_interval {
    my $self = shift;
    if (@_) {
        $self->{poll_interval} = shift;
    }
    return $self->{poll_interval};
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
