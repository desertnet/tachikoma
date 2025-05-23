#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Broker
# ----------------------------------------------------------------------
#
#   - Manages Partitions and ConsumerGroups
#

package Tachikoma::Nodes::Broker;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::Partition;
use Tachikoma::Nodes::ConsumerGroup;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_PING TM_COMMAND TM_RESPONSE TM_INFO TM_REQUEST
    TM_STORABLE TM_ERROR TM_EOF
);
use Getopt::Long qw( GetOptionsFromString );
use POSIX        qw( strftime );
use parent       qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.165');

my $PATH                = '/tmp/topics';
my $REBALANCE_INTERVAL  = 0.2;            # timer during rebalance
my $HEARTBEAT_INTERVAL  = 1;              # ping timer
my $HEARTBEAT_TIMEOUT   = 900;            # keep this less than LCO timeout
my $HALT_TIME           = 10;             # wait to catch up
my $RESET_TIME          = 1;              # wait after tear down
my $DELETE_INTERVAL     = 60;             # delete old logs this often
my $CHECK_INTERVAL      = 1800;           # look for better balance this often
my $SAVE_INTERVAL       = 3600;           # re-save topic configs this often
my $REBALANCE_THRESHOLD = 0.90;           # have 90% of our share of leaders
my $ELECTION_SHORT      = 5;              # wait if everyone is online
my $ELECTION_LONG       = 120;            # wait if a broker is offline
my $ELECTION_TIMEOUT    = 300;   # how long to wait before starting over
my $LCO_SEND_INTERVAL   = 15;    # how often to send last commit offsets
my $LCO_TIMEOUT         = 3600;  # how long to wait before expiring cached LCO
my $LAST_LCO_SEND       = 0;     # time we last sent LCO
my $DEFAULT_CACHE_SIZE  = 1;     # config for cache partitions
my $NUM_CACHE_SEGMENTS  = 2;
my %C                   = ();

die 'ERROR: data will be lost if HEARTBEAT_TIMEOUT < LCO_SEND_INTERVAL'
    if ( $HEARTBEAT_TIMEOUT < $LCO_SEND_INTERVAL );

die 'ERROR: data will be lost if HEARTBEAT_TIMEOUT >= LCO_TIMEOUT'
    if ( $HEARTBEAT_TIMEOUT >= $LCO_TIMEOUT );

my %BROKER_COMMANDS = map { uc $_ => $_ } qw(
    empty_topics
    empty_groups
    purge_topics
    purge_groups
);

my %CONTROLLER_COMMANDS = map { uc $_ => $_ } qw(
    add_broker
    add_topic
    add_consumer_group
);

my %BROKER_REQUESTS = map { uc $_ => $_ } qw(
    get_controller
    get_leader
    get_topics
    get_partitions
);

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{broker_pools}     = {};
    $self->{broker_id}        = undef;
    $self->{path}             = undef;
    $self->{broker_stats}     = {};
    $self->{brokers}          = {};
    $self->{caches}           = {};
    $self->{consumer_groups}  = {};
    $self->{controller}       = q();
    $self->{default_settings} = {
        num_partitions     => 1,
        replication_factor => 2,
        num_segments       => 2,
        segment_size       => 128 * 1024 * 1024,
        max_lifespan       => 7 * 86400,
    };
    $self->{generation}          = 0;
    $self->{is_leader}           = undef;
    $self->{is_controller}       = undef;
    $self->{last_commit_offsets} = {};
    $self->{last_check}          = undef;
    $self->{last_delete}         = undef;
    $self->{last_save}           = undef;
    $self->{last_election}       = $Tachikoma::Now;
    $self->{last_halt}           = 0;
    $self->{last_reset}          = 0;
    $self->{mapping}             = {};
    $self->{partitions}          = {};
    $self->{stage}               = 'INIT';
    $self->{status}              = 'REBALANCING_PARTITIONS';
    $self->{starting_up}         = 1;
    $self->{topics}              = {};
    $self->{votes}               = {};
    $self->{waiting_for_halt}    = undef;
    $self->{waiting_for_reset}   = undef;
    $self->{waiting_for_map}     = undef;
    $self->{interpreter}         = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Broker broker <host>:<port> [ <path> ]
EOF
}

sub name {
    my ( $self, @args ) = @_;
    if (@args) {
        my ($name) = @args;
        die "ERROR: Broker node must be named 'broker'\n"
            if ( $name ne 'broker' );
    }
    return $self->SUPER::name(@args);
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        die "ERROR: Broker node requires a <host>:<port>\n"
            if ( not $arguments );
        my ( $broker_id, $path, $stick ) = split q( ), $arguments, 3;
        $path //= $PATH;
        $self->{arguments}   = $arguments;
        $self->{broker_id}   = $broker_id;
        $self->{path}        = $path;
        $self->{stick}       = $stick;
        $self->{last_check}  = $Tachikoma::Now;
        $self->{last_delete} = $Tachikoma::Now;
        $self->{last_save}   = $Tachikoma::Now;
        $self->load_topics if ( -e $self->{path} );
    }
    return $self->{arguments};
}

sub load_topics {
    my $self       = shift;
    my $broker_id  = $self->{broker_id};
    my $path       = $self->{path};
    my $broker_lco = {};
    opendir my $dh, $path or die "couldn't opendir $path: $!";
    my @topics = grep m{^[^.]}, readdir $dh;
    closedir $dh or die "couldn't closedir $path: $!";

    for my $topic_name (@topics) {
        my $partitions_path = "$path/$topic_name/partition";
        my $cache_path      = "$path/$topic_name/cache";
        if ( -d $partitions_path ) {
            opendir $dh, $partitions_path
                or die "couldn't opendir $partitions_path: $!";
            my @partitions = grep m{^[^.]}, readdir $dh;
            closedir $dh or die "couldn't closedir $partitions_path: $!";
            for my $i (@partitions) {
                my $log_name = "$topic_name:partition:$i";
                my $filename = "$partitions_path/$i";
                $broker_lco->{$log_name} = $self->determine_lco($filename);
            }
            if ( opendir $dh, $cache_path ) {
                my @caches = grep m{^[^.]}, readdir $dh;
                closedir $dh or die "couldn't closedir $cache_path: $!";
                for my $group_name (@caches) {
                    for my $i (@partitions) {
                        my $log_name = "$topic_name:partition:$i:$group_name";
                        my $filename = "$cache_path/$group_name/$i";
                        $broker_lco->{$log_name} =
                            $self->determine_lco($filename);
                    }
                }
            }
        }
    }
    $self->{last_commit_offsets}->{$broker_id} = $broker_lco;
    return;
}

sub load_configs {
    my $self = shift;
    my $path = $self->{path};
    opendir my $dh, $path or die "couldn't opendir $path: $!";
    my @topics = grep m{^[^.]}, readdir $dh;
    closedir $dh or die "couldn't closedir $path: $!";

    for my $topic_name (@topics) {
        $self->load_topic_config($topic_name);
    }
    return;
}

sub load_topic_config {
    my $self        = shift;
    my $topic_name  = shift;
    my $path        = $self->{path};
    my $config_file = "$path/$topic_name/.config";
    if ( -e $config_file ) {
        open my $fh, '<', $config_file
            or die "couldn't open $config_file: $!";
        my $args = <$fh>;
        close $fh or die "couldn't close $config_file: $!";
        chomp $args;
        $self->add_topic($args);
    }
    else {
        $self->stderr("WARNING: couldn't find $config_file");
    }
    return;
}

sub determine_lco {
    my $self        = shift;
    my $filename    = shift;
    my $offsets_dir = join q(/), $filename, 'offsets';
    my $stats       = {
        lco       => undef,
        filename  => $filename,
        is_active => $Tachikoma::Now,
    };
    if ( -d $offsets_dir ) {
        opendir my $dh, $offsets_dir
            or die "couldn't opendir $offsets_dir: $!";
        my @offsets = sort { $a <=> $b } grep m{^[^.]}, readdir $dh;
        closedir $dh or die "couldn't closedir $offsets_dir: $!";
        my $last_commit_offset = pop @offsets // 0;
        $stats->{lco} = $last_commit_offset;
    }
    else {
        $self->stderr("WARNING: couldn't find $offsets_dir");
    }
    return $stats;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_COMMAND or $message->[TYPE] & TM_EOF ) {
        return $self->interpreter->fill($message);
    }
    elsif ( $message->[TYPE] & TM_PING ) {
        $self->receive_heartbeat($message);
    }
    elsif ( $message->[TYPE] & TM_STORABLE ) {
        my $payload = $message->payload;
        if ($payload) {
            if (    $payload->{type}
                and $payload->{type} eq 'LAST_COMMIT_OFFSETS' )
            {
                $self->update_lco($message);
            }
            elsif ( $self->validate( $message, 'RESET' ) ) {
                $self->update_mapping($message);
            }
        }
        else {
            $self->drop_message( $message, 'empty payload' );
        }
    }
    elsif ( $self->{is_controller}
        and $message->[ID]
        and $message->[ID] < $self->{generation} )
    {
        $self->stderr(
            'WARNING: stale message: ID ', $message->id,
            q( < ),                        $self->generation,
            q( - ),                        $message->type_as_string,
            ' from: ',                     $message->from
        );
    }
    elsif ( $message->[TYPE] & TM_INFO ) {
        $self->process_info($message);
    }
    elsif ( $message->[TYPE] & TM_REQUEST ) {
        $self->process_request($message);
    }
    elsif ( $message->[TYPE] & TM_RESPONSE ) {
        $self->handle_response($message);
    }
    elsif ( $message->[TYPE] & TM_ERROR ) {
        $self->handle_error($message);
    }
    else {
        $self->stderr( 'ERROR: unexpected message type: ',
            $message->type_as_string );
    }
    return;
}

sub fire {
    my $self = shift;
    if ( $self->{stage} eq 'INIT' or $self->{stage} eq 'COMPLETE' ) {
        $self->determine_controller;
    }
    $self->send_heartbeat;
    my $total              = keys %{ $self->{broker_pools} };
    my $online             = $self->check_heartbeats;
    my $replication_factor = $self->{default_settings}->{replication_factor};
    if ( not $total or $online <= $total / 2 ) {
        $self->print_less_often('ERROR: not enough servers for quorum')
            if ( not $self->{starting_up} );
        $self->rebalance_partitions('inform_brokers');
    }
    elsif ( $replication_factor > $total ) {
        $self->print_less_often('ERROR: not enough servers for replication');
        $self->rebalance_partitions('inform_brokers');
    }
    elsif ( $self->{status} eq 'REBALANCING_PARTITIONS' ) {
        $self->process_rebalance( $total, $online );
    }
    elsif ( $Tachikoma::Right_Now - $LAST_LCO_SEND > $LCO_SEND_INTERVAL ) {
        $self->send_lco;
        $LAST_LCO_SEND = $Tachikoma::Right_Now;
    }
    if (    $self->{is_controller}
        and defined $self->{last_check}
        and $Tachikoma::Now - $self->{last_check} > $CHECK_INTERVAL )
    {
        $self->check_mapping;
    }
    elsif ( $Tachikoma::Now - $self->{last_delete} > $DELETE_INTERVAL ) {
        $self->process_delete;
    }
    elsif ( $Tachikoma::Now - $self->{last_save} > $SAVE_INTERVAL ) {
        $self->save_topic_states;
    }
    if ( $self->{stage} eq 'COMPLETE' ) {
        $self->set_timer( $HEARTBEAT_INTERVAL * 1000 )
            if ( $self->{timer_interval} != $HEARTBEAT_INTERVAL * 1000 );
    }
    elsif ( $self->{status} eq 'REBALANCING_PARTITIONS' ) {
        $self->set_timer( $REBALANCE_INTERVAL * 1000 )
            if ( not $self->{timer_is_active}
            or $self->{timer_interval} != $REBALANCE_INTERVAL * 1000 );
    }
    return;
}

sub process_info {
    my $self    = shift;
    my $message = shift;
    if (    $self->{stick}
        and $message->[PAYLOAD] ne "REBALANCE_PARTITIONS\n" )
    {
        sleep rand $self->{stick};
    }
    if ( $message->[PAYLOAD] eq "REBALANCE_PARTITIONS\n" ) {
        $self->rebalance_partitions;
    }
    elsif ( $message->[PAYLOAD] eq "HALT\n" ) {
        return if ( not $self->validate( $message, 'INIT' ) );
        $self->generation( $message->[ID] );
        $self->stage('HALT');
        $self->send_lco( $message->[FROM] );
        $self->send_response( $message, "HALT_COMPLETE\n" );
    }
    elsif ( $message->[PAYLOAD] eq "RESET\n" ) {
        return if ( not $self->validate( $message, 'HALT' ) );
        $self->reset_partitions;
        $self->stage('RESET');
        $self->send_response( $message, "RESET_COMPLETE\n" );
    }
    elsif ( $message->[PAYLOAD] eq "ALL_CLEAR\n" ) {
        return if ( not $self->validate( $message, 'FINISH' ) );
        $self->status('ACTIVE');
        $self->stage('COMPLETE');
    }
    elsif ( $message->[STREAM] eq 'RECONNECT' ) {
        $self->offline( $message->[FROM] );
    }
    else {
        $self->process_command($message);
    }
    return;
}

sub process_command {
    my $self    = shift;
    my $message = shift;
    my $line    = $message->[PAYLOAD];
    chomp $line;
    my ( $cmd, $args ) = split q( ), $line, 2;
    if ( $self->{status} eq 'REBALANCING_PARTITIONS' ) {
        $self->send_error( $message, "REBALANCING_PARTITIONS\n" );
    }
    elsif ( $BROKER_COMMANDS{$cmd} ) {
        my $method = $BROKER_COMMANDS{$cmd};
        $self->$method( $args, $message );
    }
    elsif ( $self->{status} ne 'CONTROLLER' ) {
        $self->send_error( $message, "NOT_CONTROLLER\n" );
    }
    elsif ( $CONTROLLER_COMMANDS{$cmd} ) {
        my $method = $CONTROLLER_COMMANDS{$cmd};
        $self->$method( $args, $message );
    }
    else {
        $self->stderr( 'ERROR: unrecognized command: ', $message->[PAYLOAD] );
    }
    return;
}

sub process_request {
    my $self    = shift;
    my $message = shift;
    my $line    = $message->[PAYLOAD];
    chomp $line;
    my ( $cmd, $args ) = split q( ), $line, 2;
    if ( $self->{status} eq 'REBALANCING_PARTITIONS' ) {
        $self->send_error( $message, "REBALANCING_PARTITIONS\n" );
    }
    elsif ( $BROKER_REQUESTS{$cmd} ) {
        my $method = $BROKER_REQUESTS{$cmd};
        $self->$method( $args, $message );
    }
    else {
        $self->stderr( 'ERROR: unrecognized request: ', $message->[PAYLOAD] );
    }
    return;
}

sub validate {
    my $self    = shift;
    my $message = shift;
    my $stage   = shift;
    my $rv      = undef;
    if ( $message->[STREAM] ne $self->{controller} ) {
        $self->stderr("$stage: ERROR: message is not from controller");
    }
    elsif ( $self->{stage} ne $stage ) {
        $self->stderr( $self->{stage}, ": ERROR: unexpected $stage message" );
    }
    elsif ( $stage ne 'INIT' and $message->[ID] < $self->{generation} ) {
        $self->stderr( $self->{stage}, 'ERROR: stale message: ID ',
            $message->id, q( < ), $self->generation );
    }
    else {
        $rv = 1;
    }
    return $rv;
}

sub determine_controller {
    my $self       = shift;
    my $leader     = undef;
    my $controller = undef;
    my $count      = 0;
    my %leaders    = ();
    for my $broker_id ( sort keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        if ( not $leaders{ $broker->{pool} } ) {
            $leaders{ $broker->{pool} } = 1;
            $broker->{is_leader}        = 1;
            $self->{is_leader}          = 1
                if ( $broker_id eq $self->{broker_id} );
            $controller //= $broker_id
                if ( $self->{broker_pools}->{ $broker->{pool} } );
        }
        else {
            $broker->{is_leader} = undef;
            $self->{is_leader}   = undef
                if ( $broker_id eq $self->{broker_id} );
        }
    }
    $self->{controller} = $controller // q();
    return $controller;
}

sub send_heartbeat {
    my $self = shift;
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        next
            if ( $Tachikoma::Right_Now - $broker->{last_ping}
            < $HEARTBEAT_INTERVAL );
        if ( $broker_id eq $self->{broker_id} ) {
            $broker->{last_heartbeat} = $Tachikoma::Right_Now;
            $broker->{last_ping}      = $Tachikoma::Right_Now;
            $broker->{is_online}      = $Tachikoma::Now;
            next;
        }
        $broker->{last_ping} = $Tachikoma::Right_Now;
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_PING;
        $message->[FROM]    = $self->{name};
        $message->[TO]      = $broker->{path};
        $message->[ID]      = $self->{generation};
        $message->[STREAM]  = $self->{broker_id};
        $message->[PAYLOAD] = $self->{controller};
        $self->{sink}->fill($message);
    }
    return;
}

sub receive_heartbeat {
    my $self    = shift;
    my $message = shift;
    my $broker  = $self->{brokers}->{ $message->[STREAM] };
    return $self->stderr( 'ERROR: bad ping: ', $message->[STREAM] )
        if ( not $broker );
    return $self->stderr( 'ERROR: stale ping: ', $message->[STREAM] )
        if ( $Tachikoma::Now - $message->[TIMESTAMP] > $HEARTBEAT_TIMEOUT );
    $broker->{last_heartbeat} = $Tachikoma::Right_Now;
    return
        if ($self->{stage} ne 'INIT'
        and $self->{stage} ne 'COMPLETE' );
    if ( $message->[PAYLOAD] eq $self->{broker_id} ) {
        $self->{votes}->{ $message->[STREAM] } = 1;
    }
    else {
        delete $self->{votes}->{ $message->[STREAM] };
    }
    my $now         = time;
    my $back_online = not $broker->{is_online};
    $broker->{is_online} = $Tachikoma::Now;
    my $total  = 0;
    my $online = 0;
    for my $id ( keys %{ $self->{brokers} } ) {
        my $other = $self->{brokers}->{$id};
        if ( $other->{pool} eq $broker->{pool} ) {
            $total++;
            $online++
                if ( $now - $other->{is_online} < $HEARTBEAT_TIMEOUT );
        }
    }
    if ( $total and $online == $total ) {
        $self->{broker_pools}->{ $broker->{pool} } = $Tachikoma::Now;
        if ($back_online) {
            $self->stderr( $broker->{pool} . ' back online' );
            $self->rebalance_partitions('inform_brokers');
        }
    }
    return;
}

sub send_lco {
    my ( $self, $from ) = @_;
    my $broker_id = $self->{broker_id};
    $self->{last_commit_offsets}->{$broker_id} //= {};
    $self->{broker_stats}->{$broker_id}        //= {};
    my $broker_lco   = $self->{last_commit_offsets}->{$broker_id};
    my $broker_stats = $self->{broker_stats}->{$broker_id};
    for my $name ( keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        next if ( not $node->isa('Tachikoma::Nodes::Partition') );
        $broker_lco->{$name} = {
            is_active => $Tachikoma::Now,
            filename  => $node->{filename},
            lco       => $node->{last_commit_offset},
        };
        $broker_stats->{$name} = {
            offset => $node->{offset},
            isr    => scalar keys %{ $node->{in_sync_replicas} },
        };
    }
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_STORABLE;
    $message->[FROM]    = $self->{name};
    $message->[ID]      = $self->{generation};
    $message->[STREAM]  = $self->{broker_id};
    $message->[PAYLOAD] = {
        type       => 'LAST_COMMIT_OFFSETS',
        partitions => $broker_lco,
        stats      => $broker_stats,
        status     => $self->{status},
    };
    if ($from) {
        $message->[TO] = $from;
        $self->{sink}->fill($message);
    }
    else {
        for my $id ( keys %{ $self->{brokers} } ) {
            my $broker = $self->{brokers}->{$id};
            next
                if ( $id eq $broker_id
                or not $broker->{is_leader}
                or not $broker->{is_online} );
            $message->[TO] = $broker->{path};
            $self->{sink}->fill($message);
        }
    }
    return;
}

sub update_lco {
    my $self    = shift;
    my $message = shift;
    my $payload = $message->payload;
    $self->{last_commit_offsets}->{ $message->[STREAM] } =
        $payload->{partitions};
    $self->{broker_stats}->{ $message->[STREAM] } =
        $payload->{stats};
    my $local_status  = $self->{status};
    my $remote_status = $payload->{status};
    $local_status  = 'ACTIVE' if ( $local_status eq 'CONTROLLER' );
    $remote_status = 'ACTIVE' if ( $remote_status eq 'CONTROLLER' );

    if ( $local_status ne $remote_status ) {
        $self->stderr(
            'WARNING: local status ', $self->{status},
            ' != remote status ',     $payload->{status}
        );
        $self->rebalance_partitions('inform_brokers');
    }
    return;
}

sub check_heartbeats {
    my $self          = shift;
    my $total_pools   = keys %{ $self->{broker_pools} };
    my %online_pools  = ();
    my %offline_pools = ();
    my $online        = 0;
    my $votes         = 0;
    $votes++ if ( $self->{controller} eq $self->{broker_id} );
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        $self->offline($broker_id)
            if ( $Tachikoma::Right_Now - $broker->{last_heartbeat}
            > $HEARTBEAT_TIMEOUT );
        $offline_pools{ $broker->{pool} } = 1
            if ( not $broker->{is_online} );
    }
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        next if ( $offline_pools{ $broker->{pool} } );
        $online_pools{ $broker->{pool} } = 1;
        $online++;
        $votes++ if ( $self->{votes}->{$broker_id} );
    }
    if ( keys %online_pools > $total_pools / 2 and $online == $votes ) {
        $self->{is_controller} = 1;
    }
    else {
        $self->{is_controller} = undef;
    }
    return scalar keys %online_pools;
}

sub offline {
    my $self      = shift;
    my $broker_id = shift;
    my $broker    = $self->{brokers}->{$broker_id};
    return
        if ( not $broker
        or not $broker->{is_online}
        or $self->{starting_up} );
    $broker->{is_online} = 0;
    $self->{broker_pools}->{ $broker->{pool} } = 0;
    delete $self->{waiting_for_halt}->{$broker_id};
    delete $self->{waiting_for_reset}->{$broker_id};
    delete $self->{waiting_for_map}->{$broker_id};

    $self->stderr( $broker->{pool} . ' has gone offline' );
    $self->rebalance_partitions('inform_brokers');
    return;
}

sub rebalance_partitions {
    my $self           = shift;
    my $inform_brokers = shift;
    $self->halt_partitions if ( $self->{stage} ne 'INIT' );
    $self->{is_leader}         = undef;
    $self->{is_controller}     = undef;
    $self->{votes}             = {};
    $self->{status}            = 'REBALANCING_PARTITIONS';
    $self->{stage}             = 'INIT';
    $self->{waiting_for_halt}  = {};
    $self->{waiting_for_reset} = {};
    $self->{waiting_for_map}   = {};
    $self->{last_election}     = $Tachikoma::Now;
    $self->inform_brokers("REBALANCE_PARTITIONS\n") if ($inform_brokers);
    $self->set_timer( $REBALANCE_INTERVAL * 1000 )
        if ( not $self->{starting_up} );
    return;
}

sub process_rebalance {
    my $self   = shift;
    my $total  = shift;
    my $online = shift;
    my $span   = $Tachikoma::Now - $self->{last_election};
    my $wait   = $total == $online ? $ELECTION_SHORT : $ELECTION_LONG;
    if ( $span > $wait ) {
        $self->{starting_up} = undef;
        if ( $self->{is_controller} ) {
            $self->send_halt          if ( $self->{stage} eq 'INIT' );
            $self->wait_for_halt      if ( $self->{stage} eq 'HALT' );
            $self->send_reset         if ( $self->{stage} eq 'RESET' );
            $self->wait_for_reset     if ( $self->{stage} eq 'PAUSE' );
            $self->determine_mapping  if ( $self->{stage} eq 'MAP' );
            $self->send_mapping       if ( $self->{stage} eq 'SEND' );
            $self->apply_mapping      if ( $self->{stage} eq 'APPLY' );
            $self->wait_for_responses if ( $self->{stage} eq 'WAIT' );
            $self->send_all_clear     if ( $self->{stage} eq 'FINISH' );
        }
    }
    $self->{last_check}  = $Tachikoma::Now;
    $self->{last_delete} = $Tachikoma::Now;
    $self->{last_save}   = $Tachikoma::Now;
    return;
}

sub send_halt {
    my $self = shift;
    $self->{waiting_for_halt} = {};
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        next
            if ( $broker_id eq $self->{broker_id}
            or not $broker->{is_online} );
        $self->{waiting_for_halt}->{$broker_id} = 1
            if ( $self->{broker_pools}->{ $broker->{pool} } );
    }
    $self->{generation}++;
    $self->inform_brokers("REBALANCE_PARTITIONS\n");
    $self->inform_brokers("HALT\n");
    $self->{stage} = 'HALT';
    return;
}

sub halt_partitions {
    my $self           = shift;
    my $topics         = $self->{topics};
    my $groups         = $self->{consumer_groups};
    my $broker_mapping = $self->{mapping}->{ $self->{broker_id} };
    for my $topic_name ( keys %{$broker_mapping} ) {
        my $topic  = $topics->{$topic_name};
        my $caches = $self->{caches}->{$topic_name};
        for my $log_name ( keys %{ $broker_mapping->{$topic_name} } ) {
            $Tachikoma::Nodes{$log_name}->halt
                if ( $Tachikoma::Nodes{$log_name} );
            for my $group_name ( keys %{$caches} ) {
                my $group_log = "$log_name:$group_name";
                next if ( not $Tachikoma::Nodes{$group_log} );
                $Tachikoma::Nodes{$group_log}->halt
                    if ( $groups->{$group_name} );
            }
        }
    }
    return;
}

sub wait_for_halt {
    my $self = shift;
    return $self->rebalance_partitions('inform_brokers')
        if ( $Tachikoma::Now - $self->{last_election} > $ELECTION_TIMEOUT );
    if ( not keys %{ $self->{waiting_for_halt} }
        and $Tachikoma::Right_Now - $self->{last_halt} > $HALT_TIME )
    {
        $self->stderr( $self->{stage} . ' HALT_COMPLETE' );
        $self->{stage} = 'RESET';
    }
    return;
}

sub send_reset {
    my $self = shift;
    $self->{waiting_for_reset} = {};
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        next
            if ( $broker_id eq $self->{broker_id}
            or not $broker->{is_online} );
        $self->{waiting_for_reset}->{$broker_id} = 1
            if ( $self->{broker_pools}->{ $broker->{pool} } );
    }
    $self->inform_brokers("RESET\n");
    $self->reset_partitions;
    $self->{stage} = 'PAUSE';
    return;
}

sub reset_partitions {
    my $self   = shift;
    my $topics = $self->{topics};
    my $groups = $self->{consumer_groups};
    my $path   = $self->{path};
    for my $name ( keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        $node->remove_node
            if ( $node->isa('Tachikoma::Nodes::ConsumerGroup')
            and not $groups->{$name} );
    }
    my $broker_mapping = $self->{mapping}->{ $self->{broker_id} };
    my @purge          = ();
    for my $topic_name ( keys %{$broker_mapping} ) {
        my $topic  = $topics->{$topic_name};
        my $caches = $self->{caches}->{$topic_name};
        for my $log_name ( keys %{ $broker_mapping->{$topic_name} } ) {
            if ( $Tachikoma::Nodes{$log_name} ) {
                if ($topic) {
                    $Tachikoma::Nodes{$log_name}->remove_node;
                }
                else {
                    $self->stderr("PURGE: $log_name");
                    $Tachikoma::Nodes{$log_name}->purge_tree;
                    $Tachikoma::Nodes{$log_name}->remove_node;
                    push @purge, $log_name;
                }
            }
            for my $group_name ( keys %{$caches} ) {
                my $group_log = "$log_name:$group_name";
                next if ( not $Tachikoma::Nodes{$group_log} );
                if ( $groups->{$group_name} ) {
                    $Tachikoma::Nodes{$group_log}->remove_node;
                }
                else {
                    $self->stderr("PURGE: $group_log");
                    $Tachikoma::Nodes{$group_log}->purge_tree;
                    $Tachikoma::Nodes{$group_log}->remove_node;
                    ## no critic (RequireCheckedSyscalls)
                    rmdir "$path/$topic_name/cache";
                    push @purge, $group_log;
                }
            }
        }
        if ( not $topic ) {
            delete $broker_mapping->{$topic_name};
            delete $self->{caches}->{$topic_name};
            $self->purge_broker_config($topic_name);
        }
    }
    for my $log_name (@purge) {
        for my $broker_id ( keys %{ $self->{last_commit_offsets} } ) {
            my $broker_lco = $self->{last_commit_offsets}->{$broker_id};
            delete $broker_lco->{$log_name};
        }
        for my $broker_id ( keys %{ $self->{broker_stats} } ) {
            my $broker_stats = $self->{broker_stats}->{$broker_id};
            delete $broker_stats->{$log_name};
        }
    }
    return;
}

sub wait_for_reset {
    my $self = shift;
    return $self->rebalance_partitions('inform_brokers')
        if ( $Tachikoma::Now - $self->{last_election} > $ELECTION_TIMEOUT );
    if ( not keys %{ $self->{waiting_for_reset} }
        and $Tachikoma::Right_Now - $self->{last_reset} > $RESET_TIME )
    {
        $self->stderr( $self->{stage} . ' RESET_COMPLETE' );
        $self->{stage} = 'MAP';
    }
    return;
}

sub determine_mapping {
    my $self                = shift;
    my $mapping             = {};
    my $leaders_by_pool     = {};
    my $leaders_by_broker   = {};
    my $followers_by_pool   = {};
    my $followers_by_broker = {};
    my $online_brokers      = {};
    $self->{partitions} = {};

    # partitions assigned to each pool
    for my $id ( keys %{ $self->{brokers} } ) {
        my $pool = $self->{brokers}->{$id}->{pool};
        next if ( not $self->{broker_pools}->{$pool} );
        $leaders_by_pool->{$pool} = 0;
        $leaders_by_broker->{$pool} //= {};
        $leaders_by_broker->{$pool}->{$id} = 0;
        $followers_by_pool->{$pool} = 0;
        $followers_by_broker->{$pool} //= {};
        $followers_by_broker->{$pool}->{$id} = 0;
        $online_brokers->{$id} = 1;
    }

    for my $topic_name ( sort keys %{ $self->{topics} } ) {
        my $topic      = $self->{topics}->{$topic_name};
        my @partitions = ();
        my $i          = 0;
        while ( $i < $topic->{num_partitions} ) {
            my $log_name = "$topic_name:partition:$i";
            my $leader   = $self->determine_leader(
                {   topic_name     => $topic_name,
                    log            => $log_name,
                    mapping        => $mapping,
                    by_pool        => $leaders_by_pool,
                    by_broker      => $leaders_by_broker,
                    online_brokers => $online_brokers,
                }
            ) or next;
            $self->determine_followers(
                {   topic_name     => $topic_name,
                    log            => $log_name,
                    leader         => $leader,
                    count          => $topic->{replication_factor} - 1,
                    mapping        => $mapping,
                    by_pool        => $followers_by_pool,
                    by_broker      => $followers_by_broker,
                    online_brokers => $online_brokers,
                }
            ) if ( $topic->{replication_factor} > 1 );
            $partitions[$i] = $leader;
            $i++;
        }
        $self->{partitions}->{$topic_name} = \@partitions;
    }
    $self->{mapping} = $mapping;
    my @brokers = sort keys %{$online_brokers};
    my $i       = 0;
    for my $group_name ( sort keys %{ $self->{consumer_groups} } ) {
        my $group = $self->{consumer_groups}->{$group_name};
        $group->{broker_id} = $brokers[ $i % @brokers ];
        $i++;
    }
    $self->{stage} = 'SEND'
        if ( keys %{ $self->{mapping} } or not keys %{ $self->{topics} } );

    $self->stderr('DEBUG: CONTROLLER rebalanced partitions')
        if ( $self->{debug_state} );
    return;
}

sub check_mapping {
    my $self   = shift;
    my $total  = 0;
    my %counts = ();
    for my $broker_id ( keys %{ $self->{mapping} } ) {
        my $broker_mapping = $self->{mapping}->{$broker_id};
        $counts{$broker_id} = 0;
        for my $topic_name ( keys %{$broker_mapping} ) {
            for my $log_name ( keys %{ $broker_mapping->{$topic_name} } ) {
                next if ( not $broker_mapping->{$topic_name}->{$log_name} );
                $counts{$broker_id}++;
                $total++;
            }
        }
    }
    if ($total) {
        my $ratio   = 0;
        my $ideal   = undef;
        my $current = undef;
        $ideal = int( $total / scalar keys %counts ) if ( keys %counts );
        for my $broker_id ( keys %{ $self->{mapping} } ) {
            $current //= $counts{$broker_id};
            $current = $counts{$broker_id}
                if ( $counts{$broker_id} < $current );
        }
        $ratio = $current / $ideal if ($ideal);
        $ratio = 1.0               if ( $ideal - $current <= 1 );
        $self->stderr( sprintf 'CHECK_MAPPING: %.2f%% optimal',
            $ratio * 100 );
        if ( $ratio < $REBALANCE_THRESHOLD ) {
            $self->rebalance_partitions('inform_brokers');
            $self->{last_check} = $Tachikoma::Now;
        }
        else {
            # done until something changes
            $self->{last_check} = undef;
        }
    }
    else {
        $self->{last_check} = $Tachikoma::Now;
    }
    return;
}

sub determine_leader {
    my $self       = shift;
    my $query      = shift;
    my $topic_name = $query->{topic_name};
    my $log_name   = $query->{log};
    my $brokers    = $self->{brokers};
    my $leader     = undef;
    $query->{want_replica} = 0;
    $self->determine_in_sync_replicas($query);

    $self->stderr(
        "DEBUG: CANDIDATE LEADERS for $log_name - ",
        map join( q(), $_, ' => ', $query->{candidates}->{$_}, ', ' ),
        sort keys %{ $query->{candidates} }
    ) if ( $self->{debug_state} and $self->{debug_state} >= 2 );
    $leader = $self->best_broker($query)
        if ( keys %{ $query->{candidates} } );

    $self->stderr("DEBUG: BEST LEADER for $log_name - $leader")
        if ( $self->{debug_state} and $self->{debug_state} >= 2 and $leader );

    # assign a new leader
    if ( not $leader and not keys %{ $query->{candidates} } ) {
        $leader = $self->next_broker($query);
        $self->stderr( "NEW LEADER for $log_name - ", $leader ) if ($leader);
    }
    if ($leader) {
        my $leader_pool = $brokers->{$leader}->{pool};
        $query->{by_pool}->{$leader_pool}++;
        $query->{by_broker}->{$leader_pool}->{$leader}++;
    }
    return $leader;
}

sub determine_in_sync_replicas {
    my $self               = shift;
    my $query              = shift;
    my $log_name           = $query->{log};
    my $brokers            = $self->{brokers};
    my %in_sync_replicas   = ();
    my $last_commit_offset = 0;
    for my $broker_id ( keys %{ $query->{online_brokers} } ) {
        my $broker_lco = $self->{last_commit_offsets}->{$broker_id};
        my $log        = $broker_lco->{$log_name} or next;
        next
            if ( not defined $log->{lco}
            or $log->{lco} <= $last_commit_offset );
        $last_commit_offset = $log->{lco};
    }
    for my $broker_id ( keys %{ $query->{online_brokers} } ) {
        my $broker_lco = $self->{last_commit_offsets}->{$broker_id};
        my $log        = $broker_lco->{$log_name} or next;
        next
            if ( not defined $log->{lco}
            or $log->{lco} < $last_commit_offset );
        $in_sync_replicas{ $brokers->{$broker_id}->{pool} } = $log->{lco};
    }
    $query->{candidates} = \%in_sync_replicas;
    return;
}

sub determine_followers {
    my $self        = shift;
    my $query       = shift;
    my $topic_name  = $query->{topic_name};
    my $log_name    = $query->{log};
    my $leader      = $query->{leader};
    my $count       = $query->{count};
    my $brokers     = $self->{brokers};
    my %skip        = ();
    my $leader_pool = $leader ? $brokers->{$leader}->{pool} : undef;
    my %candidates  = ();
    my @followers   = ();
    die "ERROR: no replication factor specified\n" if ( not defined $count );
    $skip{$leader_pool} = 1                        if ($leader_pool);

    # find candidate pools
    for my $broker_id ( keys %{ $query->{online_brokers} } ) {
        my $broker_pool = $brokers->{$broker_id}->{pool};
        next if ( $skip{$broker_pool} );
        $candidates{$broker_pool} = 1;
    }

    # find best followers
    $query->{skip}         = \%skip;
    $query->{want_replica} = 1;
    while ( @followers < $count ) {
        $query->{candidates} = \%candidates;
        my $follower = $self->best_broker($query);
        if ( not $follower ) {
            $follower = $self->next_broker($query);
            last if ( not $follower );
        }
        push @followers, $follower;
        my $follower_pool = $brokers->{$follower}->{pool};
        $query->{by_pool}->{$follower_pool}++;
        $query->{by_broker}->{$follower_pool}->{$follower}++;
    }
    return \@followers;
}

sub next_broker {
    my $self      = shift;
    my $query     = shift;
    my $broker_id = undef;

    # find the broker with the fewest partitions
    $query->{candidates} = $self->{broker_pools};
    $broker_id = $self->best_broker($query);
    return $broker_id;
}

sub best_broker {
    my $self         = shift;
    my $query        = shift;
    my $candidates   = $query->{candidates};
    my $want_replica = $query->{want_replica};
    my $skip         = $query->{skip};
    my $mapping      = $query->{mapping};
    my $by_pool      = $query->{by_pool};
    my $min          = undef;
    my $best_pool    = undef;
    my $broker_id    = undef;

    # pick the pool with the fewest partitions
    for my $pool ( sort keys %{$by_pool} ) {
        next
            if ( not exists $candidates->{$pool}
            or ( $skip and $skip->{$pool} )
            or ( defined $min and $by_pool->{$pool} > $min ) );
        $min       = $by_pool->{$pool};
        $best_pool = $pool;
    }

    # pick the broker with the fewest partitions
    if ($best_pool) {
        my $by_broker = $query->{by_broker}->{$best_pool};
        $broker_id = (
            sort {
                ( $by_broker->{$a} != $by_broker->{$b} )
                    ? $by_broker->{$a} <=> $by_broker->{$b}
                    : $a cmp $b
                }
                keys %{$by_broker}
        )[0];
        if ($broker_id) {
            my $topic_name = $query->{topic_name};
            my $log_name   = $query->{log};
            $mapping->{$broker_id}->{$topic_name}->{$log_name} =
                $query->{leader};
            if ($skip) {
                $skip->{ $self->{brokers}->{$broker_id}->{pool} } = 1;
            }
        }
    }
    return $broker_id;
}

sub send_mapping {
    my $self = shift;
    $self->{waiting_for_map} = {};

    $self->stderr('DEBUG: CONTROLLER sending mappings')
        if ( $self->{debug_state} );
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        next
            if ( $broker_id eq $self->{broker_id}
            or not $broker->{is_online} );
        my $message = Tachikoma::Message->new;
        $message->[TYPE]   = TM_STORABLE;
        $message->[FROM]   = $self->{name};
        $message->[TO]     = $broker->{path};
        $message->[ID]     = $self->{generation};
        $message->[STREAM] = $self->{broker_id};
        $message->payload(
            {   topics          => $self->{topics},
                mapping         => $self->{mapping},
                partitions      => $self->{partitions},
                consumer_groups => $self->{consumer_groups},
            }
        );
        $self->{sink}->fill($message);
        $self->{waiting_for_map}->{$broker_id} = $Tachikoma::Right_Now;
    }
    $self->{stage} = 'APPLY';
    return;
}

sub update_mapping {
    my $self    = shift;
    my $message = shift;
    my $stream  = $message->[STREAM];
    my $update  = $message->payload;
    $self->{topics}          = $update->{topics};
    $self->{mapping}         = $update->{mapping};
    $self->{partitions}      = $update->{partitions};
    $self->{consumer_groups} = $update->{consumer_groups};
    $self->apply_mapping;
    $self->send_response( $message, "MAP_COMPLETE\n" );
    return;
}

sub apply_mapping {
    my $self   = shift;
    my $path   = $self->{path};
    my $topics = $self->{topics};
    my $groups = $self->{consumer_groups};
    $self->{caches} = {};
    for my $group_name ( keys %{$groups} ) {
        my $group = $groups->{$group_name};
        my $node  = $Tachikoma::Nodes{$group_name};
        if ( not $node ) {
            $node = Tachikoma::Nodes::ConsumerGroup->new;
            $node->name($group_name);
        }
        $node->arguments(q());
        $node->debug_state( $self->debug_state );
        $node->sink( $self->sink );
        for my $topic_name ( keys %{ $group->{topics} } ) {
            next if ( not $group->{topics}->{$topic_name} );
            $self->{caches}->{$topic_name} //= {};
            $self->{caches}->{$topic_name}->{$group_name} =
                $group->{topics}->{$topic_name};
        }
        $node->topics( { map { $_ => 1 } keys %{ $group->{topics} } } );
        $node->mapping(undef);
        if ( $group->{broker_id} eq $self->{broker_id} ) {
            $node->is_leader(1);
        }
        else {
            $node->is_leader(undef);
        }
    }
    my $broker_mapping = $self->{mapping}->{ $self->{broker_id} };
    for my $topic_name ( keys %{$broker_mapping} ) {
        my $topic  = $topics->{$topic_name};
        my $caches = $self->{caches}->{$topic_name};
        for my $log_name ( keys %{ $broker_mapping->{$topic_name} } ) {
            my $leader = $broker_mapping->{$topic_name}->{$log_name};
            my $i      = ( $log_name =~ m{:(\d+)$} )[0];
            my $node   = $Tachikoma::Nodes{$log_name};
            if ( not $node ) {
                $node = Tachikoma::Nodes::Partition->new;
                $node->name($log_name);
            }
            $node->arguments(q());
            $node->filename("$path/$topic_name/partition/$i");
            $node->num_segments( $topic->{num_segments} );
            $node->segment_size( $topic->{segment_size} );
            $node->max_lifespan( $topic->{max_lifespan} );
            $node->debug_state( $topic->{debug_state} );
            $node->replication_factor( $topic->{replication_factor} );
            $node->sink( $self->sink );

            if ($leader) {
                $node->leader("$leader/$log_name");
                $node->restart_follower;
            }
            else {
                $node->in_sync_replicas( {} );
                $node->restart_leader;
            }
            for my $group_name ( keys %{$caches} ) {
                next if ( not $caches->{$group_name} );
                my $cache     = $caches->{$group_name};
                my $group_log = "$log_name:$group_name";
                $node = $Tachikoma::Nodes{$group_log};
                if ( not $node ) {
                    $node = Tachikoma::Nodes::Partition->new;
                    $node->name($group_log);
                }
                $node->arguments(q());
                $node->filename("$path/$topic_name/cache/$group_name/$i");
                $node->num_segments($NUM_CACHE_SEGMENTS);
                $node->segment_size( $cache->{segment_size} );
                $node->max_lifespan( $cache->{max_lifespan} );
                $node->debug_state( $cache->{debug_state} );
                $node->replication_factor( $topic->{replication_factor} );
                $node->sink( $self->sink );

                if ($leader) {
                    $node->leader("$leader/$group_log");
                    $node->restart_follower;
                }
                else {
                    $node->in_sync_replicas( {} );
                    $node->restart_leader;
                }
            }
        }
    }
    $self->save_topic_states;
    if ( $self->{is_controller} ) {
        $self->stderr( $self->{stage}, ' MAP_COMPLETE' );
        $self->{stage} = 'WAIT';
    }
    else {
        $self->{stage} = 'FINISH';
    }
    return;
}

sub wait_for_responses {
    my $self = shift;
    return $self->rebalance_partitions('inform_brokers')
        if ( $Tachikoma::Now - $self->{last_election} > $ELECTION_TIMEOUT );
    $self->{stage} = 'FINISH' if ( not keys %{ $self->{waiting_for_map} } );
    return;
}

sub handle_response {
    my ( $self, $message ) = @_;
    my $stream = $message->[STREAM]
        or return $self->stderr('ERROR: bad response');
    return $self->stderr("ERROR: $stream is controller")
        if ( $stream eq $self->{controller} );
    my $check_stage = sub {
        my $stage = shift;
        return $self->stderr( $self->{stage},
            ": ERROR: unexpected $stage message" )
            if ( $self->{stage} ne $stage );
        return 1;
    };
    if ( $message->[PAYLOAD] eq "HALT_COMPLETE\n" ) {
        &{$check_stage}('HALT') or return;
        $self->{last_halt} = $Tachikoma::Right_Now;
        delete $self->{waiting_for_halt}->{$stream};
    }
    elsif ( $message->[PAYLOAD] eq "RESET_COMPLETE\n" ) {
        &{$check_stage}('PAUSE') or return;
        $self->{last_reset} = $Tachikoma::Right_Now;
        delete $self->{waiting_for_reset}->{$stream};
    }
    elsif ( $message->[PAYLOAD] eq "MAP_COMPLETE\n" ) {
        &{$check_stage}('WAIT') or return;
        delete $self->{waiting_for_map}->{$stream};
    }
    else {
        $self->stderr( 'ERROR: unexpected response: ', $message->[PAYLOAD] );
    }
    return;
}

sub handle_error {
    my ( $self, $message ) = @_;
    return if ( $self->{starting_up} );
    my $error = $message->[PAYLOAD];
    chomp $error;
    my $name = ( split m{/}, $message->[FROM], 2 )[0];
    if ( $error eq 'NOT_AVAILABLE' and $self->{brokers}->{$name} ) {
        $self->print_less_often( "ERROR: got $error from ",
            $message->[FROM] );
    }
    else {
        $self->stderr( "ERROR: got $error from ", $message->[FROM] );
    }
    return;
}

sub send_all_clear {
    my $self = shift;
    if ( not keys %{ $self->{waiting_for_map} } ) {
        $self->inform_brokers("ALL_CLEAR\n");
        $self->{status} = 'CONTROLLER';
        $self->{stage}  = 'COMPLETE';
    }
    return;
}

sub inform_brokers {
    my $self    = shift;
    my $payload = shift;
    my $stage   = $self->{stage};
    if ( $stage eq 'COMPLETE' ) {
        $stage = q();
    }
    else {
        $stage .= q( );
    }
    $self->stderr( $stage, $self->{generation}, ' inform_brokers ', $payload )
        if ( $payload ne "REBALANCE_PARTITIONS\n" );
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        next
            if ( $broker_id eq $self->{broker_id}
            or not $broker->{is_online} );
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_INFO;
        $message->[FROM]    = $self->{name};
        $message->[TO]      = $broker->{path};
        $message->[ID]      = $self->{generation};
        $message->[STREAM]  = $self->{broker_id};
        $message->[PAYLOAD] = $payload;
        $self->{sink}->fill($message);
    }
    return;
}

sub process_delete {
    my $self         = shift;
    my $broker_id    = $self->{broker_id};
    my $broker_lco   = $self->{last_commit_offsets}->{$broker_id};
    my $broker_stats = $self->{broker_stats}->{$broker_id};
    my $now          = time;
    for my $name ( keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        next
            if ( not $node->isa('Tachikoma::Nodes::Partition')
            or $node->{leader} );
        $node->process_delete;
        $node->update_offsets;
    }

    # purge stale logs, except those for other processes on this pool
    $self->purge_stale_logs if ( $self->{is_leader} );
    for my $log_name ( keys %{$broker_lco} ) {
        next if ( $Tachikoma::Nodes{$log_name} );
        my $log = $broker_lco->{$log_name} or next;
        if ( $now - $log->{is_active} >= $LCO_TIMEOUT ) {
            delete $broker_lco->{$log_name};
        }
    }
    for my $log_name ( keys %{$broker_stats} ) {
        delete $broker_stats->{$log_name}
            if ( not exists $broker_lco->{$log_name} );
    }
    $self->{last_delete} = $Tachikoma::Now;
    return;
}

sub purge_stale_logs {
    my $self      = shift;
    my $broker_id = $self->{broker_id};
    my $this_pool = $self->{brokers}->{$broker_id}->{pool};
    my $now       = time;
    if ( $now - $self->{broker_pools}->{$this_pool} < $HEARTBEAT_TIMEOUT ) {
        my $brokers            = $self->{brokers};
        my $topics             = $self->{topics};
        my %logs_for_this_pool = ();
        for my $id ( keys %{ $self->{last_commit_offsets} } ) {
            next
                if ( not $brokers->{$id}
                or $brokers->{$id}->{pool} ne $this_pool );
            my $lco = $self->{last_commit_offsets}->{$id};
            for my $log_name ( keys %{$lco} ) {
                my $log = $lco->{$log_name} or next;
                $logs_for_this_pool{$log_name} = 1
                    if ( $now - $log->{is_active} < $LCO_TIMEOUT );
            }
        }
        for my $id ( keys %{ $self->{last_commit_offsets} } ) {
            next
                if ( not $brokers->{$id}
                or $brokers->{$id}->{pool} ne $this_pool );
            my $lco = $self->{last_commit_offsets}->{$id};
            for my $log_name ( keys %{$lco} ) {
                my $log        = $lco->{$log_name} or next;
                my $topic_name = ( split m{:}, $log_name, 2 )[0];
                if ( not $logs_for_this_pool{$log_name}
                    and -e $log->{filename} )
                {
                    $self->stderr("PURGE: $log_name");
                    Tachikoma::Nodes::Partition->purge_tree(
                        $log->{filename} );
                    my @path_components = split m{/}, $log->{filename};
                    while (@path_components) {
                        my $path = join q(/), @path_components;
                        last if ( length $path <= length $self->{path} );
                        ## no critic (RequireCheckedSyscalls)
                        rmdir $path;
                        pop @path_components;
                    }
                }
                if ( not $topics->{$topic_name} ) {
                    $self->purge_broker_config($topic_name);
                }
            }
        }
    }
    return;
}

sub add_broker {
    my $self      = shift;
    my $arguments = shift;
    my ( $broker_id, $path ) = split q( ), $arguments, 2;
    my ( $host,      $port ) = split m{:}, $broker_id, 2;
    return if ( not $port );
    $path ||= $self->{path};
    my $pool   = join q(:), $host, $path;
    my $online = $broker_id eq $self->{broker_id};
    $self->brokers->{$broker_id} = {
        pool           => $pool,
        host           => $host,
        port           => $port,
        path           => "$broker_id/broker",
        last_heartbeat => 0,
        last_ping      => 0,
        is_online      => $online,
        is_leader      => undef,
    };
    $self->broker_pools->{$pool} = $online ? $Tachikoma::Now : 0;

    if ( $broker_id ne $self->{broker_id} ) {
        my $node = $Tachikoma::Nodes{$broker_id};
        if ( not $node ) {
            $node =
                Tachikoma::Nodes::Socket->inet_client_async( $host, $port );
            $node->name($broker_id);
            $node->on_EOF('reconnect');
            $node->register( 'RECONNECT' => $self->{name} );
            $node->sink( $self->sink );
        }
    }
    return;
}

sub add_topic {
    my $self      = shift;
    my $arguments = shift;
    my ( $topic_name, $num_partitions, $replication_factor, $num_segments,
        $segment_size, $max_lifespan, $debug_state, @groups );
    my ( $r, $argv ) = GetOptionsFromString(
        $arguments,
        'topic=s'              => \$topic_name,
        'group=s'              => \@groups,
        'num_partitions=i'     => \$num_partitions,
        'replication_factor=i' => \$replication_factor,
        'num_segments=i'       => \$num_segments,
        'segment_size=i'       => \$segment_size,
        'max_lifespan=i'       => \$max_lifespan,
        'debug_state:i'        => \$debug_state,
    );
    $topic_name //= shift @{$argv};
    return $self->stderr("ERROR: bad arguments: ADD_TOPIC $arguments")
        if ( not $r or not length $topic_name );
    my $current = $self->topics->{$topic_name};
    if ($current) {
        $num_partitions     //= $current->{num_partitions};
        $replication_factor //= $current->{replication_factor};
        $num_segments       //= $current->{num_segments};
        $segment_size       //= $current->{segment_size};
        $max_lifespan       //= $current->{max_lifespan};
        $debug_state        //= $current->{debug_state};
    }
    my $default = $self->default_settings;
    $num_partitions     ||= $default->{num_partitions};
    $replication_factor ||= $default->{replication_factor};
    $num_segments       ||= $default->{num_segments};
    $segment_size       ||= $default->{segment_size};
    $max_lifespan //= $default->{max_lifespan};
    $debug_state  //= 0;
    $self->topics->{$topic_name} = {
        num_partitions     => $num_partitions,
        replication_factor => $replication_factor,
        num_segments       => $num_segments,
        segment_size       => $segment_size,
        max_lifespan       => $max_lifespan,
        debug_state        => $debug_state,
    };

    for my $group_name (@groups) {
        $self->consumer_groups->{$group_name} //= {
            broker_id => undef,
            topics    => {}
        };
        $self->consumer_groups->{$group_name}->{topics}->{$topic_name} = 1;
    }
    $self->rebalance_partitions;
    return;
}

sub save_topic_states {
    my $self = shift;
    if ( $self->{is_leader} ) {
        for my $topic_name ( keys %{ $self->{topics} } ) {
            $self->save_topic_state($topic_name);
        }
    }
    $self->{last_save} = $Tachikoma::Now;
    return;
}

sub save_topic_state {
    my $self        = shift;
    my $topic_name  = shift;
    my $path        = $self->{path};
    my $config_file = "$path/$topic_name/.config";
    my $tmp_file    = "$config_file.tmp";
    $self->make_parent_dirs($config_file);
    open my $fh, '>', $tmp_file
        or die "ERROR: couldn't open $tmp_file: $!";
    my $topic = $self->topics->{$topic_name}
        or die "ERROR: save_topic_state couldn't find topic: $topic_name";
    my $groups          = $self->{consumer_groups};
    my @consumer_groups = ();

    for my $group_name ( keys %{$groups} ) {
        push @consumer_groups, $group_name
            if ( $groups->{$group_name}->{topics}->{$topic_name} );
    }
    print {$fh} join( q( ),
        '--topic_name="' . $topic_name . q("),
        '--num_partitions=' . $topic->{num_partitions},
        '--replication_factor=' . $topic->{replication_factor},
        '--num_segments=' . $topic->{num_segments},
        '--segment_size=' . $topic->{segment_size},
        '--max_lifespan=' . $topic->{max_lifespan},
        map "--group=$_",
        @consumer_groups ),
        "\n";
    close $fh or die "ERROR: couldn't close $tmp_file: $!";
    rename $tmp_file, $config_file
        or die "ERROR: couldn't rename $tmp_file: $!";
    return;
}

sub add_consumer_group {
    my $self      = shift;
    my $arguments = shift;
    my ($group_name,   $topic_name, $segment_size,
        $max_lifespan, $debug_state
    );
    my ( $r, $argv ) = GetOptionsFromString(
        $arguments,
        'group=s'        => \$group_name,
        'topic=s'        => \$topic_name,
        'segment_size=s' => \$segment_size,
        'max_lifespan=i' => \$max_lifespan,
        'debug_state=i'  => \$debug_state,
    );
    $group_name //= shift @{$argv};
    return $self->stderr(
        "ERROR: bad arguments: ADD_CONSUMER_GROUP $arguments")
        if ( not $r or not length $group_name or not length $topic_name );
    my $group   = $self->consumer_groups->{$group_name};
    my $current = undef;
    $current = $group->{topics}->{$topic_name} if ($group);

    if ($current) {
        $segment_size //= $current->{segment_size};
        $max_lifespan //= $current->{max_lifespan};
        $debug_state  //= $current->{debug_state};
    }
    $segment_size ||= $DEFAULT_CACHE_SIZE;
    $max_lifespan                         //= 0;
    $debug_state                          //= 0;
    $self->consumer_groups->{$group_name} //= {
        broker_id => undef,
        topics    => {}
    };
    $group = $self->consumer_groups->{$group_name};
    $group->{topics}->{$topic_name} = {
        segment_size => $segment_size,
        max_lifespan => $max_lifespan,
        debug_state  => $debug_state,
    };
    $self->rebalance_partitions;
    return;
}

sub get_controller {
    my $self       = shift;
    my $args       = shift;
    my $message    = shift;
    my $controller = $self->{controller};
    if ($controller) {
        $self->send_info( $message, $controller . "\n" );
    }
    else {
        $self->send_error( $message, "CANT_FIND_CONTROLLER\n" );
    }
    return;
}

sub get_leader {
    my $self    = shift;
    my $args    = shift;
    my $message = shift;
    my $group   = $self->{consumer_groups}->{$args};
    if ( $group and $group->{broker_id} ) {
        $self->send_info( $message, $group->{broker_id} . "\n" );
    }
    else {
        $self->send_error( $message, "CANT_FIND_GROUP\n" );
    }
    return;
}

sub get_partitions {
    my $self    = shift;
    my $args    = shift;
    my $message = shift;
    if ( $self->{partitions}->{$args} ) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE] = TM_STORABLE;
        $response->[FROM] = $self->{name};
        $response->[TO]   = $message->[FROM];
        $response->[ID]   = $self->{generation};
        $response->payload( $self->{partitions}->{$args} );
        $self->{sink}->fill($response);
    }
    else {
        $self->send_error( $message, "CANT_FIND_TOPIC\n" );
    }
    return;
}

sub get_topics {
    my $self     = shift;
    my $args     = shift;
    my $message  = shift;
    my $response = Tachikoma::Message->new;
    $response->[TYPE] = TM_STORABLE;
    $response->[FROM] = $self->{name};
    $response->[TO]   = $message->[FROM];
    $response->[ID]   = $self->{generation};
    $response->payload( [ keys %{ $self->{partitions} } ] );
    $self->{sink}->fill($response);
    return;
}

sub empty_topics {
    my $self    = shift;
    my $glob    = shift;
    my $mapping = $self->{mapping}->{ $self->{broker_id} };
    return $self->stderr("ERROR: EMPTY_TOPICS requires a glob\n")
        if ( not length $glob );
    return $self->stderr("ERROR: no mapping\n") if ( not $mapping );
    for my $topic_name ( keys %{ $self->{topics} } ) {
        next
            if ( ( $glob and $topic_name !~ m{^$glob$} )
            or not $mapping->{$topic_name} );
        for my $name ( keys %{ $mapping->{$topic_name} } ) {
            $self->stderr("INFO: empty_partition $name");
            $self->empty_partition($name);
            for my $group_name ( keys %{ $self->{consumer_groups} } ) {
                my $group = $self->{consumer_groups}->{$group_name};
                next if ( not $group->{topics}->{$topic_name} );
                my $cache_name = join q(:), $name, $group_name;
                $self->empty_partition($cache_name);
                $self->stderr("INFO: empty_partition $cache_name");
            }
        }
    }
    return;
}

sub empty_groups {
    my $self       = shift;
    my $arguments  = shift;
    my $group_glob = undef;
    my $topic_glob = undef;
    my $mapping    = $self->{mapping}->{ $self->{broker_id} };
    my ( $r, $argv ) = GetOptionsFromString(
        $arguments,
        'group=s' => \$group_glob,
        'topic=s' => \$topic_glob,
    );
    $group_glob //= shift @{$argv};
    return $self->stderr("ERROR: bad arguments: EMPTY_GROUPS $arguments")
        if ( not $r );
    return $self->stderr("ERROR: EMPTY_GROUPS requires a glob\n")
        if ( not length $group_glob );
    return $self->stderr("ERROR: no mapping\n") if ( not $mapping );

    for my $group_name ( keys %{ $self->{consumer_groups} } ) {
        next if ( $group_name !~ m{^$group_glob$} );
        my $group = $self->{consumer_groups}->{$group_name};
        for my $topic_name ( keys %{ $group->{topics} } ) {
            next if ( $topic_glob and $topic_name !~ m{^$topic_glob} );
            for my $name ( keys %{ $mapping->{$topic_name} } ) {
                my $cache_name = join q(:), $name, $group_name;
                $self->empty_partition($cache_name);
                $self->stderr("INFO: empty_partition $cache_name");
            }
        }
    }
    return;
}

sub empty_partition {
    my $self = shift;
    my $name = shift;
    my $node = $Tachikoma::Nodes{$name};
    $node->empty_partition
        if ( $node and $node->isa('Tachikoma::Nodes::Partition') );
    return;
}

sub purge_topics {
    my $self = shift;
    my $glob = shift;
    return $self->stderr("ERROR: PURGE_TOPICS requires a glob\n")
        if ( not length $glob );
    for my $topic_name ( keys %{ $self->{topics} } ) {
        next if ( $glob and $topic_name !~ m{^$glob$} );
        delete $self->{topics}->{$topic_name};
        for my $group_name ( keys %{ $self->{consumer_groups} } ) {
            my $group = $self->{consumer_groups}->{$group_name};
            delete $group->{topics}->{$topic_name};
            delete $self->{consumer_groups}->{$group_name}
                if ( not keys %{ $group->{topics} } );
        }
    }
    $self->rebalance_partitions;
    return;
}

sub purge_groups {
    my $self       = shift;
    my $arguments  = shift;
    my $group_glob = undef;
    my $topic_glob = undef;
    my ( $r, $argv ) = GetOptionsFromString(
        $arguments,
        'group=s' => \$group_glob,
        'topic=s' => \$topic_glob,
    );
    $group_glob //= shift @{$argv};
    return $self->stderr("ERROR: bad arguments: PURGE_GROUPS $arguments")
        if ( not $r );
    return $self->stderr("ERROR: PURGE_GROUPS requires a glob\n")
        if ( not length $group_glob );

    for my $group_name ( keys %{ $self->{consumer_groups} } ) {
        next if ( $group_name !~ m{^$group_glob$} );
        if ($topic_glob) {
            my $group = $self->{consumer_groups}->{$group_name};
            for my $topic_name ( keys %{ $group->{topics} } ) {
                next if ( $topic_name !~ m{^$topic_glob} );
                delete $group->{topics}->{$topic_name};
            }
            delete $self->{consumer_groups}->{$group_name}
                if ( not keys %{ $group->{topics} } );
        }
        else {
            delete $self->{consumer_groups}->{$group_name};
        }
    }
    $self->rebalance_partitions;
    return;
}

sub purge_broker_config {
    my $self       = shift;
    my $topic_name = shift;
    my $path       = $self->{path};
    ## no critic (RequireCheckedSyscalls)
    unlink "$path/$topic_name/.config.tmp";
    unlink "$path/$topic_name/.config";
    rmdir "$path/$topic_name/brokers";
    rmdir "$path/$topic_name/partition";
    rmdir "$path/$topic_name";
    return;
}

sub send_info {
    my $self     = shift;
    my $message  = shift;
    my $info     = shift;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_INFO;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $message->[FROM];
    $response->[ID]      = $self->{generation};
    $response->[STREAM]  = $self->{broker_id};
    $response->[PAYLOAD] = $info;

    if ( $self->{debug_state} and $self->{debug_state} >= 2 ) {
        chomp $info;
        $self->stderr( 'DEBUG: ', $info, q( ), $self->{generation}, ' for ',
            $message->[FROM] );
    }
    return $self->{sink}->fill($response);
}

sub send_response {
    my $self     = shift;
    my $message  = shift;
    my $payload  = shift;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_RESPONSE;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $message->[FROM];
    $response->[ID]      = $self->{generation};
    $response->[STREAM]  = $self->{broker_id};
    $response->[PAYLOAD] = $payload;
    $self->{sink}->fill($response);
    chomp $payload;
    $self->stderr(
        "INFO: $payload ", $self->{generation},
        ' for ',           $self->{controller}
    );
    return;
}

sub send_error {
    my $self     = shift;
    my $message  = shift;
    my $error    = shift;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_ERROR;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $message->[FROM];
    $response->[ID]      = $self->{generation};
    $response->[STREAM]  = $self->{broker_id};
    $response->[PAYLOAD] = $error;
    chomp $error;

    if ( $error ne 'REBALANCING_PARTITIONS' ) {
        $self->stderr(
            "DEBUG: $error ", $self->{generation},
            ' for ',          $message->[FROM]
        );
    }
    return $self->{sink}->fill($response);
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: load_configs                           \\\n"
            . "          set_broker <host>:<port> <path>        \\\n"
            . "          set_topic --topic=<topic>              \\\n"
            . "                    --num_partitions=<count>     \\\n"
            . "                    --replication_factor=<count> \\\n"
            . "                    --num_segments=<count>       \\\n"
            . "                    --segment_size=<bytes>       \\\n"
            . "                    --max_lifespan=<seconds>\n"
            . "          set_consumer_group --group=<group>          \\\n"
            . "                             --topic=<topic>          \\\n"
            . "                             --segment_size=<bytes>   \\\n"
            . "                             --max_lifespan=<seconds>\n"
            . "          set_default <name> <value>\n"
            . "          list_brokers\n"
            . "          list_topics [ <glob> ]\n"
            . "          list_consumer_groups [ <glob> ]\n"
            . "          list_partitions [ <glob> ]\n"
            . "          list_defaults\n"
            . "          empty_topics <glob>\n"
            . "          empty_groups --group=<glob> \\\n"
            . "                       --topic=<glob>\n"
            . "          purge_topics <glob>\n"
            . "          purge_groups --group=<glob> \\\n"
            . "                       --topic=<glob>\n"
            . "          start_broker\n" );
};

$C{load_configs} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $error    = $self->patron->check_status;
    return $self->error($error) if ($error);
    return $self->error("ERROR: invalid arguments\n")
        if ( $command->arguments );
    $self->patron->load_configs;
    return $self->okay($envelope);
};

$C{set_broker} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $error    = $self->patron->check_status;
    return $self->error($error) if ($error);
    return $self->error("ERROR: invalid arguments\n")
        if ( not $command->arguments );
    $self->patron->add_broker( $command->arguments );
    return $self->okay($envelope);
};

$C{list_brokers} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $results  = [
        [   [ 'HOST'           => 'right' ],
            [ 'PORT'           => 'left' ],
            [ 'POOL'           => 'right' ],
            [ 'LAST HEARTBEAT' => 'right' ],
            [ 'ONLINE'         => 'right' ],
            [ 'LEADER'         => 'right' ],
            [ 'CONTROLLER'     => 'right' ],
        ]
    ];
    my $controller = $self->patron->controller;
    for my $broker_id ( sort keys %{ $self->patron->brokers } ) {
        my $broker = $self->patron->brokers->{$broker_id};
        push
            @{$results},
            [
            $broker->{host},
            $broker->{port},
            $broker->{pool},
            strftime( '%F %T %Z', localtime( $broker->{last_heartbeat} ) ),
            $broker->{is_online}      ? q(*) : q(),
            $broker->{is_leader}      ? q(*) : q(),
            $broker_id eq $controller ? q(*) : q(),
            ];
    }
    return $self->response( $envelope, $self->tabulate($results) );
};

$C{set_topic} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $error    = $self->patron->check_status;
    return $self->error($error) if ($error);
    return $self->error("ERROR: invalid arguments\n")
        if ( not $command->arguments );
    $self->patron->add_topic( $command->arguments );
    return $self->okay($envelope);
};

$C{list_topics} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $glob     = $command->arguments;
    my $results  = [
        [   [ 'TOPIC'        => 'left' ],
            [ '#PARTITIONS'  => 'right' ],
            [ '#REPLICAS'    => 'right' ],
            [ '#SEGMENTS'    => 'right' ],
            [ 'SEGMENT_SIZE' => 'right' ],
            [ 'LIFESPAN'     => 'right' ],
        ]
    ];
    for my $topic_name ( sort keys %{ $self->patron->topics } ) {
        next if ( $glob and $topic_name !~ m{$glob} );
        my $topic = $self->patron->topics->{$topic_name};
        push
            @{$results},
            [
            $topic_name,                  $topic->{num_partitions},
            $topic->{replication_factor}, $topic->{num_segments},
            $topic->{segment_size},       $topic->{max_lifespan},
            ];
    }
    return $self->response( $envelope, $self->tabulate($results) );
};

$C{set_consumer_group} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $error    = $self->patron->check_status;
    return $self->error($error) if ($error);
    return $self->error("ERROR: invalid arguments\n")
        if ( not $command->arguments );
    $self->patron->add_consumer_group( $command->arguments );
    return $self->okay($envelope);
};

$C{set_group} = $C{set_consumer_group};

$C{list_consumer_groups} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $glob     = $command->arguments;
    my $results  = [
        [   [ 'GROUP NAME' => 'left' ],
            [ 'TOPIC'      => 'left' ],
            [ 'CACHE SIZE' => 'right' ],
            [ 'LIFESPAN'   => 'right' ],
        ]
    ];
    for my $group_name ( sort keys %{ $self->patron->consumer_groups } ) {
        next if ( $glob and $group_name !~ m{$glob} );
        my $consumer_group = $self->patron->consumer_groups->{$group_name};
        for my $topic_name ( sort keys %{ $consumer_group->{topics} } ) {
            my $group = $consumer_group->{topics}->{$topic_name};
            push @{$results},
                [
                $group_name,            $topic_name,
                $group->{segment_size}, $group->{max_lifespan}
                ];
        }
    }
    return $self->response( $envelope, $self->tabulate($results) );
};

$C{list_groups} = $C{list_consumer_groups};

$C{list_partitions} = sub {
    my $self         = shift;
    my $command      = shift;
    my $envelope     = shift;
    my $glob         = $command->arguments;
    my %unsorted     = ();
    my $results      = undef;
    my $by_partition = $command->name eq 'list_partitions';
    if ($by_partition) {
        $results = [
            [   [ 'PARTITION' => 'left' ],
                [ 'BROKER'    => 'left' ],
                [ 'OFFSET'    => 'right' ],
                [ 'LCO'       => 'right' ],
                [ 'LEADER'    => 'right' ],
                [ 'ISR'       => 'right' ],
            ]
        ];
        if ($glob) {
            push
                @{ $results->[0] },
                [ 'ACTIVE' => 'right' ],
                [ 'ONLINE' => 'right' ];
        }
    }
    else {
        $results = [
            [   [ 'BROKER'    => 'left' ],
                [ 'PARTITION' => 'left' ],
                [ 'OFFSET'    => 'right' ],
                [ 'LCO'       => 'right' ],
                [ 'LEADER'    => 'right' ],
                [ 'ISR'       => 'right' ],
            ]
        ];
        if ($glob) {
            push
                @{ $results->[0] },
                [ 'ACTIVE' => 'right' ],
                [ 'ONLINE' => 'right' ];
        }
    }
    for my $broker_id ( keys %{ $self->patron->last_commit_offsets } ) {
        my $broker       = $self->patron->brokers->{$broker_id} or next;
        my $broker_lco   = $self->patron->last_commit_offsets->{$broker_id};
        my $broker_stats = $self->patron->broker_stats->{$broker_id};
        for my $name ( keys %{$broker_lco} ) {
            next if ( $glob and $name !~ m{$glob} );
            my $log       = $broker_lco->{$name};
            my $stats     = $broker_stats->{$name};
            my $count     = $stats ? $stats->{isr}    : q();
            my $offset    = $stats ? $stats->{offset} : q();
            my $is_leader = $count ? q(*)             : q();
            my $is_active = q();
            my $is_online = $broker->{is_online} ? q(*) : q();
            $is_active = q(*)
                if ($log->{is_active}
                and $Tachikoma::Now - $log->{is_active}
                < $LCO_SEND_INTERVAL * 2 );
            next if ( not $glob and ( not $is_active or not $is_online ) );

            if ($by_partition) {
                my ( $topic_name, $type, $i, $etc ) = split m{:}, $name, 4;
                my $key = sprintf '%s:%s:%09d:%s:%s',
                    $topic_name, $type, $i, $etc // q(), $broker_id;
                $unsorted{$key} = [
                    $name,       $broker_id, $offset,
                    $log->{lco}, $is_leader, $count
                ];
                push @{ $unsorted{$key} }, $is_active, $is_online
                    if ($glob);
            }
            else {
                my ( $topic_name, $type, $i, $etc ) = split m{:}, $name, 4;
                my $key = sprintf '%s:%s:%s:%09d:%s',
                    $broker_id, $topic_name, $type, $i, $etc // q();
                $unsorted{$key} = [
                    $broker_id,  $name,      $offset,
                    $log->{lco}, $is_leader, $count
                ];
                push @{ $unsorted{$key} }, $is_active, $is_online
                    if ($glob);
            }
        }
    }
    push @{$results}, $unsorted{$_} for ( sort keys %unsorted );
    return $self->response( $envelope, $self->tabulate($results) );
};

$C{list_broker_partitions} = $C{list_partitions};
$C{ls}                     = $C{list_partitions};

$C{set_default} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $error    = $self->patron->check_status;
    return $self->error($error) if ($error);
    my ( $name, $value ) = split q( ), $command->arguments, 2;
    return $self->error("ERROR: invalid arguments\n")
        if ( not length $name and not length $value );
    return $self->error("ERROR: invalid setting\n")
        if ( not exists $self->patron->default_settings->{$name} );
    return $self->error("ERROR: invalid value\n")
        if ( $value !~ m{^\d+$} );
    $self->patron->default_settings->{$name} = $value;
    return $self->okay($envelope);
};

$C{list_defaults} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $results  = [ [ [ 'NAME' => 'left' ], [ 'VALUE' => 'left' ], ] ];
    my $default  = $self->patron->default_settings;
    for my $name ( sort keys %{$default} ) {
        push @{$results}, [ $name, $default->{$name} ];
    }
    return $self->response( $envelope, $self->tabulate($results) );
};

$C{empty_topics} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $self->patron->status eq 'REBALANCING_PARTITIONS' ) {
        return $self->error("ERROR: rebalance in progress\n");
    }
    my $error = $self->patron->check_status;
    return $self->error($error) if ($error);
    my $glob = $command->arguments;
    return $self->error("ERROR: no glob\n") if ( not length $glob );
    $self->patron->inform_brokers("EMPTY_TOPICS $glob\n");
    $self->patron->empty_topics($glob);
    return $self->okay($envelope);
};

$C{empty_groups} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $self->patron->status eq 'REBALANCING_PARTITIONS' ) {
        return $self->error("ERROR: rebalance in progress\n");
    }
    my $error = $self->patron->check_status;
    return $self->error($error) if ($error);
    my $arguments = $command->arguments;
    return $self->error("ERROR: no arguments\n") if ( not length $arguments );
    $self->patron->inform_brokers("EMPTY_GROUPS $arguments\n");
    $self->patron->empty_groups($arguments);
    return $self->okay($envelope);
};

$C{purge_topics} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $self->patron->status eq 'REBALANCING_PARTITIONS' ) {
        return $self->error("ERROR: rebalance in progress\n");
    }
    my $error = $self->patron->check_status;
    return $self->error($error) if ($error);
    my $glob = $command->arguments;
    return $self->error("ERROR: no glob\n") if ( not length $glob );
    $self->patron->inform_brokers("PURGE_TOPICS $glob\n");
    $self->patron->purge_topics($glob);
    return $self->okay($envelope);
};

$C{purge_groups} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $self->patron->status eq 'REBALANCING_PARTITIONS' ) {
        return $self->error("ERROR: rebalance in progress\n");
    }
    my $error = $self->patron->check_status;
    return $self->error($error) if ($error);
    my $arguments = $command->arguments;
    return $self->error("ERROR: no arguments\n") if ( not length $arguments );
    $self->patron->inform_brokers("PURGE_GROUPS $arguments\n");
    $self->patron->purge_groups($arguments);
    return $self->okay($envelope);
};

$C{check_mapping} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->check_mapping;
    return $self->okay($envelope);
};

$C{rebalance} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->rebalance_partitions('inform_brokers');
    return $self->okay($envelope);
};

$C{start_broker} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->last_check($Tachikoma::Now);
    $self->patron->last_delete($Tachikoma::Now);
    $self->patron->last_save($Tachikoma::Now);
    $self->patron->last_election($Tachikoma::Now);
    $self->patron->set_timer( $REBALANCE_INTERVAL * 1000 );
    return $self->okay($envelope);
};

sub check_status {
    my $self  = shift;
    my $error = undef;
    if ( not $self->starting_up ) {
        if ( $self->status eq 'REBALANCING_PARTITIONS' ) {
            $error = "ERROR: rebalance in progress\n";
        }
        elsif ( $self->status ne 'CONTROLLER' ) {
            my $controller = $self->controller;
            $error = "ERROR: not controller - go to $controller\n";
        }
    }
    return $error;
}

sub broker_pools {
    my $self = shift;
    if (@_) {
        $self->{broker_pools} = shift;
    }
    return $self->{broker_pools};
}

sub broker_id {
    my $self = shift;
    if (@_) {
        $self->{broker_id} = shift;
    }
    return $self->{broker_id};
}

sub broker_stats {
    my $self = shift;
    if (@_) {
        $self->{broker_stats} = shift;
    }
    return $self->{broker_stats};
}

sub brokers {
    my $self = shift;
    if (@_) {
        $self->{brokers} = shift;
    }
    return $self->{brokers};
}

sub caches {
    my $self = shift;
    if (@_) {
        $self->{caches} = shift;
    }
    return $self->{caches};
}

sub consumer_groups {
    my $self = shift;
    if (@_) {
        $self->{consumer_groups} = shift;
    }
    return $self->{consumer_groups};
}

sub controller {
    my $self = shift;
    if (@_) {
        $self->{controller} = shift;
    }
    return $self->{controller};
}

sub default_settings {
    my $self = shift;
    if (@_) {
        $self->{default_settings} = shift;
    }
    return $self->{default_settings};
}

sub generation {
    my $self = shift;
    if (@_) {
        $self->{generation} = shift;
    }
    return $self->{generation};
}

sub is_leader {
    my $self = shift;
    if (@_) {
        $self->{is_leader} = shift;
    }
    return $self->{is_leader};
}

sub is_controller {
    my $self = shift;
    if (@_) {
        $self->{is_controller} = shift;
    }
    return $self->{is_controller};
}

sub last_commit_offsets {
    my $self = shift;
    if (@_) {
        $self->{last_commit_offsets} = shift;
    }
    return $self->{last_commit_offsets};
}

sub last_check {
    my $self = shift;
    if (@_) {
        $self->{last_check} = shift;
    }
    return $self->{last_check};
}

sub last_delete {
    my $self = shift;
    if (@_) {
        $self->{last_delete} = shift;
    }
    return $self->{last_delete};
}

sub last_save {
    my $self = shift;
    if (@_) {
        $self->{last_save} = shift;
    }
    return $self->{last_save};
}

sub last_election {
    my $self = shift;
    if (@_) {
        $self->{last_election} = shift;
    }
    return $self->{last_election};
}

sub last_halt {
    my $self = shift;
    if (@_) {
        $self->{last_halt} = shift;
    }
    return $self->{last_halt};
}

sub last_reset {
    my $self = shift;
    if (@_) {
        $self->{last_reset} = shift;
    }
    return $self->{last_reset};
}

sub mapping {
    my $self = shift;
    if (@_) {
        $self->{mapping} = shift;
    }
    return $self->{mapping};
}

sub partitions {
    my $self = shift;
    if (@_) {
        $self->{partitions} = shift;
    }
    return $self->{partitions};
}

sub stage {
    my $self = shift;
    if (@_) {
        $self->{stage} = shift;
    }
    return $self->{stage};
}

sub status {
    my $self = shift;
    if (@_) {
        $self->{status} = shift;
    }
    return $self->{status};
}

sub starting_up {
    my $self = shift;
    if (@_) {
        $self->{starting_up} = shift;
    }
    return $self->{starting_up};
}

sub topics {
    my $self = shift;
    if (@_) {
        $self->{topics} = shift;
    }
    return $self->{topics};
}

sub votes {
    my $self = shift;
    if (@_) {
        $self->{votes} = shift;
    }
    return $self->{votes};
}

sub waiting_for_halt {
    my $self = shift;
    if (@_) {
        $self->{waiting_for_halt} = shift;
    }
    return $self->{waiting_for_halt};
}

sub waiting_for_reset {
    my $self = shift;
    if (@_) {
        $self->{waiting_for_reset} = shift;
    }
    return $self->{waiting_for_reset};
}

sub waiting_for_map {
    my $self = shift;
    if (@_) {
        $self->{waiting_for_map} = shift;
    }
    return $self->{waiting_for_map};
}

1;
