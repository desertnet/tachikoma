#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Broker
# ----------------------------------------------------------------------
#
# $Id: Broker.pm 29406 2017-04-29 11:18:09Z chris $
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
    TM_PING TM_COMMAND TM_RESPONSE TM_INFO TM_STORABLE TM_ERROR TM_EOF
);
use Tachikoma;
use Getopt::Long qw( GetOptionsFromString );
use POSIX qw( strftime );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = 'v2.0.165';

my $Path                = '/tmp/topics';
my $Rebalance_Interval  = 0.2;           # timer during rebalance
my $Heartbeat_Interval  = 1;             # ping timer
my $Heartbeat_Timeout   = 60;            # keep this less than delete interval
my $Halt_Time           = 0;             # wait to catch up
my $Reset_Time          = 0;             # wait after tear down
my $Delete_Interval     = 60;            # delete old logs this often
my $Check_Interval      = 900;           # look for better balance this often
my $Save_Interval       = 3600;          # re-save topic configs this often
my $Rebalance_Threshold = 0.90;          # have 90% of our share of leaders
my $Election_Short      = 0;             # wait if everyone is online
my $Election_Long       = 10;            # wait if a broker is offline
my $Startup_Delay       = 0;             # wait at least this long on startup
my $Election_Timeout  = 60;     # how long to wait before starting over
my $LCO_Send_Interval = 10;     # how often to send last commit offsets
my $LCO_Timeout       = 300;    # how long to wait before expiring cached LCO
my $Last_LCO_Send     = 0;      # time we last sent LCO
my $Default_Cache_Size = 8 * 1024 * 1024;    # config for cache partitions
my %C                  = ();

die 'ERROR: data will be lost if Heartbeat_Timeout >= LCO_Timeout'
    if ( $Heartbeat_Timeout >= $LCO_Timeout );

my %Broker_Commands = map { uc $_ => $_ } qw(
    empty_topics
    empty_groups
    purge_topics
    purge_groups
);

my %Controller_Commands = map { uc $_ => $_ } qw(
    add_broker
    add_topic
    add_consumer_group
);

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{broker_hosts}     = {};
    $self->{broker_id}        = undef;
    $self->{broker_stats}     = {};
    $self->{brokers}          = {};
    $self->{caches}           = {};
    $self->{consumer_groups}  = {};
    $self->{controller}       = q();
    $self->{default_settings} = {
        num_partitions     => 1,
        replication_factor => 2,
        num_segments       => 8,
        segment_size       => 128 * 1024 * 1024,
        segment_lifespan   => 7 * 86400,
    };
    $self->{generation}          = 0;
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
        $path //= $Path;
        $self->{arguments} = $arguments;
        $self->{broker_id} = $broker_id;
        $self->{path}      = $path;
        $self->{stick}     = $stick;
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
        else {
            $self->stderr("couldn't find $partitions_path");
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
    my $broker_id   = $self->{broker_id};
    my $path        = $self->{path};
    my $config_file = "$path/$topic_name/brokers/$broker_id/config";
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
        my $type = $message->payload->{type};
        if ( $type and $type eq 'LAST_COMMIT_OFFSETS' ) {
            $self->update_lco($message);
        }
        elsif ( $self->validate( $message, 'RESET' ) ) {
            $self->update_mapping($message);
        }
    }
    elsif ( $self->{is_controller}
        and $message->[ID]
        and $message->[ID] < $self->{generation} )
    {
        $self->stderr(
            'WARNING: stale message: ID ',
            $message->id,
            q( < ),
            $self->{generation},
            q( - ),
            $message->type_as_string
                . ( $message->from ? ' from: ' . $message->from : q() )
                . ( $message->to   ? ' to: ' . $message->to     : q() )
                . (
                ( $message->type & TM_INFO or $message->type & TM_ERROR )
                ? ' payload: ' . $message->payload
                : q()
                )
        );
    }
    elsif ( $message->[TYPE] & TM_RESPONSE ) {
        $self->handle_response($message);
    }
    elsif ( $message->[TYPE] & TM_INFO ) {
        $self->process_info($message);
    }
    elsif ( $message->[TYPE] & TM_ERROR ) {
        my $error = $message->[PAYLOAD];
        chomp $error;
        $self->stderr( "ERROR: got $error from ", $message->[FROM] )
            if ( not $self->{starting_up} );
    }
    else {
        $self->stderr( 'ERROR: unexpected message type: ',
            $message->type_as_string );
    }
    return;
}

sub fire {    ## no critic (ProhibitExcessComplexity)
    my $self = shift;
    if ( $self->{stage} eq 'INIT' or $self->{stage} eq 'COMPLETE' ) {
        $self->determine_controller;
    }
    $self->send_heartbeat;
    if ( $Tachikoma::Right_Now - $Last_LCO_Send > $LCO_Send_Interval ) {
        $self->send_lco;
        $Last_LCO_Send = $Tachikoma::Right_Now;
    }
    my $total  = keys %{ $self->{brokers} };
    my $online = $self->check_heartbeats;
    if ( $online < $total / 2 ) {
        $self->print_less_often('ERROR: not enough servers')
            if ( not $self->{starting_up} );
        $self->rebalance_partitions('inform_brokers');
    }
    elsif ( $self->{status} eq 'REBALANCING_PARTITIONS' ) {
        my $span = $Tachikoma::Now - $self->{last_election};
        my $wait = $total == $online ? $Election_Short : $Election_Long;
        if ( $span > $wait ) {
            $self->{starting_up} = undef;
            if ( $self->{is_controller} ) {

                # $self->stderr("CONTROLLER $self->{stage} cycle");
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
    }
    elsif ( $self->{is_controller}
        and defined $self->{last_check}
        and $Tachikoma::Now - $self->{last_check} > $Check_Interval )
    {
        $self->check_mapping;
    }
    elsif ( $Tachikoma::Now - $self->{last_delete} > $Delete_Interval ) {
        $self->process_delete;
    }
    elsif ( $Tachikoma::Now - $self->{last_save} > $Save_Interval ) {
        $self->save_topic_states;
    }
    if (    $self->{stage} eq 'COMPLETE'
        and $self->{timer_interval} != $Heartbeat_Interval * 1000 )
    {
        $self->set_timer( $Heartbeat_Interval * 1000 );
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
        $self->send_lco;
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
    elsif ( $message->[STREAM] eq 'reconnect' ) {
        $self->offline( $message->[FROM] );
    }
    else {
        $self->process_command($message);
    }
    return;
}

sub process_command {    ## no critic (ProhibitExcessComplexity)
    my $self    = shift;
    my $message = shift;
    my $line    = $message->[PAYLOAD];
    chomp $line;
    my ( $cmd, $args ) = split q( ), $line, 2;
    $self->stderr( "DEBUG: $line - from ", $message->[FROM] )
        if (
            $cmd ne 'GET_CONTROLLER'
        and $cmd ne 'GET_LEADER'
        and $cmd ne 'GET_PARTITIONS'
        and (  $cmd ne 'ADD_CONSUMER_GROUP'
            or $self->{status} ne 'REBALANCING_PARTITIONS' )
        );
    if ( $self->{status} eq 'REBALANCING_PARTITIONS' ) {
        $self->send_error( $message, "REBALANCING_PARTITIONS\n" );
    }
    elsif ( $cmd eq 'GET_PARTITIONS' ) {
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
    }
    elsif ( $cmd eq 'GET_LEADER' ) {
        my $group = $self->{consumer_groups}->{$args};
        if ( $group and $group->{broker_id} ) {
            $self->send_info( $message, $group->{broker_id} . "\n" );
        }
        else {
            $self->send_error( $message, "CANT_FIND_GROUP\n" );
        }
    }
    elsif ( $cmd eq 'GET_CONTROLLER' ) {
        my $controller = $self->{controller};
        if ($controller) {
            $self->send_info( $message, $controller . "\n" );
        }
        else {
            $self->send_error( $message, "CANT_FIND_CONTROLLER\n" );
        }
    }
    elsif ( $Broker_Commands{$cmd} ) {
        my $method = $Broker_Commands{$cmd};
        $self->$method($args);
    }
    elsif ( $self->{status} ne 'CONTROLLER' ) {
        $self->send_error( $message, "NOT_CONTROLLER\n" );
    }
    elsif ( $Controller_Commands{$cmd} ) {
        my $method = $Controller_Commands{$cmd};
        $self->$method( $args, $message );
    }
    else {
        $self->stderr( 'ERROR: unrecognized command: ', $message->[PAYLOAD] );
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
            $message->id, q( < ), $self->{generation} );
    }
    else {
        $rv = 1;
    }
    return $rv;
}

sub determine_controller {
    my $self       = shift;
    my $controller = undef;
    my $count      = 0;
    my %leaders    = ();
    for my $broker_id ( sort keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        if ( not $leaders{ $broker->{host} } ) {
            $broker->{leader} = 1;
            $leaders{ $broker->{host} } = 1;
            $controller //= $broker_id
                if ( $self->{broker_hosts}->{ $broker->{host} } );
        }
        else {
            $broker->{leader} = undef;
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
            < $Heartbeat_Interval );
        if ( $broker_id eq $self->{broker_id} ) {
            $broker->{last_heartbeat} = $Tachikoma::Right_Now;
            $broker->{last_ping}      = $Tachikoma::Right_Now;
            $broker->{online}         = $Tachikoma::Now;
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
        if ( $Tachikoma::Now - $message->[TIMESTAMP] > $Heartbeat_Timeout );
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
    my $back_online = undef;
    $back_online = 1
        if ( not $broker->{online}
        or $now - $broker->{online} >= $Heartbeat_Timeout );
    $broker->{online} = $Tachikoma::Now;
    my $total  = 0;
    my $online = 0;
    for my $id ( keys %{ $self->{brokers} } ) {
        my $other = $self->{brokers}->{$id};
        if ( $other->{host} eq $broker->{host} ) {
            $total++;
            $online++
                if ( $now - $other->{online} < $Heartbeat_Timeout );
        }
    }
    if ( $total and $online == $total ) {
        $self->{broker_hosts}->{ $broker->{host} } = $Tachikoma::Now;
        if ($back_online) {
            $self->stderr( $broker->{host} . ' back online' );
            $self->rebalance_partitions('inform_brokers');
        }
    }
    return;
}

sub send_lco {
    my $self      = shift;
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
        stats      => $broker_stats
    };
    for my $id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$id};
        next
            if ( $id eq $broker_id
            or not $broker->{leader}
            or not $broker->{online} );
        $message->[TO] = $broker->{path};
        $self->{sink}->fill($message);
    }
    return;
}

sub update_lco {
    my $self    = shift;
    my $message = shift;
    $self->{last_commit_offsets}->{ $message->[STREAM] } =
        $message->payload->{partitions};
    $self->{broker_stats}->{ $message->[STREAM] } =
        $message->payload->{stats};
    return;
}

sub check_heartbeats {
    my $self   = shift;
    my $total  = 0;
    my $online = 0;
    my $votes  = 0;
    $votes++ if ( $self->{controller} eq $self->{broker_id} );
    my %offline = ();
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        $self->offline($broker_id)
            if ( $Tachikoma::Right_Now - $broker->{last_heartbeat}
            > $Heartbeat_Timeout );
        $offline{ $broker->{host} } = 1
            if ( not $broker->{online} );
    }
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        $total++;
        next if ( $offline{ $broker->{host} } );
        $online++;
        $votes++ if ( $self->{votes}->{$broker_id} );
    }
    if ( $online >= $total / 2 and $online == $votes ) {
        $self->{is_controller} = 1;
    }
    else {
        $self->{is_controller} = undef;
    }
    return $online;
}

sub offline {
    my $self      = shift;
    my $broker_id = shift;
    my $broker    = $self->{brokers}->{$broker_id};
    return
        if ( not $broker or not $broker->{online} or $self->{starting_up} );
    $broker->{online} = 0;
    $self->{broker_hosts}->{ $broker->{host} } = 0;
    delete $self->{waiting_for_halt}->{$broker_id};
    delete $self->{waiting_for_reset}->{$broker_id};
    delete $self->{waiting_for_map}->{$broker_id};

    $self->stderr("$broker_id has gone offline");
    $self->rebalance_partitions('inform_brokers');
    return;
}

sub rebalance_partitions {
    my $self           = shift;
    my $inform_brokers = shift;
    $self->halt_partitions if ( $self->{stage} ne 'INIT' );
    $self->{is_controller}     = undef;
    $self->{votes}             = {};
    $self->{status}            = 'REBALANCING_PARTITIONS';
    $self->{stage}             = 'INIT';
    $self->{waiting_for_halt}  = {};
    $self->{waiting_for_reset} = {};
    $self->{waiting_for_map}   = {};
    $self->{last_election}     = $Tachikoma::Now;
    $self->inform_brokers("REBALANCE_PARTITIONS\n") if ($inform_brokers);
    $self->set_timer( $Rebalance_Interval * 1000 )
        if ( not $self->{starting_up} );
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

sub send_halt {
    my $self = shift;
    $self->{waiting_for_halt} = {};
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        next
            if ( $broker_id eq $self->{broker_id}
            or not $broker->{online} );
        $self->{waiting_for_halt}->{$broker_id} = 1
            if ( $self->{broker_hosts}->{ $broker->{host} } );
    }
    $self->{generation}++;
    $self->inform_brokers("REBALANCE_PARTITIONS\n");
    $self->inform_brokers("HALT\n");
    $self->{stage} = 'HALT';
    return;
}

sub wait_for_halt {
    my $self = shift;
    return $self->rebalance_partitions('inform_brokers')
        if ( $Tachikoma::Now - $self->{last_election} > $Election_Timeout );
    if ( not keys %{ $self->{waiting_for_halt} }
        and $Tachikoma::Right_Now - $self->{last_halt} > $Halt_Time )
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
            or not $broker->{online} );
        $self->{waiting_for_reset}->{$broker_id} = 1
            if ( $self->{broker_hosts}->{ $broker->{host} } );
    }
    $self->inform_brokers("RESET\n");
    $self->reset_partitions;
    $self->{stage} = 'PAUSE';
    return;
}

sub wait_for_reset {
    my $self = shift;
    return $self->rebalance_partitions('inform_brokers')
        if ( $Tachikoma::Now - $self->{last_election} > $Election_Timeout );
    if ( not keys %{ $self->{waiting_for_reset} }
        and $Tachikoma::Right_Now - $self->{last_reset} > $Reset_Time )
    {
        $self->stderr( $self->{stage} . ' RESET_COMPLETE' );
        $self->{stage} = 'MAP';
    }
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

sub determine_mapping {
    my $self                = shift;
    my $mapping             = {};
    my $leaders_by_host     = {};
    my $leaders_by_broker   = {};
    my $followers_by_host   = {};
    my $followers_by_broker = {};
    my $online_brokers      = {};
    $self->{partitions} = {};

    # partitions assigned to each host
    for my $id ( keys %{ $self->{brokers} } ) {
        my $host = ( split m{:}, $id, 2 )[0];
        next if ( not $self->{broker_hosts}->{$host} );
        $leaders_by_host->{$host} = 0;
        $leaders_by_broker->{$host} //= {};
        $leaders_by_broker->{$host}->{$id} = 0;
        $followers_by_host->{$host} = 0;
        $followers_by_broker->{$host} //= {};
        $followers_by_broker->{$host}->{$id} = 0;
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
                    by_host        => $leaders_by_host,
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
                    by_host        => $followers_by_host,
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

    # $self->stderr('CONTROLLER rebalanced partitions');
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
        $ratio = 1.0 if ( $ideal - $current <= 1 );
        $self->stderr( sprintf 'CHECK_MAPPING: %.2f%% optimal',
            $ratio * 100 );
        if ( $ratio < $Rebalance_Threshold ) {
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

    # $self->stderr(
    #     "CANDIDATE LEADERS for $log_name - ",
    #     join q(, ),
    #     grep $query->{candidates}->{$_},
    #     sort keys %{ $query->{candidates} }
    # );
    $leader = $self->best_broker($query)
        if ( keys %{ $query->{candidates} } );

    # $self->stderr("BEST LEADER for $log_name - $leader") if ($leader);

    # assign a new leader
    if ( not $leader and not keys %{ $query->{candidates} } ) {
        $leader = $self->next_broker($query);
        $self->stderr( "NEW LEADER for $log_name - ", $leader ) if ($leader);
    }
    if ($leader) {
        my $leader_host = ( split m{:}, $leader, 2 )[0];
        $query->{by_host}->{$leader_host}++;
        $query->{by_broker}->{$leader_host}->{$leader}++;
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
        my $log = $broker_lco->{$log_name} or next;
        next
            if ( not defined $log->{lco}
            or $log->{lco} <= $last_commit_offset );
        $last_commit_offset = $log->{lco};
    }
    for my $broker_id ( keys %{ $query->{online_brokers} } ) {
        my $broker_lco = $self->{last_commit_offsets}->{$broker_id};
        my $log = $broker_lco->{$log_name} or next;
        next
            if ( not defined $log->{lco}
            or $log->{lco} < $last_commit_offset );
        $in_sync_replicas{ $brokers->{$broker_id}->{host} } = 1;
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
    my $leader_host = $leader ? ( split m{:}, $leader, 2 )[0] : undef;
    my %candidates  = ();
    my @followers   = ();
    die "ERROR: no replication factor specified\n" if ( not defined $count );
    $skip{$leader_host} = 1 if ($leader_host);

    # find existing followers
    for my $broker_id ( keys %{ $query->{online_brokers} } ) {
        my $broker_host = $brokers->{$broker_id}->{host};
        next if ( $skip{$broker_host} );
        my $broker_lco = $self->{last_commit_offsets}->{$broker_id};
        $candidates{$broker_host} = 1 if ( $broker_lco->{$log_name} );
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
        my $follower_host = ( split m{:}, $follower, 2 )[0];
        $query->{by_host}->{$follower_host}++;
        $query->{by_broker}->{$follower_host}->{$follower}++;
    }
    return \@followers;
}

sub next_broker {
    my $self      = shift;
    my $query     = shift;
    my $broker_id = undef;

    # find the broker with the fewest partitions
    $query->{candidates} = $self->{broker_hosts};
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
    my $by_host      = $query->{by_host};
    my $min          = undef;
    my $best_host    = undef;
    my $broker_id    = undef;

    # pick the host with the fewest partitions
    for my $host ( sort keys %{$by_host} ) {
        next
            if ( not $candidates->{$host}
            or ( $skip and $skip->{$host} )
            or ( defined $min and $by_host->{$host} > $min ) );
        $min       = $by_host->{$host};
        $best_host = $host;
    }

    # pick the broker with the fewest partitions
    if ($best_host) {
        my $by_broker = $query->{by_broker}->{$best_host};
        $broker_id = (
            sort { $by_broker->{$a} <=> $by_broker->{$b} }
                keys %{$by_broker}
        )[0];
        if ($broker_id) {
            my $topic_name = $query->{topic_name};
            my $log_name   = $query->{log};
            $mapping->{$broker_id}->{$topic_name}->{$log_name} =
                $query->{leader};
            if ($skip) {
                $skip->{ $self->{brokers}->{$broker_id}->{host} } = 1;
            }
        }
    }
    return $broker_id;
}

sub send_mapping {
    my $self = shift;
    $self->{waiting_for_map} = {};

    # $self->stderr('CONTROLLER sending mappings');
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $broker = $self->{brokers}->{$broker_id};
        next
            if ( $broker_id eq $self->{broker_id}
            or not $broker->{online} );
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
        $node->sink( $self->sink );
        for my $topic_name ( keys %{ $group->{topics} } ) {
            next if ( not $group->{topics}->{$topic_name} );
            $self->{caches}->{$topic_name} //= {};
            $self->{caches}->{$topic_name}->{$group_name} =
                $group->{topics}->{$topic_name};
        }
        $node->topics( $group->{topics} );
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
            $node->segment_lifespan( $topic->{segment_lifespan} );
            $node->sink( $self->sink );

            if ($leader) {
                $node->leader($leader);
                $node->restart_follower;
            }
            else {
                $node->in_sync_replicas( {} );
                $node->restart_leader;
            }
            for my $group_name ( keys %{$caches} ) {
                next if ( not $caches->{$group_name} );
                my $group_log = "$log_name:$group_name";
                $node = $Tachikoma::Nodes{$group_log};
                if ( not $node ) {
                    $node = Tachikoma::Nodes::Partition->new;
                    $node->name($group_log);
                }
                $node->arguments(q());
                $node->filename("$path/$topic_name/cache/$group_name/$i");
                $node->num_segments( $topic->{num_segments} );
                $node->segment_size( $caches->{$group_name} );
                $node->segment_lifespan( $topic->{segment_lifespan} );
                $node->sink( $self->sink );

                if ($leader) {
                    $node->leader($leader);
                    $node->restart_follower;
                }
                else {
                    $node->in_sync_replicas( {} );
                    $node->restart_leader;
                }
            }
        }
    }
    for my $topic_name ( keys %{$topics} ) {
        $self->save_topic_state($topic_name);
    }
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
        if ( $Tachikoma::Now - $self->{last_election} > $Election_Timeout );
    $self->{stage} = 'FINISH' if ( not keys %{ $self->{waiting_for_map} } );
    return;
}

sub handle_response {
    my $self    = shift;
    my $message = shift;
    my $stream  = $message->[STREAM]
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
            or not $broker->{online} );
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

    # chomp $info;
    # $self->stderr(
    #     $info, q( ), $self->{generation}, ' for ', $message->[FROM]
    # );
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

sub process_delete {    ## no critic (ProhibitExcessComplexity)
    my $self         = shift;
    my $broker_id    = $self->{broker_id};
    my $broker_lco   = $self->{last_commit_offsets}->{$broker_id};
    my $broker_stats = $self->{broker_stats}->{$broker_id};
    my $this_host    = $self->{brokers}->{$broker_id}->{host};
    for my $name ( keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        $node->process_delete
            if ( $node->isa('Tachikoma::Nodes::Partition')
            and not $node->{leader} );
    }

    # purge stale logs, except those for other processes on this host
    my $now = time;
    if (    $self->{brokers}->{$broker_id}->{leader}
        and $now - $self->{broker_hosts}->{$this_host} < $Heartbeat_Timeout )
    {
        my $brokers            = $self->{brokers};
        my $topics             = $self->{topics};
        my %logs_for_this_host = ();
        for my $id ( keys %{ $self->{last_commit_offsets} } ) {
            next
                if ( not $brokers->{$id}
                or $brokers->{$id}->{host} ne $this_host );
            my $lco = $self->{last_commit_offsets}->{$id};
            for my $log_name ( keys %{$lco} ) {
                my $log = $lco->{$log_name} or next;
                $logs_for_this_host{$log_name} = 1
                    if ( $now - $log->{is_active} < $LCO_Timeout );
            }
        }
        for my $id ( keys %{ $self->{last_commit_offsets} } ) {
            next
                if ( not $brokers->{$id}
                or $brokers->{$id}->{host} ne $this_host );
            my $lco = $self->{last_commit_offsets}->{$id};
            for my $log_name ( keys %{$lco} ) {
                my $log = $lco->{$log_name} or next;
                my $topic_name = ( split m{:}, $log_name, 2 )[0];
                if ( not $logs_for_this_host{$log_name}
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
    for my $log_name ( keys %{$broker_lco} ) {
        next if ( $Tachikoma::Nodes{$log_name} );
        my $log = $broker_lco->{$log_name} or next;
        if ( $now - $log->{is_active} >= $LCO_Timeout ) {
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

sub add_broker {
    my $self      = shift;
    my $arguments = shift;
    my $broker_id = $arguments;
    my ( $host, $port ) = split m{:}, $broker_id, 2;
    return if ( not $port );
    my $online = $broker_id eq $self->{broker_id};
    $self->brokers->{$broker_id} = {
        host           => $host,
        port           => $port,
        path           => "$broker_id/broker",
        last_heartbeat => 0,
        last_ping      => 0,
        online         => $online,
        leader         => undef,
    };
    $self->broker_hosts->{$host} = $online;

    if ( $broker_id ne $self->{broker_id} ) {
        my $node = $Tachikoma::Nodes{$broker_id};
        if ( not $node ) {
            $node = inet_client_async Tachikoma::Nodes::Socket( $host, $port,
                $broker_id );
            $node->on_EOF('reconnect');
            $node->register( 'reconnect' => $self->{name} );
            $node->sink( $self->sink );
        }
    }
    return;
}

sub add_topic {
    my $self      = shift;
    my $arguments = shift;
    my ( $topic_name, $num_partitions, $replication_factor, $num_segments,
        $segment_size, $segment_lifespan, @groups );
    my ( $r, $argv ) = GetOptionsFromString(
        $arguments,
        'topic=s'              => \$topic_name,
        'group=s'              => \@groups,
        'num_partitions=i'     => \$num_partitions,
        'replication_factor=i' => \$replication_factor,
        'num_segments=i'       => \$num_segments,
        'segment_size=i'       => \$segment_size,
        'segment_lifespan=i'   => \$segment_lifespan,
    );
    $topic_name //= shift @{$argv};
    return $self->stderr("ERROR: bad arguments: ADD_TOPIC $arguments")
        if ( not $r or not $topic_name );
    my $default = $self->default_settings;
    $num_partitions     ||= $default->{num_partitions};
    $replication_factor ||= $default->{replication_factor};
    $num_segments       ||= $default->{num_segments};
    $segment_size       ||= $default->{segment_size};
    $segment_lifespan   ||= $default->{segment_lifespan};
    $self->topics->{$topic_name} = {
        num_partitions     => $num_partitions,
        replication_factor => $replication_factor,
        num_segments       => $num_segments,
        segment_size       => $segment_size,
        segment_lifespan   => $segment_lifespan,
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
    for my $topic_name ( keys %{ $self->{topics} } ) {
        $self->save_topic_state($topic_name);
    }
    $self->{last_save} = $Tachikoma::Now;
    return;
}

sub save_topic_state {
    my $self        = shift;
    my $topic_name  = shift;
    my $path        = $self->{path};
    my $broker_id   = $self->{broker_id};
    my $config_file = "$path/$topic_name/brokers/$broker_id/config";
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
        '--segment_lifespan=' . $topic->{segment_lifespan},
        map "--group=$_",
        @consumer_groups ),
        "\n";
    close $fh or die "ERROR: couldn't close $tmp_file: $!";
    rename $tmp_file, $config_file
        or die "ERROR: couldn't rename $tmp_file: $!";
    return;
}

sub add_consumer_group {
    my $self       = shift;
    my $arguments  = shift;
    my $message    = shift;
    my $cache_size = $Default_Cache_Size;
    my ( $group_name, $topic_name );
    my ( $r, $argv ) = GetOptionsFromString(
        $arguments,
        'group=s'      => \$group_name,
        'topic=s'      => \$topic_name,
        'cache_size=s' => \$cache_size,
    );
    $group_name //= shift @{$argv};
    return $self->stderr(
        "ERROR: bad arguments: ADD_CONSUMER_GROUP $arguments")
        if ( not $r or not $topic_name );

    if ($message) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE] = TM_RESPONSE;
        $response->[FROM] = $self->{name};
        $response->[ID]   = $self->{generation};
        $response->[TO]   = $message->[FROM];
        my $this = $self->consumer_groups->{$group_name};
        if ( $this and $this->{topics}->{$topic_name} ) {
            $response->[PAYLOAD] = "OK\n";
        }
        else {
            $response->[PAYLOAD] = "WAIT\n";
        }
        $self->{sink}->fill($response);
        return if ( $response->[PAYLOAD] eq "OK\n" );
    }
    $self->consumer_groups->{$group_name} //= {
        broker_id => undef,
        topics    => {}
    };
    my $group = $self->consumer_groups->{$group_name};
    $group->{topics}->{$topic_name} = $cache_size;
    $self->rebalance_partitions;
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
            $self->empty_partition($name);
            for my $group_name ( keys %{ $self->{consumer_groups} } ) {
                my $group = $self->{consumer_groups}->{$group_name};
                next if ( not $group->{topics}->{$topic_name} );
                my $cache_name = join q(:), $name, $group_name;
                $self->empty_partition($cache_name);
            }
        }
    }

    # $self->rebalance_partitions;
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
            }
        }
    }

    # $self->rebalance_partitions;
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
    for my $broker_id ( keys %{ $self->{brokers} } ) {
        my $tmp  = "$path/$topic_name/brokers/$broker_id/config.tmp";
        my $file = "$path/$topic_name/brokers/$broker_id/config";
        unlink $tmp;
        unlink $file;
        rmdir "$path/$topic_name/brokers/$broker_id";
    }
    rmdir "$path/$topic_name/brokers";
    rmdir "$path/$topic_name/partition";
    rmdir "$path/$topic_name";
    return;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: load_configs                           \\\n"
            . "          set_broker <host>:<port>               \\\n"
            . "          set_topic --topic=<topic>              \\\n"
            . "                    --num_partitions=<count>     \\\n"
            . "                    --replication_factor=<count> \\\n"
            . "                    --num_segments=<count>       \\\n"
            . "                    --segment_size=<bytes>       \\\n"
            . "                    --segment_lifespan=<seconds>\n"
            . "          set_consumer_group --group=<group>     \\\n"
            . "                             --topic=<topic>\n"
            . "          list_brokers\n"
            . "          list_topics [ <glob> ]\n"
            . "          list_consumer_groups [ <glob> ]\n"
            . "          list_partitions [ <glob> ]\n"
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
            strftime( '%F %T %Z', localtime( $broker->{last_heartbeat} ) ),
            $broker->{online}         ? q(*) : q(),
            $broker->{leader}         ? q(*) : q(),
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
            $topic->{segment_size},       $topic->{segment_lifespan},
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
    my $topic_name = ( split q( ), $command->arguments, 2 )[1];
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
    my $results  = [ [ [ 'GROUP NAME' => 'left' ], [ 'TOPIC' => 'left' ] ] ];
    for my $group_name ( sort keys %{ $self->patron->consumer_groups } ) {
        next if ( $glob and $group_name !~ m{$glob} );
        my $consumer_group = $self->patron->consumer_groups->{$group_name};
        for my $topic_name ( sort keys %{ $consumer_group->{topics} } ) {
            push @{$results}, [ $group_name, $topic_name ];
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
            my $count     = $stats ? $stats->{isr} : q();
            my $offset    = $stats ? $stats->{offset} : q();
            my $is_leader = $count ? q(*) : q();
            my $is_active = q();
            my $is_online = $broker->{online} ? q(*) : q();
            $is_active = q(*)
                if ($log->{is_active}
                and $Tachikoma::Now - $log->{is_active}
                < $LCO_Send_Interval * 2 );
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

$C{start_broker} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->last_check($Tachikoma::Now);
    $self->patron->last_delete($Tachikoma::Now);
    $self->patron->last_save($Tachikoma::Now);
    $self->patron->last_election( time + $Startup_Delay );
    $self->patron->set_timer( $Rebalance_Interval * 1000 );
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

sub broker_hosts {
    my $self = shift;
    if (@_) {
        $self->{broker_hosts} = shift;
    }
    return $self->{broker_hosts};
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
