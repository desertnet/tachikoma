#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::ConsumerGroup
# ----------------------------------------------------------------------
#
#   - Assigns Partitions to ConsumerBrokers
#
# $Id: ConsumerGroup.pm 29406 2017-04-29 11:18:09Z chris $
#

package Tachikoma::Nodes::ConsumerGroup;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO PAYLOAD
    TM_REQUEST TM_STORABLE TM_ERROR
);
use Tachikoma;
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.256');

my $Consumer_Timeout = 900;    # wait before abandoning ConsumerBroker

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{topics}     = undef;
    $self->{is_leader}  = undef;
    $self->{mapping}    = undef;
    $self->{timestamps} = {};
    $self->{waiting}    = {};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node ConsumerGroup <node name> <topic1> [ <topic2> ... ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        $self->{arguments}  = $arguments;
        $self->{topics}     = { map { $_ => 1 } split q( ), $arguments };
        $self->{is_leader}  = undef;
        $self->{timestamps} = {};
        $self->{mapping}    = {};
        $self->{waiting}    = {};
        $self->set_timer;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_REQUEST ) {
        if ( $self->{is_leader} ) {
            my $line = $message->[PAYLOAD];
            chomp $line;
            my ( $cmd, $args ) = split q( ), $line, 2;
            if ( $cmd eq 'GET_PARTITIONS' ) {
                $self->get_partitions( $message, $args );
            }
            elsif ( $cmd eq 'DISCONNECT' ) {
                $self->disconnect_consumer($message);
            }
            else {
                $self->stderr( 'ERROR: unrecognized command: ',
                    $message->[PAYLOAD] );
            }
        }
        else {
            $self->send_error( $message, "NOT_LEADER\n" );
        }
    }
    return;
}

sub fire {
    my $self = shift;
    for my $topic ( keys %{ $self->{timestamps} } ) {
        my $timestamps = $self->{timestamps}->{$topic};
        my $rebalance  = $self->check_timestamps($timestamps);
        $self->rebalance_consumers($topic) if ($rebalance);
    }
    for my $topic ( keys %{ $self->{waiting} } ) {
        my $waiting   = $self->{waiting}->{$topic};
        my $rebalance = $self->check_timestamps($waiting);
        $self->rebalance_consumers($topic) if ($rebalance);
    }
    return;
}

sub get_partitions {
    my $self    = shift;
    my $message = shift;
    my $topic   = shift;
    my $broker  = $Tachikoma::Nodes{broker};
    my $from    = $message->[FROM];
    return $self->send_error( $message, "NO_BROKER\n" )
        if ( not $broker );
    return $self->send_error( $message, "REBALANCING_PARTITIONS\n" )
        if ( $broker->status eq 'REBALANCING_PARTITIONS' );
    return $self->send_error( $message, "CANT_FIND_TOPIC\n" )
        if ( not $broker->{topics}->{$topic} );
    return $self->send_error( $message, "NOT_SUBSCRIBED\n" )
        if ( not exists $self->{topics}->{$topic} );
    my $timestamps = $self->{timestamps}->{$topic};

    if ( not $timestamps->{$from} ) {
        my $partitions = $broker->partitions->{$topic}
            or return $self->stderr('ERROR: no partitions');
        return $self->send_error( $message, "TOO_MANY_CONSUMERS\n" )
            if ( keys %{$timestamps} >= @{$partitions} );
        $self->stderr("INFO: GET_PARTITIONS $topic - from $from");
        $self->rebalance_consumers($topic);
    }
    $timestamps->{$from} = $Tachikoma::Now;
    my $mapping = $self->determine_mapping($topic);
    delete $self->{waiting}->{$topic}->{$from};
    return $self->send_error( $message, "REBALANCING_CONSUMERS\n" )
        if ( not defined $mapping );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_STORABLE;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $from;
    $response->[PAYLOAD] = $mapping->{$from};
    $self->{sink}->fill($response);
    return;
}

sub determine_mapping {
    my $self    = shift;
    my $topic   = shift;
    my $waiting = $self->{waiting}->{$topic};
    $self->check_timestamps($waiting);
    return if ( keys %{$waiting} );
    if ( not $self->{mapping}->{$topic} ) {
        my $broker     = $Tachikoma::Nodes{broker};
        my $partitions = $broker->partitions->{$topic}
            or return $self->stderr('ERROR: no partitions');
        my @consumers = sort keys %{ $self->{timestamps}->{$topic} };
        my %mapping   = ();
        my $i         = 0;
        while ( $i <= $#{$partitions} ) {
            my $consumer = $consumers[ $i % @consumers ];
            $mapping{$consumer} //= {};
            $mapping{$consumer}->{$i} = $partitions->[$i];
            $i++;
        }
        $self->{mapping}->{$topic} = \%mapping if ( keys %mapping );
    }
    return $self->{mapping}->{$topic};
}

sub check_timestamps {
    my $self       = shift;
    my $timestamps = shift;
    my $rebalance  = undef;
    for my $consumer ( keys %{$timestamps} ) {
        if ( $Tachikoma::Now - $timestamps->{$consumer} > $Consumer_Timeout )
        {
            delete $timestamps->{$consumer};
            $rebalance = 1;
        }
        my $name = ( split m{/}, $consumer, 2 )[0];
        if ( not $Tachikoma::Nodes{$name} ) {
            delete $timestamps->{$consumer};
            $rebalance = 1;
        }
    }
    return $rebalance;
}

sub send_error {
    my $self     = shift;
    my $message  = shift;
    my $error    = shift;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_ERROR;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $message->[FROM];
    $response->[PAYLOAD] = $error;
    chomp $error;

    if (    $error ne 'TOO_MANY_CONSUMERS'
        and $error ne 'REBALANCING_CONSUMERS' )
    {
        $self->stderr( "$error for ", $message->[FROM] );
    }
    return $self->{sink}->fill($response);
}

sub rebalance_consumers {
    my $self  = shift;
    my $topic = shift;
    delete $self->{mapping}->{$topic};
    my $timestamps = $self->{timestamps}->{$topic};
    for my $consumer ( keys %{$timestamps} ) {
        my $name = ( split m{/}, $consumer, 2 )[0];
        delete $timestamps->{$consumer} if ( not $Tachikoma::Nodes{$name} );
    }
    $self->{waiting}->{$topic} = { %{ $self->{timestamps}->{$topic} } };
    return;
}

sub disconnect_consumer {
    my $self    = shift;
    my $message = shift;
    my $from    = $message->[FROM];
    for my $topic ( keys %{ $self->{timestamps} } ) {
        my $timestamp = $self->{timestamps}->{$topic}->{$from};
        delete $self->{timestamps}->{$topic}->{$from};
        delete $self->{waiting}->{$topic}->{$from};
        $self->rebalance_consumers($topic) if ($timestamp);
    }
    return;
}

sub topics {
    my $self = shift;
    if (@_) {
        $self->{topics} = shift;
        for my $topic ( keys %{ $self->{topics} } ) {
            $self->{timestamps}->{$topic} = {};
            $self->{waiting}->{$topic}    = {};
        }
    }
    return $self->{topics};
}

sub is_leader {
    my $self = shift;
    if (@_) {
        $self->{is_leader} = shift;
    }
    for my $topic ( keys %{ $self->{timestamps} } ) {
        $self->rebalance_consumers($topic);
    }
    return $self->{is_leader};
}

sub mapping {
    my $self = shift;
    if (@_) {
        $self->{mapping} = shift;
    }
    return $self->{mapping};
}

sub timestamps {
    my $self = shift;
    if (@_) {
        $self->{timestamps} = shift;
    }
    return $self->{timestamps};
}

sub waiting {
    my $self = shift;
    if (@_) {
        $self->{waiting} = shift;
    }
    return $self->{waiting};
}

1;
