#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Aggregate
# ----------------------------------------------------------------------
#
# $Id: Aggregate.pm 35512 2018-10-22 08:27:21Z chris $
#

package Tachikoma::Nodes::Aggregate;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_PERSIST
);
use Getopt::Long qw( GetOptionsFromString );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

my $Default_Num_Partitions = 1;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{num_partitions} = $Default_Num_Partitions;
    $self->{last_timestamp} = 0;
    $self->{partitions}     = {};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Aggregate <node name> --num_partitions=<int>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my $num_partitions;
        my ( $r, $argv ) = GetOptionsFromString( $arguments,
            'num_partitions=i' => \$num_partitions, );
        die "ERROR: invalid option\n" if ( not $r );
        die "ERROR: num_partitions must be greater than or equal to 1\n"
            if ( defined $num_partitions and $num_partitions < 1 );
        $self->{arguments}      = $arguments;
        $self->{num_partitions} = $num_partitions // $Default_Num_Partitions;
        $self->{last_timestamp} = 0;
        $self->{partitions}     = {};
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_STORABLE );
    if ( $message->[TIMESTAMP] != $self->{last_timestamp} ) {
        $self->{partitions}     = {};
        $self->{last_timestamp} = $message->[TIMESTAMP];
    }
    my $i = $message->[STREAM];
    $self->{partitions}->{$i} = $message->payload;
    if ( scalar keys %{ $self->{partitions} } >= $self->{num_partitions} ) {
        my $whole = {};
        for my $j ( keys %{ $self->{partitions} } ) {
            my $part = $self->{partitions}->{$j};
            for my $key ( keys %{$part} ) {
                $whole->{$key} += $part->{$key};
            }
        }
        $self->{partitions} = {};
        my $persist = $message->[TYPE] & TM_PERSIST ? TM_PERSIST : 0;
        my $response = Tachikoma::Message->new;
        $response->[TYPE] = TM_STORABLE;
        $response->[TYPE] |= $persist if ($persist);
        $response->[FROM]      = $message->[FROM];
        $response->[TO]        = $message->[TO];
        $response->[ID]        = $message->[ID];
        $response->[STREAM]    = $message->[STREAM];
        $response->[TIMESTAMP] = $message->[TIMESTAMP];
        $response->[PAYLOAD]   = $whole;
        $self->SUPER::fill($response);
    }
    else {
        $self->cancel($message);
    }
    return;
}

sub num_partitions {
    my $self = shift;
    if (@_) {
        $self->{num_partitions} = shift;
    }
    return $self->{num_partitions};
}

sub last_timestamp {
    my $self = shift;
    if (@_) {
        $self->{last_timestamp} = shift;
    }
    return $self->{last_timestamp};
}

sub partitions {
    my $self = shift;
    if (@_) {
        $self->{partitions} = shift;
    }
    return $self->{partitions};
}

1;
