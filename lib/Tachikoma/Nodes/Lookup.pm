#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Lookup
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Nodes::Lookup;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TM_BYTESTREAM TYPE TO STREAM );
use parent qw( Tachikoma::Node );

use version; our $VERSION = 'v2.0.367';

sub help {
    my $self = shift;
    return <<'EOF';
make_node Lookup <node name> <on-miss path>
connect_node     <node name> <on-hit path>
connect_edge     <node name> <Table name>
EOF
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    $message->[TO] =
        defined $self->{edge}->lookup( $message->[STREAM] )
        ? $self->{owner}
        : $self->{arguments};
    if ( $message->[TO] ) {
        $self->{sink}->fill($message);
    }
    else {
        $self->cancel($message);
    }
    return;
}

1;
