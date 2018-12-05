#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Edge
# ----------------------------------------------------------------------
#
# $Id: Edge.pm 29406 2017-04-29 11:18:09Z chris $
#

package Tachikoma::Nodes::Edge;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE FROM PAYLOAD TM_BYTESTREAM TM_STORABLE );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub help {
    my $self = shift;
    return <<'EOF';
make_node Edge <node name>
EOF
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_STORABLE ) {
        $self->{edge}->activate( $message->payload );
    }
    else {
        $self->{edge}->activate( \$message->[PAYLOAD] );
    }
    return;
}

sub activate {    ## no critic (RequireArgUnpacking, RequireFinalReturn)
    my $message = Tachikoma::Message->new;
    $message->[FROM] = $_[0]->{name};
    if ( ref $_[1] ) {
        $message->[TYPE] = TM_STORABLE;
        $message->payload( $_[1] );
    }
    else {
        $message->[TYPE]    = TM_BYTESTREAM;
        $message->[PAYLOAD] = ${ $_[1] };
    }
    $_[0]->SUPER::fill($message);
}

1;
