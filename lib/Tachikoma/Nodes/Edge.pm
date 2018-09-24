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
use Tachikoma::Message qw( TYPE FROM PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Node );

sub help {
    my $self = shift;
    return <<'EOF';
make_node Edge <node name>
EOF
}

sub fill {
    my $self    = shift;
    my $message = shift;
    $self->{edge}->activate( \$message->[PAYLOAD] );
}

sub activate {
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[FROM]    = $_[0]->{name};
    $message->[PAYLOAD] = ${ $_[1] };
    $_[0]->SUPER::fill($message);
}

1;
