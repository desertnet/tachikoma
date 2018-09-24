#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Uniq
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Nodes::Uniq;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( PAYLOAD );
use parent qw( Tachikoma::Node );

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{p} = '';
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Uniq <node name>
EOF
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->cancel($message) if ( $message->[PAYLOAD] eq $self->{p} );
    $self->{p} = $message->[PAYLOAD];
    return $self->SUPER::fill($message);
}

sub p {
    my $self = shift;
    if (@_) {
        $self->{p} = shift;
    }
    return $self->{p};
}

1;
