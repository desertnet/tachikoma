#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Callback
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Callback;
use strict;
use warnings;
use Tachikoma::Node;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{callback} = shift;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        die "ERROR: incorrect use of Callback node\n";
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return &{ $self->{callback} }($message);
}

sub callback {
    my $self = shift;
    if (@_) {
        $self->{callback} = shift;
    }
    return $self->{callback};
}

1;
