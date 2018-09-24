#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::Transform
# ----------------------------------------------------------------------
#
# $Id: Transform.pm 1805 2009-05-16 08:15:10Z chris $
#

package Tachikoma::Jobs::Transform;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::Transform;
use parent qw( Tachikoma::Job );

sub initialize_graph {
    my $self      = shift;
    my $transform = Tachikoma::Nodes::Transform->new;
    $transform->arguments( $self->arguments );
    $self->connector->sink($transform);
    $transform->sink($self);
    $self->sink( $self->router );
    return;
}

1;
