#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Jobs::Transform
# ----------------------------------------------------------------------
#
# $Id: Transform.pm 1805 2009-05-16 08:15:10Z chris $
#

package Accessories::Jobs::Transform;
use strict;
use warnings;
use Tachikoma::Job;
use Accessories::Nodes::Transform;
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.349');

sub initialize_graph {
    my $self      = shift;
    my $transform = Accessories::Nodes::Transform->new;
    $transform->arguments( $self->arguments );
    $self->connector->sink($transform);
    $transform->sink($self);
    $self->sink( $self->router );
    return;
}

1;
