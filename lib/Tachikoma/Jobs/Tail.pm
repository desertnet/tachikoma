#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::Tail
# ----------------------------------------------------------------------
#

package Tachikoma::Jobs::Tail;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::Tail;
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.280');

sub initialize_graph {
    my $self = shift;
    my $tail = Tachikoma::Nodes::Tail->new;
    $tail->arguments( $self->arguments );
    $self->connector->sink($tail);
    $tail->sink($self);
    $self->sink( $self->router );
    return;
}

1;
