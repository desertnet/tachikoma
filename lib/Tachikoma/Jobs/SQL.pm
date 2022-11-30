#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::SQL
# ----------------------------------------------------------------------
#

package Tachikoma::Jobs::SQL;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::SQL;
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.349');

sub initialize_graph {
    my $self = shift;
    my $sql  = Tachikoma::Nodes::SQL->new;
    $sql->arguments( $self->arguments );
    $self->connector->sink($sql);
    $sql->sink($self);
    $self->sink( $self->router );
    return;
}

1;
