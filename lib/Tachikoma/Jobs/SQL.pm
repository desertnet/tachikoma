#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::SQL
# ----------------------------------------------------------------------
#
# $Id: SQL.pm 35026 2018-10-07 21:39:47Z chris $
#

package Tachikoma::Jobs::SQL;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::SQL;
use parent qw( Tachikoma::Job );

use version; our $VERSION = 'v2.0.349';

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
