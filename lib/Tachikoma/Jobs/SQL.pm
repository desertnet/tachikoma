#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::SQL
# ----------------------------------------------------------------------
#
# $Id: SQL.pm 32953 2018-02-09 10:17:30Z chris $
#

package Tachikoma::Jobs::SQL;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::SQL;
use parent qw( Tachikoma::Job );

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
