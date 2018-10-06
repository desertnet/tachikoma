#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::FileReceiver
# ----------------------------------------------------------------------
#
# $Id: FileReceiver.pm 35016 2018-10-06 08:47:06Z chris $
#

package Tachikoma::Jobs::FileReceiver;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::FileReceiver;
use parent qw( Tachikoma::Job );

sub initialize_graph {
    my $self = shift;
    my $file_receiver = Tachikoma::Nodes::FileReceiver->new;
    $file_receiver->arguments( $self->arguments );
    $self->connector->sink($file_receiver);
    $file_receiver->sink($self);
    $self->sink( $self->router );
    return;
}

1;
