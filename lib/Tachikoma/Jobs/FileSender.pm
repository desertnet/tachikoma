#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::FileSender
# ----------------------------------------------------------------------
#
# $Id: FileSender.pm 32953 2018-02-09 10:17:30Z chris $
#

package Tachikoma::Jobs::FileSender;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::FileSender;
use parent qw( Tachikoma::Job );

use version; our $VERSION = 'v2.0.349';

sub initialize_graph {
    my $self        = shift;
    my $file_sender = Tachikoma::Nodes::FileSender->new;
    $file_sender->arguments( $self->arguments );
    $self->connector->sink($file_sender);
    $file_sender->sink($self);
    $self->sink( $self->router );
    return;
}

1;
