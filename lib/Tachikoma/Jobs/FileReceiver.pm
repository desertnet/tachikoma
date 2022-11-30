#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::FileReceiver
# ----------------------------------------------------------------------
#

package Tachikoma::Jobs::FileReceiver;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::FileReceiver;
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.349');

sub initialize_graph {
    my $self          = shift;
    my $file_receiver = Tachikoma::Nodes::FileReceiver->new;
    $file_receiver->arguments( $self->arguments );
    $self->connector->sink($file_receiver);
    $file_receiver->sink($self);
    $self->sink( $self->router );
    return;
}

1;
