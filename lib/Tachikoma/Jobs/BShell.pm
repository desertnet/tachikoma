#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::BShell
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Jobs::BShell;
use strict;
use warnings;
use Tachikoma::Jobs::Shell;
use parent qw( Tachikoma::Jobs::Shell );

use version; our $VERSION = 'v2.0.349';

sub initialize_shell_graph {
    my $self = shift;
    $self->SUPER::initialize_shell_graph;
    $self->shell_stdout->buffer_mode('line-buffered');
    return;
}

1;
