#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::JobSpawnTimer
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::JobSpawnTimer;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Job;
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.280');

sub fire {
    my $self = shift;
    my $sub  = shift @{ Tachikoma::Job->spawn_queue };
    if ($sub) {
        &{$sub}();
    }
    else {
        $self->remove_node;
    }
    return;
}

1;
