#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::Log
# ----------------------------------------------------------------------
#
# $Id: Log.pm 32953 2018-02-09 10:17:30Z chris $
#

package Tachikoma::Jobs::Log;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::Log;
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.280');

sub initialize_graph {
    my $self = shift;
    my $log  = Tachikoma::Nodes::Log->new;
    $log->arguments( $self->arguments );
    $self->connector->sink($log);
    $log->sink($self);
    $self->sink( $self->router );
    return;
}

1;
