#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Watchdog
# ----------------------------------------------------------------------
#
# $Id: Watchdog.pm 12579 2012-01-11 04:10:56Z chris $
#

package Tachikoma::Nodes::Watchdog;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE TM_ERROR TM_EOF );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.367');

my $Default_Timeout = 30;

sub help {
    my $self = shift;
    return <<'EOF';
make_node Watchdog <node name> [ <timeout> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $gate, $timeout ) = split q( ), $self->{arguments}, 2;
        $self->{gate}    = $gate;
        $self->{timeout} = $timeout || $Default_Timeout;
        $self->set_timer( $self->{timeout} * 1000 );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( $message->[TYPE] & TM_ERROR or $message->[TYPE] & TM_EOF );
    my $gate = $self->{gate} or return $self->stderr('ERROR: no gate set');
    my $node = $Tachikoma::Nodes{$gate}
        or return $self->stderr("ERROR: couldn't find $gate");
    return $self->stderr('ERROR: only Gate nodes are supported')
        if ( not $node->isa('Tachikoma::Nodes::Gate') );
    $node->arguments('closed');
    $self->set_timer( $self->{timeout} * 1000 );
    return $self->SUPER::fill($message) if ( $self->owner );
    return $self->cancel($message);
}

sub fire {
    my $self = shift;
    my $gate = $self->{gate} or return;
    my $node = $Tachikoma::Nodes{$gate}
        or return $self->stderr("ERROR: couldn't find $gate");
    return $self->stderr('ERROR: only Gate nodes are supported')
        if ( not $node->isa('Tachikoma::Nodes::Gate') );
    $node->arguments('open');
    return;
}

sub gate {
    my $self = shift;
    if (@_) {
        $self->{gate} = shift;
    }
    return $self->{gate};
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

1;
