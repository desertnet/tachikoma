#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Hopper
# ----------------------------------------------------------------------
#
# $Id: Hopper.pm 8952 2010-12-02 08:23:52Z chris $
#

package Tachikoma::Nodes::Hopper;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TM_PERSIST TM_RESPONSE );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.367');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{delay}          = undef;
    $self->{last_fire_time} = undef;
    $self->{queue}          = [];
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Hopper <node name> <delay>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->delay( $self->{arguments} || 1000 );
        $self->last_fire_time(0);
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $delay   = $self->{delay};
    my $queue   = $self->{queue};
    my $span    = $Tachikoma::Right_Now - $self->{last_fire_time};
    if ( not @{$queue} and $span > $delay / 1000 ) {
        $self->{last_fire_time} = $Tachikoma::Right_Now;
        $self->SUPER::fill($message);
    }
    else {
        my $copy = Tachikoma::Message->unpacked( $message->packed );
        push @{$queue}, $copy;
        $span = 0 if ( $span < 0 );
        $delay = $delay - $span * 1000 if ( $delay > $span * 1000 );
        $self->set_timer( $delay, 'oneshot' )
            if ( not $self->{timer_is_active} );
    }
    return;
}

sub fire {
    my $self    = shift;
    my $queue   = $self->{queue};
    my $message = shift @{$queue} or return;
    $self->{last_fire_time} = $Tachikoma::Right_Now;
    $self->SUPER::fill($message);
    $self->set_timer( $self->{delay}, 'oneshot' ) if ( @{$queue} );
    return;
}

sub delay {
    my $self = shift;
    if (@_) {
        $self->{delay} = shift;
    }
    return $self->{delay};
}

sub last_fire_time {
    my $self = shift;
    if (@_) {
        $self->{last_fire_time} = shift;
    }
    return $self->{last_fire_time};
}

sub queue {
    my $self = shift;
    if (@_) {
        $self->{queue} = shift;
    }
    return $self->{queue};
}

1;
