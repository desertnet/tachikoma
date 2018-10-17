#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::Smooth
# ----------------------------------------------------------------------
#
# $Id$
#

package Accessories::Nodes::Smooth;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Nodes::Timer );

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{arguments}     = undef;
    $self->{hz}            = undef;
    $self->{maxstep}       = 4;
    $self->{last_out}      = undef;
    $self->{target_out}    = undef;
    $self->{timer_enabled} = 0;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $hz, $max_step ) = split( /\s+/, $self->{arguments}, 2 );
        $self->{hz}       = $hz;
        $self->{max_step} = $max_step;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $payload = $message->payload;
    if ( $payload =~ /^(\d+\.?\d*?)$/ ) {
        $self->{target_out} = $1;
        unless ( defined $self->{last_out} ) {
            $self->{last_out} = $self->{target_out};
            $message->payload( $self->{target_out} . "\n" );
            return $self->SUPER::fill($message);
        }
        unless ( $self->{target_out} == $self->{last_out} ) {
            $self->enable_timer;
        }
    }
    else {
        $self->stderr(
            sprintf( "unrecognized message format: %s\n", $payload ) );
        return;
    }
    return;
}

sub fire {
    my $self = shift;

    my $last   = $self->{last_out};
    my $target = $self->{target_out};

    my $newmsg = Tachikoma::Message->new;
    $newmsg->type(TM_BYTESTREAM);
    $newmsg->to( $self->owner );

    my $smoothed = $self->smooth( $last, $target, 0.5 );

    #$self->stderr(sprintf("last %d target %d smoothed %d\n",
    #    $last, $target, $smoothed));

    #
    # clamp large slew values
    if ( $last > $smoothed ) {
        if ( ( $last - $smoothed ) > $self->{max_step} ) {

            #$self->stderr(sprintf("down clamping smoothed: %d -> %d\n",
            #    $smoothed, $last - $self->{max_step}));
            $smoothed = $last - $self->{max_step};
        }
    }
    elsif ( $last < $smoothed ) {
        if ( ( $smoothed - $last ) > $self->{max_step} ) {

            #$self->stderr(sprintf("up clamping smoothed: %d -> %d\n",
            #    $smoothed, $last + $self->{max_step}));
            $smoothed = $last + $self->{max_step};
        }
    }

    if ( $smoothed == $target ) {
        $self->disable_timer;
    }
    else {
        $self->enable_timer;
    }
    $self->{last_out} = $smoothed;
    $newmsg->[PAYLOAD] = "$smoothed\n";
    return $self->SUPER::fill($newmsg);
}

sub enable_timer {
    my $self = shift;
    unless ( $self->{timer_enabled} == 1 ) {

        #$self->stderr("enabling timer\n");
        $self->{timer_enabled} = 1;
        $self->set_timer( ( 1 / $self->{hz} ) * 1000 );
    }
}

sub disable_timer {
    my $self = shift;
    unless ( $self->{timer_enabled} == 0 ) {

        #$self->stderr("disabling timer\n");
        $self->{timer_enabled} = 0;
        $self->set_timer( 0, 'oneshot' );
    }
}

sub smooth {
    my $self = shift;
    my $a    = shift;
    my $b    = shift;
    my $s    = shift;
    return $a + ( $b - $a ) * $s;
}

1;
