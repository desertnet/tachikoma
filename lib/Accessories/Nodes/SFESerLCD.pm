#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::SFESerLCD
# ----------------------------------------------------------------------
#
# $Id$
#

package Accessories::Nodes::SFESerLCD;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TM_BYTESTREAM );
use parent qw( Tachikoma::Nodes::Timer );

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{last_string}       = '';
    $self->{last_brightness}   = 0;
    $self->{queue}             = [];
    $self->{last_send_time}    = 0;
    $self->{min_send_interval} = 5;    # ms
    $self->{waiting_until}     = 0;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;

    my $payload = $message->payload;

    if ( $payload =~ /^(.*?)\|\|\|(.*?)$/s ) {
        my $c      = $1;
        my $string = $2;
        my @cmds   = split( ",", $c );
        foreach my $cmd (@cmds) {
            if ( $cmd =~ /^cls$/ ) {
                $self->send( $self->cls );
            }
            elsif ( $cmd =~ /^(bright|brightness)=(\d+\.?\d+?)$/ ) {
                $self->send( $self->bl($2) );
            }
            elsif ( $cmd =~ /^(pos|position)=(\d+)$/ ) {
                $self->send( $self->pos($2) );
            }
        }
        $self->sendlines($string);
        $self->set_timer( $self->{min_send_interval}, 'oneshot' );
    }
    else {
        $self->sendlines($payload);
        $self->set_timer( $self->{min_send_interval}, 'oneshot' );
    }
    return;
}

sub sendlines {
    my $self   = shift;
    my $string = shift;
    return unless ( $string =~ /./ );

    # remove control characters that would do nasty things
    # like changing the baud rate
    $string =~ s/[\x0B\x0C\x0D\x0E\x0F\x10\x7C\xFE]//g;
    my @lines = split( /\n+/, $string );
    if ( scalar(@lines) == 1 ) {
        $self->send( $lines[0] );
    }
    elsif ( scalar(@lines) < 3 ) {
        $self->send( $lines[0] . $self->pos(64) . $lines[1] );
    }
    else {
        my ( $line1, $line2 ) = splice( @lines, 0, 2 );
        $self->send( $line1 . $self->pos(64) . $line2 );
        foreach my $l (@lines) {
            $self->send("WAIT:5");
            $self->send($l);
        }
    }
    return;
}

sub fire {
    my $self  = shift;
    my $min   = $self->{min_send_interval};
    my $last  = $self->{last_send_time};
    my $until = $self->{waiting_until} || 0;
    my $now   = $Tachikoma::Right_Now;
    return if ( ( $now - $last ) < $min );
    return if ( $now < $until );
    $self->{waiting_until} = 0;
    my $str = shift( @{ $self->{queue} } ) // q();

    if ( $str =~ /^WAIT:(\d+)$/ ) {
        my $s = $1;
        $self->{waiting_until} = $Tachikoma::Right_Now + $s;
        $self->set_timer( 1000 * $s, 'oneshot' );
        return;
    }
    if ( scalar( @{ $self->{queue} } ) > 0 ) {
        $self->set_timer( $self->{min_send_interval}, 'oneshot' );
    }
    my $msg = Tachikoma::Message->new;
    $msg->type(TM_BYTESTREAM);
    $msg->payload($str);
    return $self->SUPER::fill($msg);
}

sub send {
    my $self   = shift;
    my $string = shift;
    push( @{ $self->{queue} }, $string );
    return;
}

sub bl {
    my $self  = shift;
    my $level = shift;
    if ( $level < 0 || $level > 1 ) {
        $self->stderr("bl: illegal level $level");
        return;
    }
    my $b = sprintf( "%d", ( 128 + ( $level * ( 157 - 128 ) ) ) );
    return chr(0x7C) . chr($b);
}

sub cls {
    my $self = shift;
    return chr(0xFE) . chr(0x01);
}

sub pos {
    my $self = shift;
    my $pos  = shift;
    my $ipos = int($pos);
    $self->stderr("truncating $pos to int $ipos\n")
        if ( $pos != $ipos );
    $self->stderr("cursor position $pos is outside display area\n")
        if ( $pos < 0 || $pos > 79 );
    my $posval = 0x80 + $ipos;
    return chr(0xFE) . chr($posval);
}

1;
