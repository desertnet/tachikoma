#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Jobs::CozmoAlert
# ----------------------------------------------------------------------
#
# $Id: CozmoAlert.pm 415 2008-12-24 21:08:33Z chris $
#

package Accessories::Jobs::CozmoAlert;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw( TM_BYTESTREAM );
use parent qw( Tachikoma::Job );

sub initialize_graph {
    my $self = shift;
    $self->connector->sink($self);
    $self->sink( $self->router );
    $self->close_stdio;
    if ( $self->arguments ) {
        my $message = Tachikoma::Message->new;
        $message->type(TM_BYTESTREAM);
        $message->payload( $self->arguments );
        $self->fill($message);
    }
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->type & TM_BYTESTREAM );
    return if ( $Tachikoma::Now - $message->timestamp > 1 );
    my $arguments = $message->payload;
    $arguments =~ s(\n)( )g;
    system( '/Users/chris/Documents/bin/cozmo_alert.py', $arguments );
    return $self->SUPER::fill($message);
}

1;
