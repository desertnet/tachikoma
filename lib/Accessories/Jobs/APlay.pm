#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Jobs::APlay
# ----------------------------------------------------------------------
#

package Accessories::Jobs::APlay;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw( TM_BYTESTREAM );
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.662');

my $PLAY_CMD  = undef;
my $PLAY_ARGS = undef;

if ( -f '/usr/bin/aplay' ) {
    $PLAY_CMD  = '/usr/bin/aplay';
    $PLAY_ARGS = '-q -D pulse';
}
elsif ( -f '/usr/bin/afplay' ) {
    $PLAY_CMD  = '/usr/bin/afplay';
    $PLAY_ARGS = q();
}

sub initialize_graph {
    my $self = shift;
    $self->connector->sink($self);
    $self->sink( $self->connector );
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
    if ( $Tachikoma::Now - $message->timestamp <= 1 ) {
        my $arguments = $message->payload;
        $self->execute( $PLAY_CMD, join q( ), $PLAY_ARGS, $arguments );
    }
    return $self->cancel($message);
}

1;
