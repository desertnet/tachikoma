#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Jobs::DNS
# ----------------------------------------------------------------------
#
# $Id: DNS.pm 415 2008-12-24 21:08:33Z chris $
#

package Accessories::Jobs::DNS;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw( TM_BYTESTREAM );
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.349');

sub initialize_graph {
    my $self = shift;
    $self->connector->sink($self);
    $self->sink( $self->router );
    if ( $self->owner ) {
        my $message = Tachikoma::Message->new;
        $message->type(TM_BYTESTREAM);
        $message->payload( $self->arguments );
        $self->fill($message);
        $self->remove_node;
    }
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->type & TM_BYTESTREAM );
    my $arguments = $message->payload;
    $message->to( $message->from );
    $message->payload( $self->execute( '/usr/bin/host', $arguments ) );
    return $self->SUPER::fill($message);
}

1;
