#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Jobs::DNS
# ----------------------------------------------------------------------
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
    my $message = Tachikoma::Message->new;
    $message->type(TM_BYTESTREAM);
    $message->payload( $self->arguments );
    $self->fill($message);
    $self->shutdown_all_nodes;
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->type & TM_BYTESTREAM );
    my $arguments = $message->payload;
    $message->to('_parent');
    $message->payload( $self->execute( '/usr/bin/host', $arguments ) );
    return $self->SUPER::fill($message);
}

1;
