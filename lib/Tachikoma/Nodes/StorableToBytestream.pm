#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::StorableToBytestream
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::StorableToBytestream;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE PAYLOAD TM_BYTESTREAM TM_STORABLE TM_PERSIST
);
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->SUPER::fill($message)
        if ( not $message->[TYPE] & TM_STORABLE );
    my $persist  = $message->[TYPE] & TM_PERSIST ? TM_PERSIST : 0;
    my $payload  = $message->payload;
    my $response = bless [ @{$message} ], ref $message;
    $response->[TYPE] = TM_BYTESTREAM | $persist;
    if ( ref $payload =~ m{HASH} ) {
        $response->[PAYLOAD] = join q(), map "$_ $payload->{$_}\n",
            sort keys %{$payload};
    }
    else {
        $self->stderr('ERROR: unexpected payload type');
        $self->answer($message);
        return;
    }
    return $self->SUPER::fill($response);
}

1;
