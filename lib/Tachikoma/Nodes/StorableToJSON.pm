#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::StorableToJSON
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Nodes::StorableToJSON;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE PAYLOAD TM_BYTESTREAM TM_STORABLE TM_PERSIST
);
use JSON;    # -support_by_pp;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->SUPER::fill($message)
        if ( not $message->[TYPE] & TM_STORABLE );
    my $json = JSON->new;
    $json->canonical(1);
    $json->pretty(1);
    $json->allow_blessed(1);
    $json->convert_blessed(0);
    my $persist  = $message->[TYPE] & TM_PERSIST ? TM_PERSIST : 0;
    my $response = bless [ @{$message} ], ref $message;
    $response->[TYPE] = TM_BYTESTREAM;
    $response->[TYPE] |= $persist if ($persist);
    $response->[PAYLOAD] =
        $json->encode( $message->payload );
    return $self->SUPER::fill($response);
}

1;
