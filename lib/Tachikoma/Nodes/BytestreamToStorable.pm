#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::BytestreamToStorable
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::BytestreamToStorable;
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
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $persist  = $message->[TYPE] & TM_PERSIST ? TM_PERSIST : 0;
    my $response = bless [ @{$message} ], ref $message;
    my $okay     = eval {
        $response->[TYPE] = TM_STORABLE | $persist;
        $response->payload( { split q( ), $message->[PAYLOAD] } );
        return 1;
    };
    if ( not $okay ) {
        $self->stderr( $@ || 'unknown error' );
    }
    else {
        $self->SUPER::fill($response);
    }
    return;
}

1;
