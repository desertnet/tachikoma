#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::JSONtoStorable
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Nodes::JSONtoStorable;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE TM_STORABLE TM_BYTESTREAM );
use JSON -support_by_pp;
use parent qw( Tachikoma::Node );

use version; our $VERSION = 'v2.0.367';

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->SUPER::fill($message)
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $json = JSON->new;
    my $response = bless [ @{$message} ], ref $message;
    $response->type(TM_STORABLE);
    $response->payload( $json->decode( $message->payload ) );
    return $self->SUPER::fill($response);
}

1;
