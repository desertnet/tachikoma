#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::IndexByField
# ----------------------------------------------------------------------
#
# $Id: IndexByField.pm 3511 2009-10-08 00:18:42Z chris $
#

package Tachikoma::Nodes::IndexByField;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_PERSIST TM_STORABLE
);
use parent qw( Tachikoma::Node );

use version; our $VERSION = 'v2.0.367';

sub fill {
    my $self    = shift;
    my $message = shift;
    $self->{counter}++;
    return if ( not $message->[TYPE] & TM_STORABLE );
    my $partition = ( $message->[FROM] =~ m{(\d+)$} )[0];
    my $offset    = ( split m{:}, $message->[ID], 2 )[0] // 0;
    my $field     = $self->{arguments};
    my $response  = Tachikoma::Message->new;
    $response->[TYPE]      = TM_BYTESTREAM | TM_PERSIST;
    $response->[FROM]      = $message->[FROM];
    $response->[TO]        = $self->{owner} || $message->[TO];
    $response->[ID]        = $message->[ID];
    $response->[STREAM]    = $message->payload->{$field};
    $response->[TIMESTAMP] = $message->[TIMESTAMP];
    $response->[PAYLOAD] =
        length( $message->[PAYLOAD] )
        ? "$partition:$offset\n"
        : q();
    return $self->{sink}->fill($response);
}

1;
