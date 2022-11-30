#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::IndexByTimestamp
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::IndexByTimestamp;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_PERSIST TM_ERROR TM_EOF
);
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub fill {
    my $self    = shift;
    my $message = shift;
    $self->{counter}++;
    return $self->cancel($message)
        if ( $message->[TYPE] & TM_ERROR or $message->[TYPE] & TM_EOF );
    my $timestamp = $message->[TIMESTAMP] - $message->[TIMESTAMP] % 60;
    my $partition = ( $message->[FROM] =~ m{(\d+)$} )[0];
    my $offset    = ( split m{:}, $message->[ID], 2 )[0] // 0;
    my $response  = Tachikoma::Message->new;
    $response->[TYPE]      = TM_BYTESTREAM | TM_PERSIST;
    $response->[FROM]      = $message->[FROM];
    $response->[TO]        = $self->{owner} || $message->[TO];
    $response->[ID]        = $message->[ID];
    $response->[STREAM]    = $timestamp;
    $response->[TIMESTAMP] = $message->[TIMESTAMP];
    $response->[PAYLOAD] =
        length( $message->[PAYLOAD] )
        ? "$partition:$offset\n"
        : q();
    return $self->{sink}->fill($response);
}

1;
