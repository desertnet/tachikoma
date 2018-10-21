#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::IndexByHostname
# ----------------------------------------------------------------------
#
# $Id: IndexByHostname.pm 3511 2009-10-08 00:18:42Z chris $
#

package Accessories::Nodes::IndexByHostname;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_PERSIST
);
use parent qw( Tachikoma::Node );

sub fill {
    my $self    = shift;
    my $message = shift;
    $self->{counter}++;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $partition = ( $message->[FROM] =~ m{(\d+)$} )[0];
    my $offset    = ( split m{:}, $message->[ID], 2 )[0] // 0;
    my $hostname  = ( split q( ), $message->[PAYLOAD], 5 )[3] // '-';
    my $response  = Tachikoma::Message->new;
    $response->[TYPE]      = TM_BYTESTREAM | TM_PERSIST;
    $response->[FROM]      = $message->[FROM];
    $response->[TO]        = $self->{owner} || $message->[TO];
    $response->[ID]        = $message->[ID];
    $response->[STREAM]    = $hostname;
    $response->[TIMESTAMP] = $message->[TIMESTAMP];
    $response->[PAYLOAD]   = "$partition:$offset\n";
    return $self->{sink}->fill($response);
}

1;
