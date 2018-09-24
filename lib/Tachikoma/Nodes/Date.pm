#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Date
# ----------------------------------------------------------------------
#
# $Id: Date.pm 3511 2009-10-08 00:18:42Z chris $
#

package Tachikoma::Nodes::Date;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO PAYLOAD
    TM_BYTESTREAM TM_INFO TM_EOF
);
use POSIX qw( strftime );
use parent qw( Tachikoma::Node );

# e.g.:
# make_node Date
# listen_inet --io 0.0.0.0:5432
# register 0.0.0.0:5432 Date connected

sub fill {
    my $self    = shift;
    my $message = shift;
    $self->{counter}++;
    my $response = Tachikoma::Message->new;
    $response->[TYPE] = TM_BYTESTREAM;
    $response->[TO] = $self->{owner} ? $self->{owner} : $message->[FROM];
    $response->[PAYLOAD] =
        strftime( "%F %T %Z\n", localtime($Tachikoma::Now) );
    my $rv = $self->{sink}->fill($response);

    if ( $message->[TYPE] & TM_INFO ) {
        $response         = Tachikoma::Message->new;
        $response->[TYPE] = TM_EOF;
        $response->[TO]   = $message->[FROM];
        $rv               = $self->{sink}->fill($response);
    }
    return $rv;
}

1;
