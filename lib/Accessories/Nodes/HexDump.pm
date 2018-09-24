#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::HexDump
# ----------------------------------------------------------------------
#
# $Id: HexDump.pm 5634 2010-05-14 23:48:15Z chris $
#

package Accessories::Nodes::HexDump;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Node );

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->SUPER::fill($message)
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $copy = bless( [@$message], ref($message) );
    $copy->[PAYLOAD] = join( ' ',
        ( map { sprintf( "%02X", ord($_) ) } split( '', $copy->[PAYLOAD] ) ),
        "\n" );
    return $self->SUPER::fill($copy);
}

1;
