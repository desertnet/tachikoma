#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::HexDump
# ----------------------------------------------------------------------
#

package Accessories::Nodes::HexDump;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_BYTESTREAM
);
use parent qw( Tachikoma::Node );

our $VERSION = '2.1.144';

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{offset} = 0;
    bless $self, $class;
    return $self;
}

sub help {
    return <<'END';
usage: make_node HexDump <node name>
    Display incoming messages in hexadecimal format with ASCII representation
END
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $payload = $message->[PAYLOAD];
    my $offset  = $self->{offset};
    my $output  = q();

    ## no critic (ProhibitCStyleForLoops)
    for ( my $i = 0; $i < length $payload; $i += 16 ) {

        # Print offset
        $output .= sprintf '%08x  ', $offset + $i;

        my $chunk = substr $payload, $i, 16;
        my @bytes = unpack 'C*', $chunk;

        # Print hex values
        for ( my $j = 0; $j < 16; $j++ ) {
            if ( $j < @bytes ) {
                $output .= sprintf '%02x ', $bytes[$j];
            }
            else {
                $output .= q(   );
            }
            $output .= q( ) if $j == 7;    # Extra space after 8 bytes
        }

        # Print ASCII representation
        $output .= q( |);
        for ( my $j = 0; $j < length $chunk; $j++ ) {
            my $char = substr $chunk, $j, 1;
            $output .= ( $char =~ m{[[:print:]]} ) ? $char : q(.);
        }
        $output .= qq(|\n);
    }

    $self->{offset} += length $payload;

    # Create new message with hex dump
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = $message->[TYPE];
    $response->[FROM]    = $message->[FROM];
    $response->[TO]      = $message->[TO];
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = $output;
    $self->SUPER::fill($response);
    return;
}

1;
