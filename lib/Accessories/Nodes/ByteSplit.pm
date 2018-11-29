#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::ByteSplit
# ----------------------------------------------------------------------
#
# $Id: ByteSplit.pm 13824 2012-06-22 03:51:50Z chris $
#

package Accessories::Nodes::ByteSplit;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Nodes::Timer );

my $Default_Timeout = 60;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{num_bytes}   = 1;
    $self->{byte_buffer} = '';
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my $num_bytes = ( $self->{arguments} =~ m(^(\d+)$) )[0];
        $self->{num_bytes} = $num_bytes;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $num_bytes = $self->{num_bytes};
    my $payload   = $self->{byte_buffer} . $message->[PAYLOAD];
    my $got       = length($payload);
    $self->{counter}++;
    while ( $got >= $num_bytes ) {
        my $bytes       = substr( $payload, 0, $num_bytes );
        my $new_payload = substr( $payload, $num_bytes );
        $payload = $new_payload;
        $got -= $num_bytes;
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_BYTESTREAM;
        $response->[PAYLOAD] = $bytes;
        $self->SUPER::fill($response);
    }
    $self->{byte_buffer} = $payload;
    $self->set_timer( $Default_Timeout * 1000, 'oneshot' );
    return 1;
}

sub fire {
    my $self = shift;
    $self->{byte_buffer} = '';
    return;
}

sub num_bytes {
    my $self = shift;
    if (@_) {
        $self->{num_bytes} = shift;
    }
    return $self->{num_bytes};
}

sub byte_buffer {
    my $self = shift;
    if (@_) {
        $self->{byte_buffer} = shift;
    }
    return $self->{byte_buffer};
}

1;
