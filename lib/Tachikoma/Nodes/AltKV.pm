#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::AltKV
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::AltKV;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_STORABLE
);
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.686');

sub help {
    my $self = shift;
    return <<'EOF';
make_node AltKV <node name> [ "flatten" | "expand" ]
EOF
}

sub fill {
    my $self     = shift;
    my $message  = shift;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]      = TM_BYTESTREAM;
    $response->[FROM]      = $message->[FROM];
    $response->[TO]        = $message->[TO];
    $response->[ID]        = $message->[ID];
    $response->[TIMESTAMP] = $message->[TIMESTAMP];

    if ( $message->[TYPE] & TM_BYTESTREAM ) {

        if ( $self->{arguments} eq 'expand' ) {
            $self->expand_bytestream( $message, $response );
        }
        else {
            $self->flatten_bytestream( $message, $response );
        }
    }
    elsif ( $message->[TYPE] & TM_STORABLE ) {
        if ( $self->{arguments} eq 'expand' ) {
            $self->expand_storable( $message, $response );
        }
        else {
            $self->flatten_storable( $message, $response );
        }
    }
    else {
        return;
    }
    return $self->SUPER::fill($response);
}

sub expand_bytestream {
    my ( $self, $message, $response ) = @_;
    my %new = split q( ), $message->[PAYLOAD];
    if ( scalar keys %new > 1 ) {
        $self->print_less_often('ERROR: expand batches not supported');
    }
    else {
        for my $key ( keys %new ) {
            my $value = $new{$key};
            chomp $value;
            $response->[STREAM]  = $key;
            $response->[PAYLOAD] = $value . "\n";
        }
    }
    return;
}

sub flatten_bytestream {
    my ( $self, $message, $response ) = @_;
    my $key   = $message->[STREAM];
    my $value = $message->[PAYLOAD];
    chomp $value;
    $response->[STREAM]  = q();
    $response->[PAYLOAD] = join q(), $key, q( ), $value, "\n";
    return;
}

sub expand_storable {
    my ( $self, $message, $response ) = @_;
    $self->print_less_often('ERROR: expand storable not supported');
    return;
}

sub flatten_storable {
    my ( $self, $message, $response ) = @_;
    my $payload = $message->payload;
    $response->[STREAM]  = q();
    $response->[PAYLOAD] = q();
    for my $key ( keys %{$payload} ) {
        my $value = $payload->{$key};
        chomp $value;
        $response->[PAYLOAD] .= join q(), $key, q( ), $value, "\n";
    }
    return;
}

1;
