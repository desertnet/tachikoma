#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::JSON_Visualizer
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::JSONvisualizer;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::HTTP_Responder qw( log_entry cached_strftime );
use Tachikoma::Message               qw(
    TYPE FROM TO STREAM PAYLOAD TM_BYTESTREAM TM_STORABLE TM_EOF
);
use JSON;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.314');

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_STORABLE );

    my @nodes = $self->gather_node_information();

    my $content = encode_json( \@nodes );

    $self->send_http_response( $message, $content );
    $self->{counter}++;
    log_entry( $self, 200, $message );
    return;
}

sub gather_node_information {
    my $self = shift;
    my @nodes;
    for my $name ( sort keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        push @nodes,
            {
            name    => $name,
            sink    => $node->{sink}    ? $node->{sink}->{name} : q(),
            edge    => $node->{edge}    ? $node->{edge}->{name} : q(),
            owner   => $node->{owner}   ? $node->{owner}        : q(),
            counter => $node->{counter} ? $node->{counter}      : 0,
            };
    }
    return @nodes;
}

sub send_http_response {
    my $self = shift;
    my ( $message, $content ) = @_;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $message->[FROM];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join q(),
        "HTTP/1.1 200 OK\n",
        'Date: ', cached_strftime(), "\n",
        "Server: Tachikoma\n",
        "Connection: close\n",
        "Content-Type: application/json\n",
        'Content-Length: ',
        length($content),
        "\n\n",
        $content;
    $self->{sink}->fill($response);
    $response         = Tachikoma::Message->new;
    $response->[TYPE] = TM_EOF;
    $response->[TO]   = $message->[FROM];
    $self->{sink}->fill($response);
}

1;
