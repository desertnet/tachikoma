#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_Sigma
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::HTTP_Sigma;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::HTTP_Responder qw( log_entry cached_strftime );
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD TM_BYTESTREAM TM_STORABLE TM_EOF
);
use JSON;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.314');

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_STORABLE );

    # Gather node information
    my @nodes;
    for my $name ( sort keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        push @nodes,
            {
            name  => $name,
            sink  => $node->{sink} ? $node->{sink}->{name} : q(),
            owner => $node->{owner} ? $node->{owner} : q(),
            };
    }

    # Generate the HTML and JavaScript content
    my $content = generate_content( \@nodes );

    # Send the HTTP response
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $message->[FROM];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join q(),
        "HTTP/1.1 200 OK\n",
        'Date: ', cached_strftime(), "\n",
        "Server: Tachikoma\n",
        "Connection: close\n",
        "Content-Type: text/html\n",
        'Content-Length: ',
        length($content),
        "\n\n",
        $content;
    $self->{sink}->fill($response);
    $response         = Tachikoma::Message->new;
    $response->[TYPE] = TM_EOF;
    $response->[TO]   = $message->[FROM];
    $self->{sink}->fill($response);
    $self->{counter}++;
    log_entry( $self, 200, $message );
    return;
}

sub generate_content {
    my $nodes   = shift;
    my $content = <<"END_HTML";
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Tachikoma Nodes Visualization</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/sigma.js/1.2.1/sigma.min.js"></script>
    <style>
        html, body {
            height: 100%;
            margin: 0;
            padding: 0;
        }

        #visualization {
            height: 100%;
            width: 100%;
            border: 0px solid #ccc;
        }
    </style>
</head>
<body>
    <div id="visualization"></div>
    <script>
        var nodesData = @{[ encode_json($nodes) ]};
        var existingNodes = new Set(nodesData.map(function(node) {
            return node.name;
        }));

        var s = new sigma({
            graph: {
                nodes: [],
                edges: []
            },
            container: 'visualization'
        });

        nodesData.forEach(function (node, index) {
            s.graph.addNode({
                id: node.name,
                label: node.name,
                x: Math.random(),
                y: Math.random(),
                size: 1,
                color: '#666'
            });
        });

        nodesData.forEach(function (node, index) {
            if (node.sink && existingNodes.has(node.sink)) {
                s.graph.addEdge({
                    id: 'e-sink-' + index,
                    source: node.name,
                    target: node.sink,
                    color: '#f00',
                    type: 'arrow',
                    label: 'sink'
                });
            }

            if (node.owner && existingNodes.has(node.owner)) {
                s.graph.addEdge({
                    id: 'e-owner-' + index,
                    source: node.name,
                    target: node.owner,
                    color: '#0f0',
                    type: 'arrow',
                    label: 'owner'
                });
            }
        });

        s.refresh();
    </script>
</body>
</html>
END_HTML
    return $content;
}

1;
