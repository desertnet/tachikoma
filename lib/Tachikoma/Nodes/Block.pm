#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Block
# ----------------------------------------------------------------------
#
# $Id: Block.pm 9677 2011-01-08 01:39:41Z chris $
#

package Tachikoma::Nodes::Block;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_BYTESTREAM TM_EOF
);
use parent qw( Tachikoma::Node );

use version; our $VERSION = 'v2.0.367';

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{offset}      = 0;
    $self->{line_buffer} = q{};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Block <node name>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my $offset    = $arguments;
        $self->{arguments} = $arguments;
        $self->{offset} = $offset || 0;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    if ( $type & TM_EOF ) {
        $message->[TO] ||= $self->{owner};
        return $self->{sink}->fill($message);
    }
    return if ( not $type & TM_BYTESTREAM );
    my $offset      = $message->[ID];
    my $line_buffer = q{};
    $line_buffer = $self->{line_buffer} if ( $offset > $self->{offset} );
    my $payload = $line_buffer . $message->[PAYLOAD];
    my $part    = q{};
    if ( substr( $payload, -1, 1 ) ne "\n" ) {

        if ( $payload =~ s{\n(.+)$}{\n} ) {
            $part = $1;
        }
        else {
            $self->{line_buffer} = $payload;
            return $self->cancel($message);
        }
    }
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = $type;
    $response->[FROM]    = $message->[FROM];
    $response->[TO]      = $self->{owner};
    $response->[ID]      = $offset ? $offset - length($part) : 0;
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = $payload;
    $self->{line_buffer} = $part;
    $self->{offset}      = $offset;
    $self->{counter}++;
    return $self->{sink}->fill($response);
}

sub line_buffer {
    my $self = shift;
    if (@_) {
        $self->{line_buffer} = shift;
    }
    return $self->{line_buffer};
}

1;
