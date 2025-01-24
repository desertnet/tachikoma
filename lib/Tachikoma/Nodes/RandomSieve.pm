#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::RandomSieve
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::RandomSieve;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE FROM TO ID STREAM PAYLOAD TM_ERROR TM_EOF );
use parent             qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{probability} = 0;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node RandomSieve <node name> [ <probability> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments}   = shift;
        $self->{probability} = $self->{arguments} || 50;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( $message->[TYPE] & TM_ERROR or $message->[TYPE] & TM_EOF );
    if ( rand(100) >= $self->{probability} ) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_ERROR;
        $response->[FROM]    = $message->[TO] // q();
        $response->[TO]      = $message->[FROM];
        $response->[ID]      = $message->[ID];
        $response->[STREAM]  = $message->[STREAM];
        $response->[PAYLOAD] = "NOT_AVAILABLE\n";
        return $self->{sink}->fill($response);
    }
    return $self->SUPER::fill($message);
}

sub probability {
    my $self = shift;
    if (@_) {
        $self->{probability} = shift;
    }
    return $self->{probability};
}

1;
