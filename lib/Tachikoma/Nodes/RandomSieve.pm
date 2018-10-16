#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::RandomSieve
# ----------------------------------------------------------------------
#
# $Id: RandomSieve.pm 5634 2010-05-14 23:48:15Z chris $
#

package Tachikoma::Nodes::RandomSieve;
use strict;
use warnings;
use Tachikoma::Nodes::Echo;
use Tachikoma::Message qw( TYPE FROM TO ID STREAM PAYLOAD TM_ERROR );
use parent qw( Tachikoma::Nodes::Echo );

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
        $self->{arguments} = shift;
        $self->{probability} = $self->{arguments} || 50;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
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
