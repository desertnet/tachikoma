#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::SetType
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::SetType;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE TM_BYTESTREAM TM_INFO TM_REQUEST TM_PERSIST );
use parent             qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{message_type} = 'bytestream';
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node SetType <node name> [ <message type> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my $message_type = $self->{arguments} || 'bytestream';
        $self->{message_type} = $message_type;
    }
    return $self->{arguments};
}

sub fill {
    my $self         = shift;
    my $message      = shift;
    my $message_type = $self->{message_type};
    my $persist      = $message->[TYPE] & TM_PERSIST;
    if ( $message_type eq 'bytestream' ) {
        $message->[TYPE] = TM_BYTESTREAM;
    }
    elsif ( $message_type eq 'info' ) {
        $message->[TYPE] = TM_INFO;
    }
    elsif ( $message_type eq 'request' ) {
        $message->[TYPE] = TM_REQUEST;
    }
    $message->[TYPE] |= TM_PERSIST if ($persist);
    return $self->SUPER::fill($message);
}

sub message_type {
    my $self = shift;
    if (@_) {
        $self->{message_type} = shift;
    }
    return $self->{message_type};
}

1;
