#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::SetType
# ----------------------------------------------------------------------
#
# $Id: SetType.pm 9709 2011-01-12 00:01:13Z chris $
#

package Tachikoma::Nodes::SetType;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE TM_BYTESTREAM TM_INFO TM_PERSIST );
use parent qw( Tachikoma::Node );

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
