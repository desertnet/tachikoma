#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Grep
# ----------------------------------------------------------------------
#
# $Id: Grep.pm 35519 2018-10-22 10:28:36Z chris $
#

package Tachikoma::Nodes::Grep;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( PAYLOAD );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{pattern} = qr{.};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Grep <node name> <regex>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my $pattern = $self->{arguments} || q(.);
        $self->{pattern} = qr{$pattern};
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->cancel($message)
        if ( $message->[PAYLOAD] !~ m{$self->{pattern}} );
    return $self->SUPER::fill($message);
}

sub pattern {
    my $self = shift;
    if (@_) {
        $self->{pattern} = shift;
    }
    return $self->{pattern};
}

1;
