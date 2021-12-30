#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Substr
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Substr;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{pattern} = qr{(.*)};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Substr <node name> [ <regex> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my $pattern = $self->{arguments} || q((.*));
        $self->{pattern} = qr{$pattern};
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $payload = $message->[PAYLOAD];
    my @matches = $payload =~ m{$self->{pattern}};
    return $self->cancel($message) if ( not @matches );
    my $newline = substr( $payload, -1, 1 ) eq "\n" ? 1 : undef;
    my $copy = bless [ @{$message} ], ref $message;
    $payload = join q(), @matches;
    $payload .= "\n" if ( $newline and substr( $payload, -1, 1 ) ne "\n" );
    $copy->[PAYLOAD] = $payload;
    return $self->SUPER::fill($copy);
}

sub pattern {
    my $self = shift;
    if (@_) {
        $self->{pattern} = shift;
    }
    return $self->{pattern};
}

1;
