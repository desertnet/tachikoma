#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Substr
# ----------------------------------------------------------------------
#
# $Id: Substr.pm 11165 2011-08-03 03:11:07Z chris $
#

package Tachikoma::Nodes::Substr;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( PAYLOAD );
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
        my $pattern = $self->{arguments} || q{(.*)};
        $pattern = ( $pattern =~ m{^(.*)$} )[0];
        $self->{pattern} = qr{$pattern};
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $payload = $message->[PAYLOAD];
    my @matches = $payload =~ m{$self->{pattern}};
    return $self->cancel($message) if ( not @matches );
    my $copy = bless [ @{$message} ], ref $message;
    $payload = join q{}, @matches;
    $payload .= "\n" if ( substr( $payload, -1, 1 ) ne "\n" );
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
