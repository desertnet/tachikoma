#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Rewrite
# ----------------------------------------------------------------------
#
# $Id: Rewrite.pm 11165 2011-08-03 03:11:07Z chris $
#

package Tachikoma::Nodes::Rewrite;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.368');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{pattern} = qr{};
    $self->{rewrite} = q();
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Rewrite <node name> [ <pattern> <rewrite> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $pattern, $rewrite ) = split q( ), $self->{arguments}, 2;
        $pattern ||= q();
        $rewrite ||= q();
        $self->{pattern} = qr{$pattern};
        $self->{rewrite} = $rewrite;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $payload = $message->[PAYLOAD];
    my $pattern = $self->{pattern};
    my $rewrite = $self->{rewrite};
    my @matches = $payload =~ m{$pattern};
    $rewrite =~ s{\$$_(?!\d)}{$matches[$_ - 1]}g for ( 1 .. @matches );
    my $newline = substr( $payload, -1, 1 ) eq "\n" ? 1 : undef;
    return $self->cancel($message)
        if ( not $payload =~ s{$pattern}{$rewrite}s );
    my $copy = bless [ @{$message} ], ref $message;
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

sub rewrite {
    my $self = shift;
    if (@_) {
        $self->{rewrite} = shift;
    }
    return $self->{rewrite};
}

1;
