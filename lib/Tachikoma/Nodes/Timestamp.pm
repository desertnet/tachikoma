#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Timestamp
# ----------------------------------------------------------------------
#
# $Id: Timestamp.pm 17140 2013-07-17 06:12:25Z chris $
#

package Tachikoma::Nodes::Timestamp;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE TIMESTAMP PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{position} = 'prefix';
    $self->{offset}   = 0;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Timestamp <node name> [ "prefix" | "suffix" ] [ <offset> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $position, $offset ) = split q( ), $self->{arguments}, 2;
        $self->{position} = $position // 'prefix';
        $self->{offset}   = $offset // 0;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->SUPER::fill($message)
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $copy   = bless [ @{$message} ], ref $message;
    my $offset = $self->{offset};
    my $out    = q();
    if ( $self->{position} eq 'prefix' ) {
        for my $line ( split m{^}, $message->[PAYLOAD] ) {
            chomp $line;
            $out .= join q(), $message->[TIMESTAMP] + $offset, q( ), $line,
                "\n";
        }
    }
    else {
        for my $line ( split m{^}, $message->[PAYLOAD] ) {
            chomp $line;
            $out .= join q(), $line, q( ), $message->[TIMESTAMP] + $offset,
                "\n";
        }
    }
    $copy->[PAYLOAD] = $out;
    return $self->SUPER::fill($copy);
}

sub position {
    my $self = shift;
    if (@_) {
        $self->{position} = shift;
    }
    return $self->{position};
}

sub offset {
    my $self = shift;
    if (@_) {
        $self->{offset} = shift;
    }
    return $self->{offset};
}

1;
