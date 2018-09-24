#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::SilentDeFlapper
# ----------------------------------------------------------------------
#
# $Id: SilentDeFlapper.pm 9795 2011-01-19 02:43:21Z chris $
#

package Accessories::Nodes::SilentDeFlapper;
use strict;
use warnings;
use Tachikoma::Node;
use parent qw( Tachikoma::Node );

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{p1} = '';
    $self->{p2} = '';
    $self->{p3} = '';
    bless $self, $class;
    return $self;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $payload = $message->payload;
    chomp($payload);
    if ( $payload eq $self->{p2} ) {
        if ( $self->{p1} ne $self->{p3} ) {
            $self->{p3} = $self->{p2};
            $self->{p2} = $self->{p1};
            $self->{p1} = $payload;
            return;
        }
        $self->{p3} = $self->{p2};
        $self->{p2} = $self->{p1};
        $self->{p1} = $payload;
        return;
    }
    $self->{p3} = $self->{p2};
    $self->{p2} = $self->{p1};
    $self->{p1} = $payload;
    return $self->SUPER::fill($message);
}

sub p1 {
    my $self = shift;
    if (@_) {
        $self->{p1} = shift;
    }
    return $self->{p1};
}

sub p2 {
    my $self = shift;
    if (@_) {
        $self->{p2} = shift;
    }
    return $self->{p2};
}

sub p3 {
    my $self = shift;
    if (@_) {
        $self->{p3} = shift;
    }
    return $self->{p3};
}

1;
