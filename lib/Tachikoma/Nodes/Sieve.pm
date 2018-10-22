#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Sieve
# ----------------------------------------------------------------------
#
# $Id: Sieve.pm 5634 2010-05-14 23:48:15Z chris $
#

package Tachikoma::Nodes::Sieve;
use strict;
use warnings;
use Tachikoma::Node;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{min_size}    = 0;
    $self->{max_size}    = undef;
    $self->{should_warn} = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Sieve <node name> <min size> [ <max size> [ should_warn ] ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $min_size, $max_size, $should_warn ) =
            split q( ), $self->{arguments}, 3;
        $self->{min_size}    = $min_size // 0;
        $self->{max_size}    = $max_size;
        $self->{should_warn} = $should_warn;
    }
    return $self->{arguments};
}

sub fill {
    my $self     = shift;
    my $message  = shift;
    my $min_size = $self->{min_size};
    my $max_size = $self->{max_size};
    my $size     = $message->size;
    if ( $size < $min_size ) {
        $self->print_less_often(
            "WARNING: size $size < $min_size - dropping messages")
            if ( $self->{should_warn} );
        return $self->cancel($message);
    }
    elsif ( $max_size and $size > $max_size ) {
        $self->print_less_often(
            "WARNING: size $size > $max_size - dropping messages")
            if ( $self->{should_warn} );
        return $self->cancel($message);
    }
    return $self->SUPER::fill($message);
}

sub min_size {
    my $self = shift;
    if (@_) {
        $self->{min_size} = shift;
    }
    return $self->{min_size};
}

sub max_size {
    my $self = shift;
    if (@_) {
        $self->{max_size} = shift;
    }
    return $self->{max_size};
}

sub should_warn {
    my $self = shift;
    if (@_) {
        $self->{should_warn} = shift;
    }
    return $self->{should_warn};
}

1;
