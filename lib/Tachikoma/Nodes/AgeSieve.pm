#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::AgeSieve
# ----------------------------------------------------------------------
#
# $Id: AgeSieve.pm 5634 2010-05-14 23:48:15Z chris $
#

package Tachikoma::Nodes::AgeSieve;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TIMESTAMP );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{max_age}     = undef;
    $self->{should_warn} = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node AgeSieve <node name> [ <max age> [ should_warn ] ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $max_age, $should_warn ) = split q( ), $self->{arguments}, 2;
        $self->{max_age} = $max_age // 900;
        $self->{should_warn} = $should_warn;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $max     = $self->{max_age};
    my $age     = $Tachikoma::Now - $message->[TIMESTAMP];
    if ( $age > $max ) {
        $self->print_less_often("WARNING: age > $max - dropping messages")
            if ( $self->{should_warn} );
        return $self->cancel($message);
    }
    return $self->SUPER::fill($message);
}

sub max_age {
    my $self = shift;
    if (@_) {
        $self->{max_age} = shift;
    }
    return $self->{max_age};
}

sub should_warn {
    my $self = shift;
    if (@_) {
        $self->{should_warn} = shift;
    }
    return $self->{should_warn};
}

1;
