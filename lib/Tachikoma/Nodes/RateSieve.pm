#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::RateSieve
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::RateSieve;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TIMESTAMP );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

my $Default_Window = 60;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{max_rate}    = undef;
    $self->{window}      = undef;
    $self->{should_warn} = undef;
    $self->{timestamps}  = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node RateSieve <node name> <max rate> [ <window> [ should_warn ] ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $max_rate, $window, $should_warn ) =
            split q( ), $self->{arguments}, 3;
        $self->{max_rate}    = $max_rate // 1;
        $self->{window}      = $window || $Default_Window;
        $self->{should_warn} = $should_warn;
        $self->{timestamps}  = [];
    }
    return $self->{arguments};
}

sub fill {
    my $self       = shift;
    my $message    = shift;
    my $window     = $self->{window};
    my $timestamps = $self->{timestamps};
    my $max_rate   = $self->{max_rate};
    shift @{$timestamps}
        while ( @{$timestamps}
        and $Tachikoma::Now - $timestamps->[0] >= $window );
    my $rate = @{$timestamps} / $window;
    if ( $rate > $max_rate ) {
        $self->print_less_often(
            "WARNING: rate $rate > $max_rate - dropping messages")
            if ( $self->{should_warn} );
        return $self->cancel($message);
    }
    push @{$timestamps}, $message->[TIMESTAMP];
    return $self->SUPER::fill($message);
}

sub max_rate {
    my $self = shift;
    if (@_) {
        $self->{max_rate} = shift;
    }
    return $self->{max_rate};
}

sub window {
    my $self = shift;
    if (@_) {
        $self->{window} = shift;
    }
    return $self->{window};
}

sub should_warn {
    my $self = shift;
    if (@_) {
        $self->{should_warn} = shift;
    }
    return $self->{should_warn};
}

sub timestamps {
    my $self = shift;
    if (@_) {
        $self->{timestamps} = shift;
    }
    return $self->{timestamps};
}

1;
