#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Gate
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Nodes::Gate;
use strict;
use warnings;
use Tachikoma::Nodes::Echo;
use parent qw( Tachikoma::Nodes::Echo );

sub help {
    my $self = shift;
    return <<'EOF';
make_node Gate <node name> [ "open" ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift || '';
        my $old_arguments = $self->{arguments} || '';
        $self->stderr($arguments) if ( $arguments ne $old_arguments );
        $self->{arguments} = $arguments;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $owner   = $self->{owner};
    return $self->SUPER::fill($message) if ( $self->{arguments} eq 'open' );
    return $self->cancel($message);
}

1;
