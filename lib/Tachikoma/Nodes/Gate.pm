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
use Tachikoma::Node;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub help {
    my $self = shift;
    return <<'EOF';
make_node Gate <node name> [ "open" ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift || q();
        my $old_arguments = $self->{arguments} || q();
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
