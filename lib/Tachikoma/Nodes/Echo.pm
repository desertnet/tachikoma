#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Echo
# ----------------------------------------------------------------------
#
# $Id: Echo.pm 37661 2019-06-19 00:33:01Z chris $
#

package Tachikoma::Nodes::Echo;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE FROM TO TM_ERROR );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

sub help {
    my $self = shift;
    return <<'EOF';
make_node Echo <node name>
EOF
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $owner   = $self->{owner};
    my $to      = $message->[TO];
    return if ( $message->[TYPE] == TM_ERROR and not $to );
    $message->[TO] = join q(/), $owner, $to if ( $owner and $to );
    $message->[TO] = $message->[FROM] if ( not $owner and not $to );
    return $self->SUPER::fill($message);
}

1;
