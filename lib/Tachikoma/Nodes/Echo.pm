#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Echo
# ----------------------------------------------------------------------
#
# $Id: Echo.pm 34403 2018-07-05 19:03:14Z chris $
#

package Tachikoma::Nodes::Echo;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( FROM TO );
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
    $message->[TO] = join q{/}, $owner, $to if ( $owner and $to );
    $message->[TO] = $message->[FROM] if ( not $owner and not $to );
    return $self->SUPER::fill($message);
}

sub activate {    ## no critic (RequireArgUnpacking, RequireFinalReturn)
    $_[0]->{edge}->activate( $_[1] );
}

1;
