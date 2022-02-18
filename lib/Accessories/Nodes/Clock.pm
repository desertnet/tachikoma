#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::Clock
# ----------------------------------------------------------------------
#
# $Id: Clock.pm 9795 2011-01-19 02:43:21Z chris $
#

package Accessories::Nodes::Clock;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use POSIX qw( strftime );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub help {
    my $self = shift;
    return <<'EOF';
make_node Clock <node name>
EOF
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $format  = $Tachikoma::Now % 2 ? '%H.%M' : '%H%M';
    $message->[PAYLOAD] = strftime( $format, localtime $message->[PAYLOAD] )
        if ( $message->[TYPE] & TM_BYTESTREAM );
    return $self->SUPER::fill($message);
}

1;
