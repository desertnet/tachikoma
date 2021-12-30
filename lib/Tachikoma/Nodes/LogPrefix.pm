#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::LogPrefix
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::LogPrefix;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub help {
    my $self = shift;
    return <<'EOF';
make_node LogPrefix <node name>
EOF
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $prefix  = Tachikoma->log_prefix;
    $message->[PAYLOAD] =~ s{^}{$prefix}mg
        if ( $message->[TYPE] & TM_BYTESTREAM );
    return $self->SUPER::fill($message);
}

1;
