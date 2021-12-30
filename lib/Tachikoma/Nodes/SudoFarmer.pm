#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::SudoFarmer
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::SudoFarmer;
use strict;
use warnings;
use Tachikoma::Nodes::JobFarmer;
use parent qw( Tachikoma::Nodes::JobFarmer );

use version; our $VERSION = qv('v2.0.367');

sub help {
    my $self = shift;
    return <<'EOF';
make_node SudoFarmer <node name> <username> <count> <type> [ <arguments> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ( $username, $farmer_arguments ) = split q( ), $arguments, 2;
        die "no username specified\n" if ( not $username );
        $self->job_controller->username($username);
        $self->SUPER::arguments($farmer_arguments);
        $self->{arguments} = $arguments;
    }
    return $self->{arguments};
}

1;
