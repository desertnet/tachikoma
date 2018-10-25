#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Lookup
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Nodes::Lookup;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TM_BYTESTREAM TYPE TO STREAM );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub help {
    my $self = shift;
    return <<'EOF';
make_node Lookup <node name> <Table name>
connect_node     <node name> <on-hit path>
connect_edge     <node name> <on-miss path>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        die "ERROR: bad arguments for Lookup\n"
            if ( not $self->{arguments} );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $table = $Tachikoma::Nodes{ $self->{arguments} };
    return $self->stderr( q(ERROR: couldn't find node ), $self->{arguments} )
        if ( not $table );
    $self->{counter}++;
    if ( defined $table->lookup( $message->[STREAM] ) ) {
        return $self->cancel($message) if ( not length $self->{owner} );
        $message->[TO] = $self->{owner};
        $self->{sink}->fill($message);
    }
    else {
        return $self->cancel($message) if ( not $self->{edge} );
        $self->{edge}->fill($message);
    }
    return;
}

1;
