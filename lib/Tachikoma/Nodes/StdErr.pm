#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::StdErr
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::StdErr;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TM_BYTESTREAM );
use parent             qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

sub help {
    my $self = shift;
    return <<'EOF';
make_node StdErr <node name>
EOF
}

sub fill {
    my $self    = shift;
    my $message = shift;
    $self->{counter}++;
    $self->stderr( $message->payload )
        if ( $message->type & TM_BYTESTREAM );
    if ( length $self->{owner} ) {
        $self->SUPER::fill($message);
    }
    else {
        $self->cancel($message);
    }
    return;
}

1;
