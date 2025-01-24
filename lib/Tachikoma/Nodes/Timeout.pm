#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Timeout
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Timeout;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE FROM TM_EOF );
use parent             qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.280');

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->set_timer( $self->{arguments}, 'oneshot' );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    $self->set_timer( $self->{arguments}, 'oneshot' );
    return $self->{sink}->fill($message);
}

sub fire {
    my $self    = shift;
    my $message = Tachikoma::Message->new;
    $message->[TYPE] = TM_EOF;
    $message->[FROM] = '_stdin';
    $self->{sink}->fill($message);
    return;
}

1;
