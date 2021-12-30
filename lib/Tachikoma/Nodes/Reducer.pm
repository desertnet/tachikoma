#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Reducer
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Reducer;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.349');

my $Default_Timeout = 900;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{timeout}    = $Default_Timeout;
    $self->{messages}   = {};
    $self->{timestamps} = {};
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $timeout, $interval ) = split q( ), $self->{arguments}, 2;
        $timeout  ||= $Default_Timeout;
        $interval ||= $timeout / 60;
        $self->timeout($timeout);
        $self->set_timer( $interval * 1000 );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( not $message->[TYPE] & TM_BYTESTREAM ) {
        return $self->SUPER::fill($message);
    }
    my $payload    = $message->[PAYLOAD];
    my $messages   = $self->{messages};
    my $timestamps = $self->{timestamps};
    if ( not exists $messages->{$payload} ) {
        $messages->{$payload} = 1;
        $timestamps->{$Tachikoma::Now} ||= [];
        push @{ $timestamps->{$Tachikoma::Now} }, $payload;
        $self->SUPER::fill($message);
    }
    else {
        $self->cancel($message);
    }
    return;
}

sub fire {
    my $self       = shift;
    my $timeout    = $self->{timeout};
    my $messages   = $self->{messages};
    my $timestamps = $self->{timestamps};
    for my $timestamp ( sort { $a <=> $b } keys %{$timestamps} ) {
        last if ( ( $Tachikoma::Now - $timestamp ) < $timeout );
        delete $messages->{$_} for ( @{ $timestamps->{$timestamp} } );
        delete $timestamps->{$timestamp};
    }
    return;
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

sub messages {
    my $self = shift;
    if (@_) {
        $self->{messages} = shift;
    }
    return $self->{messages};
}

sub timestamps {
    my $self = shift;
    if (@_) {
        $self->{timestamps} = shift;
    }
    return $self->{timestamps};
}

1;
