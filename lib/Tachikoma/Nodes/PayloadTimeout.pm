#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::PayloadTimeout
# ----------------------------------------------------------------------
#
# $Id: PayloadTimeout.pm 12579 2012-01-11 04:10:56Z chris $
#

package Tachikoma::Nodes::PayloadTimeout;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_PERSIST
);
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.905');

my $DEFAULT_TIMEOUT = 900;
my $DEFAULT_EXPIRES = 3300;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{timeout}           = undef;
    $self->{expires}           = undef;
    $self->{recently_received} = {};
    $self->{recently_sent}     = {};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node PayloadTimeout <node name> [ <timeout> <expires> <interval> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $timeout, $expires, $interval ) = split q( ),
            $self->{arguments}, 2;
        $timeout  ||= $DEFAULT_TIMEOUT;
        $expires  ||= $DEFAULT_EXPIRES;
        $interval ||= $timeout / 60;
        $self->{timeout}           = $timeout;
        $self->{expires}           = $expires;
        $self->{recently_received} = {};
        $self->{recently_sent}     = {};
        $self->set_timer( $interval * 1000 );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->type & TM_BYTESTREAM );
    my $payload   = $message->[PAYLOAD];
    my $timestamp = $message->[TIMESTAMP];
    return $self->cancel($message)
        if ( not $timestamp
        or $Tachikoma::Now - $timestamp > $self->{expires} );
    my $recently_received = $self->{recently_received};
    my $recently_sent     = $self->{recently_sent};
    my $timeout           = $self->{timeout};
    chomp $payload;
    $recently_received->{$payload} = $Tachikoma::Right_Now + $timeout;
    return $self->cancel($message) if ( $recently_sent->{$payload} );
    $recently_sent->{$payload} = $Tachikoma::Right_Now + $timeout;
    return $self->SUPER::fill($message);
}

sub fire {
    my $self              = shift;
    my $recently_received = $self->{recently_received};
    my $recently_sent     = $self->{recently_sent};
    my $timeout           = $self->{timeout};
    for my $payload ( keys %{$recently_sent} ) {
        my $sent = $recently_sent->{$payload};
        next if ( $Tachikoma::Now <= $sent );
        my $received = $recently_received->{$payload};
        if ( $received and $received > $sent ) {
            $recently_sent->{$payload} = ( $Tachikoma::Right_Now + $timeout );
            my $response = Tachikoma::Message->new;
            $response->[TYPE]    = TM_BYTESTREAM | TM_PERSIST;
            $response->[FROM]    = $self->{name};
            $response->[PAYLOAD] = "$payload\n";
            $self->SUPER::fill($response);
        }
        else {
            delete $recently_sent->{$payload};
        }
    }
    for my $payload ( keys %{$recently_received} ) {
        delete $recently_received->{$payload}
            if ( $Tachikoma::Now > $recently_received->{$payload} );
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

sub expires {
    my $self = shift;
    if (@_) {
        $self->{expires} = shift;
    }
    return $self->{expires};
}

sub recently_received {
    my $self = shift;
    if (@_) {
        $self->{recently_received} = shift;
    }
    return $self->{recently_received};
}

sub recently_sent {
    my $self = shift;
    if (@_) {
        $self->{recently_sent} = shift;
    }
    return $self->{recently_sent};
}

1;
