#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Mon
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Nodes::Mon;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use Mon::Client;
use parent qw( Tachikoma::Node );

our %Auth;
my $config = '/usr/local/etc/tachikoma/mon_auth.conf';
require $config;

my $Default_MTU = 1000;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{group}   = undef;
    $self->{service} = undef;
    $self->{mon}     = undef;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $group, $service ) = split( ' ', $self->{arguments}, 2 );
        $self->{group}   = $group   || 'tachikoma_traps';
        $self->{service} = $service || 'tachikoma';
        $self->{mon}     = undef;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    if ( $message->[PAYLOAD] =~ m(^([\d.]+)$) ) {
        my $timestamp = $1;
        if ( $Tachikoma::Now - $timestamp < 60 ) {
            $self->mon->send_trap(
                group    => $self->{group},
                service  => $self->{service},
                retval   => 0,
                opstatus => 'ok'
            ) or $self->stderr( $self->mon->error );
        }
    }
    else {
        my ( $summary, $detail ) =
            split( "\n", substr( $message->[PAYLOAD], 0, $Default_MTU ), 2 );
        if ($summary) {
            $self->mon->send_trap(
                group    => $self->{group},
                service  => $self->{service},
                retval   => 1,
                opstatus => 'fail',
                summary  => $summary,
                detail   => $detail || $summary
            ) or $self->stderr( $self->mon->error );
            $self->stderr("TRAP: $summary");
        }
        else {
            $self->stderr( "ERROR: malformed payload: ",
                $message->[PAYLOAD] );
        }
    }
    $self->{counter}++;
    return;
}

sub mon {
    my $self = shift;
    if (@_) {
        $self->{mon} = shift;
    }
    if ( not $self->{mon} ) {
        $self->{mon} = Mon::Client->new(
            host     => 'localhost',
            port     => 2583,
            username => $Auth{username},
            password => $Auth{password}
        );
    }
    return $self->{mon};
}

sub group {
    my $self = shift;
    if (@_) {
        $self->{group} = shift;
    }
    return $self->{group};
}

sub service {
    my $self = shift;
    if (@_) {
        $self->{service} = shift;
    }
    return $self->{service};
}

1;
