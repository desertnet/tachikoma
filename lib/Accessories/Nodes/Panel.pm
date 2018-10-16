#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::Panel
# ----------------------------------------------------------------------
#
# $Id$
#

package Accessories::Nodes::Panel;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TM_BYTESTREAM );
use Getopt::Long qw( GetOptionsFromString );
use parent qw( Tachikoma::Nodes::Timer );

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new(@_);
    $self->{format_string} = '';
    $self->{slots}         = [];
    $self->{values}        = {};
    $self->{timestamps}    = {};
    $self->{timeout}       = undef;
    $self->{update_hz}     = 1;
    $self->{verbose}       = 0;
    bless $self, $class;
    $self->set_timer( ( 1 / $self->{update_hz} ) * 1000 );
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments     = shift;
        my $format_string = '';
        my $slots         = '';
        my $timeout       = undef;
        my ( $r, $argv ) = GetOptionsFromString(
            $arguments,
            'format_string=s' => \$format_string,
            'slots=s'         => \$slots,
            'timeout=s'       => \$timeout,
        );
        die "invalid option" if ( not $r );
        die "invalid option" if (@$argv);
        $self->{format_string} = $format_string;
        $self->{timeout} = $timeout if ( defined $timeout );
        @{ $self->{slots} } = split( /,/, $slots );

        foreach my $slot ( @{ $self->{slots} } ) {
            $self->{values}->{$slot} = '';
        }
        $self->{arguments} = $arguments;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( not $message->type & TM_BYTESTREAM ) {
        return $self->SUPER::fill($message);
    }
    my $payload = $message->payload;
    if ( $payload =~ /^(\w+):\s?(.*?)$/ ) {
        my $slot = $1;
        my $val  = $2;
        $self->{values}->{$slot}     = $val;
        $self->{timestamps}->{$slot} = $Tachikoma::Right_Now;
    }
}

sub fire {
    my $self = shift;
    my $now  = $Tachikoma::Right_Now;
    my $fmt  = $self->{format_string};
    my @values;
    foreach my $slot ( @{ $self->{slots} } ) {
        my $ts  = $self->{timestamps}->{$slot};
        my $val = $self->{values}->{$slot} // '';
        if ( defined $self->{timeout} ) {
            if ( ( $now - $ts ) > $self->{timeout} ) {
                $val = '';
            }
        }
        push( @values, $val );
    }

    my $msg = Tachikoma::Message->new;
    $msg->type(TM_BYTESTREAM);
    $msg->to( $self->{owner} );
    $msg->payload( sprintf( $fmt, @values ) );
    $Tachikoma::Nodes{"_router"}->fill($msg);
}

1;
