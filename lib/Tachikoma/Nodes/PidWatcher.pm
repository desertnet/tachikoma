#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::PidWatcher
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::PidWatcher;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE FROM TO PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.367');

my $Check_Interval_Min = 0.02;
my $Check_Interval_Max = 10;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{pids}  = {};
    $self->{queue} = [];
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node PidWatcher <node name>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $pid = ( $message->[PAYLOAD] =~ m{^(\d+)$} )[0];
    return if ( not $pid );
    $self->{pids}->{$pid} = 1;
    $self->queue if ( not $self->{timer_is_active} );
    return;
}

sub fire {
    my $self = shift;
    my $pid  = shift @{ $self->queue } or return;
    return if ( kill 0, $pid or $! ne 'No such process' );
    my $message = Tachikoma::Message->new;
    $message->[TYPE] = TM_BYTESTREAM;
    $message->[FROM] = $self->{name};
    $message->[TO]   = $self->{owner};
    $message->payload("$self->{name}: process $pid exit\n");
    $self->{counter}++;
    $self->{sink}->fill($message);
    delete $self->{pids}->{$pid};
    return;
}

sub pids {
    my $self = shift;
    if (@_) {
        $self->{pids} = shift;
    }
    return $self->{pids};
}

sub queue {
    my $self = shift;
    if (@_) {
        $self->{queue} = shift;
    }
    if ( not @{ $self->{queue} } ) {
        my $queue = [ sort keys %{ $self->{pids} } ];
        $self->{queue} = $queue;
        my $count = @{$queue};
        if ($count) {
            my $time = $Check_Interval_Max / $count;
            $time = $Check_Interval_Min if ( $time < $Check_Interval_Min );
            $self->set_timer( $time * 1000 );
        }
        else {
            $self->stop_timer;
        }
    }
    return $self->{queue};
}

1;
