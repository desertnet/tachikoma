#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::DirWatcher
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::DirWatcher;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE FROM TO PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.367');

my $CHECK_INTERVAL_MIN = 0.02;
my $CHECK_INTERVAL_MAX = 1;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{dirs}  = {};
    $self->{queue} = [];
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node DirWatcher <node name>
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
    my $dir = ( $message->[PAYLOAD] =~ m{^(/.*)$} )[0];
    return if ( not $dir );
    $self->{dirs}->{$dir} = -e $dir ? ( stat _ )[9] : undef;
    $self->queue if ( not $self->{timer_is_active} );
    return;
}

sub fire {
    my $self      = shift;
    my $dir       = shift @{ $self->queue } or return;
    my $old_mtime = $self->{dirs}->{$dir};
    my $new_mtime = undef;
    my $payload   = undef;
    if ( -e $dir ) {
        $new_mtime = ( stat _ )[9];
        if ( defined $old_mtime ) {
            if ( $new_mtime != $old_mtime ) {
                $payload = "$self->{name}: dir $dir mtime changed\n";
            }
        }
        else {
            $payload = "$self->{name}: dir $dir created\n";
        }
    }
    elsif ( defined $old_mtime ) {
        $payload = "$self->{name}: dir $dir missing\n";
    }
    $self->{dirs}->{$dir} = $new_mtime;
    if ($payload) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE] = TM_BYTESTREAM;
        $message->[FROM] = $self->{name};
        $message->[TO]   = $self->{owner};
        $message->payload($payload);
        $self->{counter}++;
        $self->{sink}->fill($message);
    }
    return;
}

sub dirs {
    my $self = shift;
    if (@_) {
        $self->{dirs} = shift;
    }
    return $self->{dirs};
}

sub queue {
    my $self = shift;
    if (@_) {
        $self->{queue} = shift;
    }
    if ( not @{ $self->{queue} } ) {
        my $queue = [ sort keys %{ $self->{dirs} } ];
        $self->{queue} = $queue;
        my $count = @{$queue};
        if ($count) {
            my $time = $CHECK_INTERVAL_MAX / $count;
            $time = $CHECK_INTERVAL_MIN if ( $time < $CHECK_INTERVAL_MIN );
            $self->set_timer( $time * 1000 );
        }
        else {
            $self->stop_timer;
        }
    }
    return $self->{queue};
}

1;
