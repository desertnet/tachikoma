#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::FileWatcher
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::FileWatcher;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE FROM TO PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.367');

my $CHECK_INTERVAL_MIN = 0.02;
my $CHECK_INTERVAL_MAX = 10;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{files} = {};
    $self->{queue} = [];
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node FileWatcher <node name>
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
    my $file = ( $message->[PAYLOAD] =~ m{^(/.*)$} )[0];
    return if ( not $file );
    $self->{files}->{$file} = -e $file ? ( stat _ )[1] : undef;
    $self->queue if ( not $self->{timer_is_active} );
    return;
}

sub fire {
    my $self      = shift;
    my $file      = shift @{ $self->queue } or return;
    my $old_inode = $self->{files}->{$file};
    my $new_inode = undef;
    my $payload   = undef;
    if ( -e $file ) {
        $new_inode = ( stat _ )[1];
        if ( defined $old_inode ) {
            if ( $new_inode != $old_inode ) {
                $payload = "$self->{name}: file $file inode changed\n";
            }
        }
        else {
            $payload = "$self->{name}: file $file created\n";
        }
    }
    elsif ( defined $old_inode ) {
        $payload = "$self->{name}: file $file missing\n";
    }
    $self->{files}->{$file} = $new_inode;
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

sub files {
    my $self = shift;
    if (@_) {
        $self->{files} = shift;
    }
    return $self->{files};
}

sub queue {
    my $self = shift;
    if (@_) {
        $self->{queue} = shift;
    }
    if ( not @{ $self->{queue} } ) {
        my $queue = [ sort keys %{ $self->{files} } ];
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
