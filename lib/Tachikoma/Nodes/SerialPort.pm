#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::SerialPort
# ----------------------------------------------------------------------
#
# $Id: SerialPort.pm 35179 2018-10-14 09:45:33Z chris $
#

package Tachikoma::Nodes::SerialPort;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::STDIO;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw(
    TYPE FROM STREAM
    TM_BYTESTREAM TM_PERSIST TM_COMMAND TM_EOF
);
use Device::SerialPort;
use POSIX qw( F_SETFL O_NONBLOCK );
use parent qw( Tachikoma::Nodes::STDIO );

use version; our $VERSION = 'v2.0.368';

my %C = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{type}       = 'filehandle';
    $self->{drain_fh}   = \&drain_fh;
    $self->{wait_timer} = undef;
    $self->{waiting}    = undef;
    $self->{settings}   = {
        baudrate  => 57600,
        databits  => 8,
        parity    => 'none',
        stopbits  => 1,
        handshake => undef,
        user_msg  => undef
    };
    $self->{interpreter} = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ( $filename, $delay, $max_unanswered ) = split q{ }, $arguments, 3;
        $filename = ( $filename =~ m{^(.*)$} )[0];
        $self->close_filehandle if ( $self->{fh} );
        ## no critic (ProhibitLocalVars)
        ## no critic (RequireInitializationForLocalVars)
        ## no critic (ProhibitTies)
        local *FH;
        tie *FH, 'Device::SerialPort', $filename
            or die "couldn't create Device::SerialPort for $filename: $!";
        $self->{arguments}      = $arguments;
        $self->{filename}       = $filename;
        $self->{delay}          = $delay;
        $self->{waiting}        = 'true' if ($delay);
        $self->{msg_unanswered} = 0;
        $self->{max_unanswered} = $max_unanswered;
        $self->{fh}             = *FH;
        $self->{fd}             = fileno FH;
        $self->apply_settings;
        Tachikoma->nodes_by_fd->{ $self->{fd} } = $self;
        $self->register_reader_node;
    }
    return $self->{arguments};
}

sub apply_settings {
    my $self = shift;
    ## no critic (ProhibitLocalVars)
    local *FH = $self->{fh};
    my $portobj  = tied *FH;
    my $settings = $self->{settings};
    for my $key ( keys %{$settings} ) {
        my $value = $settings->{$key};
        next if ( not defined $value );
        $portobj->$key($value);
    }
    $portobj->write_settings;
    return;
}

sub drain_fh {
    my $self = shift;
    if ( $self->{waiting} ) {
        $self->unregister_reader_node;
        $self->wait_timer->set_timer( $self->{delay}, 'oneshot' );
        return;
    }
    elsif ( $self->{delay} ) {
        $self->{waiting} = 'true';
    }
    return $self->SUPER::drain_fh(@_);
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_COMMAND or $message->[TYPE] & TM_EOF ) {
        return $self->interpreter->fill($message);
    }
    elsif ( not $message->[FROM] and $message->[STREAM] eq 'wait_timer' ) {
        $self->register_reader_node;
        $self->{waiting} = undef;
        return;
    }
    else {
        return $self->SUPER::fill($message);
    }
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
        "commands: list_settings\n" . "          set <setting> <value>\n" );
};

$C{list_settings} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $settings = $self->patron->settings;
    my $response = [ [ [ 'SETTING' => 'left' ], [ 'VALUE' => 'right' ] ] ];
    for my $key ( keys %{$settings} ) {
        my $value = $settings->{$key};
        $value = 'undef' if ( not defined $value );
        push @{$response}, [ $key, $value ];
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$C{ls} = $C{list_settings};

$C{set} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $settings = $self->patron->settings;
    ## no critic (ProhibitLocalVars)
    local *FH = $self->patron->{fh};
    my $portobj = tied *FH;
    my ( $key, $value ) = split q{ }, $command->arguments, 2;
    if ( not exists $settings->{$key} ) {
        return $self->error( $envelope, "no such setting: $key\n" );
    }
    $settings->{$key} = $value;
    $self->patron->apply_settings;
    return $self->okay($envelope);
};

sub wait_timer {
    my $self = shift;
    if (@_) {
        $self->{wait_timer} = shift;
    }
    if ( not defined $self->{wait_timer} ) {
        $self->{wait_timer} = Tachikoma::Nodes::Timer->new;
        $self->{wait_timer}->stream('wait_timer');
        $self->{wait_timer}->sink($self);
    }
    return $self->{wait_timer};
}

sub remove_node {
    my $self = shift;
    $self->{wait_timer}->remove_node if ( $self->{wait_timer} );
    $self->SUPER::remove_node;
    return;
}

sub delay {
    my $self = shift;
    if (@_) {
        $self->{delay} = shift;
    }
    return $self->{delay};
}

sub waiting {
    my $self = shift;
    if (@_) {
        $self->{waiting} = shift;
    }
    return $self->{waiting};
}

sub settings {
    my $self = shift;
    if (@_) {
        $self->{settings} = shift;
    }
    return $self->{settings};
}

sub owner {
    my $self = shift;
    if (@_) {
        $self->{owner}          = shift;
        $self->{msg_unanswered} = 0;
        $self->register_reader_node if ( $self->{fh} );
    }
    return $self->{owner};
}

1;
