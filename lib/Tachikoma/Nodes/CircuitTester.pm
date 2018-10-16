#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::CircuitTester
# ----------------------------------------------------------------------
#
# $Id: CircuitTester.pm 35293 2018-10-16 20:32:45Z chris $
#

package Tachikoma::Nodes::CircuitTester;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw(
    TYPE FROM TO ID PAYLOAD
    TM_COMMAND TM_PING TM_EOF
);
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = 'v2.0.367';

my $Default_Interval = 0.1;
my $Default_Timeout  = 900;
my %C                = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{circuits}         = {};
    $self->{queue}            = [];
    $self->{waiting}          = {};
    $self->{offline}          = {};
    $self->{round_trip_times} = {};
    $self->{interval}         = $Default_Interval;
    $self->{timeout}          = $Default_Timeout;
    $self->{interpreter}      = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node CircuitTester <node name> [ <interval> <timeout> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $interval, $timeout ) =
            split q( ), $self->{arguments}, 3;
        $self->{interval} = $interval || $Default_Interval;
        $self->{timeout}  = $timeout  || $Default_Timeout;
        $self->set_timer( $self->{interval} * 1000 );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_COMMAND or $message->[TYPE] & TM_EOF ) {
        return $self->{interpreter}->fill($message);
    }
    elsif ( $message->[TYPE] & TM_PING ) {
        my $path    = $message->[ID] or return;
        my $waiting = $self->{waiting};
        my $offline = $self->{offline};
        if ( $waiting->{$path} ) {
            my $how_many = $Tachikoma::Now - $waiting->{$path};
            $self->stderr("$path back online after $how_many seconds")
                if ( $offline->{$path} and $how_many > $self->{timeout} );
            delete $waiting->{$path};
            delete $offline->{$path};
            $self->{round_trip_times}->{$path} =
                $Tachikoma::Right_Now - $message->[PAYLOAD];
        }
    }
    $self->{counter}++;
    return;
}

sub fire {
    my $self    = shift;
    my $name    = $self->{name};
    my $waiting = $self->{waiting};
    my $offline = $self->{offline};
    my $path    = undef;
    my $i       = 0;
    do {
        if ( not @{ $self->{queue} } ) {
            $self->{queue} = [ sort keys %{ $self->{circuits} } ];
            $i++;
        }
        $path = shift @{ $self->{queue} };
    } while ( $path and not $self->{circuits}->{$path} and $i <= 1 );
    return if ( not $path );
    if ( $waiting->{$path} ) {
        my $how_many  = $Tachikoma::Now - $waiting->{$path};
        my $how_often = $Tachikoma::Now - ( $offline->{$path} || 0 );
        my $timeout   = $self->{timeout};
        if ( $how_many > $timeout and $how_often > $timeout ) {
            if ( not $offline->{$path} ) {
                $self->stderr("$path has gone offline for $how_many seconds");
            }
            else {
                $self->stderr("no response from $path in $how_many seconds");
            }
            $offline->{$path} = $Tachikoma::Now;
        }
    }
    else {
        $waiting->{$path} = $Tachikoma::Now;
    }
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_PING;
    $message->[FROM]    = $name;
    $message->[TO]      = $path;
    $message->[ID]      = $path;
    $message->[PAYLOAD] = $Tachikoma::Right_Now;
    $self->{sink}->fill($message);
    return;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list_circuits\n"
            . "          add_circuit <path>\n"
            . "          remove_circuit <path>\n"
            . "          disable_circuit <path>\n"
            . "          enable_circuit <path>\n"
            . "          list_waiting\n"
            . "          list_offline\n"
            . "          list_times\n" );
};

$C{list_circuits} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $glob     = $command->arguments;
    my $response = q();
    for my $path ( sort keys %{ $self->patron->circuits } ) {
        $response .= "$path\n" if ( not $glob or $path =~ $glob );
    }
    return $self->response( $envelope, $response );
};

$C{ls} = $C{list_circuits};

$C{add_circuit} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->circuits->{ $command->arguments } = 1;
    return $self->okay;
};

$C{add} = $C{add_circuit};

$C{remove_circuit} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $patron   = $self->patron;
    delete $patron->circuits->{ $command->arguments };
    delete $patron->waiting->{ $command->arguments };
    delete $patron->offline->{ $command->arguments };
    delete $patron->round_trip_times->{ $command->arguments };
    return $self->okay;
};

$C{rm} = $C{remove_circuit};

$C{disable_circuit} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $patron   = $self->patron;
    my $glob     = $command->arguments;
    for my $path ( keys %{ $patron->circuits } ) {
        if ( $path =~ $glob ) {
            $patron->circuits->{$path} = 0;
            delete $patron->waiting->{$path};
            delete $patron->offline->{$path};
            delete $patron->round_trip_times->{$path};
        }
    }
    return $self->okay;
};

$C{disable} = $C{disable_circuit};

$C{enable_circuit} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $patron   = $self->patron;
    my $glob     = $command->arguments;
    for my $path ( keys %{ $patron->circuits } ) {
        $patron->circuits->{$path} = 1 if ( $path =~ $glob );
    }
    return $self->okay;
};

$C{enable} = $C{enable_circuit};

$C{list_waiting} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $waiting  = $self->patron->waiting;
    my $response = q();
    for my $path ( sort keys %{$waiting} ) {
        $response .= sprintf "%-70s %9d\n",
            $path, $Tachikoma::Now - $waiting->{$path};
    }
    $response = "none - all nodes have responded\n" if ( $response eq q() );
    return $self->response( $envelope, $response );
};

$C{waiting} = $C{list_waiting};

$C{list_offline} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $waiting  = $self->patron->waiting;
    my $response = q();
    for my $path ( sort keys %{ $self->patron->offline } ) {
        $response .= sprintf "%-70s %9d\n",
            $path, $Tachikoma::Now - $waiting->{$path};
    }
    $response = "none - all nodes are online\n" if ( $response eq q() );
    return $self->response( $envelope, $response );
};

$C{offline} = $C{list_offline};

$C{list_disabled} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $circuits = $self->patron->circuits;
    my $response = q();
    for my $path ( sort keys %{$circuits} ) {
        $response .= "$path\n" if ( not $circuits->{$path} );
    }
    $response = "none - all nodes are enabled\n" if ( $response eq q() );
    return $self->response( $envelope, $response );
};

$C{disabled} = $C{list_disabled};

$C{list_times} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $patron   = $self->patron;
    my $times    = $patron->round_trip_times;
    my $glob     = $command->arguments;
    my $response = q();
    for my $path ( sort keys %{$times} ) {
        $response .= sprintf "%-70s %6.2f ms\n", $path, $times->{$path} * 1000
            if ( not $glob or $path =~ $glob );
    }
    if ( $response ne q() ) {
        return $self->response( $envelope, $response );
    }
    else {
        return;
    }
};

$C{times} = $C{list_times};

sub dump_config {
    my $self     = shift;
    my $response = $self->SUPER::dump_config;
    my $circuits = $self->{circuits};
    for my $name ( sort keys %{$circuits} ) {
        $response .= "command $self->{name} add $name\n";
    }
    return $response;
}

sub circuits {
    my $self = shift;
    if (@_) {
        $self->{circuits} = shift;
    }
    return $self->{circuits};
}

sub queue {
    my $self = shift;
    if (@_) {
        $self->{queue} = shift;
    }
    return $self->{queue};
}

sub waiting {
    my $self = shift;
    if (@_) {
        $self->{waiting} = shift;
    }
    return $self->{waiting};
}

sub offline {
    my $self = shift;
    if (@_) {
        $self->{offline} = shift;
    }
    return $self->{offline};
}

sub round_trip_times {
    my $self = shift;
    if (@_) {
        $self->{round_trip_times} = shift;
    }
    return $self->{round_trip_times};
}

sub interval {
    my $self = shift;
    if (@_) {
        $self->{interval} = shift;
    }
    return $self->{interval};
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

1;
