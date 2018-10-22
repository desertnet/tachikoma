#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::LoadController
# ----------------------------------------------------------------------
#
# $Id: LoadController.pm 8576 2010-09-24 06:14:51Z chris $
#

package Tachikoma::Nodes::LoadController;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_COMMAND TM_PING TM_RESPONSE TM_NOREPLY TM_INFO TM_EOF
);
use Sys::Hostname qw( hostname );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.280');

my $Kick_Delay = 10;
my %C          = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{connectors}     = {};
    $self->{controllers}    = {};
    $self->{buffers}        = {};
    $self->{load_balancers} = {};
    $self->{tees}           = {};
    $self->{circuit_tester} = 'CircuitTester';
    $self->{circuits}       = {};
    $self->{id_regex}       = '^([^:.]+)';
    $self->{hostname_regex} = '^([^:]+)';
    $self->{offline}        = {};
    $self->{should_kick}    = undef;
    $self->{interpreter}    = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node LoadController <node name>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
    }
    return $self->{arguments};
}

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    if ( $type & TM_COMMAND or $type & TM_EOF ) {
        return $self->interpreter->fill($message);
    }
    elsif ( $type & TM_PING ) {
        my $id = $message->[ID] or return;
        my $offline = $self->{offline};
        if ( $offline->{$id} ) {
            my $load_balancers = $self->{load_balancers};
            for my $name ( keys %{$load_balancers} ) {
                next if ( not $Tachikoma::Nodes{$name} );
                my $path = $load_balancers->{$name};
                $self->connect_node( $name, $path
                    ? join q(/),
                    $id, $path
                    : $id );
            }
            my $tees = $self->{tees};
            for my $name ( keys %{$tees} ) {
                next if ( not $Tachikoma::Nodes{$name} );
                my $path = $tees->{$name};
                $self->connect_node( $name, $path
                    ? join q(/),
                    $id, $path
                    : $id );
            }
            my $tester = (
                  $self->{circuit_tester}
                ? $Tachikoma::Nodes{ $self->{circuit_tester} }
                : undef
            );
            if ( $tester and $tester->isa('Tachikoma::Nodes::CircuitTester') )
            {
                $tester->circuits->{ join q(/), $id, $_ } = 1
                    for ( keys %{ $self->{circuits} } );
            }
            for my $path ( keys %{ $self->{controllers}->{$id} } ) {
                my $port = $self->{controllers}->{$id}->{$path};
                my $note = Tachikoma::Message->new;
                $note->[TYPE]    = TM_INFO;
                $note->[TO]      = join q(/), $id, $path;
                $note->[PAYLOAD] = hostname();
                $note->[PAYLOAD] .= q(:) . $port if ($port);
                $note->[PAYLOAD] .= "\n";
                $self->SUPER::fill($note);
            }
            delete $offline->{$id};
            $self->{connectors}->{$id} = $Tachikoma::Now;
            $self->{should_kick} = $Tachikoma::Right_Now + $Kick_Delay;
        }
    }
    elsif ( $type & TM_INFO ) {
        if ( $message->[STREAM] eq 'reconnect' ) {
            $self->note_reconnect( $message->[FROM] );
        }
        else {
            my $payload = $message->[PAYLOAD];
            my ( $host, $port, $use_SSL ) = split m{:}, $payload, 3;
            my $id_regex       = $self->{id_regex};
            my $hostname_regex = $self->{hostname_regex};
            my $id             = ( $payload =~ m{$id_regex} )[0];
            my $hostname       = ( $host =~ m{$hostname_regex} )[0];
            if ( $id and $hostname ) {
                $self->add_connector(
                    id      => $id,
                    host    => $hostname,
                    port    => $port,
                    use_SSL => $use_SSL
                );
            }
            else {
                $self->stderr(
                    "WARNING: couldn't get id and hostname from: $payload");
            }
        }
    }
    $self->{counter}++;
    return;
}

sub fire {
    my $self    = shift;
    my $my_name = $self->{name};
    my $offline = $self->{offline};
    for my $id ( keys %{$offline} ) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_PING;
        $message->[FROM]    = $my_name;
        $message->[TO]      = $id;
        $message->[ID]      = $id;
        $message->[PAYLOAD] = $Tachikoma::Right_Now;
        $self->{sink}->fill($message);
    }
    if (    $self->{should_kick}
        and $self->{should_kick} < $Tachikoma::Right_Now )
    {
        my $buffers        = $self->{buffers};
        my $load_balancers = $self->{load_balancers};
        for my $name ( keys %{$load_balancers} ) {
            my $command = $self->command('kick');
            $command->[TYPE] |= TM_NOREPLY;
            $command->[FROM] = '_responder';
            $command->[TO]   = $name;
            $self->{sink}->fill($command);
        }
        for my $name ( keys %{$buffers} ) {
            my $path = $buffers->{$name};
            my $node = $Tachikoma::Nodes{$name} or next;
            $self->connect_node( $name, $path )
                if ( not $node->{owner} );
            my $command = $self->command('kick');
            $command->[TYPE] |= TM_NOREPLY;
            $command->[FROM] = '_responder';
            $command->[TO]   = $name;
            $self->{sink}->fill($command);
        }
        $self->{should_kick} = undef;
    }
    $self->stop_timer
        if ( not keys %{$offline} and not $self->{should_kick} );
    return;
}

sub note_reconnect {
    my $self           = shift;
    my $id             = shift;
    my $connectors     = $self->{connectors};
    my $buffers        = $self->{buffers};
    my $load_balancers = $self->{load_balancers};
    my $tees           = $self->{tees};
    my $offline        = $self->{offline};
    if ( not $offline->{$id} ) {

        for my $name ( keys %{$buffers} ) {
            my $path = $buffers->{$name} or next;
            $self->disconnect_node( $name, $path );
        }
        for my $name ( keys %{$load_balancers} ) {
            my $path = $load_balancers->{$name};
            $self->disconnect_node( $name, $path
                ? join q(/),
                $id, $path
                : $id );
        }
        for my $name ( keys %{$tees} ) {
            my $path = $tees->{$name};
            $self->disconnect_node( $name, $path
                ? join q(/),
                $id, $path
                : $id );
        }
        $offline->{$id} = 1;
        $self->{should_kick} = $Tachikoma::Right_Now + $Kick_Delay
            if ( keys %{$offline} < keys %{$connectors} );
    }
    $self->set_timer;
    return;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list_connectors\n"
            . "          list_controllers\n"
            . "          list_buffers\n"
            . "          list_load_balancers\n"
            . "          list_tees\n"
            . "          list_circuits\n"
            . "          connect <connector name>\n"
            . "          disconnect <connector name>\n"
            . "          notify <connector name> <controller> [ port[:ssl] ]\n"
            . "          unnotify <connector name> <controller>\n"
            . "          buffer <buffer name> <path>\n"
            . "          unbuffer <buffer name>\n"
            . "          balance <load balancer name> <path>\n"
            . "          unbalance <load balancer name>\n"
            . "          tee <tee name> <path>\n"
            . "          untee <tee name>\n"
            . "          circuit_tester [ <circuit tester> ]\n"
            . "          circuit <path>\n"
            . "          uncircuit <path>\n"
            . "          hostnames_regex [ <new regex> ]\n"
            . "          kick\n" );
};

$C{list_connectors} = sub {
    my $self       = shift;
    my $command    = shift;
    my $envelope   = shift;
    my $connectors = $self->patron->connectors;
    my @output     = ();
    for my $id ( sort keys %{$connectors} ) {
        push @output, $Tachikoma::Now - $connectors->{$id}, q( ), $id, "\n";
    }
    return $self->response( $envelope, join q(), @output );
};

$C{ls} = $C{list_connectors};

$C{list_controllers} = sub {
    my $self        = shift;
    my $command     = shift;
    my $envelope    = shift;
    my $controllers = $self->patron->controllers;
    my $response    = [
        [   [ 'CONTROLLER' => 'left' ],
            [ 'PATH'       => 'left' ],
            [ 'SSL'        => 'left' ]
        ]
    ];
    for my $id ( sort keys %{$controllers} ) {
        push @{$response}, [ $id, $_, $controllers->{$id}->{$_} ]
            for ( sort keys %{ $controllers->{$id} } );
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$C{lsc} = $C{list_controllers};

$C{list_buffers} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $buffers  = $self->patron->buffers;
    my $response = [ [ [ 'BUFFER' => 'left' ], [ 'PATH' => 'left' ] ] ];
    for my $id ( sort keys %{$buffers} ) {
        push @{$response}, [ $id, $buffers->{$id} ];
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$C{lsb} = $C{list_buffers};

$C{list_load_balancers} = sub {
    my $self           = shift;
    my $command        = shift;
    my $envelope       = shift;
    my $load_balancers = $self->patron->load_balancers;
    my $response =
        [ [ [ 'LOAD_BALANCER' => 'left' ], [ 'PATH' => 'left' ] ] ];
    for my $id ( sort keys %{$load_balancers} ) {
        push @{$response}, [ $id, $load_balancers->{$id} ];
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$C{lsl} = $C{list_load_balancers};

$C{list_tees} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $tees     = $self->patron->tees;
    my $response = [ [ [ 'TEE' => 'left' ], [ 'PATH' => 'left' ] ] ];
    for my $id ( sort keys %{$tees} ) {
        push @{$response}, [ $id, $tees->{$id} ];
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$C{lst} = $C{list_tees};

$C{list_circuits} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $tees     = $self->patron->circuits;
    my $response = q();
    for my $id ( sort keys %{$tees} ) {
        $response .= "$id\n";
    }
    return $self->response( $envelope, $response );
};

$C{lsr} = $C{list_circuits};

$C{connect} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $host_port, $use_SSL ) = split q( ), $command->arguments, 2;
    my ( $host, $port );
    if ( $host_port =~ m{^(.*):(\d+)$} ) {
        $host = $1;
        $port = $2;
    }
    else {
        $host = $host_port;
        $port = undef;
    }
    if ( not length $host ) {
        return $self->error( $envelope, "no connector specified\n" );
    }
    my $id_regex = $self->patron->{id_regex};
    my $id       = ( $host_port =~ m{$id_regex} )[0];
    if ( $id and $host ) {
        $self->patron->add_connector(
            id      => $id,
            host    => $host,
            port    => $port,
            use_SSL => $use_SSL
        );
    }
    else {
        return $self->error( $envelope,
            "couldn't get id and hostname from: $host_port\n" );
    }
    return $self->okay($envelope);
};

$C{disconnect} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $name     = $command->arguments;
    if ( not length $name ) {
        return $self->error( $envelope, "no connector specified\n" );
    }
    $self->patron->remove_connector($name);
    return $self->okay($envelope);
};

$C{notify} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $name, $path, $port ) = split q( ), $command->arguments, 3;
    if ( not $path ) {
        return $self->error( $envelope,
            "usage: notify <connector name> <controller> [ port[:ssl] ]\n" );
    }
    if ( $Tachikoma::Nodes{$name} ) {
        my $note = Tachikoma::Message->new;
        $note->[TYPE]    = TM_INFO;
        $note->[TO]      = join q(/), $name, $path;
        $note->[PAYLOAD] = hostname();
        $note->[PAYLOAD] .= q(:) . $port if ($port);
        $note->[PAYLOAD] .= "\n";
        $self->patron->SUPER::fill($note);
    }
    my $controllers = $self->patron->controllers;
    $controllers->{$name} ||= {};
    $controllers->{$name}->{$path} = $port;
    return $self->okay($envelope);
};

$C{unnotify} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $name, $path ) = split q( ), $command->arguments, 2;
    if ( not $path ) {
        return $self->error( $envelope,
            "usage: unnotify <connector name> <controller>\n" );
    }
    my $controllers = $self->patron->controllers;
    delete $controllers->{$name}->{$path} if ( $controllers->{$name} );
    delete $controllers->{$name} if ( not keys %{ $controllers->{$name} } );
    return $self->okay($envelope);
};

$C{buffer} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $name, $path ) = split q( ), $command->arguments, 2;
    if ( not $name ) {
        return $self->error( $envelope,
            "usage: buffer <node name> <path>\n" );
    }
    $self->patron->buffers->{$name} = $path;
    return $self->okay($envelope);
};

$C{unbuffer} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( not length $command->arguments ) {
        return $self->error( $envelope, "no buffer specified\n" );
    }
    delete $self->patron->buffers->{ $command->arguments };
    return $self->okay($envelope);
};

$C{balance} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $name, $path ) = split q( ), $command->arguments, 2;
    if ( not $name ) {
        return $self->error( $envelope,
            "usage: balance <node name> <path>\n" );
    }
    $self->patron->load_balancers->{$name} = $path;
    return $self->okay($envelope);
};

$C{unbalance} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( not length $command->arguments ) {
        return $self->error( $envelope, "no load balancer specified\n" );
    }
    delete $self->patron->load_balancers->{ $command->arguments };
    return $self->okay($envelope);
};

$C{tee} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $name, $path ) = split q( ), $command->arguments, 2;
    if ( not $name ) {
        return $self->error( $envelope, "usage: tee <node name> <path>\n" );
    }
    $self->patron->tees->{$name} = $path;
    return $self->okay($envelope);
};

$C{untee} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( not length $command->arguments ) {
        return $self->error( $envelope, "no tee specified\n" );
    }
    delete $self->patron->tees->{ $command->arguments };
    return $self->okay($envelope);
};

$C{circuit_tester} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $name     = $command->arguments;
    $self->patron->circuit_tester($name) if ($name);
    my $response =
        'circuit_tester set to ' . $self->patron->circuit_tester . "\n";
    return $self->response( $envelope, $response );
};

$C{circuit} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $path     = $command->arguments;
    if ( not $path ) {
        return $self->error( $envelope, "usage: circuit <path>\n" );
    }
    $self->patron->circuits->{$path} = 1;
    return $self->okay($envelope);
};

$C{uncircuit} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( not length $command->arguments ) {
        return $self->error( $envelope, "no circuit specified\n" );
    }
    delete $self->patron->circuits->{ $command->arguments };
    return $self->okay($envelope);
};

$C{id_regex} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $regex    = $command->arguments;
    if ($regex) {
        my $test = 'foo';
        $test =~ m{$regex};
        $self->patron->id_regex($regex);
    }
    my $id_regex = $self->patron->id_regex;
    my $response = 'id_regex set to ' . $self->patron->id_regex . "\n";
    return $self->response( $envelope, $response );
};

$C{hostname_regex} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $regex    = $command->arguments;
    if ($regex) {
        my $test = 'foo';
        $test =~ m{$regex};
        $self->patron->hostname_regex($regex);
    }
    my $response =
        'hostname_regex set to ' . $self->patron->hostname_regex . "\n";
    return $self->response( $envelope, $response );
};

$C{kick} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->offline( { %{ $self->patron->connectors } } );
    $self->patron->should_kick(1);
    $self->patron->fire;
    return $self->okay($envelope);
};

sub add_connector {
    my ( $self, %args ) = @_;
    my $id      = $args{id};
    my $host    = $args{host};
    my $port    = $args{port};
    my $use_SSL = $args{use_SSL};
    $self->{sink}->connect_inet(
        name      => $id,
        host      => $host,
        port      => $port,
        use_SSL   => $use_SSL ? 1 : q(),
        reconnect => 1
    ) if ( not $Tachikoma::Nodes{$id} );
    $Tachikoma::Nodes{$id}->register( 'reconnect' => $self->name );
    $self->connectors->{$id} = $Tachikoma::Now;
    $self->offline->{$id}    = 1;
    $self->note_reconnect($id);
    my $tester = (
          $self->{circuit_tester}
        ? $Tachikoma::Nodes{ $self->{circuit_tester} }
        : undef
    );

    if ( $tester and $tester->isa('Tachikoma::Nodes::CircuitTester') ) {
        $tester->circuits->{ join q(/), $id, $_ } = 1
            for ( keys %{ $self->{circuits} } );
    }
    return;
}

sub remove_connector {
    my $self = shift;
    my $id   = shift;
    $Tachikoma::Nodes{$id}->remove_node if ( $Tachikoma::Nodes{$id} );
    delete $self->connectors->{$id};
    delete $self->offline->{$id};
    my $load_balancers = $self->load_balancers;
    for my $name ( keys %{$load_balancers} ) {
        my $path = $load_balancers->{$name};
        $self->disconnect_node( $name, $path ? join q(/), $id, $path : $id );
    }
    my $tees = $self->tees;
    for my $name ( keys %{$tees} ) {
        my $path = $tees->{$name};
        $self->disconnect_node( $name, $path ? join q(/), $id, $path : $id );
    }
    my $tester = (
          $self->{circuit_tester}
        ? $Tachikoma::Nodes{ $self->{circuit_tester} }
        : undef
    );
    if ( $tester and $tester->isa('Tachikoma::Nodes::CircuitTester') ) {
        for my $path ( keys %{ $self->{circuits} } ) {
            my $circuit = join q(/), $id, $path;
            delete $tester->circuits->{$circuit};
            delete $tester->waiting->{$circuit};
            delete $tester->offline->{$circuit};
        }
    }
    return;
}

sub remove_node {
    my $self = shift;
    for my $name ( keys %{ $self->{connectors} } ) {
        $Tachikoma::Nodes{$name}->unregister( 'reconnect' => $self->name )
            if ( $Tachikoma::Nodes{$name} );
    }
    return $self->SUPER::remove_node(@_);
}

sub dump_config {
    my $self           = shift;
    my $response       = $self->SUPER::dump_config;
    my $connectors     = $self->{connectors};
    my $controllers    = $self->{controllers};
    my $buffers        = $self->{buffers};
    my $load_balancers = $self->{load_balancers};
    my $tees           = $self->{tees};
    my $circuit_tester = $self->{circuit_tester};
    my $circuits       = $self->{circuits};
    my $id_regex       = $self->{id_regex};
    my $hostname_regex = $self->{hostname_regex};
    $response .= "cd $self->{name}\n";

    for my $name ( sort keys %{$controllers} ) {
        for my $path ( keys %{ $controllers->{$name} } ) {
            my $port = $controllers->{$name}->{$path};
            $response .= "  notify $name $path $port\n";
        }
    }
    for my $name ( sort keys %{$buffers} ) {
        my $path = $buffers->{$name};
        $response .= "  buffer $name" . ( $path ? " $path" : q() ) . "\n";
    }
    for my $name ( sort keys %{$load_balancers} ) {
        my $path = $load_balancers->{$name};
        $response .= "  balance $name" . ( $path ? " $path" : q() ) . "\n";
    }
    for my $name ( sort keys %{$tees} ) {
        my $path = $tees->{$name};
        $response .= "  tee $name" . ( $path ? " $path" : q() ) . "\n";
    }
    $response .= "  circuit_tester $circuit_tester\n" if ($circuit_tester);
    for my $name ( sort keys %{$circuits} ) {
        $response .= "  circuit $name\n";
    }
    $response .= "  id_regex $id_regex\n";
    $response .= "  hostname_regex $hostname_regex\n";
    for my $name ( sort keys %{$connectors} ) {
        $response .= "  connect $name\n";
    }
    $response .= "cd ..\n";
    return $response;
}

sub connectors {
    my $self = shift;
    if (@_) {
        $self->{connectors} = shift;
    }
    return $self->{connectors};
}

sub controllers {
    my $self = shift;
    if (@_) {
        $self->{controllers} = shift;
    }
    return $self->{controllers};
}

sub buffers {
    my $self = shift;
    if (@_) {
        $self->{buffers} = shift;
    }
    return $self->{buffers};
}

sub load_balancers {
    my $self = shift;
    if (@_) {
        $self->{load_balancers} = shift;
    }
    return $self->{load_balancers};
}

sub tees {
    my $self = shift;
    if (@_) {
        $self->{tees} = shift;
    }
    return $self->{tees};
}

sub circuit_tester {
    my $self = shift;
    if (@_) {
        $self->{circuit_tester} = shift;
    }
    return $self->{circuit_tester};
}

sub circuits {
    my $self = shift;
    if (@_) {
        $self->{circuits} = shift;
    }
    return $self->{circuits};
}

sub id_regex {
    my $self = shift;
    if (@_) {
        $self->{id_regex} = shift;
    }
    return $self->{id_regex};
}

sub hostname_regex {
    my $self = shift;
    if (@_) {
        $self->{hostname_regex} = shift;
    }
    return $self->{hostname_regex};
}

sub offline {
    my $self = shift;
    if (@_) {
        $self->{offline} = shift;
    }
    return $self->{offline};
}

sub should_kick {
    my $self = shift;
    if (@_) {
        $self->{should_kick} = shift;
    }
    return $self->{should_kick};
}

1;
