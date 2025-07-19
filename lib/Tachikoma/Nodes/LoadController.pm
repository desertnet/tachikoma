#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::LoadController
# ----------------------------------------------------------------------
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
use parent        qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.280');

use constant DEFAULT_PORT => 4230;

my $Kick_Delay = 10;
my %C          = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{host_ports}     = [];
    $self->{connectors}     = {};
    $self->{controllers}    = {};
    $self->{buffers}        = {};
    $self->{load_balancers} = {};
    $self->{misc}           = {};
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
        my ( $name, $path, @host_ports ) = split q( ), $self->{arguments};
        if (@host_ports) {
            $self->misc->{$name} = $path;
            $self->host_ports( \@host_ports );
            $self->set_timer;
        }
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    if ( $type & TM_COMMAND or $type & TM_EOF ) {
        return $self->interpreter->fill($message);
    }
    elsif ( $type & TM_PING ) {
        $self->handle_ping($message);
    }
    elsif ( $type & TM_INFO ) {
        if ( $message->[STREAM] eq 'RECONNECT' ) {
            $self->note_reconnect( $message->[FROM] );
        }
        elsif ( $message->[STREAM] eq 'AUTHENTICATED' ) {
            $self->note_authenticated( $message->[FROM] );
        }
        else {
            $self->handle_connection($message);
        }
    }
    $self->{counter}++;
    return;
}

sub fire {
    my $self       = shift;
    my $my_name    = $self->{name};
    my $offline    = $self->{offline};
    my $connectors = $self->{connectors};
    for my $id ( keys %{$offline} ) {
        next if ( not $offline->{$id} );
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_PING;
        $message->[FROM]    = $my_name;
        $message->[TO]      = $id;
        $message->[ID]      = $id;
        $message->[PAYLOAD] = $Tachikoma::Right_Now;
        $self->{sink}->fill($message);
    }
    if (    $self->{should_kick}
        and $self->{should_kick} < $Tachikoma::Right_Now
        and keys %{$offline} < keys %{$connectors} )
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
    if ( @{ $self->{host_ports} } ) {
        while ( my $host_port = shift @{ $self->{host_ports} } ) {
            my ( $host, $port ) = split m{:}, $host_port, 3;
            $self->add_connector(
                id   => "$host:$port",
                host => $host,
                port => $port
            );
        }
    }
    $self->stop_timer
        if ( not keys %{$offline} and not $self->{should_kick} );
    return;
}

sub handle_ping {
    my $self    = shift;
    my $message = shift;
    my $id      = $message->[ID] or return;
    my $offline = $self->{offline};
    if ( $offline->{$id} ) {
        my $load_balancers = $self->{load_balancers};
        for my $name ( keys %{$load_balancers} ) {
            next if ( not $Tachikoma::Nodes{$name} );
            my $path = $load_balancers->{$name};
            $self->connect_node(
                $name, $path
                ? join q(/), $id, $path
                : $id
            );
        }
        my $misc = $self->{misc};
        for my $name ( keys %{$misc} ) {
            next if ( not $Tachikoma::Nodes{$name} );
            my $path = $misc->{$name};
            $self->connect_node(
                $name, $path
                ? join q(/), $id, $path
                : $id
            );
        }
        my $tester = (
              $self->{circuit_tester}
            ? $Tachikoma::Nodes{ $self->{circuit_tester} }
            : undef
        );
        if ( $tester and $tester->isa('Tachikoma::Nodes::CircuitTester') ) {
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
    return;
}

sub note_reconnect {
    my $self           = shift;
    my $id             = shift;
    my $connectors     = $self->{connectors};
    my $buffers        = $self->{buffers};
    my $load_balancers = $self->{load_balancers};
    my $misc           = $self->{misc};
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
        for my $name ( keys %{$misc} ) {
            my $path = $misc->{$name};
            $self->disconnect_node( $name, $path
                ? join q(/),
                $id, $path
                : $id );
        }
        $offline->{$id} = undef;
    }
    return;
}

sub note_authenticated {
    my $self    = shift;
    my $id      = shift;
    my $offline = $self->{offline};
    if ( not $offline->{$id} ) {
        $offline->{$id} = 1;
        $self->{should_kick} = $Tachikoma::Right_Now + $Kick_Delay;
    }
    $self->set_timer;
    return;
}

sub handle_connection {
    my $self    = shift;
    my $message = shift;
    my $payload = $message->[PAYLOAD];
    chomp $payload;
    my ( $host, $port ) = split m{:}, $payload, 2;
    my $id_regex       = $self->{id_regex};
    my $hostname_regex = $self->{hostname_regex};
    my $id             = ( $payload =~ m{$id_regex} )[0];
    my $hostname       = ( $host    =~ m{$hostname_regex} )[0];

    if ( $id and $hostname ) {
        $self->add_connector(
            id   => $id,
            host => $hostname,
            port => $port
        );
    }
    else {
        $self->stderr("WARNING: couldn't get id and hostname from: $payload");
    }
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
            . "          list_misc\n"
            . "          list_circuits\n"
            . "          connect <connector name>\n"
            . "          disconnect <connector name>\n"
            . "          notify <connector name> <controller> [ port ]\n"
            . "          unnotify <connector name> <controller>\n"
            . "          buffer <buffer name> <path>\n"
            . "          unbuffer <buffer name>\n"
            . "          balance <load balancer name> <path>\n"
            . "          unbalance <load balancer name>\n"
            . "          add <name> <path>\n"
            . "          remove <name>\n"
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
    my $response = [ [ [ 'CONTROLLER' => 'left' ], [ 'PATH' => 'left' ], ] ];
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

$C{list_misc} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $misc     = $self->patron->misc;
    my $response = [ [ [ 'TEE' => 'left' ], [ 'PATH' => 'left' ] ] ];
    for my $id ( sort keys %{$misc} ) {
        push @{$response}, [ $id, $misc->{$id} ];
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$C{lst} = $C{list_misc};

$C{list_circuits} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $misc     = $self->patron->circuits;
    my $response = q();
    for my $id ( sort keys %{$misc} ) {
        $response .= "$id\n";
    }
    return $self->response( $envelope, $response );
};

$C{lsr} = $C{list_circuits};

$C{connect} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $host_port = $command->arguments;
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
            id   => $id,
            host => $host,
            port => $port
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
            "usage: notify <connector name> <controller> [ port ]\n" );
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
    delete $controllers->{$name}
        if ( not keys %{ $controllers->{$name} } );
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
    $self->patron->should_kick( $Tachikoma::Right_Now + $Kick_Delay );
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
    $self->patron->should_kick( $Tachikoma::Right_Now + $Kick_Delay );
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
    $self->patron->should_kick( $Tachikoma::Right_Now + $Kick_Delay );
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
    $self->patron->should_kick( $Tachikoma::Right_Now + $Kick_Delay );
    return $self->okay($envelope);
};

$C{add} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $name, $path ) = split q( ), $command->arguments, 2;
    if ( not $name ) {
        return $self->error( $envelope, "usage: add <node name> <path>\n" );
    }
    $self->patron->misc->{$name} = $path;
    $self->patron->should_kick( $Tachikoma::Right_Now + $Kick_Delay );
    return $self->okay($envelope);
};

$C{remove} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( not length $command->arguments ) {
        return $self->error( $envelope, "no name specified\n" );
    }
    delete $self->patron->misc->{ $command->arguments };
    $self->patron->should_kick( $Tachikoma::Right_Now + $Kick_Delay );
    return $self->okay($envelope);
};

$C{rm} = $C{remove};

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
    $self->patron->should_kick( $Tachikoma::Right_Now + $Kick_Delay );
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
    $self->patron->should_kick( $Tachikoma::Right_Now + $Kick_Delay );
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
    $self->patron->should_kick(1);
    $self->patron->fire;
    return $self->okay($envelope);
};

sub add_connector {
    my ( $self, %args ) = @_;
    my $id         = $args{id};
    my $host       = $args{host};
    my $port       = $args{port};
    my $connection = $Tachikoma::Nodes{$id};
    return if ( not $self->{sink} );
    if ( not $connection ) {
        require Tachikoma::Nodes::Socket;
        $port ||= DEFAULT_PORT;
        $connection =
            Tachikoma::Nodes::Socket->inet_client_async( $host, $port );
        $connection->name($id);
        $connection->on_EOF('reconnect');
        $connection->sink( $self->sink );
        $Tachikoma::Nodes{$id} = $connection;
    }
    $self->connectors->{$id} = $Tachikoma::Now;
    $self->offline->{$id}    = undef;
    $self->note_reconnect($id)
        if ( $connection->{set_state}->{RECONNECT} );
    $self->note_authenticated($id)
        if ( $connection->{set_state}->{AUTHENTICATED} );
    my $tester = (
          $self->{circuit_tester}
        ? $Tachikoma::Nodes{ $self->{circuit_tester} }
        : undef
    );
    if ( $tester and $tester->isa('Tachikoma::Nodes::CircuitTester') ) {
        $tester->circuits->{ join q(/), $id, $_ } = 1
            for ( keys %{ $self->{circuits} } );
    }
    $connection->register( 'RECONNECT'     => $self->name );
    $connection->register( 'AUTHENTICATED' => $self->name );
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
        $self->disconnect_node(
            $name, $path
            ? join q(/), $id, $path
            : $id
        );
    }
    my $misc = $self->misc;
    for my $name ( keys %{$misc} ) {
        my $path = $misc->{$name};
        $self->disconnect_node(
            $name, $path
            ? join q(/), $id, $path
            : $id
        );
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
        $Tachikoma::Nodes{$name}->unregister( 'RECONNECT' => $self->name )
            if ( $Tachikoma::Nodes{$name} );
        $Tachikoma::Nodes{$name}->unregister( 'AUTHENTICATED' => $self->name )
            if ( $Tachikoma::Nodes{$name} );
    }
    $self->SUPER::remove_node;
    return;
}

sub dump_config {
    my $self           = shift;
    my $response       = $self->SUPER::dump_config;
    my $connectors     = $self->{connectors};
    my $controllers    = $self->{controllers};
    my $buffers        = $self->{buffers};
    my $load_balancers = $self->{load_balancers};
    my $misc           = $self->{misc};
    my $circuit_tester = $self->{circuit_tester};
    my $circuits       = $self->{circuits};
    my $id_regex       = $self->{id_regex};
    my $hostname_regex = $self->{hostname_regex};
    $response .= "cd $self->{name}\n";

    for my $name ( sort keys %{$controllers} ) {
        for my $path ( keys %{ $controllers->{$name} } ) {
            my $port = $controllers->{$name}->{$path} // q();
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
    for my $name ( sort keys %{$misc} ) {
        my $path = $misc->{$name};
        $response .= "  add $name" . ( $path ? " $path" : q() ) . "\n";
    }
    $response .= "  circuit_tester $circuit_tester\n"
        if ($circuit_tester);
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

sub host_ports {
    my $self = shift;
    if (@_) {
        $self->{host_ports} = shift;
    }
    return $self->{host_ports};
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

sub misc {
    my $self = shift;
    if (@_) {
        $self->{misc} = shift;
    }
    return $self->{misc};
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
        if ( $self->{should_kick} ) {
            $self->offline( { %{ $self->connectors } } );
            $self->set_timer;
        }
    }
    return $self->{should_kick};
}

1;
