#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::CommandInterpreter
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::CommandInterpreter;
use strict;
use warnings;
use Tachikoma;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_EOF TM_PING
    TM_COMMAND TM_RESPONSE TM_ERROR
    TM_INFO TM_REQUEST TM_STORABLE
    TM_COMPLETION TM_NOREPLY
);
use Tachikoma::Command;
use Tachikoma::Crypto;
use Tachikoma::Nodes::Shell2;
use Tachikoma::Nodes::Socket;
use Tachikoma::Nodes::STDIO;
use Data::Dumper;
use Getopt::Long qw( GetOptionsFromString );
use POSIX qw( strftime SIGHUP );
use Storable qw( thaw );
use Sys::Hostname qw( hostname );
use Time::HiRes qw();
use parent qw( Tachikoma::Node Tachikoma::Crypto );

use version; our $VERSION = qv('v2.0.280');

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Useperl  = 1;

Getopt::Long::Configure('bundling');

my $HELP     = Tachikoma->configuration->help;
my %H        = ();
my %L        = ();
my %C        = ();
my %DISABLED = (
    1 => { map { $_ => 1 } qw( config func make_node remote_env slurp var ) },
    2 => { map { $_ => 1 } qw( command_node ) },
    3 => { map { $_ => 1 } qw( connect_node ) },
);
my @CONFIG_VARIABLES = qw(
    prefix
    log_dir
    log_file
    pid_dir
    pid_file
    home
    buffer_size
    low_water_mark
    keep_alive
    hz
    ssl_client_ca_file
    ssl_client_cert_file
    ssl_client_key_file
    ssl_server_ca_file
    ssl_server_cert_file
    ssl_server_key_file
    ssl_version
);

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{commands}    = \%C;
    $self->{help_topics} = \%H;
    $self->{help_links}  = \%L;
    $self->{patron}      = undef;
    $self->disabled( \%DISABLED );
    bless $self, $class;
    return $self;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    $self->{counter}++;
    if ( not length $message->[TO] ) {
        if (    $message->[TYPE] & TM_COMMAND
            and not $message->[TYPE] & TM_RESPONSE
            and not $message->[TYPE] & TM_ERROR )
        {
            return $self->interpret($message);
        }
        elsif ( $message->[TYPE] & TM_PING ) {
            $message->[TO] = $message->[FROM];
        }
        elsif ( $message->[TYPE] & TM_EOF ) {
            return
                if ($message->[FROM] !~ m{/}
                and $message->[FROM] ne '_responder' );
            $message->[TO] = $message->[FROM];
        }
    }
    return $self->drop_message( $message, 'no sink' )
        if ( not $self->{sink} );
    return $self->{sink}->fill($message);
}

sub interpret {
    my $self      = shift;
    my $message   = shift;
    my $command   = Tachikoma::Command->new( $message->[PAYLOAD] );
    my $cmd_name  = $command->{name};
    my $functions = $self->configuration->functions;
    if ( not $self->verify_command( $message, $command ) ) {
        $self->send_response( $message,
            $self->error("verification failed\n") );
        return;
    }
    if ( length $message->[TO] ) {
        my $name = ( split m{/}, $message->[TO], 2 )[0];
        if ( $Tachikoma::Nodes{$name} ) {
            return $self->{sink}->fill($message);
        }
        else {
            return $self->send_response( $message,
                $self->error( $message, qq(can't find node "$name"\n) ) );
        }
    }
    $self->log_command( $message, $command );
    my $sub = $self->{commands}->{$cmd_name};
    if ($sub) {
        my $response = undef;
        my $okay     = eval {
            $response = &{$sub}( $self, $command, $message );
            return 1;
        };
        if ( not $okay ) {
            my $error = $@ || 'unknown error';
            return $self->send_response( $message,
                $self->error( $message, qq($cmd_name failed: $error) ) );
        }
        else {
            return $self->send_response( $message, $response );
        }
    }
    elsif ( $cmd_name eq 'prompt' ) {
        return $self->send_response(
            $message,
            $self->response(
                $message,
                join q(), ref( $self->{patron} || $self->{sink} ), '> '
            )
        );
    }
    elsif ( $cmd_name eq 'pwd' ) {
        return $self->send_response( $message,
            $self->pwd( $command, $message ) );
    }
    elsif ( $cmd_name eq 'help' ) {
        if ( $message->type & TM_COMPLETION ) {
            my $b = Tachikoma::Nodes::Shell2::builtins;
            my %u = ( %C, %{$b}, %{$functions} );
            return $self->send_response( $message,
                $self->response( $message, join( "\n", keys %u ) . "\n" ) );
        }
        else {
            return $self->send_response( $message,
                $self->topical_help( $command, $message ) );
        }
    }
    elsif ( $functions->{$cmd_name} ) {
        return $self->send_response( $message,
            $self->call_function( $functions, $command, $message ) );
    }
    return $self->send_response(
        $message,
        $self->error(
            $message,
            'unrecognized command: '
                . $cmd_name
                . (
                $command->{arguments} ne q()
                ? q( ) . $command->{arguments} . "\n"
                : "\n"
                )
        )
    );
}

sub call_function {
    my $self      = shift;
    my $functions = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $name      = $command->name;
    $self->verify_key( $envelope, ['meta'], 'remote' )
        or return $self->error("verification failed\n");
    return $self->error( $envelope, "ERROR: no such function: $name\n" )
        if ( not exists $functions->{$name} );
    my $rv        = [];
    my $responder = $Tachikoma::Nodes{_responder};
    die "ERROR: can't find _responder\n" if ( not $responder );
    my $okay = eval {
        $rv = $responder->shell->call_function(
            $name,
            {   q(@)            => $command->arguments,
                q(0)            => $name,
                q(1)            => $command->arguments,
                q(_C)           => 1,
                q(message.from) => $envelope->from
            }
        );
        return 1;
    };
    $self->stderr( $@ || 'ERROR: call_function: unknown error' )
        if ( not $okay );
    return $self->response( $envelope, join q(), @{$rv} );
}

sub send_response {
    my $self     = shift;
    my $message  = shift;
    my $response = shift or return;
    return $response if ( not ref $response );
    if ( $message->[TYPE] & TM_NOREPLY or not $self->{sink} ) {
        my $payload = $response->[PAYLOAD];
        $payload = Tachikoma::Command->new($payload)->{payload}
            if ( $response->[TYPE] & TM_COMMAND );
        if ( $response->[TYPE] & TM_ERROR ) {
            $self->stderr( 'error from TM_NOREPLY command: ', $payload );
        }
        return $payload;
    }
    if ( not $response->[FROM] ) {
        if ( $self->{patron} ) {
            $response->[FROM] = $self->{patron}->{name};
        }
        else {
            $response->[FROM] = $self->{name};
        }
    }
    $response->[TO] = $message->[FROM] if ( not $response->[TO] );
    $response->[ID] = $message->[ID];
    $response->[TYPE] |= TM_COMPLETION
        if ( $message->[TYPE] & TM_COMPLETION );
    return $self->{sink}->fill($response);
}

sub topical_help {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $glob     = $command->arguments;
    my $config   = $self->configuration;
    my $h        = $self->help_topics;
    my $l        = $self->help_links;
    if ( $HELP->{$glob} ) {
        return $self->response( $envelope, join q(), @{ $HELP->{$glob} } );
    }
    elsif ( $h->{$glob} ) {
        return $self->response( $envelope, join q(), @{ $h->{$glob} } );
    }
    elsif ( $l->{$glob} ) {
        return $self->response( $envelope, join q(), @{ $l->{$glob} } );
    }
    my $type = ( $glob =~ m{^([\w:]+)$} )[0];
    if ($type) {
        my $class = undef;
        for my $prefix ( @{ $config->include_nodes }, 'Tachikoma::Nodes' ) {
            next if ( not $prefix );
            $class = join q(::), $prefix, $type;
            my $class_path = $class;
            $class_path =~ s{::}{/}g;
            $class_path .= '.pm';
            if ( eval { require $class_path } ) {
                my $help = eval { $class->help };
                return $self->response( $envelope, $help ) if ($help);
                last;
            }
        }
    }
    my $output = undef;
    if ( $glob ne q() ) {
        $output = $self->tabulate_help( $glob, $HELP, $h );
    }
    else {
        $output = join q(),
            "### SHELL BUILTINS ###\n",
            $self->tabulate_help( $glob, $HELP ),
            "\n### SERVER COMMANDS ###\n",
            $self->tabulate_help( $glob, $h );
    }
    if ($output) {
        return $self->response( $envelope, $output );
    }
    else {
        return $self->error( $envelope, qq(no such topic: "$glob"\n) );
    }
}

sub tabulate_help {
    my ( $self, $glob, @groups ) = @_;
    my $table    = [ [ 'left', 'left', 'left', 'left' ] ];
    my @unsorted = ();
    my @topics   = ();
    my $row      = [];
    my $output   = q();
    if ( $glob ne q() ) {
        @unsorted = grep m{$glob}, keys %{$_} for (@groups);
    }
    else {
        my $functions = $self->configuration->functions;
        @unsorted = grep not( $functions->{$_} ), keys %{$_} for (@groups);
    }
    @topics = sort @unsorted;
    for my $i ( 0 .. $#topics ) {
        push @{$row}, $topics[$i];
        next if ( ( $i + 1 ) % 4 );
        push @{$table}, $row;
        $row = [];
    }
    push @{$table}, $row if ( @{$row} );
    $output = $self->tabulate($table) if ( @{$table} > 1 );
    return $output;
}

$H{list_nodes} = [
    "list_nodes [ -celos ] [ <node name> ]\n",
    "list_nodes -a [ -celos ] [ <regex glob> ]\n",
    "    -c show message counters\n",
    "    -e show edges\n",
    "    -l show message counters and owners\n",
    "    -o show owners\n",
    "    -s show sinks\n",
    "    -a show all nodes matching regex glob\n",
    "       show all nodes if regex glob is omitted\n",
    "    note: Without -a, the argument specifies a node.\n",
    "          All nodes sinking into the specified node are displayed.\n",
    "          This is useful when using CommandInterpreters as groups.\n",
    "          It can also be used to show the jobs in a JobController, etc.\n",
    "    alias: ls\n",
    "    examples: ls -c jobs\n",
    "              ls -al .*:buffer\n",
    "              ls -aceos\n"
];

$C{list_nodes} = sub {
    my $self         = shift;
    my $command      = shift;
    my $envelope     = shift;
    my $list_matches = undef;
    my $show_count   = undef;
    my $show_edge    = undef;
    my $show_etc     = undef;
    my $show_owner   = undef;
    my $show_sink    = undef;
    my ( $r, $argv ) = GetOptionsFromString(
        $command->arguments,
        'a' => \$list_matches,
        'c' => \$show_count,
        'e' => \$show_edge,
        'l' => \$show_etc,
        'o' => \$show_owner,
        's' => \$show_sink,
    );
    die qq(invalid option\n) if ( not $r );

    if ($show_etc) {
        $show_count = 'true';
        $show_owner = 'true';
    }
    my $response = [ ['left'] ];
    unshift @{ $response->[0] }, 'right' if ($show_count);
    push @{ $response->[0] }, 'left' if ($show_sink);
    push @{ $response->[0] }, 'left' if ($show_edge);
    push @{ $response->[0] }, 'left' if ($show_owner);
    if ( $show_owner or $show_sink or $show_edge ) {
        my $header = ['NAME'];
        unshift @{$header}, 'COUNT' if ($show_count);
        push @{$header}, 'SINK'  if ($show_sink);
        push @{$header}, 'EDGE'  if ($show_edge);
        push @{$header}, 'OWNER' if ($show_owner);
        push @{$response}, $header;
    }
    if ( @{$argv} ) {
        for my $glob ( @{$argv} ) {
            if ( not $list_matches and not $Tachikoma::Nodes{$glob} ) {
                return $self->error( $envelope,
                    qq(can't find node "$glob"\n) );
            }
            $self->list_nodes(
                {   glob         => $glob,
                    list_matches => $list_matches,
                    show_count   => $show_count,
                    show_sink    => $show_sink,
                    show_edge    => $show_edge,
                    show_owner   => $show_owner,
                    response     => $response,
                }
            );
        }
    }
    else {
        $self->list_nodes(
            {   list_matches => $list_matches,
                show_count   => $show_count,
                show_sink    => $show_sink,
                show_edge    => $show_edge,
                show_owner   => $show_owner,
                response     => $response,
            }
        );
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$L{ls} = $H{list_nodes};

$C{ls} = $C{list_nodes};

$C{list_fds} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $options, $glob ) = ( $command->arguments =~ m{^(-t)?\s*(.*?)$} );
    my $list_types = $options ? $options =~ m{t} : undef;
    my $nodes      = Tachikoma->nodes_by_fd;
    my $response   = [
        [   [ ' FD' => 'right' ], [ 'TYPE' => 'right' ], [ 'NAME' => 'left' ],
        ]
    ];
    for my $fd ( sort { $a <=> $b } keys %{$nodes} ) {
        my $node   = $nodes->{$fd};
        my $name   = $node->{name} || 'unknown';
        my $type   = $node->{type} || 'unknown';
        my $sortby = $list_types ? $type : $name;
        next if ( $glob ne q() and $sortby !~ m{$glob} );
        push @{$response}, [ $fd, $node->{type}, $name ];
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$C{list_ids} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $glob     = $command->arguments;
    my $nodes    = Tachikoma->nodes_by_id;
    my $response = [
        [   [ ' ID'      => 'right' ],
            [ 'ACTIVE'   => 'right' ],
            [ 'INTERVAL' => 'right' ],
            [ 'TYPE'     => 'right' ],
            [ 'NAME'     => 'left' ],
        ]
    ];
    for my $id ( sort { $a <=> $b } keys %{$nodes} ) {
        my $node = $nodes->{$id};
        my $name = $node->{name} || 'unknown';
        next if ( length $glob and $name !~ m{$glob} );
        my $is_active = $node->timer_is_active;
        my $interval  = $node->timer_interval;
        if ( defined $interval ) {
            $interval /= 1000;
        }
        else {
            $interval = '_router';
        }
        push @{$response},
            [
            $id,
            $is_active ? 'yes'     : 'no',
            $is_active ? $interval : 'undef',
            $node->{type}, $name
            ];
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$C{list_timers} = $C{list_ids};

$C{list_pids} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $nodes    = Tachikoma->nodes_by_pid;
    my $response = sprintf "%16s %s\n", 'PID', 'NAME';
    for my $pid ( sort { $a <=> $b } keys %{$nodes} ) {
        my $node = $nodes->{$pid};
        $response .= sprintf "%16d %s\n", $pid, $node->{name} || 'unknown';
    }
    return $self->response( $envelope, $response );
};

$C{list_reconnecting} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $response = q();
    for my $node ( @{ Tachikoma->nodes_to_reconnect } ) {
        $response .= $node->{name} . "\n" if ( length $node->{name} );
    }
    return $self->response( $envelope, $response );
};

$C{list_disabled} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $response = q();
    my $secure_level = $self->configuration->secure_level;
    for my $cmd_name ( sort keys %{ $self->disabled->{$secure_level} } ) {
        $response .= "$cmd_name\n";
    }
    return $self->response( $envelope, $response );
};

$H{scheme} = ["scheme <rsa,rsa-sha256,ed25519>\n"];

$C{scheme} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my $response = undef;
    if ( $command->arguments ) {
        $self->configuration->scheme( $command->arguments );
        $response = $self->okay($envelope);
    }
    else {
        my $scheme = $self->configuration->scheme;
        $response = $self->response( $envelope, "$scheme\n" );
    }
    return $response;
};

$H{make_node} = [
    "make_node <node type> [ <node name> [ <arguments> ] ]\n",
    "    alias: make\n"
];

$C{make_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $type, $name, $arguments ) = split q( ), $command->arguments, 3;
    $self->make_node( $type, $name, $arguments );
    return $self->okay($envelope);
};

$L{make} = $H{make_node};

$C{make} = $C{make_node};

$H{make_connected_node} = [
    "make_connected_node <owner> <node type> [ <node name> [ <arguments> ] ]\n",
    "    alias: make_x\n"
];

$C{make_connected_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $owner, $type, $name, $arguments ) =
        split q( ), $command->arguments, 4;
    $owner = $envelope->from if ( $owner eq q(-) );
    $self->make_node( $type, $name, $arguments, $owner );
    return $self->okay($envelope);
};

$L{make_x} = $H{make_connected_node};

$C{make_x} = $C{make_connected_node};

$H{set_arguments} =
    [ "set_arguments <node name> <arguments>\n", "    alias: set\n" ];

$C{set_arguments} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $name, $arguments ) = split q( ), $command->arguments, 2;
    die qq(no node specified\n) if ( not length $name );
    my $node = $Tachikoma::Nodes{$name};
    die qq(can't find node "$name"\n) if ( not $node );
    $node->arguments($arguments);
    return $self->okay($envelope);
};

$L{set} = $H{set_arguments};

$C{set} = $C{set_arguments};

$H{reinitialize} = [ "reinitialize <node name>\n", "    alias: reinit\n" ];

$C{reinitialize} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $name     = $command->arguments;
    die qq(no node specified\n) if ( not length $name );
    my $node = $Tachikoma::Nodes{$name};
    die qq(can't find node "$name"\n) if ( not $node );
    $node->arguments( $node->arguments );
    return $self->okay($envelope);
};

$L{reinit} = $H{reinitialize};

$C{reinit} = $C{reinitialize};

$H{register} = ["register <source name> <target name> <event>\n"];

$C{register} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'connect_node' )
        or return $self->error("verification failed\n");
    my ( $source, $target, $event ) = split q( ), $command->arguments, 3;
    die qq(no source specified\n) if ( not length $source );
    my $node = $Tachikoma::Nodes{$source};
    die qq(can't find node "$source"\n) if ( not $node );
    die qq(no target specified\n)       if ( not $target );
    die qq(can't find node "$target"\n) if ( not $Tachikoma::Nodes{$target} );
    $node->register( $event => $target );
    return $self->okay($envelope);
};

$H{unregister} = ["unregister <source name> <target name> <event>\n"];

$C{unregister} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'connect_node' )
        or return $self->error("verification failed\n");
    my ( $source, $target, $event ) = split q( ), $command->arguments, 3;
    die qq(no node specified\n) if ( not length $source );
    my $node = $Tachikoma::Nodes{$source};
    die qq(can't find node "$source"\n) if ( not $node );
    die qq(no target specified\n)       if ( not $target );
    $node->unregister( $event => $target );
    return $self->okay($envelope);
};

$H{move_node} = [ "move_node <node name> <new name>\n", "    alias: mv\n" ];

$C{move_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $name, $new_name ) = split q( ), $command->arguments, 2;
    my ( $old_name, $path ) = split m{/}, $envelope->[FROM], 2;
    die qq(no name specified\n)
        if ( not length $name or not length $new_name );
    my $node = $Tachikoma::Nodes{$name};
    die qq(can't find node "$name"\n) if ( not $node );
    $node->name($new_name);
    return $self->okay($envelope);
};

$L{move} = $H{move_node};

$C{move} = $C{move_node};

$L{mv} = $H{move_node};

$C{mv} = $C{move_node};

$H{remove_node} = [
    "remove_node <node name>\n",
    "remove_node -a <anchored regex glob>\n",
    "    alias: rm\n"
];

$C{remove_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $options, $glob ) = ( $command->arguments =~ m{^(-a)?\s*(.*?)$} );
    my $list_matches = $options ? $options =~ m{a} : undef;
    if ( not $glob ) {
        return $self->error( $envelope, qq(no node specified\n) );
    }
    if ( not $list_matches and not $Tachikoma::Nodes{$glob} ) {
        return $self->error( $envelope, qq(can't find node "$glob"\n) );
    }
    my @names = (
        $list_matches
        ? grep m{^$glob$},
        sort keys %Tachikoma::Nodes
        : $glob
    );
    my $out = q();
    for my $name (@names) {
        my $node = $Tachikoma::Nodes{$name};
        my $sink = $node->{sink};
        if ( $sink and $sink->isa('Tachikoma::Nodes::JobController') ) {
            return $self->error( $envelope,
                      $out
                    . qq(ERROR: "$name" is a job,)
                    . qq( use "cmd $sink->{name} stop $name"\n) );
        }
        else {
            if ( $node eq $self ) {
                return $self->error( $envelope,
                    $out . qq(ERROR: refusing to destroy interpreter\n) );
            }
            $node->remove_node;
            $out .= "removed $name\n";
        }
    }
    if ($list_matches) {
        if ($out) {
            $out .= "ok\n";
            return $self->response( $envelope, $out );
        }
        else {
            return $self->error( $envelope, qq(no matches\n) );
        }
    }
    return $self->okay($envelope);
};

$C{remove} = $C{remove_node};

$L{rm} = $H{remove_node};

$C{rm} = $C{remove_node};

$C{dump_metadata} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $response = q();
    for my $name ( sort keys %Tachikoma::Nodes ) {
        my $node     = $Tachikoma::Nodes{$name};
        my $received = $node->{counter};
        my $sent     = $node->{counter};
        my $sink     = $node->{sink} ? $node->{sink}->{name} : q();
        my $owner    = $node->owner;
        my $class    = ref $node;
        my $type     = q();
        my @extra    = ();

        if ( $node->isa('Tachikoma::Nodes::FileHandle') ) {

            if ( $node->{type} eq 'connect' ) {
                $type  = 'host';
                @extra = (
                    exists( $node->{output_buffer} )
                    ? scalar( @{ $node->{output_buffer} } )
                    : 0,
                    $node->{high_water_mark} || 0
                );
            }
            elsif ( $class eq 'Tachikoma::Nodes::FileHandle' ) {
                $type  = 'connector';
                @extra = (
                    exists( $node->{output_buffer} )
                    ? scalar( @{ $node->{output_buffer} } )
                    : 0,
                    $node->{high_water_mark} || 0
                );
            }
        }
        elsif ( $node->isa('Tachikoma::Nodes::Timer') ) {
            if ( $node->isa('Tachikoma::Nodes::Buffer') ) {
                $received = $node->{counter};
                $sent     = $node->{pmsg_sent} + $node->{msg_sent};
                $type     = 'buffer';
                @extra    = (
                    scalar( keys %{ $node->{msg_unanswered} } ),
                    $node->{max_unanswered}
                );
            }
            elsif ( $node->isa('Tachikoma::Nodes::LoadBalancer') ) {
                my $n = 0;
                $n += $node->{msg_unanswered}->{$_}
                    for ( keys %{ $node->{msg_unanswered} } );
                @extra = (
                    $n,
                    $node->{max_unanswered} ? $node->{max_unanswered} : $n
                );
            }
            elsif ( $node->isa('Tachikoma::Nodes::Router') ) {
                $type = 'router';
            }
        }
        $owner = join q(, ), @{$owner} if ( ref $owner eq 'ARRAY' );
        $response .= join( q(|),
            $received, $sent, $name, $sink, $owner || q(),
            $class, $type, @extra )
            . "\n";
    }
    return $self->response( $envelope, $response );
};

$H{dump_node} = [ "dump_node <node name>\n", "    alias: dump\n" ];

$C{dump_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $name, @keys ) = split q( ), $command->arguments;
    my %want     = map { $_ => 1 } @keys;
    my $response = q();
    if ( not length $name ) {
        return $self->error( $envelope, qq(no node specified\n) );
    }
    my $node = $Tachikoma::Nodes{$name};
    if ( not $node ) {
        return $self->error( $envelope, qq(can't find node "$name"\n) );
    }
    my $copy = bless { %{$node} }, 'main';
    $copy->{sink} = $copy->{sink}->{name} if ( $copy->{sink} );
    $copy->{edge} = $copy->{edge}->{name} if ( $copy->{edge} );
    my %normal = map { $_ => 1 } qw( SCALAR ARRAY HASH );
    for my $key ( keys %{$copy} ) {
        my $value = $copy->{$key};
        my $type  = ref $value;
        $copy->{$key} = $type if ( $type and not $normal{$type} );
    }
    for my $key (qw( buffer input_buffer output_buffer inflight messages )) {
        $self->dump_flat( $copy, $key );
    }
    delete $copy->{interpreter};
    for my $key (qw( jobs consumers )) {
        $copy->{$key} = [ keys %{ $copy->{$key} } ] if ( $copy->{$key} );
    }
    if (@keys) {
        for my $key ( keys %{$copy} ) {
            delete $copy->{$key} if ( not $want{$key} );
        }
        for my $key (@keys) {
            return $self->error( $envelope, qq(can't find key "$key"\n) )
                if ( not exists $copy->{$key} );
        }
    }
    $response = Dumper $copy;
    if (@keys) {
        $response =~ s{^.*\n|\n.*$}{}g;
        $response .= "\n";
    }
    return $self->response( $envelope, $response );
};

$L{dump} = $H{dump_node};

$C{dump} = $C{dump_node};

$H{dump_hex} = ["dump_hex <node name> <keys>\n"];

$C{dump_hex} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $name, @keys ) = split q( ), $command->arguments;
    my %want     = map { $_ => 1 } @keys;
    my $response = q();
    if ( not length $name ) {
        return $self->error( $envelope, qq(no node specified\n) );
    }
    if ( not @keys ) {
        return $self->error( $envelope, qq(no keys specified\n) );
    }
    my $node = $Tachikoma::Nodes{$name};
    if ( not $node ) {
        return $self->error( $envelope, qq(can't find node "$name"\n) );
    }
    my $copy = bless { %{$node} }, ref $node;
    for my $key ( keys %{$copy} ) {
        delete $copy->{$key} if ( not $want{$key} );
    }
    for my $key (@keys) {
        return $self->error( $envelope, qq(can't find key "$key"\n) )
            if ( not exists $copy->{$key} );
        my $value = \$copy->{$key};
        if ( ref ${$value} ) {
            return $self->error( $envelope, qq($key is a ref\n) )
                if ( ref ${$value} ne 'SCALAR' );
            $value = ${$value};
        }
        $copy->{$key} = join q(:),
            map sprintf( '%02X', ord ), split m{}, ${$value}
            if ( defined ${$value} );
    }
    $response = Dumper $copy;
    $response =~ s{^.*\n|\n.*$}{}g;
    $response .= "\n";
    bless $copy, 'main';    # don't call DESTROY()
    return $self->response( $envelope, $response );
};

$H{dump_dec} = ["dump_dec <node name> <keys>\n"];

$C{dump_dec} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $name, @keys ) = split q( ), $command->arguments;
    my %want     = map { $_ => 1 } @keys;
    my $response = q();
    if ( not length $name ) {
        return $self->error( $envelope, qq(no node specified\n) );
    }
    if ( not @keys ) {
        return $self->error( $envelope, qq(no keys specified\n) );
    }
    my $node = $Tachikoma::Nodes{$name};
    if ( not $node ) {
        return $self->error( $envelope, qq(can't find node "$name"\n) );
    }
    my $copy = bless { %{$node} }, ref $node;
    for my $key ( keys %{$copy} ) {
        delete $copy->{$key} if ( not $want{$key} );
    }
    for my $key (@keys) {
        return $self->error( $envelope, qq(can't find key "$key"\n) )
            if ( not exists $copy->{$key} );
        my $value = \$copy->{$key};
        if ( ref ${$value} ) {
            return $self->error( $envelope, qq($key is a ref\n) )
                if ( ref ${$value} ne 'SCALAR' );
            $value = ${$value};
        }
        $copy->{$key} = join q(.), map sprintf( '%d', ord ), split m{},
            ${$value}
            if ( defined ${$value} );
    }
    $response = Dumper $copy;
    $response =~ s{^.*\n|\n.*$}{}g;
    $response .= "\n";
    bless $copy, 'main';    # don't call DESTROY()
    return $self->response( $envelope, $response );
};

$H{list_connections} =
    [ "list_connections [ <regex glob> ]\n", "    alias: connections\n" ];

$C{list_connections} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $glob     = $command->arguments;
    my $response = [
        [   [ 'NAME'    => 'right' ],
            [ 'ADDRESS' => 'right' ],
            [ 'SCORE'   => 'right' ],
        ]
    ];
    for my $name ( sort keys %Tachikoma::Nodes ) {
        next if ( length $glob and $name !~ m{$glob} );
        my $node = $Tachikoma::Nodes{$name};
        next
            if ( not $node->isa('Tachikoma::Nodes::Socket')
            or $node->type ne 'connect' );
        my $address = '...';
        if ( $node->{port} ) {
            $address =
                join q(:),
                $node->{address}
                ? join q(.),
                map sprintf( '%d', ord ), split m{}, $node->{address}
                : '...',
                $node->{port};
        }
        elsif ( $node->{filename} ) {
            $address = join q(:), 'unix', $node->{filename};
        }
        my $score =
            $node->{latency_score}
            ? sprintf '%.4f', $node->{latency_score}
            : q(-);
        push @{$response}, [ $name, $address, $score ];
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$L{connections} = $H{list_connections};

$C{connections} = $C{list_connections};

$H{listen_inet} = [
    "listen_inet <address>:<port>\n",
    "listen_inet --address=<address>               \\\n",
    "            --port=<port>                     \\\n",
    "            --io                              \\\n",
    "            --use-ssl                         \\\n",
    "            --ssl-verify                      \\\n",
    "            --ssl-delegate=<node>             \\\n",
    "            --scheme=<rsa,rsa-sha256,ed25519> \\\n",
    "            --delegate=<node>\n",
    "    alias: listen\n"
];

$C{listen_inet} = sub {
    my $self         = shift;
    my $command      = shift;
    my $envelope     = shift;
    my $address      = undef;
    my $port         = undef;
    my $io_mode      = undef;
    my $use_SSL      = undef;
    my $ssl_verify   = undef;
    my $ssl_delegate = undef;
    my $delegate     = undef;
    my $scheme       = undef;
    my $owner        = undef;
    my $id           = $self->configuration->id;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");

    if ( not $command->arguments ) {
        my $config = $self->configuration;
        for my $listen ( @{ $config->listen_sockets } ) {
            my $server_node = undef;
            if ( $listen->{Socket} ) {
                $server_node =
                    unix_server Tachikoma::Nodes::Socket( $listen->{Socket},
                    '_listener' );
            }
            else {
                die qq(inet sockets disabled for keyless servers\n)
                    if ( not length $id );
                $server_node =
                    inet_server Tachikoma::Nodes::Socket( $listen->{Addr},
                    $listen->{Port} );
            }
            $server_node->use_SSL( $listen->{use_SSL} );
            my $okay = eval {
                $server_node->scheme( $listen->{Scheme} )
                    if ( $listen->{Scheme} );
                return 1;
            };
            $self->stderr( $@ || 'FAILED: Tachikoma::Socket::scheme()' )
                if ( not $okay );
            $server_node->sink($self);
        }
        return $self->okay($envelope);
    }

    my ( $r, $argv ) = GetOptionsFromString(
        $command->arguments,
        'address=s'      => \$address,
        'port=i'         => \$port,
        'io'             => \$io_mode,
        'use-ssl'        => \$use_SSL,
        'ssl-verify'     => \$ssl_verify,
        'ssl-delegate=s' => \$ssl_delegate,
        'delegate=s'     => \$delegate,
        'scheme=s'       => \$scheme,
        'owner:s'        => \$owner
    );
    die qq(invalid option\n) if ( not $r );

    # create node
    my $node = undef;
    if ( @{$argv} and not $address ) {
        if ($port) {
            $address = ( $argv->[0] =~ m{^([\w.]+)$} )[0];
        }
        else {
            ( $address, $port ) = ( $argv->[0] =~ m{^([\w.]+):(\d+)$} );
        }
    }
    die qq(no address specified\n) if ( not $address );
    die qq(no port specified\n)    if ( not $port );
    if ($io_mode) {
        $node = inet_server Tachikoma::Nodes::STDIO( $address, $port );
    }
    else {
        die qq(inet sockets disabled for keyless servers\n)
            if ( not length $id );
        $node = inet_server Tachikoma::Nodes::Socket( $address, $port );
    }
    $owner = $envelope->from
        if ( defined $owner and ( not length $owner or $owner eq q(-) ) );
    $node->owner($owner) if ( length $owner );
    $node->use_SSL( $ssl_verify ? 'verify' : 'noverify' ) if ($use_SSL);
    $node->delegates->{ssl}       = $ssl_delegate if ($ssl_delegate);
    $node->delegates->{tachikoma} = $delegate     if ($delegate);
    $node->scheme($scheme) if ($scheme);
    $node->sink($self);
    return $self->okay($envelope);
};

$L{listen} = $H{listen_inet};

$C{listen} = $C{listen_inet};

$H{listen_unix} = [
    "listen_unix <filename> <node name>\n",
    "listen_unix --filename=<filename>             \\\n",
    "            --name=<node name>                \\\n",
    "            --perms=<perms>                   \\\n",
    "            --gid=<gid>                       \\\n",
    "            --io                              \\\n",
    "            --use-ssl                         \\\n",
    "            --ssl-delegate=<node>             \\\n",
    "            --scheme=<rsa,rsa-sha256,ed25519> \\\n",
    "            --delegate=<node>\n",
];

$C{listen_unix} = sub {
    my $self         = shift;
    my $command      = shift;
    my $envelope     = shift;
    my $filename     = undef;
    my $name         = undef;
    my $perms        = undef;
    my $gid          = undef;
    my $io_mode      = undef;
    my $use_SSL      = undef;
    my $ssl_verify   = undef;
    my $ssl_delegate = undef;
    my $delegate     = undef;
    my $scheme       = undef;
    my $owner        = undef;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $r, $argv ) = GetOptionsFromString(
        $command->arguments,
        'filename=s'     => \$filename,
        'name=s'         => \$name,
        'perms=s'        => \$perms,
        'gid=s'          => \$gid,
        'io'             => \$io_mode,
        'use-ssl'        => \$use_SSL,
        'ssl-verify'     => \$ssl_verify,
        'ssl-delegate=s' => \$ssl_delegate,
        'delegate=s'     => \$delegate,
        'scheme=s'       => \$scheme,
        'owner:s'        => \$owner
    );
    die qq(invalid option\n) if ( not $r );

    # create node
    my $node = undef;
    die qq(no filename specified\n) if ( not $filename and not @{$argv} );
    $filename ||= $argv->[0];
    $name     ||= $argv->[1];
    $perms    ||= $argv->[2];
    $gid      ||= $argv->[3];
    die qq(no node name specified\n) if ( not length $name );

    if ($io_mode) {
        $node =
            unix_server Tachikoma::Nodes::STDIO( $filename, $name, $perms,
            $gid );
    }
    else {
        $node =
            unix_server Tachikoma::Nodes::Socket( $filename, $name, $perms,
            $gid );
    }
    $owner = $envelope->from
        if ( defined $owner and ( not length $owner or $owner eq q(-) ) );
    $node->owner($owner) if ( length $owner );
    $node->use_SSL( $ssl_verify ? 'verify' : 'noverify' ) if ($use_SSL);
    $node->delegates->{ssl}       = $ssl_delegate if ($ssl_delegate);
    $node->delegates->{tachikoma} = $delegate     if ($delegate);
    $node->scheme($scheme) if ($scheme);
    $node->sink($self);
    return $self->okay($envelope);
};

$H{connect_inet} = [
    "connect_inet <hostname>[:<port>] [ <node name> ]\n",
    "connect_inet --host <hostname>                 \\\n",
    "             --port <port>                     \\\n",
    "             --name <node name>                \\\n",
    "             --owner <node path>               \\\n",
    "             --io                              \\\n",
    "             --use-ssl                         \\\n",
    "             --scheme=<rsa,rsa-sha256,ed25519> \\\n",
    "             --reconnect\n"
];

$C{connect_inet} = sub {
    my $self        = shift;
    my $command     = shift;
    my $envelope    = shift;
    my $host        = undef;
    my $port        = undef;
    my $name        = undef;
    my $io_mode     = undef;
    my $use_SSL     = undef;
    my $ssl_ca_file = undef;
    my $scheme      = undef;
    my $reconnect   = undef;
    my $owner       = undef;
    my $id          = $self->configuration->id;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    die qq(inet sockets disabled for keyless servers\n)
        if ( not length $id );
    my ( $r, $argv ) = GetOptionsFromString(
        $command->arguments,
        'host=s'        => \$host,
        'port=i'        => \$port,
        'name=s'        => \$name,
        'io'            => \$io_mode,
        'use-ssl'       => \$use_SSL,
        'ssl-ca-file=s' => \$ssl_ca_file,
        'scheme=s'      => \$scheme,
        'reconnect'     => \$reconnect,
        'owner:s'       => \$owner
    );
    die qq(invalid option\n) if ( not $r );

    if ( not $host ) {
        die qq(no host specified\n) if ( not @{$argv} );
        my $host_port = shift @{$argv};
        my ( $host_part, $port_part ) = split m{:}, $host_port, 2;
        $host = $host_part;
        $port ||= $port_part;
    }
    $name ||= shift @{$argv};
    $owner = $envelope->from
        if ( defined $owner and ( not length $owner or $owner eq q(-) ) );
    $self->connect_inet(
        host        => $host,
        port        => $port,
        name        => $name,
        mode        => $io_mode ? 'io' : 'message',
        use_SSL     => $use_SSL,
        SSL_ca_file => $ssl_ca_file,
        scheme      => $scheme,
        reconnect   => $reconnect,
        owner       => $owner
    );
    return $self->okay($envelope);
};

$H{connect_unix} = [
    "connect_unix <unix domain socket> <node name>\n",
    "connect_unix --filename <unix domain socket>   \\\n",
    "             --name <node name>                \\\n",
    "             --io                              \\\n",
    "             --use-ssl                         \\\n",
    "             --scheme=<rsa,rsa-sha256,ed25519> \\\n",
    "             --reconnect\n"
];

$C{connect_unix} = sub {
    my $self        = shift;
    my $command     = shift;
    my $envelope    = shift;
    my $filename    = undef;
    my $name        = undef;
    my $io_mode     = undef;
    my $use_SSL     = undef;
    my $ssl_ca_file = undef;
    my $scheme      = undef;
    my $reconnect   = undef;
    my $owner       = undef;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $r, $argv ) = GetOptionsFromString(
        $command->arguments,
        'filename=s'    => \$filename,
        'name=s'        => \$name,
        'io'            => \$io_mode,
        'use-ssl'       => \$use_SSL,
        'ssl-ca-file=s' => \$ssl_ca_file,
        'scheme=s'      => \$scheme,
        'reconnect'     => \$reconnect,
        'owner:s'       => \$owner
    );
    die qq(invalid option\n) if ( not $r );

    if ( not $filename ) {
        die qq(no filename specified\n) if ( not @{$argv} );
        $filename = shift @{$argv};
    }
    $name ||= shift @{$argv};
    die qq(no node name specified\n) if ( not length $name );
    $owner = $envelope->from
        if ( defined $owner and ( not length $owner or $owner eq q(-) ) );
    $self->connect_unix(
        filename    => $filename,
        name        => $name,
        mode        => $io_mode ? 'io' : 'message',
        use_SSL     => $use_SSL,
        SSL_ca_file => $ssl_ca_file,
        scheme      => $scheme,
        reconnect   => $reconnect,
        owner       => $owner
    );
    return $self->okay($envelope);
};

$H{connect_node} =
    [ "connect_node <node name> [ <node path> ]\n", "    alias: connect\n" ];

$C{connect_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'connect_node' )
        or return $self->error("verification failed\n");
    my ( $name, $owner ) = split q( ), $command->arguments, 2;
    $owner = $envelope->from if ( not length $owner );
    $self->connect_node( $name, $owner );
    return $self->okay($envelope);
};

$L{connect} = $H{connect_node};

$C{connect} = $C{connect_node};

$H{connect_sink} = ["connect_sink <node name> <node name>\n"];

$C{connect_sink} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'connect_node' )
        or return $self->error("verification failed\n");
    my ( $first_name, $second_name ) = split q( ), $command->arguments, 2;
    die qq(no node specified\n) if ( not length $second_name );
    $self->connect_sink( $first_name, $second_name );
    return $self->okay($envelope);
};

$H{connect_edge} = ["connect_edge <node name> <node name>\n"];

$C{connect_edge} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'connect_node' )
        or return $self->error("verification failed\n");
    my ( $first_name, $second_name ) = split q( ), $command->arguments, 2;
    die qq(no node specified\n) if ( not length $second_name );
    $self->connect_edge( $first_name, $second_name );
    return $self->okay($envelope);
};

$L{disconnect_inet} = $H{remove_node};

$C{disconnect_inet} = $C{remove_node};

$L{disconnect_unix} = $H{remove_node};

$C{disconnect_unix} = $C{remove_node};

$H{disconnect_node} = [
    "disconnect_node <node path> [ <node path> ]\n",
    "    alias: disconnect\n"
];

$C{disconnect_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'connect_node' )
        or return $self->error("verification failed\n");
    my ( $name, $owner ) = split q( ), $command->arguments, 2;
    $self->disconnect_node( $name, $owner || $envelope->from );
    return $self->okay($envelope);
};

$L{disconnect} = $H{disconnect_node};

$C{disconnect} = $C{disconnect_node};

$H{disconnect_edge} = ["disconnect_edge <node path>\n"];

$C{disconnect_edge} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'connect_node' )
        or return $self->error("verification failed\n");
    $self->disconnect_edge( $command->arguments );
    return $self->okay($envelope);
};

$H{slurp_file} = [ "slurp_file <file path>\n", "    alias: slurp\n" ];

$C{slurp_file} = sub {
    my $self        = shift;
    my $command     = shift;
    my $envelope    = shift;
    my $buffer_mode = shift // 'binary';
    $self->verify_key( $envelope, ['meta'], 'slurp' )
        or return $self->error("verification failed\n");
    my ( $path, $sink_name ) = split q( ), $command->arguments, 2;
    my $owner = $envelope->[FROM];
    my $name  = ( split m{/}, $owner, 2 )[0];
    my $sink  = undef;
    die qq(no path specified\n)       if ( not $path );
    die qq(no such file: "$path"\n)   if ( not -f $path );
    die qq(can't find node "$name"\n) if ( not $Tachikoma::Nodes{$name} );

    if ( length $sink_name ) {
        $sink = $Tachikoma::Nodes{$sink_name};
        die qq(can't find node "$sink_name"\n) if ( not $sink );
    }
    else {
        $sink = $self;
    }
    do {
        $name = sprintf 'slurp-%016d', Tachikoma->counter;
    } while ( exists $Tachikoma::Nodes{$name} );
    my $node;
    my $okay = eval {
        require Tachikoma::Nodes::Tail;
        $node = Tachikoma::Nodes::Tail->new;
        $node->name($name);
        $node->arguments(
            {   filename       => $path,
                offset         => 0,
                buffer_mode    => $buffer_mode,
                max_unanswered => length $sink_name ? 0
                : (         $buffer_mode
                        and $buffer_mode eq 'line-buffered' ) ? 64
                : 8,
            }
        );
        $node->owner($owner) if ( length $owner );
        $node->sink($sink);
        $node->on_EOF('close');
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ || 'unknown error';
        $okay = eval {
            $node->remove_node;
            return 1;
        };
        if ( not $okay ) {
            my $trap = $@ || 'unknown error';
            $self->stderr("ERROR: remove_node $name failed: $trap");
        }
        die $error;
    }
    return;
};

$L{slurp} = $H{slurp_file};

$C{slurp} = $C{slurp_file};

$H{slurp_lines} = ["slurp_lines <file path>\n"];

$C{slurp_lines} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return &{ $C{slurp_file} }( $self, $command, $envelope, 'line-buffered' );
};

$H{slurp_blocks} = ["slurp_blocks <file path>\n"];

$C{slurp_blocks} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return &{ $C{slurp_file} }
        ( $self, $command, $envelope, 'block-buffered' );
};

$C{tell_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $path, $arguments ) = split q( ), $command->arguments, 2;
    die qq(no path specified\n) if ( not $path );
    my $name = ( split m{/}, $path, 2 )[0];
    die qq(can't find node "$name"\n) if ( not $Tachikoma::Nodes{$name} );
    my $message = Tachikoma::Message->new;
    $message->type(TM_INFO);
    $message->from( $envelope->from );
    $message->to($path);
    $message->payload($arguments) if ( defined $arguments );
    $self->sink->fill($message);
    return $self->okay($envelope);
};

$L{tell} = $HELP->{tell_node};

$C{tell} = $C{tell_node};

$C{request_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $path, $arguments ) = split q( ), $command->arguments, 2;
    die qq(no path specified\n) if ( not $path );
    my $name = ( split m{/}, $path, 2 )[0];
    die qq(can't find node "$name"\n) if ( not $Tachikoma::Nodes{$name} );
    my $message = Tachikoma::Message->new;
    $message->type(TM_REQUEST);
    $message->from( $envelope->from );
    $message->to($path);
    $message->payload($arguments) if ( defined $arguments );
    $self->sink->fill($message);
    return $self->okay($envelope);
};

$L{request} = $HELP->{request_node};

$C{request} = $C{request_node};

$C{send_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $path, $arguments ) = split q( ), $command->arguments, 2;
    die qq(no path specified\n) if ( not $path );
    my $name = ( split m{/}, $path, 2 )[0];
    die qq(can't find node "$name"\n) if ( not $Tachikoma::Nodes{$name} );
    my $message = Tachikoma::Message->new;
    $message->type(TM_BYTESTREAM);
    $message->from( $envelope->from );
    $message->to($path);
    $message->payload("$arguments\n") if ( defined $arguments );
    return $self->sink->fill($message);
};

$L{send} = $HELP->{send_node};

$C{send} = $C{send_node};

$H{send_hex} = ["send_hex <node path> <hex>\n"];

$C{send_hex} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $path, $arguments ) = split q( ), $command->arguments, 2;
    die qq(no path specified\n)       if ( not $path );
    die qq(no hex values specified\n) if ( not defined $arguments );
    my $name = ( split m{/}, $path, 2 )[0];
    die qq(can't find node "$name"\n) if ( not $Tachikoma::Nodes{$name} );
    my $message = Tachikoma::Message->new;
    $message->type(TM_BYTESTREAM);
    $message->from( $envelope->from );
    $message->to($path);
    $message->payload( pack 'C*', map hex,
        @{ [ split m{\s+|:}, $arguments ] } );
    $self->sink->fill($message);
    return;
};

$H{send_eof} = ["send_eof <node path>\n"];

$C{send_eof} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $path     = $command->arguments;
    die qq(no path specified\n) if ( not $path );
    my $name = ( split m{/}, $path, 2 )[0];
    die qq(can't find node "$name"\n) if ( not $Tachikoma::Nodes{$name} );
    my $message = Tachikoma::Message->new;
    $message->type(TM_EOF);
    $message->from( $envelope->from );
    $message->to($path);
    $self->sink->fill($message);
    return;
};

$H{reply_to} = ["reply_to <node path> <command>\n"];

$C{reply_to} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'command_node' )
        or return $self->error("verification failed\n");
    my ( $path, $arguments ) = split q( ), $command->arguments, 2;
    die qq(no path specified\n) if ( not $path );
    my $name = ( split m{/}, $path, 2 )[0];
    die qq(can't find node "$name"\n) if ( not $Tachikoma::Nodes{$name} );
    my ( $cmd_name, $cmd_arguments ) = split q( ), $arguments, 2;
    my $message = $self->command( $cmd_name, $cmd_arguments );
    $message->from($path);
    $self->fill($message);
    return;
};

# XXX: this exists for backward compatibility with the old Shell:
$C{command_node} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'command_node' )
        or return $self->error("verification failed\n");
    my ( $path, $arguments ) = split q( ), $command->arguments, 2;
    die qq(no path specified\n)    if ( not $path );
    die qq(no command specified\n) if ( not $arguments );
    my $name = ( split m{/}, $path, 2 )[0];
    die qq(can't find node "$name"\n) if ( not $Tachikoma::Nodes{$name} );
    my ( $cmd_name, $cmd_arguments ) = split q( ), $arguments, 2;
    my $message = $self->command( $cmd_name, $cmd_arguments );
    $message->type( $envelope->type );
    $message->from( $envelope->from );
    $message->to($path);
    $self->sink->fill($message);
    return;
};

$L{command} = $HELP->{command_node};
$L{cmd}     = $HELP->{command_node};

$C{command} = $C{command_node};
$C{cmd}     = $C{command_node};

$C{on} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'command_node' )
        or return $self->error("verification failed\n");
    my ( $name, $event ) = split q( ), $command->arguments, 2;
    my $func_tree = thaw( $command->payload );
    die qq(no name specified\n)     if ( not length $name );
    die qq(no event specified\n)    if ( not $event );
    die qq(no function specified\n) if ( not $func_tree );
    my $responder = $Tachikoma::Nodes{_responder};
    die "can't find _responder\n" if ( not $responder );
    my $shell = $responder->shell;
    die "can't find shell\n" if ( not $shell );
    my $node = $Tachikoma::Nodes{$name};
    die "can't find node: $name\n" if ( not $node );
    my $id = '999' . $shell->msg_counter;
    $shell->callbacks->{$id} = $func_tree;
    my $okay = eval {
        $node->register( $event, $id, 1 );
        return 1;
    };

    if ( not $okay ) {
        delete $shell->callbacks->{$id};
        die $@;
    }
    return $self->okay($envelope);
};

$H{reset} = ["reset <node name>\n"];

$C{reset} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $name     = $command->arguments;
    die qq(no node specified\n) if ( not length $name );
    my $node = $Tachikoma::Nodes{$name};
    die qq(can't find node "$name"\n) if ( not $node );
    $node->counter(0);
    return $self->okay($envelope);
};

$H{stats} = ["stats [ -a ] [ <regex glob> ]\n"];

$C{stats} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $options, $glob ) = ( $command->arguments =~ m{^(-a)?\s*(.*?)$} );
    my $list_matches = $options ? $options =~ m{a} : undef;
    my $response     = [
        [   [ 'NAME'       => 'left' ],
            [ 'COUNT'      => 'right' ],
            [ 'BUF_SIZE'   => 'right' ],
            [ 'LGST_MSG'   => 'right' ],
            [ 'HIGH_WATER' => 'right' ],
            [ 'READ'       => 'right' ],
            [ 'WRITTEN'    => 'right' ]
        ]
    ];
    for my $name ( sort keys %Tachikoma::Nodes ) {
        next if ( $list_matches and length $glob and $name !~ m{$glob} );
        my $node = $Tachikoma::Nodes{$name};
        my $sink = $node->{sink} ? $node->{sink}->name : q();
        if ( not $list_matches ) {
            if ($glob) {
                next if ( $sink ne $glob );
            }
            else {
                next
                    if (
                    $sink ne $self->{name}
                    or ( ref $node eq 'Tachikoma::Nodes::Socket'
                        and $node->{type} eq 'accept' )
                    );
            }
        }
        push @{$response},
            [
            $name,
            $node->{counter},
            exists( $node->{output_buffer} )
            ? scalar( @{ $node->{output_buffer} } )
            : 0,
            $node->{largest_msg_sent} || 0,
            $node->{high_water_mark}  || 0,
            $node->{bytes_read}       || 0,
            $node->{bytes_written}    || 0
            ];
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$H{dump_config} = ["dump_config [ <regex glob> ]\n"];

$C{dump_config} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $glob     = $command->arguments;
    my $response = q();
    my %skip     = ();
    for my $name ( sort keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        if ( length $glob and $name !~ m{$glob} ) {
            $skip{$name} = 1;
            next;
        }
        if (   $name eq 'command_interpreter'
            or $name eq '_parent'
            or not $node->{sink}
            or not $node->{sink}->{name}
            or $node->isa('Tachikoma::Job')
            or $node->{sink}->isa('Tachikoma::Nodes::JobController') )
        {
            $skip{$name} = 1;
        }
        elsif ( $node->isa('Tachikoma::Nodes::JobFarmer') ) {
            $skip{ $node->{load_balancer}->{name} }  = 1;
            $skip{ $node->{job_controller}->{name} } = 1;
            $skip{ $node->{tee}->{name} }            = 1 if ( $node->{tee} );
        }
        elsif ( $node->isa('Tachikoma::Nodes::Socket')
            and $node->{type} eq 'accept' )
        {
            $skip{ $node->{name} } = 1;
        }
    }
    for my $name ( sort keys %Tachikoma::Nodes ) {
        next if ( $skip{$name} );
        $response .= $Tachikoma::Nodes{$name}->dump_config;
    }
    for my $name ( sort keys %Tachikoma::Nodes ) {
        next if ( $skip{$name} );
        my $node = $Tachikoma::Nodes{$name};
        if ( $node->{sink}->{name} ne 'command_interpreter' ) {
            $response .= "connect_sink $name $node->{sink}->{name}\n";
        }
        if ( $node->{edge} ) {
            $response .= "connect_edge $name $node->{edge}->{name}\n";
        }
        if ( ref $node->{owner} ) {
            for my $owner ( @{ $node->{owner} } ) {
                next if ( $skip{$owner} );
                $response .= "connect_node $name $owner\n";
            }
        }
        elsif ( $node->{owner} ) {
            $response .= "connect_node $name $node->{owner}\n"
                if ( not $skip{ $node->{owner} } );
        }
    }
    return $self->response( $envelope, $response );
};

$H{dump_tachikoma_conf} = ["dump_tachikoma_conf\n"];

$C{dump_tachikoma_conf} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $copy     = { %{ $self->configuration } };
    for my $key (qw( private_key private_ed25519_key )) {
        $copy->{$key} = $copy->{$key} ? q(...) : undef;
    }
    for my $key (qw( public_keys help functions var )) {
        $copy->{$key} = keys %{ $copy->{$key} } ? q({...}) : undef;
    }
    my $response = Dumper $copy;
    return $self->response( $envelope, $response );
};

$H{reload_config} = ["reload_config\n"];

$C{reload_config} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    kill SIGHUP, $$ or die $!;
    return $self->okay($envelope);
};

$L{reload} = $H{reload_config};

$C{reload} = $C{reload_config};

$H{remote_env} = ["env [ <name> [ = <value> ] ]\n"];

$C{remote_env} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'remote_env' )
        or return $self->error("verification failed\n");
    my ( $key, $op, $value ) =
        split m{\s*(=)\s*}, $command->arguments, 2;
    return $self->error("invalid operator: $op\n") if ( $op and $op ne q(=) );
    if ( length $value ) {
        $ENV{$key} = $value;    ## no critic (RequireLocalizedPunctuationVars)
    }
    elsif ( defined $op ) {
        delete $ENV{$key};
    }
    elsif ( defined $key ) {
        if ( defined $ENV{$key} ) {
            my $line = $ENV{$key};
            chomp $line;
            return $self->response( $envelope, "$line\n" );
        }
        else {
            return $self->response( $envelope, qq(undef\n) );
        }
    }
    else {
        my @response = ();
        for my $key ( sort keys %ENV ) {
            next if ( ref $ENV{$key} );
            my $line = "$key=";
            if ( defined $ENV{$key} ) {
                $line .= $ENV{$key};
            }
            else {
                $line .= q(undef);
            }
            chomp $line;
            push @response, $line, "\n";
        }
        return $self->response( $envelope, join q(), @response );
    }
    return $self->okay($envelope);
};

$H{config} = ["config [ <name> [ = <value> ] ]\n"];

$C{config} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'config' )
        or return $self->error("verification failed\n");
    my ( $key, $op, $value ) =
        split m{\s*(=)\s*}, $command->arguments, 2;
    my $var = {};
    $var->{$_} = $self->configuration->{$_} for (@CONFIG_VARIABLES);
    return $self->error("invalid key: $key\n")
        if ( $key and not exists $var->{$key} );
    return $self->error("invalid operator: $op\n") if ( $op and $op ne q(=) );

    if ( length $value ) {
        $self->configuration->{$key} = $value;
    }
    elsif ( defined $op ) {
        $self->configuration->{$key} = undef;
    }
    elsif ( defined $key ) {
        if ( defined $var->{$key} ) {
            my $line = $var->{$key};
            chomp $line;
            return $self->response( $envelope, "$line\n" );
        }
        else {
            return $self->response( $envelope, "\n" );
        }
    }
    else {
        my @response = ();
        for my $key ( sort keys %{$var} ) {
            next if ( ref $var->{$key} );
            my $line = "$key=" . ( $var->{$key} // q() );
            chomp $line;
            push @response, $line, "\n";
        }
        return $self->response( $envelope, join q(), @response );
    }
    return $self->okay($envelope);
};

$H{remote_var} = ["remote_var [ <name> [ = <value> ] ]\n"];

$C{remote_var} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'var' )
        or return $self->error("verification failed\n");
    my ( $key, $op, $value ) =
        split m{\s*([.]=|[|][|]=|=)\s*}, $command->arguments, 2;
    my $var = $self->configuration->var;
    if ( length $value ) {
        my $v = $var->{$key};
        $v = []   if ( not defined $v or not length $v );
        $v = [$v] if ( not ref $v );
        if    ( $op eq q(.=) and @{$v} ) { push @{$v}, q( ); }
        if    ( $op eq q(=) )            { $v = [$value]; }
        elsif ( $op eq q(.=) )           { push @{$v}, $value; }
        elsif ( $op eq q(+=) )           { $v->[0] //= 0; $v->[0] += $value; }
        elsif ( $op eq q(-=) )           { $v->[0] //= 0; $v->[0] -= $value; }
        elsif ( $op eq q(*=) )           { $v->[0] //= 0; $v->[0] *= $value; }
        elsif ( $op eq q(/=) )           { $v->[0] //= 0; $v->[0] /= $value; }
        elsif ( $op eq q(//=) and not @{$v} )           { $v = [$value]; }
        elsif ( $op eq q(||=) and not join q(), @{$v} ) { $v = [$value]; }
        else { return $self->error("invalid operator: $op"); }

        if ( @{$v} > 1 ) {
            $var->{$key} = $v;
        }
        else {
            $var->{$key} = $v->[0];
        }
    }
    elsif ( defined $op ) {
        return $self->error("invalid operator: $op\n") if ( $op ne q(=) );
        delete $var->{$key};
    }
    elsif ( defined $key ) {
        if ( defined $var->{$key} ) {
            if ( ref $var->{$key} ) {
                return $self->response( $envelope,
                          '["'
                        . join( q(", "), grep m{\S}, @{ $var->{$key} } )
                        . qq("]\n) );
            }
            else {
                return $self->response( $envelope, $var->{$key} . "\n" );
            }
        }
        else {
            return $self->response( $envelope, q() );
        }
    }
    else {
        my @response = ();
        for my $key ( sort keys %{$var} ) {
            my $line = "$key=";
            if ( ref $var->{$key} ) {
                $line .= '["'
                    . join( q(", "), grep m{\S}, @{ $var->{$key} } ) . '"]';
            }
            else {
                $line .= $var->{$key};
            }
            chomp $line;
            push @response, $line, "\n";
        }
        return $self->response( $envelope, join q(), @response );
    }
    return $self->okay($envelope);
};

$C{remote_func} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'func' )
        or return $self->error("verification failed\n");
    my $name      = $command->arguments;
    my $payload   = $command->payload;
    my $func_tree = thaw( $payload ? $payload : undef );
    my $functions = $self->configuration->functions;

    if ( defined $func_tree ) {
        $functions->{$name} = $func_tree;
    }
    elsif ( length $name ) {
        delete $functions->{$name};
    }
    else {
        my @response = ();
        for my $name ( sort keys %{$functions} ) {
            push @response, $name, "\n";
        }
        return $self->response( $envelope, join q(), @response );
    }
    return $self->okay($envelope);
};

$C{list_callbacks} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $responder = $Tachikoma::Nodes{_responder};
    die "ERROR: can't find _responder\n" if ( not $responder );
    my $callbacks = $responder->shell->callbacks;
    my $response  = join q(), map "$_\n", sort keys %{$callbacks};
    return $self->response( $envelope, $response );
};

$C{dump_callback} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $responder = $Tachikoma::Nodes{_responder};
    die "ERROR: can't find _responder\n" if ( not $responder );
    my $callbacks = $responder->shell->callbacks;
    my $response  = Dumper( $callbacks->{ $command->arguments } );
    return $self->response( $envelope, $response );
};

$C{remove_callback} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $responder = $Tachikoma::Nodes{_responder};
    die "ERROR: can't find _responder\n" if ( not $responder );
    my $callbacks = $responder->shell->callbacks;
    delete $callbacks->{ $command->arguments };
    return $self->okay($envelope);
};

$H{log} = ["log <message>\n"];

$C{log} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->stderr( $command->arguments );
    return $self->okay($envelope);
};

$H{dmesg} = ["dmesg\n"];

$C{dmesg} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $out      = q();
    return $self->response( $envelope, join q(), @{ Tachikoma->recent_log } );
};

$C{getrusage} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $out      = q();
    require BSD::Resource;
    my @rusage = BSD::Resource::getrusage();
    my @labels = qw(
        usertime systemtime
        maxrss ixrss idrss isrss minflt majflt nswap
        inblock oublock msgsnd msgrcv
        nsignals nvcsw nivcsw
    );

    for my $i ( 0 .. 1 ) {
        $out .= sprintf "%10s: %20f\n", $labels[$i], $rusage[$i];
    }
    for my $i ( 2 .. $#rusage ) {
        $out .= sprintf "%10s: %13d\n", $labels[$i], $rusage[$i];
    }
    return $self->response( $envelope, $out );
};

$H{date} = ["date [ -e | <epoch seconds> ]\n"];

$C{date} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $time     = $command->arguments || $Tachikoma::Now;
    my $out      = q();
    if ( $time eq '-e' ) {
        return $self->response( $envelope, "$Tachikoma::Now\n" );
    }
    elsif ( $time =~ m{^-?[\d.]+$} and $time !~ m{[.].*[.]} ) {
        $out = strftime( "%F %T %Z\n", localtime $time );
        return $self->response( $envelope, $out );
    }
    else {
        return $self->error( $envelope,
            "usage: date [ -e | <epoch seconds> ]\n" );
    }
};

$H{uptime} = ["uptime\n"];

$C{uptime} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $uptime   = $Tachikoma::Now - Tachikoma->init_time;
    my $response = strftime( '%T  up ', localtime $Tachikoma::Now );
    my $days     = int( $uptime / 86400 );
    $uptime -= $days * 86400;
    $response .= $days . ' days, ';
    my $time = strftime( '%T', gmtime $uptime );
    $response .= "$time\n";
    return $self->response( $envelope, $response );
};

$H{hostname} = ["hostname\n"];

$C{hostname} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $hostname = hostname();
    return $self->response( $envelope, "$hostname\n" );
};

$H{version} = ["version\n"];

$C{version} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $version  = $self->configuration->wire_version;
    return $self->response( $envelope, "Tachikoma wire format $version\n" );
};

$H{enable_profiling} = ["enable_profiling\n"];

$C{enable_profiling} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $router   = $Tachikoma::Nodes{_router};
    die "ERROR: can't find _router\n" if ( not $router );
    if ( defined $router->profiles ) {
        return $self->response( $envelope, "profiling already enabled\n" );
    }
    $router->profiles( {} );
    return $self->response( $envelope, "profiling enabled\n" );
};

$H{list_profiles} = ["list_profiles [ <regex glob> ]\n"];

$C{list_profiles} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $glob     = $command->arguments;
    my $router   = $Tachikoma::Nodes{_router};
    my $count    = 0;
    my $total    = {
        time      => 0,
        count     => 0,
        timestamp => 0,
        oldest    => 0
    };
    my @responses = ();
    die "ERROR: can't find _router\n" if ( not $router );
    my $p = $router->profiles;
    push @responses,
        sprintf "%12s %8s %8s %6s %8s %6s %s\n",
        'AVERAGE', 'TIME', 'COUNT', 'WINDOW', 'RATE', 'AGE', 'WHAT';

    for my $key (
        reverse sort { $p->{$a}->{avg} <=> $p->{$b}->{avg} }
        keys %{$p}
        )
    {
        next if ( $glob ne q() and $glob ne 'total' and $key !~ m{$glob} );
        my $info = $p->{$key};
        $total->{time}  += $info->{time};
        $total->{count} += $info->{count};
        $total->{timestamp} = $info->{timestamp}
            if ( $info->{timestamp} > ( $total->{timestamp} // 0 ) );
        $total->{oldest} = $info->{oldest}
            if ( not $total->{oldest} or $info->{oldest} < $total->{oldest} );
        my $age = $info->{timestamp} - $info->{oldest};
        next if ( $glob ne q() and $glob eq 'total' );
        push @responses, sprintf "%12.12s %8.8s %8d %6.6s %8.8s %6d %s\n",
            sprintf( '%.6f', $info->{avg} ),
            sprintf( '%.2f', $info->{time} ), $info->{count},
            sprintf( '%.2f', $age ),
            sprintf( '%.2f',
            ( $age and $info->{count} > 1 ) ? $info->{count} / $age : 1 ),
            $Tachikoma::Right_Now - $info->{timestamp}, $key;
        $count++;
    }
    my $age = $total->{timestamp} - $total->{oldest};
    push @responses, sprintf "%12.12s %8.8s %8d %6.6s %8.8s %6d %s\n",
        sprintf( '%.6f',
        $total->{count} ? $total->{time} / $total->{count} : 0 ),
        sprintf( '%.2f', $total->{time} ), $total->{count},
        sprintf( '%.2f', $age ),
        sprintf( '%.2f',
        ( $age and $total->{count} > 1 ) ? $total->{count} / $age : 1 ),
        $total->{timestamp} ? $Tachikoma::Right_Now - $total->{timestamp} : 0,
        '--total--';
    push @responses, sprintf "returned %d profiles in %.4f seconds\n",
        $count, Time::HiRes::time - $Tachikoma::Right_Now;
    return $self->response( $envelope, join q(), @responses );
};

$H{disable_profiling} = ["disable_profiling\n"];

$C{disable_profiling} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $router   = $Tachikoma::Nodes{_router};
    die "ERROR: can't find _router\n" if ( not $router );
    if ( not $router->profiles ) {
        return $self->response( $envelope, "profiling already disabled\n" );
    }
    $router->profiles(undef);
    return $self->response( $envelope, "profiling disabled\n" );
};

$H{secure} = ["secure [ <level> ]\n"];

$C{secure} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $num       = $command->arguments;
    my $responder = $Tachikoma::Nodes{_responder};
    if ($responder) {
        my $shell = $responder->shell;
        $shell->{last_prompt} = 0 if ($shell);
    }
    my $config       = $self->configuration;
    my $secure_level = $config->secure_level;
    if ( length $num ) {
        if ( $num =~ m{\D} or $num < 1 ) {
            die "ERROR: invalid secure level\n";
        }
        elsif ( $num == $secure_level ) {
            die "ERROR: already at secure level $num\n";
        }
        elsif ( $num < $secure_level ) {
            die "ERROR: can't lower secure level.\n";
        }
        elsif ( $num > 3 ) {
            $config->secure_level(3);
        }
        else {
            $config->secure_level($num);
        }
    }
    elsif ( $secure_level < 1 ) {
        $config->secure_level(1);
    }
    elsif ( $secure_level < 3 ) {
        $config->secure_level( $secure_level + 1 );
    }
    return $self->okay($envelope);
};

$C{insecure} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $responder = $Tachikoma::Nodes{_responder};
    if ($responder) {
        my $shell = $responder->shell;
        $shell->{last_prompt} = 0 if ($shell);
    }
    my $config = $self->configuration;
    die "ERROR: process already secured\n"
        if ( defined $config->secure_level
        and $config->secure_level > 0 );
    $config->secure_level(-1);
    return $self->okay($envelope);
};

$H{initialize} = ["initialize [ <process name> ]\n"];

$C{initialize} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $daemonize = $command->name eq 'daemonize' ? 1 : undef;
    my $name      = $command->arguments || 'tachikoma-server';
    my $router    = $Tachikoma::Nodes{_router};
    my $responder = $Tachikoma::Nodes{_responder};
    die "ERROR: can't find _router\n"    if ( not $router );
    die "ERROR: can't find _responder\n" if ( not $responder );
    die "ERROR: already initialized\n"   if ( $router->type ne 'router' );
    my $interval = $router->timer_interval;
    $router->stop_timer;
    my $node = $responder->sink;

    while ( my $sink = $node->sink ) {
        $node->remove_node;
        $node = $sink;
    }
    Tachikoma->event_framework->close_filehandle($node);
    delete( Tachikoma->nodes_by_fd->{ $node->fd } );
    $responder->client(undef);
    $responder->sink(undef);
    $responder->edge(undef);
    my $okay = eval {
        Tachikoma->initialize( $name, $daemonize );
        return 1;
    };
    if ( not $okay ) {
        print {*STDERR} $@ || "ERROR: initialize: unknown error\n";
        exit 1;
    }
    $router->type('root');
    $router->register_router_node;
    $router->set_timer($interval);
    return;
};

$H{daemonize} = ["daemonize [ <process name> ]\n"];

$C{daemonize} = $C{initialize};

$H{shutdown} = ["shutdown\n"];

$C{shutdown} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->stderr('shutting down - received command');
    $self->shutdown_all_nodes;
    return;
};

$C{pwd} = sub {
    my $self = shift;
    return $self->pwd(@_);
};

$C{prompt} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope, 'tachikoma> ' );
};

sub verify_command {
    my $self         = shift;
    my $message      = shift;
    my $command      = shift;
    my $config       = $self->configuration;
    my $my_id        = $config->id;
    my $secure_level = $config->secure_level;
    return 1 if ( $self->verify_startup( $message, $my_id, $secure_level ) );
    my ( $id, $proto ) = split m{\n}, $command->{signature}, 2;

    if ( not $id ) {
        $self->stderr( 'ERROR: verification of message from ',
            $message->[FROM], q( failed: couldn't find ID) );
        return;
    }
    my ( $scheme, $signature ) = split m{\n}, $proto, 2;
    $signature = $proto
        if ($scheme ne 'rsa'
        and $scheme ne 'rsa-sha256'
        and $scheme ne 'ed25519' );
    if ( not $config->public_keys->{$id} ) {
        $self->stderr( 'ERROR: verification of message from ',
            $message->[FROM], ' failed: ', $id, ' not in authorized_keys' );
        return;
    }
    $self->verify_key( $message, ['command'], $command->{name} )
        or return;
    my $response = undef;
    my $signed   = join q(:),
        $id, $message->[TIMESTAMP], $command->{name},
        $command->{arguments}, $command->{payload};
    if ( $scheme eq 'ed25519' ) {
        return if ( not $self->verify_ed25519( $signed, $id, $signature ) );
    }
    elsif ( $scheme eq 'rsa-sha256' ) {
        return if ( not $self->verify_sha256( $signed, $id, $signature ) );
    }
    else {
        return if ( not $self->verify_rsa( $signed, $id, $signature ) );
    }
    if ( $Tachikoma::Now - $message->[TIMESTAMP] > 300 ) {
        $self->stderr(
            'ERROR: verification of message from ',
            $message->[FROM],
            " failed for $id: ",
            'timestamp too far in the past'
        );
        return;
    }
    elsif ( $message->[TIMESTAMP] - $Tachikoma::Now > 300 ) {
        $self->stderr(
            'ERROR: verification of message from ',
            $message->[FROM],
            " failed for $id: ",
            'timestamp too far in the future'
        );
        return;
    }
    return 1;
}

sub verify_key {
    my $self         = shift;
    my $message      = shift;
    my $tags         = shift;
    my $cmd_name     = shift;
    my $config       = $self->configuration;
    my $my_id        = $config->id;
    my $secure_level = $config->secure_level;
    return 1 if ( $self->verify_startup( $message, $my_id, $secure_level ) );
    my $command = Tachikoma::Command->new( $message->[PAYLOAD] );
    my $id      = ( split m{\n}, $command->{signature}, 2 )[0];
    my $entry   = $config->public_keys->{$id};
    return $self->stderr( 'ERROR: verification of message from ',
        $message->[FROM], ' failed: ', $id, ' not in authorized_keys' )
        if ( not $entry );
    return 1 if ( $id eq $my_id and $secure_level < 1 );

    my $allow_tag = 1;
    for my $tag ( @{$tags} ) {
        next if ( $tag eq 'meta' and $id eq $my_id );
        $allow_tag = undef if ( not $entry->{allow}->{$tag} );
    }
    if ( $allow_tag or $secure_level < 0 ) {
        if ($cmd_name) {
            return 1 if ( not $self->disabled->{$secure_level}->{$cmd_name} );
        }
        else {
            return 1;
        }
    }
    $self->stderr(
        'ERROR: verification of message from ',
        $message->[FROM], ' failed: ', $id,
        ' not allowed to ',
        $cmd_name || $tags->[0]
    );
    return;
}

sub verify_startup {
    my $self         = shift;
    my $message      = shift;
    my $id           = shift;
    my $secure_level = shift;
    return 1
        if (
        ( not length $id and not $secure_level )
        or (    defined $secure_level
            and $secure_level == 0
            and $message->[FROM] =~ m{^(_parent/)*_responder$} )
        );
    return;
}

sub log_command {
    my $self    = shift;
    my $message = shift;
    my $command = shift;
    return
        if ( not length $self->configuration->id
        or $message->[FROM] =~ m{^(_parent/)*_responder$} );
    my $cmd_name = $command->{name};
    my $id       = ( split m{\n}, $command->{signature}, 2 )[0];
    my %comp     = map { $_ => 1 } qw( help ls );

    if ( $cmd_name ne 'prompt'
        and not( $message->[TYPE] & TM_COMPLETION and $comp{$cmd_name} ) )
    {
        my $node          = $self->patron || $self;
        my $cmd_arguments = $command->{arguments};
        $cmd_arguments =~ s{\n}{\\n}g;
        $node->stderr( join q( ), 'FROM:', $message->[FROM], 'ID:',
            $id // q(-),
            'COMMAND:', $cmd_name, $cmd_arguments );
    }
    return;
}

sub make_node {
    my $self      = shift;
    my $type      = shift;
    my $name      = shift;
    my $arguments = shift;
    my $owner     = shift;
    die qq(no type specified\n) if ( not $type );
    $type = ( $type =~ m{^([\w:]+)$} )[0];
    die qq(invalid type specified\n) if ( not $type );
    $name = $type if ( not length $name );
    my $path = $type;
    $path =~ s{::}{/}g;
    die qq(can't create node: "$name" exists\n)
        if ( exists $Tachikoma::Nodes{$name} );
    my $config = $self->configuration;
    my $class  = undef;
    my $rv     = undef;
    my $error  = undef;

    for my $prefix ( @{ $config->include_nodes }, 'Tachikoma::Nodes' ) {
        next if ( not $prefix );
        $class = join q(::), $prefix, $type;
        my $class_path = $class;
        $class_path =~ s{::}{/}g;
        $class_path .= '.pm';
        $rv    = eval { require $class_path };
        $error = $@
            if ( not $rv
            and ( not $error or $error =~ m{^Can't locate \S*$path} ) );
        last if ($rv);
    }
    die $error if ( not $rv );
    my $node = $class->new;
    my $okay = eval {
        $node->name($name);
        $node->arguments( $arguments // q() );
        $node->sink($self);
        $self->connect_node( $name, $owner ) if ( length $owner );
        return 1;
    };
    if ( not $okay ) {
        $error = $@;
        $okay  = eval {
            $node->remove_node;
            return 1;
        };
        $self->stderr("ERROR: can't remove_node $name: $@")
            if ( not $okay );
        die $error;
    }
    return;
}

sub connect_inet {
    my ( $self, %options ) = @_;
    my $host      = $options{host};
    my $port      = $options{port} || q();
    my $name      = $options{name} || $host;
    my $mode      = $options{mode} || 'message';
    my $reconnect = $options{reconnect};
    my $owner     = $options{owner};
    $host = ( $host =~ m{^([\w.-]+)$} )[0];
    $port = ( $port =~ m{^(\d+)$} )[0];
    die qq(can't create node: "$name" exists\n)
        if ( exists $Tachikoma::Nodes{$name} );
    my $connection = undef;

    if ( $mode eq 'message' ) {
        $port ||= 4230;
        $reconnect //= 'true';
        $connection =
            inet_client_async Tachikoma::Nodes::Socket( $host, $port, $name );
    }
    else {
        $connection =
            inet_client_async Tachikoma::Nodes::STDIO( $host, $port, $name );
    }
    $connection->on_EOF('reconnect') if ($reconnect);
    if ( $options{SSL_ca_file} ) {
        $connection->configuration( bless { %{ $self->configuration } },
            'Tachikoma::Config' );
        $connection->configuration->ssl_client_ca_file(
            $options{SSL_ca_file} );
    }
    $connection->use_SSL( $options{use_SSL} );
    $connection->scheme( $options{scheme} ) if ( $options{scheme} );
    $connection->owner($owner)              if ( length $owner );
    $connection->sink($self);
    return;
}

sub connect_unix {
    my ( $self, %options ) = @_;
    my $filename  = $options{filename};
    my $name      = $options{name};
    my $mode      = $options{mode} || 'message';
    my $reconnect = $options{reconnect};
    my $owner     = $options{owner};
    $filename = ( $filename =~ m{^(\S+)$} )[0];
    die qq(no node name specified\n) if ( not length $name );
    die qq(can't create node: "$name" exists\n)
        if ( exists $Tachikoma::Nodes{$name} );
    my $connection = undef;

    if ( $mode eq 'message' ) {
        $reconnect //= 'true';
        $connection =
            unix_client_async Tachikoma::Nodes::Socket( $filename, $name );
    }
    else {
        $connection =
            unix_client_async Tachikoma::Nodes::STDIO( $filename, $name );
    }
    $connection->on_EOF('reconnect') if ($reconnect);
    if ( $options{SSL_ca_file} ) {
        $connection->configuration( bless { %{ $self->configuration } },
            'Tachikoma::Config' );
        $connection->configuration->ssl_client_ca_file(
            $options{SSL_ca_file} );
    }
    $connection->use_SSL( $options{use_SSL} ) if ( $options{use_SSL} );
    $connection->scheme( $options{scheme} )   if ( $options{scheme} );
    $connection->owner($owner)                if ( length $owner );
    $connection->sink($self);
    return;
}

sub connect_sink {
    my $self        = shift;
    my $first_name  = shift;
    my $second_name = shift;
    my $first_node  = $Tachikoma::Nodes{$first_name};
    my $second_node = $Tachikoma::Nodes{$second_name};
    die qq(can't find node "$first_name"\n)  if ( not $first_node );
    die qq(can't find node "$second_name"\n) if ( not $second_node );
    $first_node->sink($second_node);
    return;
}

sub connect_edge {
    my $self        = shift;
    my $first_name  = shift;
    my $second_name = shift;
    my $first_node  = $Tachikoma::Nodes{$first_name};
    my $second_node = $Tachikoma::Nodes{$second_name};
    die qq(can't find node "$first_name"\n)  if ( not $first_node );
    die qq(can't find node "$second_name"\n) if ( not $second_node );
    $first_node->edge($second_node);
    return;
}

sub disconnect_edge {
    my $self = shift;
    my $name = shift;
    my $node = $Tachikoma::Nodes{$name};
    die qq(can't find node "$name"\n) if ( not $node );
    $node->edge(undef);
    return;
}

sub pwd {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              q( )
            . ( $command->arguments || q(/) ) . ' -> '
            . $envelope->from
            . "\n" );
}

sub list_nodes {    ## no critic (ProhibitExcessComplexity)
    my $self         = shift;
    my $options      = shift;
    my $response     = $options->{response};
    my $glob         = $options->{glob};
    my $list_matches = $options->{list_matches};
    my $show_count   = $options->{show_count};
    my $show_sink    = $options->{show_sink};
    my $show_edge    = $options->{show_edge};
    my $show_owner   = $options->{show_owner};

    for my $name ( sort keys %Tachikoma::Nodes ) {
        next if ( $list_matches and length $glob and $name !~ m{$glob} );
        my $node = $Tachikoma::Nodes{$name};
        my $sink = (
              $node->{sink}
            ? $node->{sink}->{name} // '--UNKNOWN--'
            : q()
        );
        my $edge = (
              $node->{edge}
            ? $node->{edge}->{name} // '--UNKNOWN--'
            : q()
        );
        my $owner = q();
        my @row   = ();
        if ( not $list_matches ) {
            if ($glob) {
                next if ( $sink ne $glob );
            }
            else {
                next
                    if (
                    $sink ne $self->{name}
                    or ( ref $node eq 'Tachikoma::Nodes::Socket'
                        and $node->{type} eq 'accept' )
                    );
            }
        }
        push @row, $node->{counter} if ($show_count);
        push @row, $name;
        push @row, $sink ? "> $sink"  : q(- ) if ($show_sink);
        push @row, $edge ? ">> $edge" : q(- ) if ($show_edge);
        if ($show_owner) {
            my $rv = $node->owner;
            if ( ref $rv eq 'ARRAY' ) {
                $owner = join q(, ), @{$rv};
            }
            elsif ( length $rv ) {
                $owner = $rv;
            }
        }
        push @row, length $owner ? "-> $owner" : q(- ) if ($show_owner);
        push @{$response}, \@row;
    }
    if ( $list_matches and $glob and $response eq q() ) {
        push @{$response}, ['no matches'];
    }
    return;
}

sub dump_flat {
    my $self = shift;
    my $copy = shift;
    my $key  = shift;
    return if ( not exists $copy->{$key} );
    if ( ref $copy->{$key} eq 'SCALAR' ) {
        $copy->{$key} = length ${ $copy->{$key} };
    }
    elsif ( ref $copy->{$key} eq 'ARRAY' ) {
        my $first = $copy->{$key}->[0];
        my $count = scalar @{ $copy->{$key} };
        if ($count) {
            $copy->{$key} = [ $first, $count ];
        }
        else {
            $copy->{$key} = [];
        }
    }
    elsif ( ref $copy->{$key} eq 'HASH' ) {
        my ( $first_key, $first_value ) = each %{ $copy->{$key} };
        my $count = scalar keys %{ $copy->{$key} };
        if ($count) {
            $copy->{$key} = { '_COUNT' => $count };
            $copy->{$key}->{$first_key} = $first_value;
        }
        else {
            $copy->{$key} = {};
        }
    }
    return;
}

sub tabulate {
    my $self       = shift;
    my $data       = shift;
    my $header     = shift @{$data};
    my @max        = ();
    my @format     = ();
    my $show_title = ref $header->[0];
    for my $col ( 0 .. $#{$header} ) {
        $max[$col] = length $header->[$col]->[0]
            if ( $show_title
            and length $header->[$col]->[0] > ( $max[$col] || 0 ) );
        for my $row ( @{$data} ) {
            $row->[$col] = q() if ( not defined $row->[$col] );
            $max[$col] = length $row->[$col]
                if ( length $row->[$col] > ( $max[$col] || 0 ) );
        }
    }
    for my $col ( 0 .. $#max ) {
        my $dir = $show_title ? $header->[$col]->[1] : $header->[$col];
        push @format,
            join q(),
            ( $dir eq 'left' ? q(%-) : q(%) ),
            ( $dir eq 'right' or $col < $#max ) ? $max[$col] : q(), 's';
    }
    my $format = join( q( ), @format ) . "\n";
    my $output = $show_title ? sprintf $format, map $_->[0], @{$header} : q();
    for my $row ( @{$data} ) {
        $output .= sprintf $format, @{$row};
    }
    return $output;
}

sub okay {
    my $self    = shift;
    my $message = shift;
    return $self->response( $message, "ok\n" );
}

sub response {
    my ( $self, @args ) = @_;
    my $message  = @args > 1 ? shift @args : undef;
    my $payload  = shift @args;
    my $response = Tachikoma::Message->new;
    if ( $message and $message->type & TM_COMMAND ) {
        my $command = Tachikoma::Command->new( $message->payload );
        $command->payload($payload);
        $response->type( $message->type | TM_RESPONSE );
        $response->payload( $command->packed );
    }
    else {
        $response->type(TM_RESPONSE);
        $response->payload($payload);
    }
    return $response;
}

sub error {
    my ( $self, @args ) = @_;
    my $message  = @args > 1 ? shift @args : undef;
    my $payload  = shift @args;
    my $response = Tachikoma::Message->new;
    if ( $message and $message->type & TM_COMMAND ) {
        my $command = Tachikoma::Command->new( $message->payload );
        $command->payload($payload);
        $response->type( $message->type | TM_ERROR );
        $response->payload( $command->packed );
    }
    else {
        $response->type(TM_ERROR);
        $response->payload($payload);
    }
    return $response;
}

sub name {
    my $self = shift;
    if ( not defined $self->configuration->secure_level ) {
        $self->configuration->secure_level(0);
    }
    return $self->SUPER::name(@_);
}

sub remove_node {
    my $self = shift;
    $self->{patron} = undef;
    $self->SUPER::remove_node;
    return;
}

sub help_topics {
    my $self = shift;
    if (@_) {
        $self->{help_topics} = shift;
    }
    return $self->{help_topics};
}

sub help_links {
    my $self = shift;
    if (@_) {
        $self->{help_links} = shift;
    }
    return $self->{help_links};
}

sub commands {
    my $self = shift;
    if (@_) {
        $self->{commands} = shift;
    }
    return $self->{commands};
}

sub disabled {
    my $self = shift;
    if (@_) {
        my $disabled = shift;
        for my $cmd_name ( keys %{ $disabled->{1} } ) {
            $disabled->{2}->{$cmd_name} = $disabled->{1}->{$cmd_name};
        }
        for my $cmd_name ( keys %{ $disabled->{2} } ) {
            $disabled->{3}->{$cmd_name} = $disabled->{2}->{$cmd_name};
        }
        $self->{disabled} = $disabled;
    }
    return $self->{disabled};
}

sub patron {
    my $self = shift;
    if (@_) {
        $self->{patron} = shift;
    }
    return $self->{patron};
}

1;
