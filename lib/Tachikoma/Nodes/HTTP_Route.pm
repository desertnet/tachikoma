#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_Route
# ----------------------------------------------------------------------
#
# $Id: HTTP_Route.pm 1733 2009-05-06 22:36:14Z chris $
#

package Tachikoma::Nodes::HTTP_Route;
use strict;
use warnings;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Nodes::HTTP_Responder qw( log_entry cached_strftime );
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_EOF
);
use parent qw( Tachikoma::Nodes::CommandInterpreter );

use version; our $VERSION = 'v2.0.314';

my %C = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{commands} = \%C;
    $self->{servers}  = undef;
    $self->{paths}    = {};
    bless $self, $class;
    return $self;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( not $message->[TYPE] & TM_STORABLE ) {
        return $self->SUPER::fill($message);
    }
    my $request      = $message->payload;
    my $servers      = $self->{servers};
    my $server_class = (
          $servers
        ? $servers->{ $request->{headers}->{host} || q() }
        : undef
    );
    my $paths = (
          $servers
        ? $self->{paths}->{ $server_class || q() }
        : $self->{paths}
    );
    if ( not $paths ) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_BYTESTREAM;
        $response->[TO]      = $message->[FROM];
        $response->[STREAM]  = $message->[STREAM];
        $response->[PAYLOAD] = join q(),
            "HTTP/1.1 404 NOT FOUND\n",
            'Date: ', cached_strftime(), "\n",
            "Server: Tachikoma\n",
            "Connection: close\n",
            "Content-Type: text/plain; charset=utf8\n",
            "\n",
            "Requested URL not found.\n";
        $self->{sink}->fill($response);
        log_entry( $self, 404, $message );
        $response->[TYPE] = TM_EOF;
        $response->[TO]   = $message->[FROM];
        return $self->{sink}->fill($response);
    }
    my $path        = $request->{path};
    my $destination = undef;
    my @components  = grep length, split m{/+}, $path;
    my @new_path    = ();
    while (@components) {
        my $test_path = q(/) . join q(/), @components;
        my $test = $paths->{$test_path};
        ( $destination = $test ) and last if ($test);
        unshift @new_path, pop @components;
    }
    $request->{path} = join q(/), q(), @new_path;
    $destination ||= $paths->{q(/)};
    $message->[TO] = $destination;
    $self->{counter}++;
    return $self->{sink}->fill($message);
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list_servers\n"
            . "          add_server <virtual server> <class>\n"
            . "          remove_server <virtual server>\n"
            . "          list_paths [ <class> ]\n"
            . "          add_path [ <class> ] <path> <destination path>\n"
            . "          remove_path <class> <path>\n" );
};

$C{list_servers} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $servers  = $self->servers;
    my $response = undef;
    if ( $command->arguments eq '-a' ) {
        $response = join( "\n", sort keys %{$servers} ) . "\n";
    }
    else {
        $response = sprintf "%-40s %s\n", 'SERVER', 'CLASS';
        for my $server ( sort keys %{$servers} ) {
            $response .= sprintf "%-40s %s\n", $server, $servers->{$server};
        }
    }
    return $self->response( $envelope, $response );
};

$C{add_server} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $server, $server_class ) = split q( ), $command->arguments, 2;
    if ( not $server_class ) {
        return $self->error( $envelope, "please specify a server class\n" );
    }
    if ( not $self->servers ) {
        return $self->error(
            $envelope,
            join q(),
            'if specified, servers must be specified before paths.',
            "  you can remove the current paths and try again.\n"
        ) if ( keys %{ $self->paths } );
        $self->servers( {} );
    }
    if ( not exists $self->servers->{$server} ) {
        $self->servers->{$server} = $server_class;
        return $self->okay($envelope);
    }
    else {
        return $self->error( $envelope,
            "can't create, server exists: $server\n" );
    }
};

$C{remove_server} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $server   = $command->arguments;
    if ( exists $self->servers->{$server} ) {
        delete $self->servers->{$server};
        return $self->okay($envelope);
    }
    else {
        return $self->error( $envelope,
            "can't remove, no such server: $server\n" );
    }
};

$C{list_paths} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $paths    = $self->paths;
    my $response = undef;
    if ( $command->arguments eq '-a' ) {
        $response = join( "\n", sort keys %{$paths} ) . "\n";
    }
    elsif ( $command->arguments ) {
        if ( not $self->servers ) {
            return $self->error( $envelope, "no server classes defined\n" );
        }
        my $server_paths = $paths->{ $command->arguments };
        if ( not $server_paths ) {
            return $self->error( $envelope, "no servers in class\n" );
        }
        $response = sprintf "%-40s %s\n", 'PATH', 'DESTINATION';
        for my $path ( sort keys %{$server_paths} ) {
            $response .= sprintf " %-39s %s\n", $path, $paths->{$path};
        }
    }
    elsif ( $self->servers ) {
        $response = sprintf "%-10s %-40s %s\n", 'CLASS', 'PATH',
            'DESTINATION';
        for my $server_class ( sort keys %{$paths} ) {
            my $server_paths = $paths->{$server_class};
            for my $path ( sort keys %{$server_paths} ) {
                $response .= sprintf "%-10s %-40s %s\n",
                    $server_class, $path, $server_paths->{$path};
            }
        }
    }
    else {
        $response = sprintf "%-40s %s\n", 'PATH', 'DESTINATION';
        for my $path ( sort keys %{$paths} ) {
            $response .= sprintf " %-39s %s\n", $path, $paths->{$path};
        }
    }
    return $self->response( $envelope, $response );
};

$C{ls} = $C{list_paths};

$C{add_path} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $paths    = $self->paths;
    my ( $server_class, $path, $destination );
    if ( $self->servers ) {
        ( $server_class, $path, $destination ) =
            split q( ), $command->arguments, 3;
        if ( not exists $paths->{$server_class} ) {
            $paths->{$server_class} = {};
        }
        $paths = $paths->{$server_class};
    }
    else {
        my $extra;
        ( $path, $destination, $extra ) =
            split q( ), $command->arguments, 3;
        if ($extra) {
            return $self->error(
                $envelope, join q(),
                'extra arguments to add_path.',
                "  did you mean to add servers first?\n"
            );
        }
    }
    if ( not exists $paths->{$path} ) {
        $paths->{$path} = $destination;
        return $self->okay($envelope);
    }
    else {
        return $self->error( $envelope,
            "can't create, path exists: $path\n" );
    }
};

$C{add} = $C{add_path};

$C{remove_path} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $paths    = $self->paths;
    if ( $self->servers ) {
        my ( $server_class, $path ) = split q( ), $command->arguments, 2;
        if ( exists $paths->{$server_class} ) {
            my $server_paths = $paths->{$server_class};
            if ( exists $server_paths->{$path} ) {
                delete $server_paths->{$path};
                delete $paths->{$server_class}
                    if ( not keys %{$server_paths} );
                return $self->okay($envelope);
            }
            else {
                return $self->error( $envelope,
                    "can't remove, no such path: $path\n" );
            }
        }
        else {
            return $self->error( $envelope,
                "can't remove, no such server class: $server_class\n" );
        }
    }
    else {
        my $path = $command->arguments;
        if ( exists $paths->{$path} ) {
            delete $paths->{$path};
            return $self->okay($envelope);
        }
        else {
            return $self->error( $envelope,
                "can't remove, no such path: $path\n" );
        }
    }
};

$C{rm} = $C{remove_path};

sub dump_config {
    my $self     = shift;
    my $response = $self->SUPER::dump_config;
    my $servers  = $self->{servers};
    my $paths    = $self->{paths};
    if ($servers) {
        for my $server ( sort keys %{$servers} ) {
            my $server_class = $servers->{$server};
            $response .= join q(),
                "command $self->{name}",
                " add_server $server $server_class\n";
        }
        for my $server_class ( sort keys %{$paths} ) {
            my $server_paths = $paths->{$server_class};
            for my $path ( sort keys %{$server_paths} ) {
                my $destination = $server_paths->{$path};
                $response .= join q(),
                    "command $self->{name}",
                    " add_path $server_class $path $destination\n";
            }
        }
    }
    else {
        for my $path ( sort keys %{$paths} ) {
            my $destination = $paths->{$path};
            $response .= join q(),
                "command $self->{name}",
                " add_path $path $destination\n";
        }
    }
    return $response;
}

sub owner {
    my $self   = shift;
    my $owners = {};
    my $paths  = $self->{paths};
    if ( $self->{servers} ) {
        for my $server_class ( sort keys %{$paths} ) {
            my $server_paths = $paths->{$server_class};
            for my $path ( sort keys %{$server_paths} ) {
                my $destination = $server_paths->{$path};
                $owners->{$destination} = undef;
            }
        }
    }
    else {
        for my $path ( sort keys %{$paths} ) {
            my $destination = $paths->{$path};
            $owners->{$destination} = undef;
        }
    }
    return [ sort keys %{$owners} ];
}

sub servers {
    my $self = shift;
    if (@_) {
        $self->{servers} = shift;
    }
    return $self->{servers};
}

sub paths {
    my $self = shift;
    if (@_) {
        $self->{paths} = shift;
    }
    return $self->{paths};
}

1;
