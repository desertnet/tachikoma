#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::RegexTee
# ----------------------------------------------------------------------
#
# $Id: RegexTee.pm 35525 2018-10-22 11:59:58Z chris $
#

package Tachikoma::Nodes::RegexTee;
use strict;
use warnings;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw( TYPE TO PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Nodes::CommandInterpreter );

use version; our $VERSION = qv('v2.0.368');

my %C = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{commands} = \%C;
    $self->{branches} = {};
    bless $self, $class;
    return $self;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( not $message->[TYPE] & TM_BYTESTREAM ) {
        return $self->SUPER::fill($message);
    }
    my $branches = $self->{branches};
    for my $name ( keys %{$branches} ) {
        my ( $destination, $regex ) = @{ $branches->{$name} };
        if ( not defined $regex or $message->[PAYLOAD] =~ m{$regex} ) {
            my $copy = bless [ @{$message} ], ref $message;
            $copy->[TO] = $destination;
            $self->{sink}->fill($copy);
        }
    }
    $self->{counter}++;
    return;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list_branches\n"
            . "          add_branch <name> <destination path> [ <regex> ]\n"
            . "          remove_branch <name>\n" );
};

$C{list_branches} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $branches = $self->branches;
    my $response = undef;
    if ( $command->arguments eq '-a' ) {
        $response = join( "\n", sort keys %{$branches} ) . "\n";
    }
    else {
        my $table = [
            [   [ 'NAME'        => 'left' ],
                [ 'DESTINATION' => 'right' ],
                [ 'REGEX'       => 'right' ]
            ]
        ];
        for my $name ( sort keys %{$branches} ) {
            push @{$table}, [ $name, @{ $branches->{$name} } ];
        }
        $response = $self->tabulate($table);
    }
    return $self->response( $envelope, $response );
};

$C{ls} = $C{list_branches};

$C{add_branch} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'connect_node' )
        or return $self->error("verification failed\n");
    my ( $name, $destination, $regex ) = split q( ), $command->arguments, 3;
    if ( not exists $self->branches->{$name} ) {
        $self->branches->{$name} =
            [ $destination, $regex ? qr{$regex} : undef ];
        return $self->okay($envelope);
    }
    else {
        return $self->error( $envelope,
            "can't create, branch exists: $name\n" );
    }
};

$C{add} = $C{add_branch};

$C{remove_branch} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $name     = $command->arguments;
    $self->verify_key( $envelope, ['meta'], 'connect_node' )
        or return $self->error("verification failed\n");
    if ( exists $self->branches->{$name} ) {
        delete $self->branches->{$name};
        return $self->okay($envelope);
    }
    else {
        return $self->error( $envelope,
            "can't remove, no such branch: $name\n" );
    }
};

$C{rm} = $C{remove_branch};

sub dump_config {
    my $self     = shift;
    my $response = $self->SUPER::dump_config;
    my $branches = $self->{branches};
    for my $name ( sort keys %{$branches} ) {
        my ( $destination, $regex ) = @{ $branches->{$name} };
        $response .= "command $self->{name} add_branch"
            . " $name $destination $regex\n";
    }
    return $response;
}

sub owner {
    my $self = shift;
    return [
        sort map { $self->{branches}->{$_}->[0] }
            keys %{ $self->{branches} }
    ];
}

sub branches {
    my $self = shift;
    if (@_) {
        $self->{branches} = shift;
    }
    return $self->{branches};
}

1;
