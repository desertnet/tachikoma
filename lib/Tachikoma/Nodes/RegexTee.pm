#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::RegexTee
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::RegexTee;
use strict;
use warnings;
use Tachikoma::Nodes::Tee;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw(
    TYPE TO ID STREAM PAYLOAD
    TM_BYTESTREAM TM_COMMAND TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use parent qw( Tachikoma::Nodes::Tee );

use version; our $VERSION = qv('v2.0.368');

my %C = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{branches}    = {};
    $self->{owner}       = q();
    $self->{interpreter} = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    return $self;
}

sub fill {
    my $self       = shift;
    my $message    = shift;
    my $branches   = $self->{branches};
    my $message_id = undef;
    my $persist    = undef;
    my $count      = 0;
    return $self->handle_response( $message, scalar keys %{$branches} )
        if ( $message->[TYPE] == ( TM_PERSIST | TM_RESPONSE )
        or $message->[TYPE] == TM_ERROR );
    return $self->interpreter->fill($message)
        if ( $message->[TYPE] & TM_COMMAND or $message->[TYPE] & TM_EOF );

    if ( $message->[TYPE] & TM_PERSIST ) {
        $message_id = $self->msg_counter;
        $self->{messages}->{$message_id} = {
            original  => $message,
            count     => undef,
            answer    => 0,
            cancel    => 0,
            timestamp => $Tachikoma::Now
        };
        $persist = 'true';
        if ( not $self->{timer_is_active} ) {
            $self->set_timer;
        }
    }
    for my $name ( keys %{$branches} ) {
        my ( $destination, $regex ) = @{ $branches->{$name} };
        if ( not defined $regex or $message->[PAYLOAD] =~ m{$regex} ) {
            my $copy = bless [ @{$message} ], ref $message;
            $copy->[TO] = $destination;
            if ($persist) {
                $self->stamp_message( $copy, $self->{name} ) or return;
                $copy->[ID] = $message_id;
            }
            $self->{sink}->fill($copy);
            $count++;
        }
    }
    if ( $self->{owner} ) {
        my $copy = bless [ @{$message} ], ref $message;
        $copy->[TO] = $self->{owner};
        if ($persist) {
            $self->stamp_message( $copy, $self->{name} ) or return;
            $copy->[ID] = $message_id;
        }
        $self->{sink}->fill($copy);
        $count++;
    }
    if ($persist) {
        if ( $count > 0 ) {
            $self->{messages}->{$message_id}->{count} = $count;
        }
        else {
            delete $self->{messages}->{$message_id};
            $self->cancel($message);
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
    my $branches = $self->patron->branches;
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
    if ( not exists $self->patron->branches->{$name} ) {
        $self->patron->branches->{$name} =
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
    if ( exists $self->patron->branches->{$name} ) {
        delete $self->patron->branches->{$name};
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
    my $branches = $self->branches;
    for my $name ( sort keys %{$branches} ) {
        my ( $destination, $regex ) = @{ $branches->{$name} };
        $response .= "command $self->{name} add_branch"
            . " $name $destination $regex\n";
    }
    return $response;
}

sub owner {
    my $self = shift;
    if (@_) {
        my $owner = shift;
        $self->SUPER::owner($owner);
    }
    else {
        my $branches = $self->{branches};
        my $owners   = [
            sort grep {length}
            map       { $branches->{$_}->[0] } keys %{$branches}
        ];
        push @{$owners}, $self->{owner} if ( length $self->{owner} );
        return $owners;
    }
    return $self->{owner};
}

sub branches {
    my $self = shift;
    if (@_) {
        $self->{branches} = shift;
    }
    return $self->{branches};
}

1;
