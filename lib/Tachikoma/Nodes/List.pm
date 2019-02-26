#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::List
# ----------------------------------------------------------------------
#
# $Id: List.pm 9677 2011-01-08 01:39:41Z chris $
#

package Tachikoma::Nodes::List;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use File::Temp qw( tempfile );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.368');

my %C = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{filename}    = undef;
    $self->{list}        = [];
    $self->{interpreter} = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    $self->{registrations}->{ADD} = {};
    $self->{registrations}->{RM}  = {};
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        if ( $self->{arguments} ) {
            my $filename = $self->{arguments};
            my @new_list = ();
            $self->{filename} = $filename;
            my $fh = undef;
            if ( not open $fh, '<', $filename ) {
                $self->stderr("WARNING: couldn't open: $!");
            }
            else {
                push @new_list, $_ while (<$fh>);
                close $fh or die "couldn't close $filename: $!";
                $self->{list} = \@new_list;
            }
        }
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->{interpreter}->fill($message)
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $item = $message->[PAYLOAD];
    $self->add_item($item);
    $self->write_list;
    $self->notify( 'ADD' => "add $item" );
    return $self->SUPER::fill($message);
}

sub add_item {
    my $self = shift;
    my $item = shift;
    $item .= "\n" if ( substr( $item, -1, 1 ) ne "\n" );
    push @{ $self->{list} }, $item;
    return;
}

sub remove_item {
    my $self = shift;
    my $item = shift;
    $item .= "\n" if ( substr( $item, -1, 1 ) ne "\n" );
    my @new_list = ();
    for my $old_item ( @{ $self->{list} } ) {
        next if ( $old_item eq $item );
        push @new_list, $old_item;
    }
    $self->{list} = \@new_list;
    return;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list <regex>\n"
            . "          add <entry>\n"
            . "          remove <entry>\n"
            . "          clear <regex>\n" );
};

$C{list} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $glob      = $command->arguments;
    my @responses = ();
    for my $item ( @{ $self->patron->list } ) {
        next if ( length $glob and $item !~ m{$glob} );
        push @responses, $item;
    }
    return $self->response( $envelope, join q(), @responses );
};

$C{ls} = $C{list};

$C{add} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $item     = $command->arguments;
    return $self->error( $envelope, 'no item' ) if ( not length $item );
    $self->patron->add_item($item);
    $self->patron->write_list;
    $self->patron->notify( 'ADD' => "add $item" );
    return $self->okay($envelope);
};

$C{remove} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $item     = $command->arguments;
    return $self->error( $envelope, 'no pattern' ) if ( not length $item );
    $self->patron->remove_item($item);
    $self->patron->write_list;
    $self->patron->notify( 'RM' => "rm $item" );
    return $self->okay($envelope);
};

$C{rm} = $C{remove};

$C{clear} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $glob     = $command->arguments;
    my @new_list = ();
    return $self->error( $envelope, 'no pattern' ) if ( not length $glob );
    for my $item ( @{ $self->patron->list } ) {
        if ( $item =~ m{$glob} ) {
            $self->patron->notify( 'RM' => "rm $item" );
            next;
        }
        push @new_list, $item;
    }
    $self->patron->list( \@new_list );
    $self->patron->write_list;
    return $self->okay($envelope);
};

sub write_list {
    my $self = shift;
    my $path = $self->{filename} or return;
    my $fh;
    my $template;
    my $parent = ( $path =~ m{^(.*)/[^/]+$} )[0];
    my $okay   = eval {
        $self->make_dirs($parent);
        ( $fh, $template ) =
            tempfile( '.temp-' . ( 'X' x 16 ), DIR => $parent );
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ || 'unknown error';
        return $self->stderr("ERROR: tempfile failed: $error");
    }
    print {$fh} join q(), @{ $self->{list} };
    close $fh
        or $self->stderr("ERROR: couldn't close $template: $!");
    rename $template, $path
        or $self->stderr("ERROR: couldn't move $template to $path: $!");
    return;
}

sub list {
    my $self = shift;
    if (@_) {
        $self->{list} = shift;
    }
    return $self->{list};
}

1;
