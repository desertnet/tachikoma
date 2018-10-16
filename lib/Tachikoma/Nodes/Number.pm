#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Number
# ----------------------------------------------------------------------
#
# $Id: Number.pm 9677 2011-01-08 01:39:41Z chris $
#

package Tachikoma::Nodes::Number;
use strict;
use warnings;
use Tachikoma::Nodes::Echo;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use File::MkTemp;
use parent qw( Tachikoma::Nodes::Echo );

use version; our $VERSION = 'v2.0.368';

my $Default_Start_Offset = 0;
my %C                    = ();

sub help {
    my $self = shift;
    return <<'EOF';
make_node Number <node name> <filename> [ <start offset> ]
EOF
}

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{filename}    = undef;
    $self->{list}        = {};
    $self->{offset}      = undef;
    $self->{interpreter} = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        if ( $self->{arguments} ) {
            my ( $filename, $start_offset ) = split q( ), $self->{arguments},
                2;
            $start_offset //= $Default_Start_Offset;
            my $offset   = $start_offset - 1;
            my %new_list = ();
            $self->{filename} = $filename;
            my $fh = undef;
            if ( not open $fh, '<', $filename ) {
                $self->stderr("WARNING: couldn't open: $!");
            }
            else {
                while ( my $line = <$fh> ) {
                    my ( $number, $value ) = split q( ), $line, 2;
                    $new_list{$value} = $number;
                    $offset = $number if ( $number > $offset );
                }
                close $fh or warn;
            }
            $self->{list}   = \%new_list;
            $self->{offset} = $offset + 1;
        }
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->{interpreter}->fill($message)
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $output = q();
    for my $item ( split m{^}, $message->[PAYLOAD] ) {
        my $number = $self->add_item($item);
        $output .= join q( ), $number, $item;
    }
    $self->write_list;
    $message->[PAYLOAD] = $output;
    return $self->SUPER::fill($message);
}

sub add_item {
    my $self = shift;
    my $item = shift;
    $item .= "\n" if ( substr( $item, -1, 1 ) ne "\n" );
    my $number = $self->{list}->{$item};
    if ( not defined $number ) {
        $number = $self->{offset}++;
    }
    $self->{list}->{$item} = $number;
    return $number;
}

sub remove_item {
    my $self = shift;
    my $item = shift;
    $item .= "\n" if ( substr( $item, -1, 1 ) ne "\n" );
    delete $self->{list}->{$item};
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
    my $list      = $self->patron->list;
    my @responses = ();
    for my $item ( sort { $list->{$a} <=> $list->{$b} } keys %{$list} ) {
        next if ( length $glob and $item !~ m{$glob} );
        push @responses, $list->{$item}, q( ), $item;
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
    return $self->okay($envelope);
};

$C{rm} = $C{remove};

$C{clear} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $glob     = $command->arguments;
    my $list     = $self->patron->list;
    return $self->error( $envelope, 'no pattern' ) if ( not length $glob );
    for my $item ( keys %{$list} ) {
        if ( $item =~ m{$glob} ) {
            delete $list->{$item};
            next;
        }
    }
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
        ( $fh, $template ) = mkstempt( '.temp-' . ( 'X' x 16 ), $parent );
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ // 'unknown error';
        return $self->stderr("ERROR: mkstempt failed: $error");
    }
    my $list = $self->{list};
    my $tmp = join q(/), $parent, $template;
    for my $item ( sort { $list->{$a} <=> $list->{$b} } keys %{$list} ) {
        print {$fh} $list->{$item}, q( ), $item;
    }
    close $fh or warn;
    rename $tmp, $path
        or $self->stderr("ERROR: couldn't move $tmp to $path: $!");
    return;
}

sub list {
    my $self = shift;
    if (@_) {
        $self->{list} = shift;
    }
    return $self->{list};
}

sub offset {
    my $self = shift;
    if (@_) {
        $self->{offset} = shift;
    }
    return $self->{offset};
}

1;
