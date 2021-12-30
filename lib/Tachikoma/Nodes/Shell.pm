#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Shell
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Shell;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TM_BYTESTREAM TM_COMMAND TM_NOREPLY );
use Tachikoma::Command;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{mode}          = 'command';
    $self->{concatenation} = undef;
    bless $self, $class;
    return $self;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->sink->fill($message)
        if ( not $message->type & TM_BYTESTREAM );
    for my $line ( split m{^}, $message->payload ) {
        $self->parse_line($line);
    }
    return 1;
}

sub parse_line {
    my $self    = shift;
    my $line    = shift;
    my $message = Tachikoma::Message->new;
    my $command = Tachikoma::Command->new;
    chomp $line;
    $line =~ s{^\s*|\s*$}{}g;
    my ( $name, $arguments ) = split q( ), $line, 2;
    if ( $line =~ m{^#} ) {
        return;
    }
    elsif ( $line =~ m{\\$} ) {
        $self->mode('concatenation');
        my $concatenation = $self->concatenation || {};
        if ( not $concatenation->{name} ) {
            $arguments ||= q();
            $arguments =~ s{\\$}{};
            $concatenation->{name} = $name;
            $concatenation->{arguments} = join q(), $arguments, "\n";
        }
        else {
            $line =~ s{\\$}{};
            $concatenation->{arguments} .= join q(), $line, "\n";
        }
        $self->concatenation($concatenation);
        return;
    }
    elsif ( $self->mode eq 'concatenation' ) {
        $self->mode('command');
        $name = $self->concatenation->{name};
        $arguments = join q(), $self->concatenation->{arguments}, $line;
        $self->concatenation(undef);
    }
    elsif ( $line !~ m{\S} ) {
        return;
    }
    if ( $name eq 'chdir' or $name eq 'cd' ) {
        my $cwd = $self->path;
        $self->path( $self->cd( $cwd, $arguments ) );
        return;
    }
    elsif ( $name eq 'command' or $name eq 'cmd' ) {
        my ( $path, $new_name, $new_arguments ) =
            ( split q( ), $arguments // q(), 3 );
        $message->to( $self->prefix($path) );
        $command->name( $new_name // q() );
        $command->arguments( $new_arguments // q() );
    }
    else {
        $message->to( $self->path );
        $command->name($name);
        $command->arguments($arguments);
    }
    $command->sign( $self->scheme, $message->timestamp );
    $message->type( TM_COMMAND | TM_NOREPLY );
    $message->from('_responder');
    $message->payload( $command->packed );
    return $self->sink->fill($message);
}

sub cd {
    my $self = shift;
    my $cwd  = shift || q();
    my $path = shift || q();
    if ( $path =~ m{^/} ) {
        $cwd = $path;
    }
    elsif ( $path =~ m{^[.][.]/?} ) {
        $cwd =~ s{/?[^/]+$}{};
        $path =~ s{^[.][.]/?}{};
        $cwd = $self->cd( $cwd, $path );
    }
    elsif ($path) {
        $cwd .= "/$path";
    }
    $cwd =~ s{(^/|/$)}{}g if ($cwd);
    return $cwd;
}

sub name {
    my $self = shift;
    if (@_) {
        die "ERROR: named Shell nodes are not allowed\n";
    }
    return $self->{name};
}

sub mode {
    my $self = shift;
    if (@_) {
        $self->{mode} = shift;
    }
    return $self->{mode};
}

sub concatenation {
    my $self = shift;
    if (@_) {
        $self->{concatenation} = shift;
    }
    return $self->{concatenation};
}

sub path {
    my $self = shift;
    if (@_) {
        $self->{path} = shift;
    }
    return $self->{path};
}

sub prefix {
    my $self  = shift;
    my $path  = shift;
    my @paths = ();
    push @paths, $self->{path} if ( length $self->{path} );
    push @paths, $path         if ( length $path );
    return join q(/), @paths;
}

1;
