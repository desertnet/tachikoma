#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Shell
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Shell;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( FROM TM_BYTESTREAM TM_COMMAND TM_PING TM_NOREPLY );
use Tachikoma::Command;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

my %BUILTINS = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{concatenation} = undef;
    $self->{is_attached}   = undef;
    $self->{isa_tty}       = undef;
    $self->{last_prompt}   = 0;
    $self->{mode}          = 'command';
    $self->{path}          = undef;
    $self->{prefix}        = undef;
    $self->{prompt}        = undef;
    $self->{show_commands} = undef;
    $self->{validate}      = undef;
    $self->{want_reply}    = undef;
    bless $self, $class;
    return $self;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[FROM] eq '_stdin' ) {
        $message->[FROM] = '_responder';
    }
    else {
        $self->stderr( 'ERROR: invalid shell input: ', $message->as_string );
        exit 1;
    }
    return $self->sink->fill($message)
        if ( not $message->type & TM_BYTESTREAM );
    for my $line ( split m{^}, $message->payload ) {
        $self->parse_line($line);
    }
    return 1;
}

sub parse_line {
    my $self = shift;
    my $line = shift;
    chomp $line;
    $line =~ s{^\s*|\s*$}{}g;
    my ( $name, $arguments ) = split q( ), $line, 2;
    if ( $line =~ m{^#} ) {
        $self->prompt;
        return;
    }
    elsif ( $line =~ m{\\$} ) {
        $self->mode('concatenation');
        my $concatenation = $self->concatenation || {};
        if ( not $concatenation->{name} ) {
            $arguments ||= q();
            $arguments =~ s{\\$}{};
            $concatenation->{name}      = $name;
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
        $name      = $self->concatenation->{name};
        $arguments = join q(), $self->concatenation->{arguments}, $line;
        $self->concatenation(undef);
    }
    elsif ( $line !~ m{\S} ) {
        $self->prompt;
        return;
    }
    if ( $BUILTINS{$name} ) {
        &{ $BUILTINS{$name} }( $self, $arguments );
    }
    else {
        $self->send_command( $self->path, $name, $arguments );
    }
    $self->prompt if ( $self->{isa_tty} );
    return;
}

$BUILTINS{'chdir'} = sub {
    my $self      = shift;
    my $arguments = shift;
    my $cwd       = $self->path;
    $self->path( $self->cd( $cwd, $arguments ) );
    return;
};

$BUILTINS{'cd'} = $BUILTINS{'chdir'};

$BUILTINS{'command_node'} = sub {
    my $self      = shift;
    my $arguments = shift;
    my ( $path, $new_name, $new_arguments ) =
        ( split q( ), $arguments // q(), 3 );
    $self->send_command( $self->prefix($path), $new_name, $new_arguments );
    return;
};

$BUILTINS{'command'} = $BUILTINS{'command_node'};
$BUILTINS{'cmd'}     = $BUILTINS{'command_node'};

$BUILTINS{'show_commands'} = sub {
    my $self      = shift;
    my $arguments = shift;
    $self->show_commands($arguments);
    return;
};

$BUILTINS{'want_reply'} = sub {
    my $self      = shift;
    my $arguments = shift;
    my $value     = not $self->want_reply;
    $value = $arguments if ( length $arguments );
    $self->want_reply($value);
    return;
};

$BUILTINS{'respond'} = sub {
    my $self      = shift;
    my $arguments = shift;
    Tachikoma->nodes->{_responder}->ignore(undef);
    return;
};

$BUILTINS{'ignore'} = sub {
    my $self      = shift;
    my $arguments = shift;
    Tachikoma->nodes->{_responder}->ignore('true');
    return;
};

$BUILTINS{'ping'} = sub {
    my $self      = shift;
    my $arguments = shift;
    my $message   = Tachikoma::Message->new;
    $message->type(TM_PING);
    $message->from('_responder');
    $message->to( $self->prefix($arguments) );
    $message->payload($Tachikoma::Right_Now);
    $self->sink->fill($message);
    return;
};

$BUILTINS{'pwd'} = sub {
    my $self = shift;
    $self->send_command( 'pwd', $self->path );
    return;
};

$BUILTINS{'sleep'} = sub {
    my $self      = shift;
    my $arguments = shift;
    sleep $arguments;
    return;
};

$BUILTINS{'shell'} = sub {
    my $self      = shift;
    my $arguments = shift;
    $self->is_attached('true');
    return;
};

sub send_command {
    my $self      = shift;
    my $path      = shift;
    my $name      = shift;
    my $arguments = shift;
    my $message   = $self->command( $name, $arguments );
    $message->type( TM_COMMAND | TM_NOREPLY ) if ( not $self->{want_reply} );
    $message->from('_responder');
    $message->to($path);
    $self->stderr("+ $name $arguments") if ( $self->{show_commands} );
    $self->sink->fill($message);
    return;
}

sub cd {
    my $self = shift;
    my $cwd  = shift || q();
    my $path = shift || q();
    if ( $path =~ m{^/} ) {
        $cwd = $path;
    }
    elsif ( $path =~ m{^[.][.]/?} ) {
        $cwd  =~ s{/?[^/]+$}{};
        $path =~ s{^[.][.]/?}{};
        $cwd = $self->cd( $cwd, $path );
    }
    elsif ($path) {
        $cwd .= "/$path";
    }
    $cwd =~ s{(^/|/$)}{}g if ($cwd);
    return $cwd;
}

sub get_completions {}

sub name {
    my $self = shift;
    if (@_) {
        die "ERROR: named Shell nodes are not allowed\n";
    }
    return $self->{name};
}

sub concatenation {
    my $self = shift;
    if (@_) {
        $self->{concatenation} = shift;
    }
    return $self->{concatenation};
}

sub errors {
    my $self = shift;
    if (@_) {
        $self->{errors} = shift;
    }
    return $self->{errors};
}

sub is_attached {
    my $self = shift;
    if (@_) {
        $self->{is_attached} = shift;
    }
    return $self->{is_attached};
}

sub isa_tty {
    my $self = shift;
    if (@_) {
        $self->{isa_tty} = shift;
    }
    return $self->{isa_tty};
}

sub last_prompt {
    my $self = shift;
    if (@_) {
        $self->{last_prompt} = shift;
    }
    return $self->{last_prompt};
}

sub mode {
    my $self = shift;
    if (@_) {
        $self->{mode} = shift;
    }
    return $self->{mode};
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
    push @paths, $self->{prefix} if ( length $self->{prefix} );
    push @paths, $self->{path}   if ( length $self->{path} );
    push @paths, $path           if ( length $path );
    return join q(/), @paths;
}

sub prompt {
    my $self = shift;
    if ( $Tachikoma::Now - $self->{last_prompt} > 60 ) {
        $self->{prompt}      = $self->command('prompt');
        $self->{last_prompt} = $Tachikoma::Now;
    }
    my $message = $self->{prompt};
    $message->from('_responder');
    $message->to( $self->path );
    $self->sink->fill($message);
    return;
}

sub set_prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
}

sub show_commands {
    my $self = shift;
    if (@_) {
        $self->{show_commands} = shift;
    }
    return $self->{show_commands};
}

sub validate {
    my $self = shift;
    if (@_) {
        $self->{validate} = shift;
    }
    return $self->{validate};
}

sub want_reply {
    my $self = shift;
    if (@_) {
        $self->{want_reply} = shift;
    }
    return $self->{want_reply};
}

sub builtins {
    return \%BUILTINS;
}

1;
