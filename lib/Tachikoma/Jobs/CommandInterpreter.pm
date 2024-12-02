#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::CommandInterpreter
# ----------------------------------------------------------------------
#

package Tachikoma::Jobs::CommandInterpreter;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Nodes::Shell;
use Tachikoma::Nodes::Shell2;
use Tachikoma::Message qw( TYPE FROM PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.280');

sub initialize_graph {
    my $self        = shift;
    my $shell       = undef;
    my $interpreter = Tachikoma::Nodes::CommandInterpreter->new;
    my @lines       = split m{^}, $self->arguments || q();
    if ( @lines and $lines[0] eq "v1\n" ) {
        shift @lines;
        $shell = Tachikoma::Nodes::Shell->new;
        $shell->{counter}++;
    }
    else {
        $shell = Tachikoma::Nodes::Shell2->new;
        if ( @lines and $lines[0] eq "v2\n" ) {
            shift @lines;
            $shell->{counter}++;
        }
    }
    Tachikoma->nodes->{_responder}->shell($shell);
    $shell->sink($interpreter);
    $interpreter->name('_command_interpreter');
    $interpreter->sink( $self->router );
    $self->connector->sink($interpreter);
    $self->sink( $self->router );

    for my $line (@lines) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_BYTESTREAM;
        $message->[FROM]    = '_stdin';
        $message->[PAYLOAD] = $line;
        $shell->fill($message);
    }
    return;
}

1;
