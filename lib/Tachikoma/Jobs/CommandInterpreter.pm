#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::CommandInterpreter
# ----------------------------------------------------------------------
#
# $Id: CommandInterpreter.pm 36854 2019-03-23 06:12:03Z chris $
#

package Tachikoma::Jobs::CommandInterpreter;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Nodes::Shell;
use Tachikoma::Nodes::Shell2;
use Tachikoma::Nodes::Responder;
use Tachikoma::Message qw( TYPE FROM PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.280');

sub initialize_graph {
    my $self        = shift;
    my $interpreter = Tachikoma::Nodes::CommandInterpreter->new;
    $self->connector->sink($interpreter);
    $interpreter->name('command_interpreter');
    $interpreter->sink( $self->router );
    $self->sink( $self->router );
    my $shell     = undef;
    my $responder = Tachikoma::Nodes::Responder->new;
    my @lines     = split m{^}, $self->arguments || q();
    if ( @lines and $lines[0] eq "v1\n" ) {
        shift @lines;
        $shell = Tachikoma::Nodes::Shell->new;
        $shell->{counter}++;
    }
    else {
        $shell = Tachikoma::Nodes::Shell2->new;
        $shell->responder($responder);
        if ( @lines and $lines[0] eq "v2\n" ) {
            shift @lines;
            $shell->{counter}++;
        }
    }
    $responder->name('_responder');
    $responder->shell($shell);
    $shell->sink($interpreter);
    for my $line (@lines) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_BYTESTREAM;
        $message->[PAYLOAD] = $line;
        $shell->fill($message);
    }
    return;
}

1;
