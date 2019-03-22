#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Dumper
# ----------------------------------------------------------------------
#
# $Id: Dumper.pm 36808 2019-03-20 18:08:28Z chris $
#

package Tachikoma::Nodes::Dumper;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_EOF TM_PING TM_COMMAND TM_RESPONSE TM_ERROR
    TM_INFO TM_PERSIST TM_STORABLE TM_COMPLETION TM_BATCH TM_REQUEST
);
use Tachikoma::Command;
use Data::Dumper;
use POSIX qw( strftime );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Useperl  = 1;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{debug}   = undef;
    $self->{stdin}   = undef;
    $self->{client}  = undef;
    $self->{newline} = 1;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->{debug}     = $self->{arguments};
    }
    return $self->{arguments};
}

sub fill {
    my $self         = shift;
    my $message      = shift;
    my $type         = undef;
    my $use_readline = undef;
    $use_readline = $self->{stdin}->use_readline if ( $self->{stdin} );
    if ( $message->[TYPE] & TM_STORABLE ) {
        $type = $self->dump_storable($message);
    }
    elsif ( $message->[TYPE] & TM_PING ) {
        $type = $self->dump_ping($message);
    }
    elsif ( $message->[TYPE] & TM_RESPONSE or $message->[TYPE] & TM_ERROR ) {
        $type = $self->dump_response( $message, $use_readline ) or return;
    }
    elsif ( $message->[TYPE] & TM_COMMAND ) {
        $type = $self->dump_command($message);
    }
    elsif ($message->[TYPE] & TM_INFO
        or $message->[TYPE] & TM_REQUEST
        or $message->[TYPE] & TM_BATCH
        or ( $message->[TYPE] & TM_EOF and $self->{debug} ) )
    {
        $type = TM_BYTESTREAM;
    }
    $self->dump_message($message) if ( $self->{debug} );
    $message->[TYPE] = $type if ($type);
    $self->add_style( $message, $use_readline )
        if ( $message->[TYPE] & TM_BYTESTREAM );
    $self->SUPER::fill($message);
    $self->update_prompt( $message, $use_readline )
        if ( $message->[TYPE] & TM_BYTESTREAM );
    return;
}

sub dump_storable {
    my $self    = shift;
    my $message = shift;
    if ( $self->{debug} and $self->{debug} >= 2 ) {
        $message->payload;
    }
    else {
        my $dumped = Dumper $message->payload;
        $message->payload($dumped);
    }
    return TM_BYTESTREAM;
}

sub dump_ping {
    my $self    = shift;
    my $message = shift;
    if ( not $self->{debug} or $self->{debug} <= 1 ) {
        my $rtt = sprintf '%.2f',
            ( $Tachikoma::Right_Now - $message->payload ) * 1000;
        $message->payload("round trip time: $rtt ms\n");
    }
    return TM_BYTESTREAM;
}

sub dump_response {
    my $self         = shift;
    my $message      = shift;
    my $use_readline = shift;
    my $type         = $message->[TYPE];
    my $command;
    return if ( $type & TM_COMPLETION and $type & TM_ERROR );
    $message->[TYPE] = TM_BYTESTREAM;
    return TM_BYTESTREAM if ( not $type & TM_COMMAND );
    $command = Tachikoma::Command->new( $message->[PAYLOAD] );
    my ( $id, $signature ) = split m{\n}, $command->{signature}, 2;
    $command->{signature} = $id;

    if ($use_readline) {
        if ( $command->{name} eq 'prompt' ) {
            my $prompt = $command->{payload};
            if ( $type & TM_ERROR ) {
                chomp $prompt;
                $prompt .= '> ' if ( $prompt !~ m{> $} );
            }
            $self->{newline} = 1;
            $self->{stdin}->prompt($prompt);
            return;
        }
        elsif ( $type & TM_COMPLETION ) {
            $self->{stdin}
                ->set_completions( $command->{name}, $command->{payload} );
            return;    # TLDNR
        }
    }
    if ( $self->{debug} and $self->{debug} >= 2 ) {
        $message->payload($command);
    }
    else {
        $message->[PAYLOAD] = $command->{payload};
    }
    return TM_BYTESTREAM;
}

sub dump_command {
    my $self    = shift;
    my $message = shift;
    my $command = Tachikoma::Command->new( $message->[PAYLOAD] );
    my ( $id, $signature ) = split m{\n}, $command->{signature}, 2;
    $command->{signature} = $id;
    if ( $self->{debug} and $self->{debug} >= 2 ) {
        $message->[PAYLOAD] = $command;
    }
    else {
        $message->[PAYLOAD] = Dumper $command;
    }
    return TM_BYTESTREAM;
}

sub dump_message {
    my $self    = shift;
    my $message = shift;
    if ( $self->{debug} > 1 ) {
        $message->[PAYLOAD] = join q(),
            Dumper(
            {   type   => $message->type_as_string,
                from   => $message->[FROM],
                to     => $message->[TO],
                id     => $message->[ID],
                stream => $message->[STREAM],
                timestamp =>
                    strftime( '%F %T %Z', localtime $message->[TIMESTAMP] ),
                payload => $message->[PAYLOAD],
            }
            );
    }
    else {
        $message->[PAYLOAD] = join q(),
            $message->type_as_string, ' from ', $message->[FROM], ":\n",
            $message->[PAYLOAD];
    }
    return;
}

sub add_style {
    my $self         = shift;
    my $message      = shift;
    my $use_readline = shift;
    return if ( not length $message->[PAYLOAD] );
    if ( $use_readline and $self->{newline} ) {
        my $clear  = q();
        my $length = $self->{stdin}->line_length;
        my $width  = $self->{stdin}->width;
        if ( $length > $width ) {
            my $lines = $length / $width;
            $clear .= "\e[A" x int $lines;
        }
        $clear .= "\r\e[J";
        $message->[PAYLOAD] = join q(), $clear, $message->[PAYLOAD];
    }
    elsif ( $self->{client} and $self->{client} eq 'SILCbot' ) {
        my $payload = q();
        for my $line ( split m{^}, $message->[PAYLOAD] ) {
            chomp $line;
            $payload .= join q(), "\e[31m", $line, "\e[0m\n";
        }
        $message->[PAYLOAD] = $payload;
    }
    return;
}

sub update_prompt {
    my $self         = shift;
    my $message      = shift;
    my $use_readline = shift;
    return if ( not $use_readline );
    if ( $message->[PAYLOAD] =~ m{\n$} ) {
        $self->{newline} = 1;
        $self->{stdin}->prompt;
    }
    else {
        $self->{newline} = undef;
    }
    return;
}

sub debug {
    my $self = shift;
    if (@_) {
        $self->{debug} = shift;
    }
    return $self->{debug};
}

sub stdin {
    my $self = shift;
    if (@_) {
        $self->{stdin} = shift;
    }
    return $self->{stdin};
}

sub client {
    my $self = shift;
    if (@_) {
        $self->{client} = shift;
    }
    return $self->{client};
}

sub newline {
    my $self = shift;
    if (@_) {
        $self->{newline} = shift;
    }
    return $self->{newline};
}

1;
