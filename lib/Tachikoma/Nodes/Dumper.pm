#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Dumper
# ----------------------------------------------------------------------
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
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Useperl  = 1;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{newline} = 1;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
    }
    return $self->{arguments};
}

sub fill {
    my $self         = shift;
    my $message      = shift;
    my $type         = undef;
    my $use_readline = undef;
    my $stdin        = $Tachikoma::Nodes{_stdin};
    $use_readline = $stdin->use_readline
        if ( $stdin and $stdin->isa('Tachikoma::Nodes::TTY') );
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
    elsif (
           $message->[TYPE] & TM_INFO
        or $message->[TYPE] & TM_REQUEST
        or $message->[TYPE] & TM_BATCH
        or (    $message->[TYPE] & TM_EOF
            and $self->{configuration}->{debug_level} )
        )
    {
        $type = TM_BYTESTREAM;
    }
    $self->dump_message($message)
        if ( $self->{configuration}->{debug_level} );
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
    if (    $self->{configuration}->{debug_level}
        and $self->{configuration}->{debug_level} >= 2 )
    {
        $message->payload;
    }
    else {
        my $dumped = Dumper( $message->payload );
        $message->payload($dumped);
    }
    return TM_BYTESTREAM;
}

sub dump_ping {
    my $self    = shift;
    my $message = shift;
    if ( not $self->{configuration}->{debug_level}
        or $self->{configuration}->{debug_level} <= 1 )
    {
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
    return               if ( $type & TM_COMPLETION and $type & TM_ERROR );
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
            $Tachikoma::Nodes{_stdin}->prompt($prompt);
            return;
        }
        elsif ( $type & TM_COMPLETION ) {
            $Tachikoma::Nodes{_stdin}
                ->set_completions( $command->{name}, $command->{payload} );
            return;    # TLDNR
        }
    }
    elsif ( $command->{name} eq 'prompt' ) {
        $message->[TYPE]    = TM_BYTESTREAM;
        $message->[PAYLOAD] = $command->{payload};
        $self->SUPER::fill($message);
        return;
    }
    if (    $self->{configuration}->{debug_level}
        and $self->{configuration}->{debug_level} >= 2 )
    {
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
    if (    $self->{configuration}->{debug_level}
        and $self->{configuration}->{debug_level} >= 2 )
    {
        $message->[PAYLOAD] = $command;
    }
    else {
        $message->[PAYLOAD] = Dumper($command);
    }
    return TM_BYTESTREAM;
}

sub dump_message {
    my $self    = shift;
    my $message = shift;
    if ( $self->{configuration}->{debug_level} > 1 ) {
        $message->[PAYLOAD] = $message->as_string;
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
        my $length = $Tachikoma::Nodes{_stdin}->line_length;
        my $width  = $Tachikoma::Nodes{_stdin}->width;
        if ( $length > $width ) {
            my $lines = $length / $width;
            $clear .= "\e[A" x int $lines;
        }
        $clear .= "\r\e[J";
        $message->[PAYLOAD] = join q(), $clear, $message->[PAYLOAD];
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
        $Tachikoma::Nodes{_stdin}->prompt;
    }
    else {
        $self->{newline} = undef;
    }
    return;
}

sub newline {
    my $self = shift;
    if (@_) {
        $self->{newline} = shift;
    }
    return $self->{newline};
}

1;
