#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Dumper
# ----------------------------------------------------------------------
#
# $Id: Dumper.pm 34670 2018-09-01 06:44:49Z chris $
#

package Tachikoma::Nodes::Dumper;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_EOF TM_PING TM_COMMAND TM_RESPONSE TM_ERROR
    TM_INFO TM_PERSIST TM_STORABLE TM_COMPLETION TM_BATCH
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

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self         = shift;
    my $message      = shift;
    my $type         = $message->[TYPE];
    my $new_type     = $type;
    my $client       = $self->{client};
    my $stdin        = $self->{stdin};
    my $debug        = $self->{debug};
    my $use_readline = $stdin && $stdin->use_readline;
    if ( $type & TM_STORABLE ) {
        $new_type = TM_BYTESTREAM;
        if ( $debug and $debug >= 2 ) {
            $message->payload;
        }
        else {
            my $dumped = Dumper $message->payload;
            $message->payload($dumped);
        }
    }
    elsif ( $type & TM_PING ) {
        $new_type = TM_BYTESTREAM;
        if ( not $debug or $debug <= 1 ) {
            my $rtt = sprintf '%.2f',
                ( $Tachikoma::Right_Now - $message->payload ) * 1000;
            $message->payload("round trip time: $rtt ms\n");
        }
    }
    elsif ( $type & TM_RESPONSE or $type & TM_ERROR ) {
        return 1 if ( $type & TM_COMPLETION and $type & TM_ERROR );
        my $command;
        if ( $type & TM_COMMAND ) {
            $command = Tachikoma::Command->new( $message->[PAYLOAD] );
        }
        if ( $command and $use_readline ) {
            if ( $command->{name} eq 'prompt' ) {
                my $prompt = $command->{payload};
                if ( $type & TM_ERROR ) {
                    chomp $prompt;
                    $prompt .= '> ' if ( $prompt !~ m{> $} );
                }
                $stdin->prompt($prompt);
                return 1;
            }
            elsif ( $type & TM_COMPLETION ) {
                $stdin->set_completions( $command->{name},
                    $command->{payload} );
                return 1;    # TLDNR
            }
        }
        $new_type = TM_BYTESTREAM;
        if ($command) {
            if ( $debug and $debug >= 2 ) {
                $message->payload($command);
            }
            else {
                $message->[PAYLOAD] = $command->{payload};
            }
        }
    }
    elsif ( $type & TM_COMMAND ) {
        my $command = Tachikoma::Command->new( $message->[PAYLOAD] );
        $new_type = TM_BYTESTREAM;
        my ( $id, $signature ) = split m{\n}, $command->{signature}, 2;
        $command->{signature} = $id;
        if ( $debug and $debug >= 2 ) {
            $message->[PAYLOAD] = $command;
        }
        else {
            $message->[PAYLOAD] = Dumper $command;
        }
    }
    elsif ($type & TM_INFO
        or $type & TM_BATCH
        or ( $type & TM_EOF and $debug ) )
    {
        $new_type = TM_BYTESTREAM;
    }
    if ($debug) {
        if ( $debug > 1 ) {
            $message->[PAYLOAD] = join q{},
                Dumper(
                {   type      => $message->type_as_string,
                    from      => $message->[FROM],
                    to        => $message->[TO],
                    id        => $message->[ID],
                    stream    => $message->[STREAM],
                    timestamp => strftime(
                        '%F %T %Z', localtime( $message->[TIMESTAMP] )
                    ),
                    payload => $message->[PAYLOAD],
                }
                );
        }
        else {
            $message->[PAYLOAD] = join q{},
                $message->type_as_string, ' from ', $message->[FROM], ":\n",
                $message->[PAYLOAD];
        }
    }
    $message->[TYPE] = $new_type;
    if ( $new_type & TM_BYTESTREAM ) {
        if ( $use_readline and $self->{newline} ) {
            my $clear  = q{};
            my $length = $stdin->line_length;
            my $width  = $stdin->width;
            if ( $length > $width ) {
                my $lines = $length / $width;
                $clear .= "\e[A" x int $lines;
            }
            $clear .= "\r\e[J";
            $message->[PAYLOAD] = join q{}, $clear, $message->[PAYLOAD];
        }
        elsif ( $client and $client eq 'SILCbot' ) {
            my $payload = q{};
            for my $line ( split m{^}, $message->[PAYLOAD] ) {
                chomp $line;
                $payload .= join q{}, "\e[31m", $line, "\e[0m\n";
            }
            $message->[PAYLOAD] = $payload;
        }
    }
    my $rv = $self->SUPER::fill($message);
    if ( $use_readline and $new_type & TM_BYTESTREAM ) {
        if ( $message->[PAYLOAD] =~ m{\n$} ) {
            $self->{newline} = 1;
            $stdin->prompt;
        }
        else {
            $self->{newline} = undef;
        }
    }
    return $rv;
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
