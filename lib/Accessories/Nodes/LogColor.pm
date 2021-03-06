#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::LogColor
# ----------------------------------------------------------------------
#
# $Id$
#

package Accessories::Nodes::LogColor;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE TIMESTAMP PAYLOAD TM_BYTESTREAM );
use POSIX qw( strftime );
use Time::Local;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.400');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{months} = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node LogColor <node name>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my $i = 0;
        my $m = {
            map { $_ => $i++ }
                qw(
                jan feb mar apr may jun jul aug sep oct nov dec
                )
        };
        $self->months($m);
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->SUPER::fill($message)
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $payload = $message->[PAYLOAD];
    chomp $payload;
    if ( not length $payload ) {
        $self->stderr( 'ERROR: empty log message from ', $message->from );
        $self->cancel($message);
        return;
    }
    if ( $payload =~ m{^\d+} ) {
        if ( $payload =~ m{COMMAND} ) {
            $payload = "\e[41m\e[2K$payload\e[0m";
        }
        elsif ( $payload =~ m{TRAP} ) {
            $payload = "\e[41m\e[2K$payload\e[0m";
        }
        elsif ( $payload =~ m{ERROR} ) {
            $payload = "\e[41m\e[2K$payload\e[0m";
        }
        elsif ( $payload =~ m{WARNING} ) {
            $payload = "\e[43m\e[2K$payload\e[0m";
        }
        elsif ( $payload =~ m{INFO} ) {
            $payload = "\e[90m$payload\e[0m";
        }
        elsif ( $payload =~ m{DEBUG} ) {
            $payload = "\e[90m$payload\e[0m";
        }
        else {
            $payload = "\e[93m$payload\e[0m";
        }
    }
    else {
        $payload = $self->cleanup_syslog( $message, $payload );
        if ( $payload =~ m{ su\b| sudo\b} ) {
            $payload = "\e[42m\e[2K$payload\e[0m";
        }
        elsif ( $payload =~ m{ svnagent} ) {
            $payload = "\e[92m$payload\e[0m";
        }
        elsif ( $payload =~ m{(?<!without )(error|fail)}i
            and $payload =~ m{(?<!\bno )$1}i )
        {
            $payload = "\e[91m$payload\e[0m";
        }
        elsif ( $payload =~ m{warn}i ) {
            $payload = "\e[91m$payload\e[0m";
        }
        elsif ( $payload =~ m{congestion drops}i ) {
            $payload = "\e[95m\e[2K$payload\e[0m";
        }
        else {
            $payload = "\e[90m$payload\e[0m";
        }
    }
    $message->[PAYLOAD] = "$payload\n";
    $self->SUPER::fill($message);
    return;
}

sub cleanup_syslog {
    my $self     = shift;
    my $message  = shift;
    my $original = shift;
    my $payload  = $original;
    my $m        = $self->months;
    my $months   = join q(|), keys %{$m};
    $payload =~ s{^($months)\s+(\d{1,2}) (\d\d):(\d\d):(\d\d)}{}i;
    if ( $payload =~ s{^(\s+\S+)\s+(\d{4}-\d\d-\d\d [\d:,]+)}{$2$1} ) {
        $payload =~ s{(\s+])}{]}g;
    }
    else {
        $payload =~ s{^(\S)}{ $1};
        $payload = strftime( '%F %T %Z', localtime $message->[TIMESTAMP] )
            . $payload;
    }
    return $payload;
}

sub months {
    my $self = shift;
    if (@_) {
        $self->{months} = shift;
    }
    return $self->{months};
}

1;
