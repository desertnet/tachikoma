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
make_node LogColor <node name> <directory> [ <timeout> ]
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
    return $self->stderr( 'ERROR: empty log message from ', $message->from )
        if ( not length $payload );
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
        $payload = $self->cleanup_syslog( $message, $payload )
            // return "$payload\n";
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
    $payload =~ s{^($months)\s+(\d{1,2}) (\d\d):(\d\d):(\d\d)}{}gi or return;
    my ( $mon, $day, $hour, $min, $sec ) = ( $m->{ lc $1 }, $2, $3, $4, $5 );
    return $original if ( not defined $mon );

    if ( $payload =~ s{^(\s+\S+)\s+(\d{4}-\d\d-\d\d [\d:,]+)}{$2$1} ) {
        $payload =~ s{(\s+])}{]}g;
        return $payload;
    }
    my $year = ( localtime $message->[TIMESTAMP] )[5];
    return

       # strftime('%F %T %Z', $sec, $min, $hour, $day, $mon, $year) . $payload
        strftime( '%F %T %Z', localtime $message->[TIMESTAMP] ) . $payload;
}

sub months {
    my $self = shift;
    if (@_) {
        $self->{months} = shift;
    }
    return $self->{months};
}

1;
