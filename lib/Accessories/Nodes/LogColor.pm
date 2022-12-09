#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::LogColor
# ----------------------------------------------------------------------
#

package Accessories::Nodes::LogColor;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE TIMESTAMP PAYLOAD TM_BYTESTREAM );
use POSIX qw( strftime );
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
    $payload = $self->cleanup_syslog( $message, $payload )
        if ( $payload =~ m{^\D} );
    $payload =~ s/#033\[/\e[/g;
    my $date_re = qr{\d{4}-\d\d-\d\d \d\d:\d\d:\d\d \S+};
    if ( $payload =~ m{^($date_re) (\S+) (.*?:) (.*)$} ) {
        my ( $date, $host, $process, $msg ) = ( $1, $2, $3, $4 );
        my $color = $self->get_color($msg);
        $payload = sprintf "\e[95m%s \e[96m%s \e[94m%s $color%s\e[0m",
            $date, $host, $process, $msg;
    }
    elsif ( $payload =~ m{^($date_re) (\S+) (.*)$} ) {
        my ( $date, $host, $msg ) = ( $1, $2, $3 );
        my $color = $self->get_color($msg);
        $payload = sprintf "\e[95m%s \e[96m%s $color%s\e[0m",
            $date, $host, $msg;
    }
    else {
        my $color = $self->get_color($payload);
        $payload = sprintf "$color%s\e[0m", $payload;
    }
    $message->[PAYLOAD] = "\r$payload\n";
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

sub get_color {
    my $self    = shift;
    my $payload = shift;
    my $color   = undef;
    if ( $payload =~ m{ERROR:} ) {

        # $color = "\e[30;101m";
        $color = "\e[91m";
    }
    elsif ( $payload =~ m{WARNING:} ) {

        # $color = "\e[30;103m";
        $color = "\e[93m";
    }
    elsif ( $payload =~ m{INFO:|DEBUG:|systemd[[]} ) {
        $color = "\e[90m";
    }
    elsif ( $payload =~ m{auth|ssh|sftp}i ) {
        $color = "\e[92m";
    }
    elsif ( $payload =~ m{fail|error}i ) {
        $color = "\e[91m";
    }
    elsif ( $payload =~ m{warning}i ) {
        $color = "\e[93m";
    }
    else {
        $color = "\e[0m";
    }
    return $color;
}

sub months {
    my $self = shift;
    if (@_) {
        $self->{months} = shift;
    }
    return $self->{months};
}

1;
