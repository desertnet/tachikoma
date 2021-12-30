#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::TailProbe
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::TailProbe;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE FROM TO PAYLOAD TM_BYTESTREAM );
use Time::HiRes;
use Sys::Hostname qw( hostname );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.368');

my $Default_Interval = 5;    # seconds

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{my_hostname} = hostname();
    $self->{prefix}      = q();
    $self->{last_time}   = $Tachikoma::Right_Now;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node TailProbe <node name> <seconds> [ <prefix> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $seconds, $prefix ) = split q( ), $self->{arguments}, 2;
        die "ERROR: bad arguments for TailProbe\n"
            if ( $seconds and $seconds =~ m{\D} );
        $seconds ||= $Default_Interval;
        $self->set_timer( $seconds * 1000 );
        $self->prefix( $prefix || $0 );
    }
    return $self->{arguments};
}

sub fire {
    my $self     = shift;
    my $out      = q();
    my $interval = $self->{timer_interval} / 1000;
    my $elapsed  = Time::HiRes::time - $self->{last_time};
    $self->stderr(
        sprintf 'WARNING: degraded performance detected'
            . ' - timer fired %.2f seconds late',
        $elapsed - $interval
    ) if ( $elapsed > $interval * 2 );
    $self->{last_time} = Time::HiRes::time;
    my $node = $Tachikoma::Nodes{$0};

    if ( $node and $node->can('tiedhash') and $node->can('files') ) {
        my $tiedhash = $node->tiedhash;
        my $files    = $node->files;
        for my $name ( sort keys %{$tiedhash} ) {
            my $filename = $files->{$name}->[0];
            my $tail_name = join q(/), $self->{prefix}, $name;
            $tail_name =~ s{:}{_}g;
            $filename =~ s{:}{_}g;
            $out .= join q(),
                'hostname:'        => $self->{my_hostname},
                ' tail_name:'      => $tail_name,
                ' filename:'       => $filename,
                ' bytes_answered:' => $tiedhash->{$name},
                ' file_size:'      => ( stat $filename )[7],
                "\n";
        }
    }
    if ($out) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_BYTESTREAM;
        $message->[FROM]    = $self->{name};
        $message->[TO]      = $self->{owner};
        $message->[PAYLOAD] = $out;
        $self->{sink}->fill($message);
        $self->{counter}++;
    }
    return;
}

sub my_hostname {
    my $self = shift;
    if (@_) {
        $self->{my_hostname} = shift;
    }
    return $self->{my_hostname};
}

sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
}

1;
