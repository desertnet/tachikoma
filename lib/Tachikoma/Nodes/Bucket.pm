#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Bucket
# ----------------------------------------------------------------------
#
# $Id: Bucket.pm 536 2009-01-04 00:43:07Z chris $
#

package Tachikoma::Nodes::Bucket;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use POSIX qw( strftime );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = 'v2.0.367';

my $Counter          = 0;
my $Default_Interval = 900;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{base}     = undef;
    $self->{interval} = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Bucket <node name> <directory> [ <interval> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $base, $interval ) = split q{ }, $self->{arguments}, 2;
        $self->{base} = $base;
        $self->{interval} = $interval || $Default_Interval;
        $self->make_dirs($base);
        $self->set_timer;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $base    = $self->{base};
    my ( $time, $payload ) = split q{ }, $message->[PAYLOAD], 2;
    return $self->stderr('ERROR: unexpected payload')
        if ( not $message->[TYPE] & TM_BYTESTREAM or not $time );
    $self->{counter}++;
    my $interval = $self->{interval};
    my $dir      = join q{/},
        $base, strftime( '%F-%T', localtime $time - $time % $interval );
    my $path = join q{/}, $dir, $self->msg_counter;
    $self->make_dirs($dir);
    open my $fh, q{>}, $path or die "ERROR: couldn't open $path: $!";
    syswrite $fh, $payload or die "ERROR: couldn't write $path: $!";
    close $fh or die "ERROR: couldn't close $path: $!";
    $self->cancel($message);
    return 1;
}

sub fire {
    my $self = shift;
    return if ( not $self->{owner} );
    my $now = strftime( '%F-%T', localtime $Tachikoma::Now );
    my $base = $self->{base};
    local $/ = undef;
    opendir my $dh, $base or die "ERROR: couldn't opendir $base: $!";
    for my $date ( sort grep m{^[^.]}, readdir $dh ) {
        last if ( $date gt $now );
        $self->process_dir( join q{/}, $base, $date );
    }
    closedir $dh or die "ERROR: clouldn't closedir $base: $!";
    return;
}

sub process_dir {
    my $self = shift;
    my $dir  = shift;
    opendir my $dh, $dir or die "ERROR: couldn't opendir $dir: $!";
    for my $file ( grep m{^[^.]}, readdir $dh ) {
        my $path = join q{/}, $dir, $file;
        my $message = Tachikoma::Message->new;
        $message->[TYPE] = TM_BYTESTREAM;
        open my $fh, q{<}, $path or die "ERROR: couldn't open $path: $!";
        $message->[PAYLOAD] = <$fh>;
        close $fh or die "ERROR: couldn't close $path: $!";
        unlink $path or die "ERROR: couldn't unlink $path: $!";
        $self->SUPER::fill($message);
    }
    closedir $dh or die "ERROR: clouldn't closedir $dir: $!";
    rmdir $dir   or die "ERROR: couldn't rmdir $dir: $!";
    return;
}

sub msg_counter {
    my $self = shift;
    $Counter = ( $Counter + 1 ) % $Tachikoma::Max_Int;
    return sprintf '%d:%010d', $Tachikoma::Now, $Counter;
}

sub base {
    my $self = shift;
    if (@_) {
        $self->{base} = shift;
    }
    return $self->{base};
}

sub interval {
    my $self = shift;
    if (@_) {
        $self->{interval} = shift;
    }
    return $self->{interval};
}

1;
