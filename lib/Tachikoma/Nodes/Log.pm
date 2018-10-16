#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Log
# ----------------------------------------------------------------------
#
# $Id: Log.pm 35279 2018-10-16 10:39:46Z chris $
#

package Tachikoma::Nodes::Log;
use strict;
use warnings;
use Tachikoma::Nodes::FileHandle qw( TK_SYNC );
use Tachikoma::Nodes::STDIO;
use Tachikoma::Message qw( TYPE FROM STREAM PAYLOAD TM_INFO TM_EOF );
use POSIX qw( strftime );
use parent qw( Tachikoma::Nodes::FileHandle );

use version; our $VERSION = qv('v2.0.280');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new(TK_SYNC);
    $self->{type} = 'regular_file';
    $self->{fill} = \&Tachikoma::Nodes::STDIO::fill_fh_sync;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Log <node name> <filename> [ <mode> [ <max size> ] ]
# valid modes: append (default), overwrite
# to configure nightly rotation:
make_node Scheduler scheduler
command scheduler at 00:00 every 24h tell <node name> rotate
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ( $filename, $mode, $max_size ) = split q( ), $arguments, 3;
        die "ERROR: bad arguments for Log\n" if (not $filename);
        $mode ||= 'append';
        my $fh;
        $self->close_filehandle if ( $self->{fh} );
        open $fh, $mode eq 'append' ? '>>' : '>', $filename
            or die "couldn't open $filename: $!";
        $self->{arguments} = $arguments;
        $self->{filename}  = $filename;
        $self->{mode}      = $mode;
        $self->{max_size}  = $max_size;
        $self->{size}      = tell($fh) || 0;
        $self->fh($fh);
        $self->utimer->set_timer( 3600 * 1000 );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_EOF ) {
        $self->remove_node if ( $self->{mode} ne 'append' );
        return;
    }
    elsif ( $message->[TYPE] & TM_INFO ) {
        my ( $command, $arguments ) = split q( ), $message->[PAYLOAD], 2;
        if ( $command eq 'rotate' ) {
            $self->rotate($arguments);
        }
        else {
            $self->stderr( 'WARNING: received bad TM_INFO: ',
                $message->[PAYLOAD] );
        }
        return;
    }
    elsif ( not $message->[FROM] and $message->[STREAM] eq 'utimer' ) {
        utime $Tachikoma::Now, $Tachikoma::Now, $self->{filename}
            or $self->stderr("ERROR: couldn't utime $self->{filename}: $!");
        return;
    }
    $self->{size} += length( $message->[PAYLOAD] );
    $self->rotate
        if ( $self->{max_size} and $self->{size} > $self->{max_size} );
    return $self->SUPER::fill($message);
}

sub activate {    ## no critic (RequireArgUnpacking, RequireFinalReturn)
    syswrite $_[0]->{fh}, ${ $_[1] } or die $!;
}

sub rotate {
    my $self     = shift;
    my $format   = shift;
    my $new_name = undef;
    if ($format) {
        $new_name = strftime( $format, localtime $Tachikoma::Now );
    }
    else {
        $new_name = join q(-),
            $self->{filename},
            strftime( '%F-%T', localtime $Tachikoma::Now ),
            Tachikoma->counter;
    }
    $self->close_filehandle;
    $self->make_parent_dirs($new_name);
    rename $self->{filename}, $new_name
        or $self->stderr("WARNING: couldn't rename $self->{filename}: $!");
    $self->arguments( $self->arguments );
    return;
}

sub utimer {
    my $self = shift;
    if (@_) {
        $self->{utimer} = shift;
    }
    if ( not defined $self->{utimer} ) {
        $self->{utimer} = Tachikoma::Nodes::Timer->new;
        $self->{utimer}->stream('utimer');
        $self->{utimer}->sink($self);
    }
    return $self->{utimer};
}

sub remove_node {
    my $self = shift;
    $self->{utimer}->remove_node if ( $self->{utimer} );
    $self->SUPER::remove_node;
    return;
}

sub filename {
    my $self = shift;
    if (@_) {
        $self->{filename} = shift;
    }
    return $self->{filename};
}

sub mode {
    my $self = shift;
    if (@_) {
        $self->{mode} = shift;
    }
    return $self->{mode};
}

sub max_size {
    my $self = shift;
    if (@_) {
        $self->{max_size} = shift;
    }
    return $self->{max_size};
}

sub size {
    my $self = shift;
    if (@_) {
        $self->{size} = shift;
    }
    return $self->{size};
}

1;
