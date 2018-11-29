#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::FileReceiver
# ----------------------------------------------------------------------
#
# $Id: FileReceiver.pm 34052 2018-06-01 16:45:27Z chris $
#

package Tachikoma::Nodes::FileReceiver;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM
    STREAM PAYLOAD TM_BYTESTREAM TM_EOF
);
use File::Temp qw( tempfile );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.349');

my $Filehandle_Timeout = 300;
my $Expire_Interval    = 60;

# my $Separator          = chr(0);
my $Separator = join q(), chr 30, ' -> ', chr 30;

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->set_timer( $Expire_Interval * 1000 );
        $self->filehandles( {} );
    }
    return $self->{arguments};
}

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    return if ( not $type & TM_BYTESTREAM and not $type & TM_EOF );
    my $prefix = $self->{arguments};
    return $self->stderr('ERROR: no prefix specified') if ( not $prefix );
    my $stream = $message->[STREAM];
    return $self->stderr('ERROR: no stream specified') if ( not $stream );
    my $op       = undef;
    my $relative = undef;

    if ( $stream =~ m{^\w+:} ) {
        ( $op, $relative ) = split m{:}, $stream, 2;
    }
    else {
        $op       = 'update';
        $relative = $stream;
    }
    my $path = undef;
    if ( $op ne 'rename' ) {
        $relative =~ s{(?:^|/)[.][.](?=/)}{}g;
        $path = join q(/), $prefix, $relative;
    }
    if ( $op eq 'update' ) {
        my $source = $message->[FROM];
        $source =~ s{_parent/|[^/]+:tee/}{}g;
        my $key         = join q( ), $source, $stream;
        my $filehandles = $self->{filehandles};
        my $fhp         = $filehandles->{$key};
        my $fh          = $fhp ? $fhp->[0] : undef;
        if ( $type & TM_EOF ) {
            if ($fh) {
                my $tmp = $fhp->[1];
                delete $filehandles->{$key};
                close $fh
                    or $self->stderr("ERROR: couldn't close $tmp: $!");
                rename $tmp, $path
                    or
                    $self->stderr("ERROR: couldn't move $tmp to $path: $!");
                $self->set_metadata( $path, $message->[PAYLOAD] )
                    if ( $message->[PAYLOAD] );
            }
            return $self->cancel($message);
        }
        if ( $fh and not length( $message->[PAYLOAD] ) ) {
            my $tmp = $fhp->[1];
            delete $filehandles->{$key};
            close $fh
                or $self->stderr("ERROR: couldn't close $tmp: $!");
            unlink $tmp
                or $self->stderr("ERROR: couldn't unlink $tmp: $!");
            $fh = undef;
        }
        if ( not $fh ) {
            return $self->cancel($message)
                if ( length( $message->[PAYLOAD] ) );
            my $parent = ( $path =~ m{(.*)/[^/]+} )[0];
            umask 0022
                or $self->stderr("ERROR: couldn't umask 0022: $!");
            my $template;
            $self->make_dirs($parent);
            my $okay = eval {
                ( $fh, $template ) =
                    tempfile( '.temp-' . ( 'X' x 16 ), DIR => $parent );
                return 1;
            };
            if ( not $okay ) {
                my $error   = $@ || 'unknown error';
                my $details = $!;
                chomp $error;
                $error =~ s{ at /\S+ line \d+[.]$}{};
                $error .= ": $details" if ($details);
                $self->stderr("ERROR: tempfile failed: $error");
            }
            else {
                $fhp = [ $fh, $template, $Tachikoma::Now ];
                $filehandles->{$key} = $fhp;
            }
            return $self->cancel($message);
        }
        $fhp->[2] = $Tachikoma::Now;
        syswrite $fh, $message->[PAYLOAD]
            or $self->stderr("ERROR: couldn't write $path: $!");
    }
    elsif ( $op eq 'symlink' ) {
        unlink $path
            or $self->stderr("ERROR: couldn't unlink $path: $!");
        $self->make_parent_dirs($path);
        umask 0022
            or $self->stderr("ERROR: couldn't umask 0022: $!");
        symlink $message->[PAYLOAD], $path
            or $self->stderr("ERROR: couldn't symlink $path: $!");
    }
    elsif ( $op eq 'mkdir' ) {
        $self->make_dirs($path);
        $self->set_metadata( $path, $message->[PAYLOAD] );
    }
    elsif ( $op eq 'touch' ) {
        $self->make_parent_dirs($path);
        umask 0022
            or $self->stderr("ERROR: couldn't umask 0022: $!");
        open my $fh, '>>', $path
            or $self->stderr("ERROR: couldn't open $path: $!");
        close $fh
            or $self->stderr("ERROR: couldn't close $path: $!");
        $self->set_metadata( $path, $message->[PAYLOAD] );
    }
    elsif ( $op eq 'rename' ) {
        my $new_relative = undef;
        ( $relative, $new_relative ) = split $Separator, $relative, 2;
        $relative     =~ s{(?:^|/)[.][.](?=/)}{}g if ($relative);
        $new_relative =~ s{(?:^|/)[.][.](?=/)}{}g if ($new_relative);
        $path = join q(/), $prefix, $relative;
        my $new_path = join q(/), $prefix, $new_relative;
        $self->make_parent_dirs($path);
        rename $path, $new_path
            or $self->stderr("ERROR: couldn't rename $path to $new_path: $!");
    }
    elsif ( $op eq 'delete' or $op eq 'rmdir' ) {
        if ( -d $path ) {
            rmdir $path or $self->stderr("ERROR: couldn't rmdir $path: $!");
        }
        else {
            unlink $path or $self->stderr("ERROR: couldn't unlink $path: $!");
        }
    }
    elsif ( $op eq 'chmod' ) {
        $self->set_metadata( $path, $message->[PAYLOAD] );
    }
    else {
        $self->stderr( "ERROR: unknown op: $op from ", $message->from );
    }
    return $self->cancel($message);
}

sub fire {
    my $self        = shift;
    my $filehandles = $self->{filehandles};
    for my $key ( keys %{$filehandles} ) {
        if ( $Tachikoma::Now - $filehandles->{$key}->[2]
            > $Filehandle_Timeout )
        {
            my $path = $filehandles->{$key}->[1];
            close $filehandles->{$key}->[0]
                or $self->stderr("ERROR: couldn't close $path: $!");
            unlink $path or $self->stderr("ERROR: couldn't unlink $path: $!");
            delete $filehandles->{$key};
            $self->stderr("WARNING: expired $key from filehandle cache");
        }
    }
    return;
}

sub make_parent_dirs {
    my $self = shift;
    my $path = shift;
    my $okay = eval {
        $self->SUPER::make_parent_dirs($path);
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ || 'make_parent_dirs failed';
        $self->stderr("ERROR: $error");
    }
    return;
}

sub make_dirs {
    my $self = shift;
    my $path = shift;
    my $okay = eval {
        $self->SUPER::make_dirs($path);
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ || 'make_dirs failed';
        $self->stderr("ERROR: $error");
    }
    return;
}

sub set_metadata {
    my $self    = shift;
    my $path    = shift;
    my $payload = shift;
    my @lstat   = split m{:}, $payload;
    my $mode    = $lstat[2] & 07777;
    chmod $mode, $path or $self->stderr("ERROR: couldn't chmod $path: $!");
    utime $lstat[8], $lstat[9], $path
        or $self->stderr("ERROR: couldn't utime $path: $!");
    return;
}

sub filehandles {
    my $self = shift;
    if (@_) {
        $self->{filehandles} = shift;
    }
    return $self->{filehandles};
}

1;
