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
use File::MkTemp;
use parent qw( Tachikoma::Nodes::Timer );

my $Filehandle_Timeout = 300;
my $Expire_Interval    = 60;
# my $Separator          = chr(0);
my $Separator          = join( '', chr(30), ' -> ', chr(30) );

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->set_timer( $Expire_Interval * 1000 );
        $self->filehandles( {} );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    return if ( not $type & TM_BYTESTREAM and not $type & TM_EOF );
    my $prefix = $self->{arguments};
    return $self->stderr("ERROR: no prefix specified") if ( not $prefix );
    my $stream = $message->[STREAM];
    return $self->stderr("ERROR: no stream specified") if ( not $stream );
    my $op     = undef;
    my $relative = undef;

    if ( $stream =~ m(^\w+:) ) {
        ( $op, $relative ) = split( ':', $stream, 2 );
    }
    else {
        $op       = 'update';
        $relative = $stream;
    }
    my $path = undef;
    if ( $op ne 'rename' ) {
        $relative =~ s((?:^|/)\.\.(?=/))()g;
        $path = join( '/', $prefix, $relative );
    }
    if ( $op eq 'update' ) {
        my $source = $message->[FROM];
        $source =~ s(_parent/|[^/]+:tee/)()g;
        my $key         = join( ' ', $source, $stream );
        my $filehandles = $self->{filehandles};
        my $fhp         = $filehandles->{$key};
        my $fh          = $fhp ? $fhp->[0] : undef;
        if ( $type & TM_EOF ) {
            if ($fh) {
                my $tmp = $fhp->[1];
                delete $filehandles->{$key};
                close($fh);
                rename( $tmp, $path )
                    or $self->stderr("ERROR: can't move $tmp to $path: $!");
                $self->set_metadata( $path, $message->[PAYLOAD] )
                    if ( $message->[PAYLOAD] );
            }
            return $self->cancel($message);
        }
        if ( $fh and not length( $message->[PAYLOAD] ) ) {
            my $tmp = $fhp->[1];
            delete $filehandles->{$key};
            close($fh);
            unlink($tmp);
            $fh = undef;
        }
        if ( not $fh ) {
            return $self->cancel($message)
                if ( length( $message->[PAYLOAD] ) );
            my $parent = ( $path =~ m((.*)/[^/]+) )[0];
            umask(0022);
            my $template;
            eval { $self->make_dirs($parent) };
            if ($@) {
                $self->print_less_often($@);
                return $self->cancel($message);
            }
            eval {
                ( $fh, $template ) =
                    mkstempt( '.temp-' . ( 'X' x 16 ), $parent );
            };
            if ($@) {
                my $error   = "ERROR: $@";
                my $details = $!;
                chomp($error);
                $error =~ s( at /\S+ line \d+\.$)();
                $error .= ": $details" if ($details);
                $self->stderr($error);
            }
            else {
                my $tmp = join( '/', $parent, $template );
                $fhp = [ $fh, $tmp, $Tachikoma::Now ];
                $filehandles->{$key} = $fhp;
            }
            return $self->cancel($message);
        }
        $fhp->[2] = $Tachikoma::Now;
        syswrite( $fh, $message->[PAYLOAD] )
            or $self->stderr("ERROR: can't write $path: $!");
    }
    elsif ( $op eq 'symlink' ) {
        unlink($path);
        $self->stderr("ERROR: can't unlink $path: $!") if ( -e $path );
        eval { $self->make_parent_dirs($path) };
        $self->stderr($@) if ($@);
        umask(0022);
        symlink( $message->[PAYLOAD], $path )
            or $self->stderr("ERROR: can't symlink $path: $!");
    }
    elsif ( $op eq 'mkdir' ) {
        eval { $self->make_dirs($path) };
        $self->stderr($@) if ($@);
        $self->set_metadata( $path, $message->[PAYLOAD] );
    }
    elsif ( $op eq 'touch' ) {
        eval { $self->make_parent_dirs($path) };
        $self->stderr($@) if ($@);
        umask(0022);
        open( my $fh, '>>', $path )
            or $self->stderr("ERROR: can't open $path: $!");
        close($fh);
        $self->set_metadata( $path, $message->[PAYLOAD] );
    }
    elsif ( $op eq 'rename' ) {
        my $new_relative = undef;
        ( $relative, $new_relative ) = split( $Separator, $relative, 2 );
        $relative =~ s((?:^|/)\.\.(?=/))()g     if ($relative);
        $new_relative =~ s((?:^|/)\.\.(?=/))()g if ($new_relative);
        $path = join( '/', $prefix, $relative );
        my $new_path = join( '/', $prefix, $new_relative );
        eval { $self->make_parent_dirs($path) };
        $self->stderr($@) if ($@);
        rename( $path, $new_path )
            or $self->stderr("ERROR: can't rename $path to $new_path: $!");
    }
    elsif ( $op eq 'delete' or $op eq 'rmdir' ) {
        if ( -d $path ) {
            rmdir($path) or $self->stderr("ERROR: can't rmdir $path: $!");
        }
        else {
            unlink($path) or $self->stderr("ERROR: can't unlink $path: $!");
        }
    }
    elsif ( $op eq 'chmod' ) {
        $self->set_metadata( $path, $message->[PAYLOAD] );
    }
    else {
        $self->stderr( "ERROR: uknown op: $op from ", $message->from );
    }
    return $self->cancel($message);
}

sub fire {
    my $self        = shift;
    my $filehandles = $self->{filehandles};
    for my $key ( keys %$filehandles ) {
        if ( $Tachikoma::Now - $filehandles->{$key}->[2]
            > $Filehandle_Timeout )
        {
            close( $filehandles->{$key}->[0] );
            unlink( $filehandles->{$key}->[1] );
            delete $filehandles->{$key};
            $self->stderr("WARNING: expired $key from filehandle cache");
        }
    }
    return;
}

sub set_metadata {
    my $self    = shift;
    my $path    = shift;
    my $payload = shift;
    my @lstat   = split( ':', $payload );
    my $mode    = $lstat[2] & 07777;
    chmod( $mode, $path ) or $self->stderr("ERROR: can't chmod $path: $!");
    utime( $lstat[8], $lstat[9], $path )
        or $self->stderr("ERROR: can't utime $path: $!");
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
