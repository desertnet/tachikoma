#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Atom
# ----------------------------------------------------------------------
#
# $Id: Atom.pm 536 2009-01-04 00:43:07Z chris $
#

package Tachikoma::Nodes::Atom;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM TM_ERROR TM_EOF );
use File::MkTemp;
use parent qw( Tachikoma::Node );

use version; our $VERSION = 'v2.0.367';

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{tmp_dir} = undef;
    $self->{path}    = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Atom <node name> <temp dir> <path>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $tmp_dir, $path ) = split q( ), $self->{arguments}, 2;
        die "ERROR: bad arguments for Atom\n" if ( not $tmp_dir );
        $self->{tmp_dir} = ( $tmp_dir =~ m{^(\S+)$} )[0];
        $self->{path}    = ( $path =~ m{^(\S+)$} )[0];
        $self->make_dirs( $self->{tmp_dir} )
            or die "couldn't make tmp dir: $!";
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $tmp_dir = $self->{tmp_dir};
    my $path    = $self->{path};
    return if ( $message->[TYPE] & TM_ERROR or $message->[TYPE] & TM_EOF );
    return $self->stderr('ERROR: no path set') if ( not $path );
    return $self->stderr('ERROR: unexpected payload')
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    $self->{counter}++;
    my ( $fh, $template ) = mkstempt( 'X' x 16, $tmp_dir )
        or return $self->stderr("ERROR: couldn't mkstempt for $path: $!");
    my $tmp = join q(/), $tmp_dir, $template;
    syswrite $fh, $message->[PAYLOAD]
        or return $self->stderr("ERROR: couldn't write $tmp: $!");
    close $fh
        or return $self->stderr("ERROR: couldn't close $tmp: $!");
    chmod 0644, $tmp
        or return $self->stderr("ERROR: couldn't chmod $tmp: $!");
    rename $tmp, $path
        or return $self->stderr("ERROR: couldn't move $tmp to $path: $!");
    return;
}

sub tmp_dir {
    my $self = shift;
    if (@_) {
        $self->{tmp_dir} = shift;
    }
    return $self->{tmp_dir};
}

sub path {
    my $self = shift;
    if (@_) {
        $self->{path} = shift;
    }
    return $self->{path};
}

1;
