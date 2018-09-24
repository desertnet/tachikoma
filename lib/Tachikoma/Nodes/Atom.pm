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
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use File::MkTemp;
use parent qw( Tachikoma::Node );

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
        my ( $tmp_dir, $path ) = split( ' ', $self->{arguments}, 2 );
        $self->{tmp_dir} = ( $tmp_dir =~ m(^(\S+)$) )[0];
        $self->{path}    = ( $path =~ m(^(\S+)$) )[0];
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
    return $self->stderr("ERROR: no path set") if ( not $path );
    return $self->stderr("ERROR: unexpected payload")
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    $self->{counter}++;
    my ( $fh, $template ) = mkstempt( 'X' x 16, $tmp_dir )
        or
        return $self->stderr("ERROR: couldn't open temp file for $path: $!");
    my $tmp = join( '/', $tmp_dir, $template );
    my $rv = syswrite( $fh, $message->[PAYLOAD] )
        or $self->stderr("ERROR: can't write temp file for $path: $!");
    close($fh);
    chmod( 0644, $tmp );
    rename( $tmp, $path )
        or $self->stderr("ERROR: couldn't move $tmp to $path: $!")
        if ($rv);
    return $rv;
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
