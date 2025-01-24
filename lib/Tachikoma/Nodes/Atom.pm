#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Atom
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Atom;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM TM_ERROR TM_EOF );
use File::Temp         qw( tempfile );
use parent             qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

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
        $self->{path}    = ( $path    =~ m{^(\S+)$} )[0];
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
    my ( $fh, $template );
    my $okay = eval {
        ( $fh, $template ) =
            tempfile( '.temp-' . ( 'X' x 16 ), DIR => $tmp_dir );
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ || 'unknown error';
        return $self->stderr("ERROR: couldn't tempfile $path: $error");
    }
    my $rv = syswrite $fh, $message->[PAYLOAD];
    if ( not defined $rv or $rv != length $message->[PAYLOAD] ) {
        return $self->stderr("ERROR: couldn't write $template: $!");
    }
    close $fh
        or return $self->stderr("ERROR: couldn't close $template: $!");
    chmod 0644, $template
        or return $self->stderr("ERROR: couldn't chmod $template: $!");
    rename $template, $path
        or
        return $self->stderr("ERROR: couldn't move $template to $path: $!");
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
