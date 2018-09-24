#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::SetStream
# ----------------------------------------------------------------------
#
# $Id: SetStream.pm 9709 2011-01-12 00:01:13Z chris $
#

package Tachikoma::Nodes::SetStream;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( STREAM PAYLOAD );
use Digest::MD5 qw( md5_hex );
use parent qw( Tachikoma::Node );

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{force} = undef;
    $self->{regex} = qr{(.*)};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node SetStream <node name> [ <regex> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my $regex = $self->{arguments} || '(.*)';
        $self->{regex} = qr{$regex};
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $regex   = $self->{regex};
    if ( $self->{force} ) {
        $message->[STREAM] = $self->{force};
    }
    elsif ( $message->[PAYLOAD] =~ m{$regex} ) {
        $message->[STREAM] = md5_hex($1);
    }
    return $self->SUPER::fill($message);
}

sub force {
    my $self = shift;
    if (@_) {
        $self->{force} = shift;
    }
    return $self->{force};
}

sub regex {
    my $self = shift;
    if (@_) {
        $self->{regex} = shift;
    }
    return $self->{regex};
}

1;
