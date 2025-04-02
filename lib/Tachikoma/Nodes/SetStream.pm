#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::SetStream
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::SetStream;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE STREAM PAYLOAD TM_BYTESTREAM );
use parent             qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

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
        if ( $regex =~ m{[(]} ) {
            $self->{regex} = qr{$regex};
        }
        else {
            $self->{force} = $regex;
        }
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
    elsif ( $message->[TYPE] & TM_BYTESTREAM
        and $message->[PAYLOAD] =~ m{$regex} )
    {
        my $stream = $1;
        chomp $stream;
        $message->[STREAM] = $stream;
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
