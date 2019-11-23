#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Rewrite
# ----------------------------------------------------------------------
#
# $Id: Rewrite.pm 11165 2011-08-03 03:11:07Z chris $
#

package Tachikoma::Nodes::Rewrite;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.368');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{pattern} = qr{};
    $self->{rewrite} = q();
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Rewrite <node name> [ <pattern> <rewrite> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $pattern, $rewrite ) = split q( ), $self->{arguments}, 2;
        $pattern ||= q();
        $rewrite ||= q();
        $self->{pattern} = qr{$pattern};
        $self->{rewrite} = $rewrite;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $pattern = $self->{pattern};
    my @output  = ();
    my $dirty   = undef;
    for my $line ( split m{^}, $message->[PAYLOAD] ) {
        chomp $line;
        my @matches = $line =~ m{$pattern};
        if (@matches) {
            my $rewrite = $self->{rewrite};
            $rewrite =~ s{\$$_(?!\d)}{$matches[$_ - 1]}g
                for ( 1 .. @matches );
            $dirty = $line =~ s{$pattern}{$rewrite};
        }
        push @output, $line, "\n";
    }
    if ($dirty) {
        my $copy = bless [ @{$message} ], ref $message;
        $copy->[PAYLOAD] = join q(), @output;
        $self->SUPER::fill($copy);
    }
    else {
        $self->cancel($message);
    }
    return;
}

sub pattern {
    my $self = shift;
    if (@_) {
        $self->{pattern} = shift;
    }
    return $self->{pattern};
}

sub rewrite {
    my $self = shift;
    if (@_) {
        $self->{rewrite} = shift;
    }
    return $self->{rewrite};
}

1;
