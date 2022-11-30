#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::TestStream
# ----------------------------------------------------------------------
#

package Accessories::Nodes::TestStream;
use strict;
use warnings;
use Tachikoma::Nodes::Table;
use parent qw( Tachikoma::Nodes::Table );

use version; our $VERSION = qv('v2.0.686');

sub collect {
    my ( $self, $i, $timestamp, $key, $value ) = @_;
    chomp $value;
    return 1 if ( not $value );
    my $bucket = $self->get_bucket( $i, $timestamp );
    if ( not $bucket or not defined $bucket->{$key} ) {
        $self->SUPER::collect( $i, $timestamp, $key, $value );
    }
    elsif ( $value <= $bucket->{$key} + 1 ) {
        $bucket->{$key} = $value;
    }
    else {
        $self->stderr( 'WARNING: missing message!',
            " got: [$value] expected [" . ( $bucket->{$key} + 1 ) . ']' );
        $bucket->{$key} = $value;
    }
    return;
}

sub remove_entry {
    my ( $self, $i, $key ) = @_;
    my $value = 0;
    for my $bucket ( reverse @{ $self->{caches}->[$i] } ) {
        next if ( not exists $bucket->{$key} );
        $value += $bucket->{$key};
        delete $bucket->{$key};
    }
    return $value;
}

sub get_partition_id {
    my ( $self, $key ) = @_;
    return $key;
}

1;
