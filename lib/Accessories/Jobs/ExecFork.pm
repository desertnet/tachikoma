#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Jobs::ExecFork
# ----------------------------------------------------------------------
#
# $Id: ExecFork.pm 33259 2018-02-17 15:06:46Z chris $
#

package Accessories::Jobs::ExecFork;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw( TM_BYTESTREAM );
use IPC::Open3;
use parent qw( Tachikoma::Job );

sub initialize_graph {
    my $self = shift;
    $self->connector->sink($self);
    $self->sink( $self->router );
    untie(*STDOUT);
    untie(*STDERR);
    if ( $self->owner ) {
        $SIG{PIPE} = sub { die $! };
        my ( $read, $write );
        open3( $write, $read, $read, $self->arguments ) or die "$!";
        local $/;
        my $output = <$read>;
        close($read);
        my $message = Tachikoma::Message->new;
        $message->type(TM_BYTESTREAM);
        $message->stream( $self->arguments );
        $message->payload($output);
        $self->SUPER::fill($message);
        exit 0;
    }
    else {
        die "ERROR: invalid request";
    }
    return;
}

1;
