#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::LWP
# ----------------------------------------------------------------------
#
# $Id: LWP.pm 3033 2009-09-15 08:02:14Z chris $
#

package Tachikoma::Jobs::LWP;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::LWP;
use Tachikoma::Message qw( TO TM_BYTESTREAM );
use parent qw( Tachikoma::Job );

use version; our $VERSION = 'v2.0.349';

sub initialize_graph {
    my $self = shift;
    my $lwp  = Tachikoma::Nodes::LWP->new;
    $self->connector->sink($lwp);
    $lwp->name('LWP');
    if ( $self->owner ) {
        $lwp->arguments(90);
        $lwp->sink($self);
        $self->sink( $self->router );
        my $message = Tachikoma::Message->new;
        $message->type(TM_BYTESTREAM);
        $message->from('_parent');
        $message->payload( $self->arguments );
        $lwp->fill($message);
        $self->remove_node;
    }
    else {
        $lwp->arguments( $self->arguments );
        $lwp->sink($self);
        $self->sink( $self->router );
    }
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    $message->[TO] = join q(/), '_parent', $message->[TO]
        if ( $message->[TO] !~ m{^_parent} );
    return $self->SUPER::fill($message);
}

1;
