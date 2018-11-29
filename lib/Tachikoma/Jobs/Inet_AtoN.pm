#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::Inet_AtoN
# ----------------------------------------------------------------------
#
# $Id: Inet_AtoN.pm 35958 2018-11-29 01:37:07Z chris $
#

package Tachikoma::Jobs::Inet_AtoN;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO PAYLOAD
    TM_BYTESTREAM TM_EOF TM_KILLME
);
use Socket;
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.280');

my $Job_Timeout = 5;     # seconds
my $DNS_Timeout = 30;    # seconds

sub initialize_graph {
    my $self = shift;
    $self->connector->sink($self);
    $self->sink( $self->router );
    if ( $self->owner ) {
        my $message = Tachikoma::Message->new;
        $message->type(TM_BYTESTREAM);
        $message->payload( $self->arguments );
        $self->fill($message);
        $self->remove_node;
    }
    else {
        $self->timer( Tachikoma::Nodes::Timer->new );
        $self->timer->name('Timer');
        $self->timer->set_timer( $Job_Timeout * 1000, 'oneshot' );
        $self->timer->sink($self);
    }
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( $message->[TYPE] & TM_EOF );

    # timeout, send a TM_KILLME request
    if ( $message->[FROM] eq 'Timer' ) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE] = TM_KILLME;
        $self->timer->stop_timer;
        return $self->SUPER::fill($response);
    }

    # looks like we're ready to die
    return $self->shutdown_all_nodes
        if ( $message->[TYPE] & TM_KILLME );

    # otherwise make sure it's a TM_BYTESTREAM
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    $self->timer->set_timer( $Job_Timeout * 1000, 'oneshot' )
        if ( $self->timer );

    my $arguments = $message->[PAYLOAD];
    chomp $arguments;
    my $number = undef;
    my $okay   = eval {
        local $SIG{ALRM} = sub { die "alarm\n" };    # NB: \n required
        alarm $DNS_Timeout;
        $number = inet_aton($arguments);
        alarm 0;
        return 1;
    };
    if ( not $okay ) {
        if ( $@ and $@ eq "alarm\n" ) {
            return $self->stderr( 'WARNING: timeout looking up: ',
                $arguments );
        }
        else {
            die $@ if ( $@ ne "alarm\n" );    # propagate unexpected errors
        }
    }
    $message->[TO]      = $message->[FROM];
    $message->[FROM]    = q();
    $message->[PAYLOAD] = $number ? join q(), inet_ntoa($number), "\n" : q();
    return $self->SUPER::fill($message);
}

sub timer {
    my $self = shift;
    if (@_) {
        $self->{timer} = shift;
    }
    return $self->{timer};
}

1;
