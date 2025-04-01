#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::Inet_AtoN
# ----------------------------------------------------------------------
#

package Tachikoma::Jobs::Inet_AtoN;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::Socket;
use Tachikoma::Message qw(
    TYPE FROM TO PAYLOAD TM_BYTESTREAM
);
use Socket;
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.280');

my $DNS_TIMEOUT = 30; # seconds

sub initialize_graph {
    my $self = shift;
    $self->connector->sink($self);
    $self->sink( $self->router );
    Tachikoma->check_pid("Inet_AtoN.$<");
    Tachikoma->write_pid("Inet_AtoN.$<");
    my $node =
        Tachikoma::Nodes::Socket->unix_server( "/tmp/Inet_AtoN.$<", 700 );
    $node->name('_socket');
    $node->owner( $self->name );
    $node->sink( $self->router );
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );

    my $arguments = $message->[PAYLOAD];
    chomp $arguments;
    my $number = undef;
    my $okay   = eval {
        local $SIG{ALRM} = sub { die "alarm\n" };    # NB: \n required
        alarm $DNS_TIMEOUT;
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

sub remove_node {
    my $self = shift;
    Tachikoma->remove_pid("Inet_AtoN.$<");
    return $self->SUPER::remove_node;
}

1;
