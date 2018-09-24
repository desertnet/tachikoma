#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Jobs::Reactor
# ----------------------------------------------------------------------
#
# $Id: Reactor.pm 5055 2010-04-03 04:57:11Z chris $
#

package Accessories::Jobs::Reactor;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw( TM_BYTESTREAM );
use Time::HiRes;
use parent qw( Tachikoma::Job );

our %Reactor = ();

sub initialize_graph {
    my $self = shift;
    $self->connector->sink($self);
    $self->sink( $self->router );
    my ( $config, $timeout ) = split( ' ', $self->arguments || '', 4 );
    $config = '/usr/local/etc/tachikoma/Reactor.conf' if ( not $config );
    if ( -f $config ) {
        require $config
            or die "couldn't read config file '$config'";
    }
    elsif ( -f "/usr/local/etc/tachikoma/$config" ) {
        my $config_path = "/usr/local/etc/tachikoma/$config";
        require $config_path
            or die "couldn't read config file '$config_path'";
    }
    else {
        die "couldn't find config file '$config' or "
            . "'/usr/local/etc/tachikoma/$config'";
    }
    $self->timeout( defined($timeout) ? $timeout : 1 );
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->type & TM_BYTESTREAM );
    my $payload = $message->payload;
    chomp $payload;
    for my $mapping ( @{ $Reactor{'mappings'} } ) {
        if ( &{ $mapping->{test} }($payload) ) {
            if ( $mapping->{reaction} ) {
                $mapping->{last_time} ||= 0;
                &{ $mapping->{reaction} }($payload)
                    if ( $self->{timeout}
                    and Time::HiRes::time - $mapping->{last_time}
                    > $self->{timeout} );
                $mapping->{last_time} = Time::HiRes::time;
            }
            last;
        }
    }
    return;
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

1;
