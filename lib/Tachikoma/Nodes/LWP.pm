#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::LWP
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::LWP;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE FROM TO PAYLOAD TM_BYTESTREAM );
use HTTP::Request::Common qw(GET);
use LWP::UserAgent;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.368');

my $Default_Timeout = 900;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{user_agent} = undef;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my $timeout = $self->{arguments};
        my $ua      = LWP::UserAgent->new;
        $ua->agent('Tachikoma (DesertNet LWP::UserAgent/2.0)');
        $ua->timeout( $timeout || $Default_Timeout );
        $self->{user_agent} = $ua;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $req = GET( $message->[PAYLOAD] );
    my $res = $self->user_agent->request($req);
    if ( $res->content ) {
        my $response = bless [ @{$message} ], ref $message;
        $response->[TO] = $message->[FROM]
            if ( $message->[TO] eq '_return_to_sender' );
        $response->[PAYLOAD] = $res->content;
        $self->{sink}->fill($response);
    }
    if ( $res->code >= 400 and $res->content ) {
        $self->stderr( join q( ), $res->protocol || 'HTTP/1.1',
            $res->code, $res->message );
    }
    return;
}

sub user_agent {
    my $self = shift;
    if (@_) {
        $self->{user_agent} = shift;
    }
    return $self->{user_agent};
}

1;
