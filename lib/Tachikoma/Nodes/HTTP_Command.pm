#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_Command
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::HTTP_Command;
use strict;
use warnings;
use Tachikoma::Nodes::HTTP_Trigger;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_PERSIST
);
use CGI;
use parent qw( Tachikoma::Nodes::HTTP_Trigger );

use version; our $VERSION = qv('v2.0.314');

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_STORABLE ) {
        $message->[ID] = $self->msg_counter;
        $self->{messages}->{ $message->[ID] } = $message;
        if ( not $self->{timer_is_active} ) {
            $self->set_timer;
        }
        my $cgi     = CGI->new( $message->payload->{query_string} );
        my $request = Tachikoma::Message->new;
        $request->[TYPE]    = TM_BYTESTREAM | TM_PERSIST;
        $request->[FROM]    = $self->{name};
        $request->[TO]      = $self->{owner};
        $request->[ID]      = $message->[ID];
        $request->[STREAM]  = $cgi->param('stream');
        $request->[PAYLOAD] = $cgi->param('payload');
        $self->{sink}->fill($request);
    }
    else {
        $self->handle_response( $message, $message->[ID] );
    }
    return;
}

1;
