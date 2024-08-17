#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_Trigger
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::HTTP_Trigger;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::HTTP_Responder qw( log_entry cached_strftime );
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.314');

my $TIMEOUT = 900;
my $COUNTER = 0;

# TODO: configurate mime types
my %TYPES = (
    gif  => 'image/gif',
    jpg  => 'image/jpeg',
    png  => 'image/png',
    ico  => 'image/vnd.microsoft.icon',
    txt  => 'text/plain; charset=utf8',
    js   => 'text/javascript; charset=utf8',
    json => 'application/json; charset=utf8',
    css  => 'text/css; charset=utf8',
    html => 'text/html; charset=utf8',
    xml  => 'text/xml; charset=utf8',
);

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{messages} = {};
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_STORABLE ) {
        my $path   = $message->payload->{path};
        my $prefix = $self->{arguments};
        $path =~ s{^$prefix}{};
        if ( not $self->{timer_is_active} ) {
            $self->set_timer;
        }
        if ( $self->{edge} ) {
            my $value = $self->{edge}->lookup($path);
            if ( defined $value ) {
                $self->send_response(
                    $path, $message,
                    {   code    => 200,
                        msg     => 'OK',
                        content => $value
                    }
                );
            }
            else {
                $self->{messages}->{$path} //= [];
                push @{ $self->{messages}->{$path} }, $message;
            }
        }
        else {
            $self->{messages}->{$path} //= [];
            push @{ $self->{messages}->{$path} }, $message;
        }
    }
    else {
        $self->handle_response( $message, $message->[STREAM] );
    }
    return;
}

sub handle_response {
    my $self       = shift;
    my $message    = shift;
    my $message_id = shift;
    my $http       = {
        code    => 400,
        msg     => 'Bad Request',
        content => 'Bad Request',
    };
    if ( $message->[TYPE] & TM_ERROR ) {
        $http->{code}    = 500;
        $http->{msg}     = 'Internal Server Error';
        $http->{content} = 'Internal Server Error';
    }
    elsif ( $message->[TYPE] & TM_BYTESTREAM ) {
        $http->{code}    = 200;
        $http->{msg}     = 'OK';
        $http->{content} = $message->[PAYLOAD];
        $http->{content} = "OK\n"
            if ( not length $http->{content} or $http->{content} !~ m{\S} );
    }
    elsif ( $message->[TYPE] & TM_PERSIST
        and $message->[TYPE] & TM_RESPONSE
        and $message->[PAYLOAD] eq 'cancel' )
    {
        $http->{code}    = 200;
        $http->{msg}     = 'OK';
        $http->{content} = "OK\n";
    }
    if ( exists $self->{messages}->{$message_id} ) {
        for my $queued ( @{ $self->{messages}->{$message_id} } ) {
            $self->send_response( $message_id, $queued, $http );
        }
        delete $self->{messages}->{$message_id};
    }
    if ( $message->[TYPE] & TM_PERSIST
        and not $message->[TYPE] & TM_RESPONSE )
    {
        $self->cancel($message);
    }
    return;
}

sub send_response {
    my ( $self, $message_id, $queued, $http ) = @_;
    my $type     = lc( ( $message_id =~ m{[.]([^.]+)$} )[0] // q() );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $queued->[FROM];
    $response->[STREAM]  = $queued->[STREAM];
    $response->[PAYLOAD] = join q(),
        'HTTP/1.1 ', $http->{code}, q( ), $http->{msg}, "\n",
        'Date: ', cached_strftime(), "\n",
        "Server: Tachikoma\n",
        "Connection: close\n",
        'Content-Type: ',
        $TYPES{$type} || $TYPES{'txt'}, "\n",
        'Content-Length: ', length( $http->{content} ),
        "\n\n",
        $http->{content};
    $self->{sink}->fill($response);
    $response         = Tachikoma::Message->new;
    $response->[TYPE] = TM_EOF;
    $response->[TO]   = $queued->[FROM];
    $self->{sink}->fill($response);
    $self->{counter}++;
    log_entry( $self, $http->{code}, $queued );
    return;
}

sub fire {
    my $self     = shift;
    my $messages = $self->{messages};
    for my $message_id ( keys %{$messages} ) {
        my @keep = ();
        for my $queued ( @{ $messages->{$message_id} } ) {
            push @keep, $queued
                if ( $Tachikoma::Now - $queued->[TIMESTAMP] < $TIMEOUT );
        }
        if (@keep) {
            $messages->{$message_id} = \@keep;
        }
        else {
            delete $messages->{$message_id};
        }
    }
    if ( not keys %{$messages} ) {
        $self->stop_timer;
    }
    return;
}

sub messages {
    my $self = shift;
    if (@_) {
        $self->{messages} = shift;
    }
    return $self->{messages};
}

1;
