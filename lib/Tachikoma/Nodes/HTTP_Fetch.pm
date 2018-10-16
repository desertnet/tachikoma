#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_Fetch
# ----------------------------------------------------------------------
#
# $Id: HTTP_Fetch.pm 1733 2009-05-06 22:36:14Z chris $
#

package Tachikoma::Nodes::HTTP_Fetch;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::HTTP_Responder qw( get_time log_entry cached_strftime );
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD TM_BYTESTREAM TM_STORABLE TM_EOF
);
use CGI;
use JSON;    # -support_by_pp;
use URI::Escape;
use parent qw( Tachikoma::Node );

use version; our $VERSION = 'v2.0.314';

# TODO: configurate mime types
my %Types = (
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
    $self->{prefix} = q{};
    $self->{tables} = {};
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $prefix, @tables ) = split q( ), $self->{arguments};
        my $json = JSON->new;

        # $json->canonical(1);
        # $json->pretty(1);
        $json->allow_blessed(1);
        $json->convert_blessed(0);
        $self->{tables} = { map { $_ => 1 } @tables };
        $self->{prefix} = $prefix if ( defined $prefix );
        $self->{json} = $json;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_STORABLE );
    my $request = $message->payload;
    my $headers = $request->{headers};
    my $path    = $request->{path};
    my $prefix  = $self->{prefix};
    $path =~ s{^$prefix}{};
    $path =~ s{^/+}{};
    my $type            = ( $path =~ m{[.]([^.]+)$} )[0] || 'json';
    my $accept_encoding = $headers->{'accept-encoding'}  || q{};
    my ( $table_name, $escaped ) = split m{/}, $path, 2;
    return $self->send404($message)
        if ( not length $table_name
        or not length $escaped
        or not $self->{tables}->{$table_name}
        or not $Tachikoma::Nodes{$table_name} );
    my $table = $Tachikoma::Nodes{$table_name};
    my $key   = uri_unescape($escaped);
    my $value = $table->lookup($key);
    return $self->send404($message) if ( not defined $value );

    if ( ref $value ) {
        $value = $self->{json}->utf8->encode($value);
    }
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $message->[FROM];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join q{},
        "HTTP/1.1 200 OK\n",
        'Date: ', cached_strftime(), "\n",
        "Server: Tachikoma\n",
        "Connection: close\n",
        'Content-Type: ',
        $Types{$type} || $Types{'json'},
        "\n",
        'Content-Length: ',
        length($value),
        "\n\n",
        $value;
    $self->{sink}->fill($response);
    $response         = Tachikoma::Message->new;
    $response->[TYPE] = TM_EOF;
    $response->[TO]   = $message->[FROM];
    $self->{sink}->fill($response);
    $self->{counter}++;
    log_entry( $self, 200, $message );
    return 1;
}

sub send404 {
    my $self     = shift;
    my $message  = shift;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $message->[FROM];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join q{},
        "HTTP/1.1 404 NOT FOUND\n",
        'Date: ', cached_strftime(), "\n",
        "Server: Tachikoma\n",
        "Connection: close\n",
        "Content-Type: text/plain; charset=utf8\n",
        "\n",
        "Requested URL not found.\n";
    $self->{sink}->fill($response);
    $response         = Tachikoma::Message->new;
    $response->[TYPE] = TM_EOF;
    $response->[TO]   = $message->[FROM];
    log_entry( $self, 404, $message );
    return $self->{sink}->fill($response);
}

sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
}

sub tables {
    my $self = shift;
    if (@_) {
        $self->{tables} = shift;
    }
    return $self->{tables};
}

sub json {
    my $self = shift;
    if (@_) {
        $self->{json} = shift;
    }
    return $self->{json};
}

1;
