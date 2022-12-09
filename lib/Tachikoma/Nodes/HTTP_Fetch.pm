#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_Fetch
# ----------------------------------------------------------------------
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

use version; our $VERSION = qv('v2.0.314');

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
    $self->{prefix}  = q();
    $self->{allowed} = qr{};
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $prefix, $allowed ) = split q( ), $self->{arguments};
        die "no allowed regex specified\n" if ( not $allowed );
        my $json = JSON->new;
        $json->canonical(1);
        $json->pretty(1);
        $json->allow_blessed(1);
        $json->convert_blessed(0);
        $self->{prefix}  = $prefix;
        $self->{allowed} = qr{$allowed};
        $self->{json}    = $json;
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
    my $allowed = $self->{allowed};
    $path =~ s{^$prefix}{};
    $path =~ s{^/+}{};
    my $type            = ( $path =~ m{[.]([^.]+)$} )[0] || 'json';
    my $accept_encoding = $headers->{'accept-encoding'}  || q();
    my ( $node_name, $escaped ) = split m{/}, $path, 2;
    my $value = undef;

    if ( not length $node_name ) {
        $value = [];
        for my $name ( sort keys %Tachikoma::Nodes ) {
            my $node = $Tachikoma::Nodes{$name};
            next if ( $name !~ m{$allowed} or not $node->can('lookup') );
            if ( $node->can('buffer_size') ) {
                push @{$value},
                    {
                    name => $name,
                    size => $node->buffer_size // $node->get_buffer_size
                    };
            }
            else {
                push @{$value}, $name;
            }
        }
    }
    else {
        my $node = $Tachikoma::Nodes{$node_name};
        return $self->send404($message)
            if ( $node_name !~ m{$allowed}
            or not $node
            or not $node->can('lookup') );
        my $key = uri_unescape( $escaped // q() );
        $value = $node->lookup($key);
    }
    return $self->send404($message) if ( not defined $value );

    if ( ref $value ) {
        $value = $self->{json}->utf8->encode($value);
    }
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $message->[FROM];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join q(),
        "HTTP/1.1 200 OK\n",
        'Date: ', cached_strftime(), "\n",
        "Server: Tachikoma\n",
        "Connection: close\n",
        'Content-Type: ',
        $TYPES{$type} || $TYPES{'json'},
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
    $response->[PAYLOAD] = join q(),
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

sub allowed {
    my $self = shift;
    if (@_) {
        $self->{allowed} = shift;
    }
    return $self->{allowed};
}

sub json {
    my $self = shift;
    if (@_) {
        $self->{json} = shift;
    }
    return $self->{json};
}

1;
