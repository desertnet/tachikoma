#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_File
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::HTTP_File;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::HTTP_Responder
    qw( get_time log_entry cached_strftime send404 );
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD TM_BYTESTREAM TM_STORABLE TM_EOF
);
use POSIX  qw( strftime );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

my $DEFAULT_EXPIRES = 900;

# TODO: configurate mime types
my %TYPES = (
    bmp  => 'image/bmp',
    gif  => 'image/gif',
    jpg  => 'image/jpeg',
    jpeg => 'image/jpeg',
    png  => 'image/png',
    svg  => 'image/svg',
    tif  => 'image/tiff',
    tiff => 'image/tiff',
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
    $self->{path}   = undef;
    $self->{prefix} = q();
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $path, $prefix ) = split q( ), $self->{arguments}, 2;
        $self->{path}   = $path;
        $self->{prefix} = $prefix if ( defined $prefix );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_STORABLE );
    my $request = $message->payload;
    my $headers = $request->{headers};
    my $uri     = $request->{path};
    my $path    = ( $uri =~ m{^(/[\w:./~-]*)} )[0] || q();
    $path =~ s{/[.][.](?=/)}{}g;
    $path =~ s{/+}{/}g;
    my $url    = $path;
    my $prefix = $self->{prefix};
    $path =~ s{^$prefix}{};
    my $filename        = join q(), $self->{path}, $path;
    my $if_modified     = $headers->{'if-modified-since'};
    my $accept_encoding = $headers->{'accept-encoding'} || q();
    my $response        = Tachikoma::Message->new;
    $response->[TYPE]   = TM_BYTESTREAM;
    $response->[TO]     = $message->[FROM];
    $response->[STREAM] = $message->[STREAM];

    if ( -d $filename ) {
        $url =~ s{/$}{};
        $response->[PAYLOAD] = join q(),
            "HTTP/1.1 302 FOUND\n",
            'Date: ', cached_strftime(), "\n",
            "Server: Tachikoma\n",
            "Connection: close\n",
            "Content-Type: text/plain; charset=utf8\n",
            "Location: $url/index.html\n",
            "\n",
            "try $url/index.html\n";
        $self->{sink}->fill($response);
        $response         = Tachikoma::Message->new;
        $response->[TYPE] = TM_EOF;
        $response->[TO]   = $message->[FROM];
        log_entry( $self, 302, $message );
        return $self->{sink}->fill($response);
    }
    elsif ( not -r _ ) {
        return $self->send404($message);
    }
    elsif ($if_modified) {
        my $last_modified = ( stat _ )[9];
        my $date          = get_time($if_modified);
        if ( $last_modified <= $date ) {
            $response->[PAYLOAD] = join q(), "HTTP/1.1 304 Not Modified\n",
                'Date: ', cached_strftime(), "\n",
                "Server: Tachikoma\n",
                "Connection: close\n",
                "\n";
            $self->{sink}->fill($response);
            $response         = Tachikoma::Message->new;
            $response->[TYPE] = TM_EOF;
            $response->[TO]   = $message->[FROM];
            log_entry( $self, 304, $message );
            return $self->{sink}->fill($response);
        }
    }
    my $type = ( $path =~ m{[.]([^.]+)$} )[0] || 'txt';
    $self->stderr("WARNING: no mime type set for $type")
        if ( not $TYPES{$type} );
    my @stat = stat $filename;
    $response->[PAYLOAD] = join q(),
        "HTTP/1.1 200 OK\n",
        'Date: ', cached_strftime(), "\n",
        strftime( "Last-Modified: %a, %d %b %Y %T GMT\n", gmtime $stat[9] ),
        strftime(
        "Expires: %a, %d %b %Y %T GMT\n",
        gmtime $Tachikoma::Now + $DEFAULT_EXPIRES
        ),
        "Server: Tachikoma\n",
        "Connection: close\n",
        'Content-Type: ',
        $TYPES{$type} || $TYPES{'txt'},
        "\n",
        'Content-Length: ',
        $stat[7],
        "\n\n";
    $self->{sink}->fill($response);
    local $/ = undef;
    my $fh = undef;
    open $fh, '<', $filename
        or return $self->stderr("ERROR: couldn't open $filename: $!");
    $response            = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $message->[FROM];
    $response->[PAYLOAD] = <$fh>;
    close $fh or $self->stderr("ERROR: couldn't close $filename: $!");
    $self->{sink}->fill($response);
    $response         = Tachikoma::Message->new;
    $response->[TYPE] = TM_EOF;
    $response->[TO]   = $message->[FROM];
    $self->{sink}->fill($response);
    $self->{counter}++;
    log_entry( $self, 200, $message, $stat[7] );
    return;
}

sub path {
    my $self = shift;
    if (@_) {
        $self->{path} = shift;
    }
    return $self->{path};
}

sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
}

1;
