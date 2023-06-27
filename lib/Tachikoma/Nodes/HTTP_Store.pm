#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_Store
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::HTTP_Store;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::HTTP_Responder qw( get_time log_entry );
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD TM_BYTESTREAM TM_STORABLE TM_EOF
);
use CGI;
use POSIX qw( strftime );
use URI::Escape;
use Digest::MD5 qw( md5_hex );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{prefix} = q();
    $self->{topics} = {};
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $tmp_path, $prefix, @topics ) = split q( ), $self->{arguments};
        $self->{topics}   = { map { $_ => 1 } @topics };
        $self->{tmp_path} = $tmp_path if ( defined $tmp_path );
        $self->{prefix}   = $prefix if ( defined $prefix );
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
    my $accept_encoding = $headers->{'accept-encoding'} || q();
    my ( $topic_name, $escaped ) = split m{/}, $path, 2;
    my $postdata = undef;

    if ( $request->{tmp} ) {
        my $tmp_path = join q(/), $self->{tmp_path}, 'post';
        my $tmp      = ( $request->{tmp} =~ m{^($tmp_path/\w+$)} )[0];
        local $/ = undef;
        my $fh = undef;
        if ( not open $fh, '<', $tmp ) {
            $self->stderr("ERROR: couldn't open $tmp: $!");
            return $self->send404($message);
        }
        $postdata = <$fh>;
        if ( not close $fh ) {
            $self->stderr("ERROR: couldn't close $tmp: $!");
            return $self->send404($message);
        }
        if ( not unlink $tmp ) {
            $self->stderr("ERROR: couldn't unlink $tmp: $!");
            return $self->send404($message);
        }
    }
    else {
        $postdata = $request->{body};
    }
    return $self->send404($message)
        if ( not length $topic_name
        or not length $postdata
        or not $self->{topics}->{$topic_name}
        or not $Tachikoma::Nodes{$topic_name} );
    my $topic  = $Tachikoma::Nodes{$topic_name};
    my $key    = length $escaped ? uri_unescape($escaped) : md5_hex(rand);
    my $update = Tachikoma::Message->new;
    $update->[TYPE]    = TM_BYTESTREAM;
    $update->[STREAM]  = $key;
    $update->[PAYLOAD] = $postdata;
    $topic->fill($update);
    my $output   = qq({ "result" : "OK" }\n);
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $message->[FROM];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join q(),
        "HTTP/1.1 200 OK\n",
        strftime( "Date: %a, %d %b %Y %T GMT\n", gmtime $Tachikoma::Now ),
        "Server: Tachikoma\n",
        "Connection: close\n",
        "Content-Type: application/json; charset=utf8\n",
        'Content-Length: ',
        length($output),
        "\n\n";
    $self->{sink}->fill($response);
    $response            = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $message->[FROM];
    $response->[PAYLOAD] = $output;
    $self->{sink}->fill($response);
    $response         = Tachikoma::Message->new;
    $response->[TYPE] = TM_EOF;
    $response->[TO]   = $message->[FROM];
    $self->{sink}->fill($response);
    $self->{counter}++;
    log_entry( $self, 200, $message );
    return;
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
        strftime( "Date: %a, %d %b %Y %T GMT\n", gmtime $Tachikoma::Now ),
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

sub tmp_path {
    my $self = shift;
    if (@_) {
        $self->{tmp_path} = shift;
    }
    return $self->{tmp_path};
}

sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
}

sub topics {
    my $self = shift;
    if (@_) {
        $self->{topics} = shift;
    }
    return $self->{topics};
}

1;
