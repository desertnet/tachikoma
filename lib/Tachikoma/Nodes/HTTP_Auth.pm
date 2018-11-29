#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_Auth
# ----------------------------------------------------------------------
#
# $Id: HTTP_Auth.pm 35959 2018-11-29 01:42:01Z chris $
#

package Tachikoma::Nodes::HTTP_Auth;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::HTTP_Responder qw( log_entry );
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD
    TM_STORABLE TM_BYTESTREAM TM_EOF
);
use MIME::Base64;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{filename} = undef;
    $self->{realm}    = undef;
    $self->{htpasswd} = undef;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $filename, $realm ) = split q( ), $self->{arguments}, 2;
        die "ERROR: bad arguments for HTTP_Auth\n" if ( not $filename );
        $self->{filename} = $filename;
        $self->{realm}    = $realm;
        $self->reload_htpasswd;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_STORABLE );
    my $request = $message->payload;
    my $auth    = $request->{headers}->{'authorization'};
    my $encoded = $auth ? ( split q( ), $auth, 2 )[1] : undef;
    my $decoded = $encoded ? decode_base64($encoded) : undef;
    my ( $user, $passwd ) = $decoded ? split m{:}, $decoded, 2 : undef;
    $self->{counter}++;

    if ($user) {
        if ( $self->authenticate( $user, $passwd ) ) {
            $request->{auth_type}   = 'Basic';
            $request->{remote_user} = $user;
            $message->[TO]          = $self->{owner};
            return $self->{sink}->fill($message);
        }
        else {
            $self->stderr("authorization failed for $user");
        }
    }
    my $realm    = $self->{realm} || $request->{headers}->{host} || 'default';
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $message->[FROM];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join q(),
        "HTTP/1.1 401 UNAUTHORIZED\n",
        "Content-Type: text/plain\n",
        'WWW-Authenticate: Basic realm="',
        $realm,
        "\"\n\n",
        "Authorization required.\n";
    $self->{sink}->fill($response);
    $response         = Tachikoma::Message->new;
    $response->[TYPE] = TM_EOF;
    $response->[TO]   = $message->[FROM];
    $self->{sink}->fill($response);
    log_entry( $self, 401, $message );
    return 1;
}

sub authenticate {
    my $self   = shift;
    my $user   = shift;
    my $passwd = shift;
    my $salt   = $self->{htpasswd}->{$user} or return;
    my $hash   = crypt $passwd, $salt;
    return $hash eq $salt;
}

sub reload_htpasswd {
    my $self     = shift;
    my $filename = $self->{filename};
    my %htpasswd = ();
    my $fh;
    open $fh, '<', $filename
        or die "can't open htpasswd file $filename: $!\n";
    while ( my $line = <$fh> ) {
        my ( $user, $passwd ) = split m{:}, $line, 2;
        chomp $passwd;
        $htpasswd{$user} = $passwd;
    }
    close $fh
        or die "can't close htpasswd file $filename: $!\n";
    $self->{htpasswd} = \%htpasswd;
    return 'success';
}

sub filename {
    my $self = shift;
    if (@_) {
        $self->{filename} = shift;
    }
    return $self->{filename};
}

sub realm {
    my $self = shift;
    if (@_) {
        $self->{realm} = shift;
    }
    return $self->{realm};
}

sub htpasswd {
    my $self = shift;
    if (@_) {
        $self->{htpasswd} = shift;
    }
    return $self->{htpasswd};
}

1;
