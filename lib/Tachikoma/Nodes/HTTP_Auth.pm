#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_Auth
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::HTTP_Auth;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::HTTP_Responder qw( log_entry );
use Tachikoma::Message               qw(
    TYPE FROM TO STREAM PAYLOAD
    TM_STORABLE TM_BYTESTREAM TM_EOF
);
use MIME::Base64;
use Digest::MD5;
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
    my $encoded = $auth    ? ( split q( ), $auth, 2 )[1] : undef;
    my $decoded = $encoded ? decode_base64($encoded)     : undef;
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
    return;
}

sub authenticate {
    my $self   = shift;
    my $user   = shift;
    my $passwd = shift;
    my $salt   = $self->{htpasswd}->{$user} or return;

    # my $hash   = crypt $passwd, $salt;
    my $hash = apache_md5_crypt( $passwd, $salt );
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

# from https://metacpan.org/pod/Crypt::PasswdMD5
# with tweaks for our perlcritic:

my ($itoa64) =
    './0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
my $Magic = q($) . q(apr1$);
my ($max_salt_length) = 8;

sub apache_md5_crypt {
    my ( $pw, $salt ) = @_;
    my ($passwd);
    if ( defined $salt ) {
        $salt =~ s/^\Q$Magic//;    # Take care of the magic string if present.
        $salt =~ s/^(.*)\$.*$/$1/; # Salt can have up to 8 chars...
        $salt = substr $salt, 0, 8;
    }
    else {
        $salt = random_md5_salt();    # In case no salt was proffered.
    }
    my ($ctx) = Digest::MD5->new;     # Here we start the calculation.
    $ctx->add($pw);                   # Original password...
    $ctx->add($Magic);                # ...our magic string...
    $ctx->add($salt);                 # ...the salt...
    my ($final) = Digest::MD5->new;
    $final->add($pw);
    $final->add($salt);
    $final->add($pw);
    $final = $final->digest;

    ## no critic (ProhibitCStyleForLoops)
    for ( my $pl = length $pw; $pl > 0; $pl -= 16 ) {
        $ctx->add( substr $final, 0, $pl > 16 ? 16 : $pl );
    }
    for ( my $i = length $pw; $i; $i >>= 1 ) {
        if ( $i & 1 ) {
            $ctx->add( pack 'C', 0 );
        }
        else {
            $ctx->add( substr $pw, 0, 1 );
        }
    }
    $final = $ctx->digest;
    for ( my $i = 0; $i < 1000; $i++ ) {
        my ($ctx1) = Digest::MD5->new;
        if ( $i & 1 ) {
            $ctx1->add($pw);
        }
        else {
            $ctx1->add( substr $final, 0, 16 );
        }
        if ( $i % 3 ) {
            $ctx1->add($salt);
        }
        if ( $i % 7 ) {
            $ctx1->add($pw);
        }
        if ( $i & 1 ) {
            $ctx1->add( substr $final, 0, 16 );
        }
        else {
            $ctx1->add($pw);
        }
        $final = $ctx1->digest;
    }
    $passwd = q();
    $passwd .= to64(
        int( unpack( 'C', substr $final, 0, 1 ) << 16 )
            | int( unpack( 'C', substr $final, 6, 1 ) << 8 )
            | int( unpack 'C', substr $final, 12, 1 ),
        4
    );
    $passwd .= to64(
        int( unpack( 'C', substr $final, 1, 1 ) << 16 )
            | int( unpack( 'C', substr $final, 7, 1 ) << 8 )
            | int( unpack 'C', substr $final, 13, 1 ),
        4
    );
    $passwd .= to64(
        int( unpack( 'C', substr $final, 2, 1 ) << 16 )
            | int( unpack( 'C', substr $final, 8, 1 ) << 8 )
            | int( unpack 'C', substr $final, 14, 1 ),
        4
    );
    $passwd .= to64(
        int( unpack( 'C', substr $final, 3, 1 ) << 16 )
            | int( unpack( 'C', substr $final, 9, 1 ) << 8 )
            | int( unpack 'C', substr $final, 15, 1 ),
        4
    );
    $passwd .= to64(
        int( unpack( 'C', substr $final, 4, 1 ) << 16 )
            | int( unpack( 'C', substr $final, 10, 1 ) << 8 )
            | int( unpack 'C', substr $final, 5, 1 ),
        4
    );
    $passwd .= to64( int( unpack 'C', substr $final, 11, 1 ), 2 );
    return $Magic . $salt . q/$/ . $passwd;
}

sub random_md5_salt {
    my ($len)  = shift || $max_salt_length;
    my ($salt) = q();
    $len = $max_salt_length
        if ( ( $len < 1 ) or ( $len > $max_salt_length ) );
    $salt .= substr $itoa64, int( rand 64 ), 1 for ( 1 .. $len );
    return $salt;
}

sub to64 {
    my ( $v, $n ) = @_;
    my ($ret) = q();
    while ( --$n >= 0 ) {
        $ret .= substr $itoa64, $v & 0x3f, 1;
        $v >>= 6;
    }
    return $ret;
}

1;
