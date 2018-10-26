#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Crypto
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Crypto;
use strict;
use warnings;
use Tachikoma::Message qw( TIMESTAMP );
my $USE_SODIUM;

BEGIN {
    $USE_SODIUM = eval {
        my $module_name = 'Crypt::NaCl::Sodium';
        my $module_path = 'Crypt/NaCl/Sodium.pm';
        require $module_path;
        import $module_name qw( :utils );
        return 1;
    };
}
use Crypt::OpenSSL::RSA qw();

use version; our $VERSION = qv('v2.0.349');

my $Scheme = 'rsa';

sub verify_signature {
    my $self      = shift;
    my $type      = shift;
    my $message   = shift;
    my $command   = shift;
    my $challenge = $command->{payload};
    my ( $id, $proto ) = split m{\n}, $command->{signature}, 2;
    if ( not $id ) {
        $self->stderr(q(ERROR: couldn't find ID));
        return;
    }
    elsif ( $type eq 'server' ) {
        $self->check_server_id($id) or return;
    }
    my ( $scheme, $signature ) = split m{\n}, $proto, 2;
    $signature = $proto
        if ($scheme ne 'rsa'
        and $scheme ne 'rsa-sha256'
        and $scheme ne 'ed25519' );
    my $public_keys = Tachikoma->configuration->{public_keys};
    if ( not $public_keys->{$id} ) {
        $self->stderr("ERROR: $id not in authorized_keys");
        return;
    }
    elsif ( not $public_keys->{$id}->{allow}->{$type} ) {
        $self->stderr("ERROR: $id not allowed to connect");
        return;
    }
    my $signed = join q(:),
        $id, $message->[TIMESTAMP], $command->{name}, $command->{arguments},
        $command->{payload};
    if ( $scheme eq 'ed25519' ) {
        return if ( not $self->verify_ed25519( $signed, $id, $signature ) );
    }
    elsif ( $scheme eq 'rsa-sha256' ) {
        return if ( not $self->verify_sha256( $signed, $id, $signature ) );
    }
    else {
        return if ( not $self->verify_rsa( $signed, $id, $signature ) );
    }
    my $time = $Tachikoma::Now // time;
    if ( $time - $message->[TIMESTAMP] > 300 ) {
        $self->stderr('ERROR: message timestamp too far in the past');
        return;
    }
    elsif ( $message->[TIMESTAMP] - $time > 300 ) {
        $self->stderr('ERROR: message timestamp too far in the future');
        return;
    }
    return 1;
}

sub check_server_id {
    my $self           = shift;
    my $id             = shift;
    my $short_id       = $id;
    my $short_hostname = $self->{hostname};
    if (    $short_hostname
        and $short_hostname ne 'localhost'
        and $short_hostname ne '127.0.0.1' )
    {
        $short_id =~ s{.*@}{};
        $short_id =~ s{[.].*}{};
        $short_hostname =~ s{[.].*}{};
        if ( $short_id ne $short_hostname ) {
            $self->print_less_often(
                "ERROR: check_server_id failed: wrong ID: $id\n");
            return;
        }
    }
    return 1;
}

sub verify_ed25519 {
    my $self      = shift;
    my $signed    = shift;
    my $id        = shift;
    my $signature = shift;
    if ( not $USE_SODIUM ) {
        $self->stderr('ERROR: Ed25519 signatures not supported');
        return;
    }
    my $public_keys = Tachikoma->configuration->{public_keys};
    my $key_text    = $public_keys->{$id}->{ed25519};
    if ( not $key_text ) {
        $self->stderr("ERROR: $id missing Ed25519 key");
        return;
    }
    my $crypto_sign = Crypt::NaCl::Sodium->sign;
    if ( not $crypto_sign->verify( $signature, $signed, $key_text ) ) {
        my $error = $@ || 'signature mismatch';
        $self->stderr("ERROR: $error");
        return;
    }
    return 1;
}

sub verify_sha256 {
    my $self        = shift;
    my $signed      = shift;
    my $id          = shift;
    my $signature   = shift;
    my $public_keys = Tachikoma->configuration->{public_keys};
    my $key_text    = $public_keys->{$id}->{public_key};
    if ( not $key_text ) {
        $self->stderr("ERROR: $id missing RSA key");
        return;
    }
    my $okay = eval {
        my $rsa = Crypt::OpenSSL::RSA->new_public_key($key_text);
        $rsa->use_sha256_hash;
        return $rsa->verify( $signed, $signature );
    };
    if ( not $okay ) {
        my $error = $@ || 'signature mismatch';
        $self->stderr("ERROR: $error");
        return;
    }
    return 1;
}

sub verify_rsa {
    my $self        = shift;
    my $signed      = shift;
    my $id          = shift;
    my $signature   = shift;
    my $public_keys = Tachikoma->configuration->{public_keys};
    my $key_text    = $public_keys->{$id}->{public_key};
    if ( not $key_text ) {
        $self->stderr("ERROR: $id missing RSA key");
        return;
    }
    my $okay = eval {
        my $rsa = Crypt::OpenSSL::RSA->new_public_key($key_text);
        $rsa->use_sha1_hash;
        return $rsa->verify( $signed, $signature );
    };
    if ( not $okay ) {
        my $error = $@ || 'signature mismatch';
        $self->stderr("ERROR: $error");
        return;
    }
    return 1;
}

sub scheme {
    my $self = shift;
    if (@_) {
        my $scheme = shift;
        die "invalid scheme: $scheme\n"
            if ($scheme ne 'rsa'
            and $scheme ne 'rsa-sha256'
            and $scheme ne 'ed25519' );
        if ( $scheme eq 'ed25519' ) {
            die "Ed25519 not supported\n" if ( not $USE_SODIUM );
            die "Ed25519 not configured\n"
                if ( not Tachikoma->configuration->{private_ed25519_key} );
        }
        $Scheme = $scheme;
    }
    return $Scheme;
}

1;
