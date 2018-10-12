#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Command
# ----------------------------------------------------------------------
#
# $Id: Command.pm 35073 2018-10-12 01:49:26Z chris $
#

package Tachikoma::Command;
use strict;
use warnings;
use Tachikoma::Message;
use Tachikoma::Config
    qw( $ID $Private_Key $Private_Ed25519_Key $Secure_Level );
use Crypt::OpenSSL::RSA;
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
use parent qw( Tachikoma::Message );

use version; our $VERSION = qv('v2.0.27');

sub new {
    my $class  = shift;
    my $packed = shift;
    my $self   = {
        name      => q{},
        arguments => q{},
        payload   => q{},
        signature => q{}
    };
    bless $self, $class;
    if ($packed) {
        (   $self->{name},    $self->{arguments},
            $self->{payload}, $self->{signature}
        ) = unpack 'Z* Z* N/a n/a', $packed;
    }
    return $self;
}

sub name {
    my $self = shift;
    if (@_) {
        $self->{name} = shift;
    }
    return $self->{name};
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
    }
    return $self->{arguments};
}

sub payload {
    my $self = shift;
    if (@_) {
        $self->{payload} = shift;
    }
    return $self->{payload};
}

sub signature {
    my $self = shift;
    if (@_) {
        $self->{signature} = shift;
    }
    return $self->{signature};
}

sub sign {
    my $self      = shift;
    my $scheme    = shift or die 'no scheme';
    my $timestamp = shift or die 'no timestamp';
    my $plaintext = join q{:},
        $ID, $timestamp,
        ( $self->{name} // q{} ),
        ( $self->{arguments} // q{} ),
        ( $self->{payload} // q{} );
    return if ( defined $Secure_Level and $Secure_Level == 0 );
    if ( $scheme eq 'ed25519' ) {
        die "ERROR: Ed25519 signatures not supported\n"
            if ( not $USE_SODIUM );
        die "ERROR: Ed25519 signatures not configured\n"
            if ( not $Private_Ed25519_Key );
        my $crypto_sign = Crypt::NaCl::Sodium->sign;
        $self->{signature} = join q{}, $ID, "\n", "ed25519\n",
            $crypto_sign->mac( $plaintext, $Private_Ed25519_Key );
    }
    else {
        return if ( not $Private_Key );
        my $rsa = Crypt::OpenSSL::RSA->new_private_key($Private_Key);
        if ( $scheme eq 'sha256' ) {
            $rsa->use_sha256_hash;
            $self->{signature} = join q{}, $ID, "\n", "sha256\n",
                $rsa->sign($plaintext);
        }
        else {
            $rsa->use_sha1_hash;
            $self->{signature} = join q{}, $ID, "\n", $rsa->sign($plaintext);
        }
    }
    return;
}

sub packed {
    my $self = shift;
    return pack 'Z* Z* N/a* n/a*',
        $self->{name} // q{},
        $self->{arguments} // q{},
        $self->{payload} // q{},
        $self->{signature} // q{};
}

1;
