#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::FileRemover
# ----------------------------------------------------------------------
#
# $Id: FileRemover.pm 35265 2018-10-16 06:42:47Z chris $
#
# FileRemover simply accepts the path to a file, and after thoroughly
# validating that path, moves it to a new location.
#

package Tachikoma::Jobs::FileRemover;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw( TYPE TO PAYLOAD TM_BYTESTREAM );
use parent qw( Tachikoma::Job );

use version; our $VERSION = 'v2.0.349';

sub initialize_graph {
    my $self = shift;
    my ( $prefix, $logger ) = split q( ), $self->arguments, 2;
    $self->prefix($prefix);
    $self->logger($logger);
    $self->connector->sink($self);
    $self->sink( $self->router );
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $prefix     = $self->{prefix};
    my $log_prefix = Tachikoma->log_prefix;
    my @log        = ();
    for my $path ( split m{^}, $message->[PAYLOAD] ) {
        $path =~ s{(?:^|/)[.][.](?=/)}{}g;
        chomp $path;
        my $relative = ( $path =~ m{^$prefix/(.*)$} )[0];
        if ( not $relative ) {
            $self->stderr( "ERROR: bad path: $path from ", $message->from );
            next;
        }
        my $new_path;
        if ( $relative =~ m{^/*deleted/(.*)$} ) {
            my $original = $1;
            $new_path = join q(/), $prefix, $original;
        }
        else {
            $new_path = join q(/), $prefix, 'deleted', $relative;
        }
        if ( $path and -e $path ) {
            umask 0022 or die;
            $self->make_parent_dirs($new_path);
            rename $path, $new_path
                or die "ERROR: couldn't rename $path to $new_path: $!\n";
            push @log, $log_prefix . "rename $path $new_path\n";
        }
    }
    if ( @log and $self->{logger} ) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_BYTESTREAM;
        $response->[TO]      = join q(/), '_parent', $self->{logger};
        $response->[PAYLOAD] = join q(), @log;
        $self->SUPER::fill($response);
    }
    $self->cancel($message);
    return;
}

sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
}

sub logger {
    my $self = shift;
    if (@_) {
        $self->{logger} = shift;
    }
    return $self->{logger};
}

1;
