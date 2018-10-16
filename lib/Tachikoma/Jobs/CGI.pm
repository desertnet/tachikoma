#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::CGI
# ----------------------------------------------------------------------
#
# $Id: CGI.pm 3033 2009-09-15 08:02:14Z chris $
#

package Tachikoma::Jobs::CGI;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::CGI;
use Tachikoma::Message qw( TO );
use parent qw( Tachikoma::Job );

use version; our $VERSION = 'v2.0.349';

sub initialize_graph {
    my $self = shift;
    my ( $config_file, $tmp_path ) = split q( ), $self->arguments || q(), 2;
    my $cgi = Tachikoma::Nodes::CGI->new;
    $self->connector->sink($cgi);
    $cgi->name('CGI');
    $cgi->arguments( $tmp_path
        ? join q( ),
        $config_file, $tmp_path
        : $config_file );
    $cgi->sink($self);
    $self->sink( $self->router );
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    $message->[TO] = join q(/), '_parent', $message->[TO]
        if ( $message->[TO] !~ m{^_parent} );
    return $self->SUPER::fill($message);
}

1;
