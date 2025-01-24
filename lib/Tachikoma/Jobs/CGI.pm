#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::CGI
# ----------------------------------------------------------------------
#

package Tachikoma::Jobs::CGI;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::CGI;
use Tachikoma::Message qw( TO );
use parent             qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.349');

sub initialize_graph {
    my $self = shift;
    my $cgi  = Tachikoma::Nodes::CGI->new;
    $self->connector->sink($cgi);
    $cgi->arguments( $self->arguments );
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
