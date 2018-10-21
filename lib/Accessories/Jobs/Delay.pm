#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Jobs::Delay
# ----------------------------------------------------------------------
#
# $Id: Delay.pm 2371 2009-07-03 08:24:35Z chris $
#

package Accessories::Jobs::Delay;
use strict;
use warnings;
use Tachikoma::Job;
use Time::HiRes;
use parent qw( Tachikoma::Job );

use version; our $VERSION = 'v2.0.349';

sub fill {
    my $self    = shift;
    my $message = shift;
    Time::HiRes::sleep( $self->arguments || 1 );
    return $self->SUPER::fill($message);
}

1;
