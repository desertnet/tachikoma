#!/usr/bin/perl
# ----------------------------------------------------------------------
# slow-env.cgi
# ----------------------------------------------------------------------
#
# $Id$
#

use strict;
use warnings;
use CGI;
use Data::Dumper;

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Useperl  = 1;

my $cgi = CGI->new;

my $output = join( '',
    '<html><head></head><body><pre>',
    Dumper( \%ENV ),
    '</pre></body></html>', "\n" );
print $cgi->header( -expires => '+1m' );
for my $line ( split( m(^), $output ) ) {
    print $line;
    sleep 1;
}

1;
