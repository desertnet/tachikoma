#!/usr/bin/perl
# ----------------------------------------------------------------------
# env.cgi
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

print(
    $cgi->header,
    '<html><head></head><body><pre>',
    Dumper(\%ENV),
    '</pre></body></html>', "\n"
);

1;
