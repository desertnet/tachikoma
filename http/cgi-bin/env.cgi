#!/usr/bin/perl
# ----------------------------------------------------------------------
# env.cgi
# ----------------------------------------------------------------------
#

use strict;
use warnings;
use CGI;
use Data::Dumper;

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Useperl  = 1;

my $cgi = CGI->new;

print $cgi->header,
    '<pre>',
    Dumper( \%ENV ),
    "</pre><hr>\n",
    '<pre>',
    $cgi->param('POSTDATA'),
    "</pre><hr>\n";

1;
