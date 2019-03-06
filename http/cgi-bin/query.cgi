#!/usr/bin/perl
# ----------------------------------------------------------------------
# query.cgi
# ----------------------------------------------------------------------
#
# $Id$
#

use strict;
use warnings;
use Tachikoma::Nodes::QueryEngine;
require '/usr/local/etc/tachikoma.conf';
use CGI;
use JSON -support_by_pp;

my $host_ports = [ 'localhost:5201', 'localhost:5202' ];
my $cgi        = CGI->new;
my $topic      = $cgi->path_info;
$topic =~ s(^/)();
die "no topic\n" if ( not $topic );
my $postdata = $cgi->param('POSTDATA') or die "ERROR: wrong method\n";
my $json     = JSON->new;
my $query    = $json->decode($postdata);
$json->canonical(1);
$json->pretty(1);
$json->allow_blessed(1);
$json->convert_blessed(0);
CORE::state %engine;

if ( not defined $engine{$topic} ) {
    $engine{$topic} = Tachikoma::Nodes::QueryEngine->new;
    $engine{$topic}->host_ports($host_ports);
    $engine{$topic}->topic($topic);
}
print $cgi->header(
    -type    => 'application/json',
    -charset => 'utf-8'
);
$engine{$topic}->query($query);
my $comma = undef;
while ( my $value = $engine{$topic}->fetchrow ) {
    my $output = undef;
    if ( ref $value eq 'Tachikoma::Message' ) {
        $output = {
            type      => $value->type_as_string,
            from      => $value->from,
            id        => $value->id,
            key       => $value->stream,
            timestamp => $value->timestamp,
            value     => $value->payload,
        };
    }
    else {
        $output = $value;
    }
    if ( ref $output ) {
        my $json_output = undef;
        if ( not $comma ) {
            $json_output = "[\n";
            $comma       = 1;
        }
        else {
            $json_output = ",\n";
        }
        $json_output .= $json->utf8->encode($output);
        chomp $json_output;
        $output = $json_output;
    }
    print $output;
}
print "\n]\n" if ($comma);
