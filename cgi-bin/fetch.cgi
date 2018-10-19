#!/usr/bin/perl
# ----------------------------------------------------------------------
# fetch.cgi
# ----------------------------------------------------------------------
#
# $Id$
#

use strict;
use warnings;
use Tachikoma::Nodes::Table;
use Tachikoma::Message qw( TIMESTAMP );
require '/usr/local/etc/tachikoma.conf';
use CGI;
use JSON; # -support_by_pp;
use URI::Escape;

my $host = 'localhost';
my $port = 5100;
my $cgi  = CGI->new;
my $path = $cgi->path_info;
$path =~ s(^/)();
my ( $topic, $field, $escaped ) = split q(/), $path, 3;
$escaped = $cgi->param('key') if ( not length $escaped );
die "no topic\n" if ( not length $topic );
die "no field\n" if ( not length $field );
die "no key\n"   if ( not length $escaped );
my $key  = uri_unescape($escaped);
my $json = JSON->new;
$json->canonical(1);
$json->pretty(1);
$json->allow_blessed(1);
$json->convert_blessed(0);
CORE::state %table;

if ( not defined $table{$topic} ) {
    $table{$topic} = Tachikoma::Nodes::Table->new;
    $table{$topic}->host($host);
    $table{$topic}->port($port);
    $table{$topic}->topic($topic);
    $table{$topic}->field($field);
}
print $cgi->header(
    -type    => 'application/json',
    -charset => 'utf-8'
);
my $value   = undef;
my $payload = $table{$topic}->fetch($key);

if ($payload) {
    my ( $partition, $offset ) = split m{:}, $payload, 2;
    $value = $table{$topic}->fetch_offset( $partition, $offset );
}
if ( ref $value eq 'ARRAY' ) {
    my $payloads = [
        map  { $_->payload }
        sort { $a->[TIMESTAMP] <=> $b->[TIMESTAMP] } @{$value}
    ];
    print $json->utf8->encode($payloads);
}
elsif ($value) {
    my $payload = $value->payload;
    if ( ref $payload ) {
        print $json->utf8->encode($payload);
    }
    else {
        print $payload;
    }
}
