#!/usr/bin/perl
# ----------------------------------------------------------------------
# store.cgi
# ----------------------------------------------------------------------
#
# $Id$
#

use strict;
use warnings;
use Tachikoma::Nodes::Topic;
require '/usr/local/etc/tachikoma.conf';
use CGI;
use Digest::MD5 qw( md5 );
use URI::Escape;

my $broker_ids = [ 'localhost:5501', 'localhost:5502' ];
my $cgi        = CGI->new;
my $path       = $cgi->path_info;
$path =~ s(^/)();
my ( $topic, $escaped ) = split q(/), $path, 2;
my $postdata = $cgi->param('POSTDATA');
die "wrong method\n" if ( not length $postdata );
die "no topic\n"     if ( not length $topic );
die "no key\n"       if ( not length $escaped );
my $key    = uri_unescape($escaped);
my $broker = Tachikoma::Nodes::Topic->new($topic);
$broker->broker_ids($broker_ids);
my $partitions   = $broker->get_partitions;
my $partition_id = 0;
$partition_id += $_ for ( unpack "C*", md5($key) );
$partition_id %= scalar @{$partitions};
$broker->send_kv( $partition_id, { $key => [$postdata] } )
    or die $broker->sync_error;

print $cgi->header(
    -type    => 'application/json',
    -charset => 'utf-8'
);
print qq({ "result" : "OK" }\n);
