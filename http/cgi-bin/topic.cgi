#!/usr/bin/perl
# ----------------------------------------------------------------------
# topic.cgi
# ----------------------------------------------------------------------
#
# $Id$
#

use strict;
use warnings;
use Tachikoma::Nodes::ConsumerBroker;
require '/usr/local/etc/tachikoma.conf';
use CGI;
use JSON -support_by_pp;

my $broker_ids = [ 'localhost:5501', 'localhost:5502' ];
my $cgi        = CGI->new;
my $path       = $cgi->path_info;
$path =~ s(^/)();
my ( $topic, $partition, $offset, $count ) = split q(/), $path, 4;
die "no topic\n" if ( not $topic );
$partition ||= 0;
$offset    ||= 'start';
$count     ||= 1;
my $json = JSON->new;

# $json->escape_slash(1);
$json->canonical(1);
$json->pretty(1);
$json->allow_blessed(1);
$json->convert_blessed(0);
CORE::state %groups;
$groups{$topic} //= Tachikoma::Nodes::ConsumerBroker->new($topic);
$groups{$topic}->broker_ids($broker_ids);
my $group    = $groups{$topic};
my $consumer = $group->consumers->{$partition}
    || $group->make_sync_consumer($partition);

if ( $offset =~ m(^\d+$) ) {
    $consumer->next_offset($offset);
}
else {
    $consumer->default_offset($offset);
}
my @messages = ();
my $results  = undef;
if ( $offset eq 'recent' ) {
    do {
        push @messages, @{ $consumer->fetch };
        shift @messages while ( @messages > $count );
    }
    while ( not $consumer->eos );
}
else {
    do { push @messages, @{ $consumer->fetch } }
        while ( @messages < $count and not $consumer->eos );
}
if ( $consumer->sync_error ) {
    print STDERR $consumer->sync_error;
    $results = {
        next_url => $cgi->url( -path_info => 1, -query => 1 ),
        error    => 'SERVER_ERROR'
    };
}
else {
    my @output      = ();
    my $i           = 1;
    my $next_offset = $offset =~ m{\D} ? $consumer->offset : $offset;
    for my $message (@messages) {
        $next_offset = ( split q(:), $message->id, 2 )[1];
        push @output, $message->payload;
        last if ( $i++ >= $count );
    }
    $results = {
        next_url =>
            join( q(/), $cgi->url, $topic, $partition, $next_offset, $count ),
        payload => \@output
    };
}

print( $cgi->header( -type => 'application/json', -charset => 'utf-8' ),
    $json->utf8->encode($results) );
