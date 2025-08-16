#!/usr/bin/perl
# ----------------------------------------------------------------------
# fetch.cgi
# ----------------------------------------------------------------------
#

use strict;
use warnings;
use Tachikoma::Nodes::ConsumerBroker;
use Tachikoma::Nodes::Table;
use Tachikoma::Message qw( TIMESTAMP );
use CGI;
use JSON;    # -support_by_pp;
use URI::Escape;

# TODO: configurate mime types
my %TYPES = (
    gif  => 'image/gif',
    jpg  => 'image/jpeg',
    png  => 'image/png',
    ico  => 'image/vnd.microsoft.icon',
    txt  => 'text/plain; charset=utf8',
    js   => 'text/javascript; charset=utf8',
    json => 'application/json; charset=utf8',
    css  => 'text/css; charset=utf8',
    html => 'text/html; charset=utf8',
    xml  => 'text/xml; charset=utf8',
);

my $home   = ( getpwuid $< )[7];
my $config = Tachikoma->configuration;
$config->load_config_file(
    "$home/.tachikoma/etc/tachikoma.conf",
    '/usr/local/etc/tachikoma.conf',
);

my $broker_ids = undef;
if ($Tachikoma::Nodes::CGI::Config) {
    $broker_ids = $Tachikoma::Nodes::CGI::Config->{broker_ids};
}
$broker_ids ||= ['localhost:5501'];
my $host = 'localhost';
my $port = 5100;
my $cgi  = CGI->new;
my $path = $cgi->path_info;
$path =~ s(^/)();
my ( $topic, $field, $escaped ) = split q(/), $path, 3;
die "no topic\n" if ( not length $topic );
die "no field\n" if ( not length $field );
my $key  = uri_unescape( $escaped // q() );
my $json = JSON->new;
$key = $cgi->param('key') if ( not length $key );
die "no key\n" if ( not length $key );
my $topic_field = "$topic.$field:table";
$json->canonical(1);
$json->pretty(1);
$json->allow_blessed(1);
$json->convert_blessed(0);
CORE::state %table;
CORE::state %consumer;

if ( not defined $consumer{$topic} ) {
    $consumer{$topic} = Tachikoma::Nodes::ConsumerBroker->new($topic);
    $consumer{$topic}->broker_ids($broker_ids);
}
if ( not defined $table{$topic_field} ) {
    $table{$topic_field} = Tachikoma::Nodes::Table->new;
    $table{$topic_field}->host($host);
    $table{$topic_field}->port($port);
    $table{$topic_field}->field($topic_field);
}
my $value   = undef;
my $type    = lc( ( $key =~ m{[.]([^.]+)$} )[0] // q() );
my $payload = $table{$topic_field}->fetch($key);

if ( ref $payload ) {
    json_header();
    print $json->utf8->encode($payload);
}
else {
    if ($payload) {
        my ( $partition, $offset ) = split m{:}, $payload, 2;
        $value = $consumer{$topic}->fetch_offset( $partition, $offset );
    }
    if ( ref $value eq 'ARRAY' ) {
        my $payloads = [
            map  { $_->payload }
            sort { $a->[TIMESTAMP] <=> $b->[TIMESTAMP] } @{$value}
        ];
        if ( ref $payload->[0] ) {
            json_header();
            print $json->utf8->encode($payloads);
        }
        else {
            plain_header($TYPES{$type} || $TYPES{'txt'});
            print @{$payloads};
        }
    }
    elsif ($value) {
        my $payload = $value->payload;
        if ( ref $payload ) {
            json_header();
            print $json->utf8->encode($payload);
        }
        else {
            plain_header($TYPES{$type} || $TYPES{'txt'});
            print $payload;
        }
    }
}

sub json_header {
    print CGI->header(
        -type    => 'application/json; charset=utf8',
        -charset => 'utf-8'
    );
    return;
}

sub plain_header {
    my $type = shift;
    print CGI->header(
        -type    => $type,
        -charset => 'utf-8'
    );
    return;
}
