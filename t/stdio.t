#!/usr/bin/env perl -T
# ----------------------------------------------------------------------
# tachikoma stdio tests
# ----------------------------------------------------------------------
#
# $Id$
#
use strict;
use warnings;
use Test::More tests => 6;

use Tachikoma;
use Tachikoma::EventFrameworks::Select;
use Tachikoma::Nodes::Router;
use Tachikoma::Nodes::STDIO;
use Tachikoma::Nodes::Callback;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );

Tachikoma->event_framework( Tachikoma::EventFrameworks::Select->new );

my $address = '127.0.0.1';
my $port    = '9197';
my $test    = "foo\n";
my $size    = length($test);
my $answer  = '';
my $total   = 10;
my $i       = $total;

my $router = Tachikoma::Nodes::Router->new;
$router->name('_router');
is( ref $router, 'Tachikoma::Nodes::Router',
    'Tachikoma::Nodes::Router->new is ok' );

my $server = inet_server Tachikoma::Nodes::STDIO( $address, $port );
is( ref $server, 'Tachikoma::Nodes::STDIO',
    'Tachikoma::Nodes::STDIO->inet_server is ok' );

$server->name( $server->name . ':server' );

my $destination = Tachikoma::Nodes::Callback->new;
is( ref $destination,
    'Tachikoma::Nodes::Callback', 'Tachikoma::Nodes::Callback->new is ok' );

$server->sink($destination);
$destination->callback(
    sub {
        $answer .= $_[0]->[PAYLOAD];
        $router->remove_node
            if ( length($answer) >= $size * $total );
    }
);

my $client = Tachikoma::Nodes::STDIO->inet_client( $address, $port );
is( ref $client, 'Tachikoma::Nodes::STDIO',
    'Tachikoma::Nodes::STDIO->inet_client is ok' );

for ( 1 .. $total ) {
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[PAYLOAD] = $test;
    $client->fill($message);
}
$router->drain;

is( $answer, $test x $total, 'Tachikoma::Nodes::STDIO->fill is ok' );
is( $answer, $test x $total, 'Tachikoma::Nodes::STDIO->drain is ok' );
