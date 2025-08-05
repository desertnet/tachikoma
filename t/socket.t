#!/usr/bin/env perl -T
# ----------------------------------------------------------------------
# tachikoma socket tests
# ----------------------------------------------------------------------
#

use strict;
use warnings;
use Test::More tests => 8;

use Tachikoma::EventFrameworks::Select;
use Tachikoma::Nodes::Router;
use Tachikoma::Nodes::Socket;
use Tachikoma::Nodes::Callback;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );

Tachikoma->event_framework( Tachikoma::EventFrameworks::Select->new );

$ENV{TKSSL} = undef;

my $address = '127.0.0.1';
my $port    = '9197';
my $test    = "foo\n";
my $size    = length $test;
my $answer  = q();
my $total   = 10;
my $i       = $total;
my $taint   = undef;
{
    local $/ = undef;
    open my $fh, '<', '/dev/null';
    $taint = <$fh>;
    close $fh;
}
$test .= $taint;

my $router = Tachikoma::Nodes::Router->new;
is( ref $router, 'Tachikoma::Nodes::Router',
    'Tachikoma::Nodes::Router->new is ok' );

my $server = Tachikoma::Nodes::Socket->inet_server( $address, $port );
is( defined $server->{address}, 1, '$server->address is ok' );

$server->name('_server');
is( ref $server, 'Tachikoma::Nodes::Socket',
    'Tachikoma::Nodes::Socket->inet_server is ok' );

my $destination = Tachikoma::Nodes::Callback->new;
is( ref $destination,
    'Tachikoma::Nodes::Callback', 'Tachikoma::Nodes::Callback->new is ok' );

$server->sink($destination);
$destination->callback(
    sub {
        $answer .= $_[0]->[PAYLOAD];
        $router->stop
            if ( length($answer) >= $size * $total );
    }
);

my $client = Tachikoma::Nodes::Socket->inet_client( $address, $port );
is( defined $client->{address}, 1, '$client->address is ok' );

$client->name('_client');
is( ref $client, 'Tachikoma::Nodes::Socket',
    'Tachikoma::Nodes::Socket->inet_client is ok' );

for ( 1 .. $total ) {
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[PAYLOAD] = $test;
    $client->fill($message);
}
$router->drain;

is( $answer, $test x $total, 'Tachikoma::Nodes::Socket->fill is ok' );
is( $answer, $test x $total, 'Tachikoma::Nodes::Socket->drain is ok' );
