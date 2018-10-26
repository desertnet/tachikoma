#!/usr/bin/env perl -T
# ----------------------------------------------------------------------
# tachikoma message tests
# ----------------------------------------------------------------------
#
# $Id$
#
use strict;
use warnings;
use Test::More tests => 28;

sub test_construction {
    my $class = shift;
    eval "use $class; return 1;" or die $@;
    is( 'ok', 'ok', "$class can be used" );
    my $node = $class->new;
    is( ref $node, $class, "$class->new is ok" );
    return $node;
}

my $class = 'Tachikoma';
test_construction($class);
$class->event_framework(
    test_construction('Tachikoma::EventFrameworks::Select') );

# test_construction('Tachikoma::EventFrameworks::Epoll');
# test_construction('Tachikoma::EventFrameworks::KQueue');

$class = 'Tachikoma::Message';
my $message = test_construction($class);
my $type    = 1;
my $time    = time;
is( $message->type($type),      $type,  "$class->type can be set" );
is( $message->type,             $type,  "$class->type is set correctly" );
is( $message->from('foo'),      'foo',  "$class->from can be set" );
is( $message->from,             'foo',  "$class->from is set correctly" );
is( $message->to('bar'),        'bar',  "$class->to can be set" );
is( $message->to,               'bar',  "$class->to is set correctly" );
is( $message->id('1:23'),       '1:23', "$class->id can be set" );
is( $message->id,               '1:23', "$class->id is set correctly" );
is( $message->stream('baz'),    'baz',  "$class->stream can be set" );
is( $message->stream,           'baz',  "$class->stream is set correctly" );
is( $message->timestamp($time), $time,  "$class->timestamp can be set" );
is( $message->timestamp,       $time,  "$class->timestamp is set correctly" );
is( $message->payload('test'), 'test', "$class->payload can be set" );
is( $message->payload,         'test', "$class->payload is set correctly" );

$class = 'Tachikoma::Command';
my $command = test_construction($class);

is( $command->name('ls'),      'ls',   "$class->name can be set" );
is( $command->name,            'ls',   "$class->name is set correctly" );
is( $command->arguments('-l'), '-l',   "$class->arguments can be set" );
is( $command->arguments,       '-l',   "$class->arguments is set correctly" );
is( $command->payload('test'), 'test', "$class->payload can be set" );
is( $command->payload,         'test', "$class->payload is set correctly" );
