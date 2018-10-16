#!/usr/bin/env perl
# ----------------------------------------------------------------------
# tachikoma message tests
# ----------------------------------------------------------------------
#
# $Id$
#
use strict;
use warnings;
use Test::More tests => 8;

use Tachikoma::Config;
use Tachikoma::Crypto;

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

my @serializers = qw(
    Tachikoma::Command
    Tachikoma::Message
);

for my $class (@serializers) {
    test_construction($class);
}
