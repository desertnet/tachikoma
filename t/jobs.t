#!/usr/bin/env perl -T
# ----------------------------------------------------------------------
# tachikoma job tests
# ----------------------------------------------------------------------
#
# $Id$
#
use strict;
use warnings;
use Test::More tests => 56;

sub test_construction {
    my $class = shift;
    eval "use $class; return 1;" or die $@;
    is( 'ok', 'ok', "$class can be used" );
    my $node = $class->new;
    is( ref $node, $class, "$class->new is ok" );
    return $node;
}

my $tachikoma = 'Tachikoma';
test_construction($tachikoma);
$tachikoma->event_framework(
    test_construction('Tachikoma::EventFrameworks::Select') );

my @jobs = qw(
    Tachikoma::Job
    Tachikoma::Jobs::BShell
    Tachikoma::Jobs::CGI
    Tachikoma::Jobs::CommandInterpreter
    Tachikoma::Jobs::DirCheck
    Tachikoma::Jobs::DirStats
    Tachikoma::Jobs::Echo
    Tachikoma::Jobs::FileReceiver
    Tachikoma::Jobs::FileRemover
    Tachikoma::Jobs::FileSender
    Tachikoma::Jobs::Inet_AtoN
    Tachikoma::Jobs::Log
    Tachikoma::Jobs::LWP
    Tachikoma::Jobs::Shell
    Tachikoma::Jobs::SQL
    Tachikoma::Jobs::Tail
    Tachikoma::Jobs::TailFork
    Tachikoma::Jobs::TailForks
    Tachikoma::Jobs::Task
    Accessories::Jobs::APlay
    Accessories::Jobs::Delay
    Accessories::Jobs::DNS
    Accessories::Jobs::ExecFork
    Accessories::Jobs::Fortune
    Accessories::Jobs::Lucky
    Accessories::Jobs::Reactor
);

for my $class (@jobs) {
    test_construction($class);
}
