#!/usr/bin/env perl
# ----------------------------------------------------------------------
# tachikoma node tests
# ----------------------------------------------------------------------
#
# $Id$
#
use strict;
use warnings;
use Test::More tests => 699;
use Tachikoma;

sub test_construction {
    my $class = shift;
    eval "use $class; return 1;" or die $@;
    is( 'ok', 'ok', "$class can be used" );
    my $node = $class->new;
    is( ref $node, $class, "$class->new is ok" );
    return $node;
}

sub test_node {
    my $node  = shift;
    my $class = ref $node;
    my $name  = lc $class;
    $name =~ s{.*:}{};
    is( $node->name($name),       $name, "$class->name can be set" );
    is( $node->name,              $name, "$class->name is set correctly" );
    is( $Tachikoma::Nodes{$name}, $node, "$class->name is ok" );
    is( $node->remove_node, undef, "$class->remove_node returns undef" );

    while ( my $close_cb = shift @Tachikoma::Closing ) {
        &{$close_cb}();
    }
    is( $Tachikoma::Nodes{$name}, undef, "$class->remove_node is ok" );
    return;
}

my $class = 'Tachikoma';
test_construction($class);
$class->event_framework(
    test_construction('Tachikoma::EventFrameworks::Select') );
my $router = test_construction('Tachikoma::Nodes::Router');
$router->name('_router');

my @nodes = qw(
    Tachikoma::Node
    Tachikoma::Nodes::Router
    Tachikoma::Nodes::FileHandle
    Tachikoma::Nodes::Socket
    Tachikoma::Nodes::STDIO
    Tachikoma::Nodes::TTY
    Tachikoma::Nodes::AgeSieve
    Tachikoma::Nodes::Atom
    Tachikoma::Nodes::Block
    Tachikoma::Nodes::Bucket
    Tachikoma::Nodes::Buffer
    Tachikoma::Nodes::Broker
    Tachikoma::Nodes::BufferMonitor
    Tachikoma::Nodes::BufferProbe
    Tachikoma::Nodes::BufferProbeToGraphite
    Tachikoma::Nodes::Callback
    Tachikoma::Nodes::CGI
    Tachikoma::Nodes::CircuitTester
    Tachikoma::Nodes::ClientConnector
    Tachikoma::Nodes::CommandInterpreter
    Tachikoma::Nodes::Consumer
    Tachikoma::Nodes::ConsumerBroker
    Tachikoma::Nodes::ConsumerGroup
    Tachikoma::Nodes::Counter
    Tachikoma::Nodes::Date
    Tachikoma::Nodes::Dumper
    Tachikoma::Nodes::Echo
    Tachikoma::Nodes::Edge
    Tachikoma::Nodes::FileController
    Tachikoma::Nodes::FileReceiver
    Tachikoma::Nodes::FileSender
    Tachikoma::Nodes::Function
    Tachikoma::Nodes::Gate
    Tachikoma::Nodes::Grep
    Tachikoma::Nodes::Hopper
    Tachikoma::Nodes::HTTP_Auth
    Tachikoma::Nodes::HTTP_File
    Tachikoma::Nodes::HTTP_Fetch
    Tachikoma::Nodes::HTTP_Responder
    Tachikoma::Nodes::HTTP_Route
    Tachikoma::Nodes::HTTP_Store
    Tachikoma::Nodes::HTTP_Timeout
    Tachikoma::Nodes::Index
    Tachikoma::Nodes::IndexByField
    Tachikoma::Nodes::IndexByStream
    Tachikoma::Nodes::IndexByTimestamp
    Tachikoma::Nodes::JobController
    Tachikoma::Nodes::JobFarmer
    Tachikoma::Nodes::Join
    Tachikoma::Nodes::List
    Tachikoma::Nodes::LoadBalancer
    Tachikoma::Nodes::LoadController
    Tachikoma::Nodes::Log
    Tachikoma::Nodes::LogPrefix
    Tachikoma::Nodes::Lookup
    Tachikoma::Nodes::LWP
    Tachikoma::Nodes::MemorySieve
    Tachikoma::Nodes::Null
    Tachikoma::Nodes::Number
    Tachikoma::Nodes::Partition
    Tachikoma::Nodes::PidWatcher
    Tachikoma::Nodes::QueryEngine
    Tachikoma::Nodes::Queue
    Tachikoma::Nodes::RandomSieve
    Tachikoma::Nodes::RateSieve
    Tachikoma::Nodes::Reducer
    Tachikoma::Nodes::RegexTee
    Tachikoma::Nodes::Responder
    Tachikoma::Nodes::Rewrite
    Tachikoma::Nodes::Ruleset
    Tachikoma::Nodes::Scheduler
    Tachikoma::Nodes::SetStream
    Tachikoma::Nodes::SetType
    Tachikoma::Nodes::Shutdown
    Tachikoma::Nodes::Sieve
    Tachikoma::Nodes::Split
    Tachikoma::Nodes::StdErr
    Tachikoma::Nodes::Substr
    Tachikoma::Nodes::SudoFarmer
    Tachikoma::Nodes::Table
    Tachikoma::Nodes::Tail
    Tachikoma::Nodes::Tee
    Tachikoma::Nodes::TimedList
    Tachikoma::Nodes::Timeout
    Tachikoma::Nodes::Timer
    Tachikoma::Nodes::Timestamp
    Tachikoma::Nodes::Topic
    Tachikoma::Nodes::TopicProbe
    Tachikoma::Nodes::Transform
    Tachikoma::Nodes::Uniq
    Tachikoma::Nodes::Watchdog
    Accessories::Nodes::ByteSplit
    Accessories::Nodes::HexDump
    Accessories::Nodes::IndexByHostname
    Accessories::Nodes::IndexByProcess
    Accessories::Nodes::Panel
    Accessories::Nodes::SFESerLCD
    Accessories::Nodes::SilentDeFlapper
    Accessories::Nodes::Smooth
);
    # Tachikoma::Nodes::BufferTop
    # Tachikoma::Nodes::JSONtoStorable
    # Tachikoma::Nodes::SerialPort
    # Tachikoma::Nodes::Shell
    # Tachikoma::Nodes::Shell2
    # Tachikoma::Nodes::SQL
    # Tachikoma::Nodes::StorableToJSON
    # Tachikoma::Nodes::TopicTop
    # Accessories::Nodes::SFE4DigitLED

for my $class (@nodes) {
    test_node( test_construction($class) );
}
