#!/usr/bin/env perl -T
# ----------------------------------------------------------------------
# tachikoma node tests
# ----------------------------------------------------------------------
#
# $Id$
#
use strict;
use warnings;
use Test::More tests => 2420;
use Tachikoma;
use Tachikoma::Message qw( TM_ERROR );

my $taint = undef;
{
    local $/ = undef;
    open my $fh, '<', '/dev/null';
    $taint = <$fh>;
    close $fh;
}

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

my $router    = test_construction('Tachikoma::Nodes::Router');
my $responder = test_construction('Tachikoma::Nodes::Responder');
my $shell     = test_construction('Tachikoma::Nodes::Shell2');
my $trap      = test_construction('Tachikoma::Nodes::Callback');

$router->name('_router');
$responder->name('_responder');
$responder->shell($shell);

$Tachikoma::Now       = time;
$Tachikoma::Right_Now = time;

sub test_node {
    my $node      = shift;
    my $test_args = shift;
    my $class     = ref $node;
    my $name      = lc $class;
    $name =~ s{.*:}{};
    is( $node->name($name),       $name, "$class->name can be set" );
    is( $node->name,              $name, "$class->name is set correctly" );
    is( $Tachikoma::Nodes{$name}, $node, "$class->name is ok" );
    if ( defined $test_args ) {
        $test_args .= $taint;
        is( $node->arguments($test_args),
            $test_args, "$class->arguments can be set" );
        is( $node->arguments, $test_args,
            "$class->arguments are set correctly" );
        is( $node->sink($trap), $trap, "$class->sink can be set" );
        is( $node->sink,        $trap, "$class->sink is set correctly" );
        $trap->callback(
            sub {
                my $message = shift;
                is( $message->from, $class,
                    "$class->fill does not stamp errors" );
                is( $message->to, q(), "$class->fill does not route errors" );
                is( $message->payload, "NOT_AVAILABLE\n",
                    "$class->fill does not rewrite errors" );
                return;
            }
        );
        my $message = Tachikoma::Message->new;
        $message->type(TM_ERROR);
        $message->from($class);
        $message->payload( "NOT_AVAILABLE\n" . $taint );
        is( $node->fill($message), undef, "$class->fill returns undef" );
    }
    is( $node->remove_node, undef, "$class->remove_node returns undef" );
    while ( my $close_cb = shift @Tachikoma::Closing ) {
        &{$close_cb}();
    }
    is( $Tachikoma::Nodes{$name}, undef, "$class->remove_node is ok" );
    return;
}

my $t     = "/tmp/tachikoma.test.$$";
my %nodes = (
    'Tachikoma::Node'                         => undef,
    'Tachikoma::Nodes::Router'                => undef,
    'Tachikoma::Nodes::FileHandle'            => undef,
    'Tachikoma::Nodes::Socket'                => undef,
    'Tachikoma::Nodes::STDIO'                 => undef,
    'Tachikoma::Nodes::TTY'                   => undef,
    'Tachikoma::Nodes::AgeSieve'              => q(),
    'Tachikoma::Nodes::Atom'                  => q(/tmp /tmp),
    'Tachikoma::Nodes::Block'                 => q(),
    'Tachikoma::Nodes::Bucket'                => qq($t/bucket),
    'Tachikoma::Nodes::Buffer'                => qq($t/buffer.db),
    'Tachikoma::Nodes::Broker'                => q(localhost:5501),
    'Tachikoma::Nodes::BufferMonitor'         => q(),
    'Tachikoma::Nodes::BufferProbe'           => q(1),
    'Tachikoma::Nodes::BufferProbeToGraphite' => q(),
    'Tachikoma::Nodes::Callback'              => undef,
    'Tachikoma::Nodes::CGI'                   => undef,
    'Tachikoma::Nodes::CircuitTester'         => q(),
    'Tachikoma::Nodes::ClientConnector'       => q(),
    'Tachikoma::Nodes::CommandInterpreter'    => q(),
    'Tachikoma::Nodes::Consumer'              => q(--partition=foo),
    'Tachikoma::Nodes::ConsumerBroker'        => q(--topic=foo),
    'Tachikoma::Nodes::ConsumerGroup'         => q(),
    'Tachikoma::Nodes::Counter'               => q(),
    'Tachikoma::Nodes::Date'                  => q(),
    'Tachikoma::Nodes::Dumper'                => undef,
    'Tachikoma::Nodes::Echo'                  => q(),
    'Tachikoma::Nodes::Edge'                  => undef,
    'Tachikoma::Nodes::FileController'        => q(),
    'Tachikoma::Nodes::FileReceiver'          => q(),
    'Tachikoma::Nodes::FileSender'            => q(),
    'Tachikoma::Nodes::Function'              => q({ return 1; }),
    'Tachikoma::Nodes::Gate'                  => q(),
    'Tachikoma::Nodes::Grep'                  => q(),
    'Tachikoma::Nodes::Hopper'                => q(),
    'Tachikoma::Nodes::HTTP_Auth'             => undef,
    'Tachikoma::Nodes::HTTP_File'             => q(),
    'Tachikoma::Nodes::HTTP_Fetch'            => q(),
    'Tachikoma::Nodes::HTTP_Responder'        => q(),
    'Tachikoma::Nodes::HTTP_Route'            => q(),
    'Tachikoma::Nodes::HTTP_Store'            => q(),
    'Tachikoma::Nodes::HTTP_Timeout'          => q(),
    'Tachikoma::Nodes::Index'                 => q(),
    'Tachikoma::Nodes::IndexByField'          => q(),
    'Tachikoma::Nodes::IndexByStream'         => q(),
    'Tachikoma::Nodes::IndexByTimestamp'      => q(),
    'Tachikoma::Nodes::JobController'         => q(),
    'Tachikoma::Nodes::JobFarmer'             => q(0 Echo),
    'Tachikoma::Nodes::Join'                  => q(),
    'Tachikoma::Nodes::List'                  => q(),
    'Tachikoma::Nodes::LoadBalancer'          => q(),
    'Tachikoma::Nodes::LoadController'        => q(),
    'Tachikoma::Nodes::Log'                   => qq($t/log),
    'Tachikoma::Nodes::LogPrefix'             => q(),
    'Tachikoma::Nodes::Lookup'                => q(),
    'Tachikoma::Nodes::LWP'                   => q(),
    'Tachikoma::Nodes::MemorySieve'           => q(),
    'Tachikoma::Nodes::Null'                  => q(),
    'Tachikoma::Nodes::Number'                => q(),
    'Tachikoma::Nodes::Partition'             => qq(--filename=$t/partition),
    'Tachikoma::Nodes::PidWatcher'            => q(),
    'Tachikoma::Nodes::QueryEngine'           => q(),
    'Tachikoma::Nodes::Queue'                 => qq($t/queue.q),
    'Tachikoma::Nodes::RandomSieve'           => q(),
    'Tachikoma::Nodes::RateSieve'             => q(),
    'Tachikoma::Nodes::Reducer'               => q(),
    'Tachikoma::Nodes::RegexTee'              => q(),
    'Tachikoma::Nodes::Responder'             => q(),
    'Tachikoma::Nodes::Rewrite'               => q(),
    'Tachikoma::Nodes::Ruleset'               => q(),
    'Tachikoma::Nodes::Scheduler'             => qq($t/scheduler.db),
    'Tachikoma::Nodes::SetStream'             => q(),
    'Tachikoma::Nodes::SetType'               => q(),
    'Tachikoma::Nodes::Shutdown'              => q(),
    'Tachikoma::Nodes::Sieve'                 => q(),
    'Tachikoma::Nodes::Split'                 => q(),
    'Tachikoma::Nodes::StdErr'                => q(),
    'Tachikoma::Nodes::Substr'                => q(),
    'Tachikoma::Nodes::SudoFarmer'            => undef,
    'Tachikoma::Nodes::Table'                 => q(),
    'Tachikoma::Nodes::Tail'                  => q(/etc/hosts),
    'Tachikoma::Nodes::Tee'                   => q(),
    'Tachikoma::Nodes::TimedList'             => q(),
    'Tachikoma::Nodes::Timeout'               => q(),
    'Tachikoma::Nodes::Timer'                 => q(),
    'Tachikoma::Nodes::Timestamp'             => q(),
    'Tachikoma::Nodes::Topic'                 => q(),
    'Tachikoma::Nodes::TopicProbe'            => q(1),
    'Tachikoma::Nodes::Transform'             => q(- return 1;),
    'Tachikoma::Nodes::Uniq'                  => q(),
    'Tachikoma::Nodes::Watchdog'              => q(),
    'Accessories::Nodes::ByteSplit'           => q(),
    'Accessories::Nodes::HexDump'             => q(),
    'Accessories::Nodes::IndexByHostname'     => q(),
    'Accessories::Nodes::IndexByProcess'      => q(),
    'Accessories::Nodes::Panel'               => undef,
    'Accessories::Nodes::SFESerLCD'           => q(),
    'Accessories::Nodes::SilentDeFlapper'     => q(),
    'Accessories::Nodes::Smooth'              => q(),
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

for my $class ( sort keys %nodes ) {
    test_node( test_construction($class), $nodes{$class} );
}
for my $class ( sort keys %nodes ) {
    test_node( test_construction($class), $nodes{$class} );
}
local %ENV = ();
system '/bin/rm', '-rf', $t;
