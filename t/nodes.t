#!/usr/bin/env perl -T
# ----------------------------------------------------------------------
# tachikoma node tests
# ----------------------------------------------------------------------
#

use strict;
use warnings;
use Test::More tests => 3482;
use Tachikoma;
use Tachikoma::Message qw( TM_ERROR TM_EOF );

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

my $t     = "/tmp/tachikoma.test.$$";
my %nodes = (
    'Tachikoma::Node'                         => undef,
    'Tachikoma::Nodes::Router'                => undef,
    'Tachikoma::Nodes::FileHandle'            => undef,
    'Tachikoma::Nodes::Socket'                => undef,
    'Tachikoma::Nodes::STDIO'                 => undef,
    'Tachikoma::Nodes::TTY'                   => undef,
    'Tachikoma::Nodes::AgeSieve'              => q(),
    'Tachikoma::Nodes::Aggregate'             => q(),
    'Tachikoma::Nodes::AltKV'                 => q(),
    'Tachikoma::Nodes::Atom'                  => q(/tmp /tmp),
    'Tachikoma::Nodes::Buffer'                => qq($t/buffer.db),
    'Tachikoma::Nodes::Broker'                => q(localhost:5501),
    'Tachikoma::Nodes::BufferMonitor'         => q(),
    'Tachikoma::Nodes::BufferProbe'           => q(1),
    'Tachikoma::Nodes::BufferProbeToGraphite' => q(),
    'Tachikoma::Nodes::BytestreamToStorable'  => q(),
    'Tachikoma::Nodes::Callback'              => undef,
    'Tachikoma::Nodes::CGI'                   => undef,
    'Tachikoma::Nodes::CircuitTester'         => q(),
    'Tachikoma::Nodes::ClientConnector'       => q(clientconnector),
    'Tachikoma::Nodes::CommandInterpreter'    => q(),
    'Tachikoma::Nodes::Consumer'              => q(--partition=foo),
    'Tachikoma::Nodes::ConsumerBroker'        => q(--topic=foo),
    'Tachikoma::Nodes::ConsumerGroup'         => q(),
    'Tachikoma::Nodes::DirWatcher'            => q(),
    'Tachikoma::Nodes::Dumper'                => undef,
    'Tachikoma::Nodes::Echo'                  => q(),
    'Tachikoma::Nodes::EmailAlert'            => q(),
    'Tachikoma::Nodes::FileController'        => q(),
    'Tachikoma::Nodes::FileReceiver'          => q(),
    'Tachikoma::Nodes::FileSender'            => q(),
    'Tachikoma::Nodes::FileWatcher'           => q(),
    'Tachikoma::Nodes::Function'         => q({ return <message.payload>; }),
    'Tachikoma::Nodes::Gate'             => q(),
    'Tachikoma::Nodes::Grep'             => q(.),
    'Tachikoma::Nodes::Hopper'           => q(),
    'Tachikoma::Nodes::HTTP_Auth'        => undef,
    'Tachikoma::Nodes::HTTP_Command'     => q(),
    'Tachikoma::Nodes::HTTP_File'        => q(),
    'Tachikoma::Nodes::HTTP_Fetch'       => q(/ .*:table),
    'Tachikoma::Nodes::HTTP_Responder'   => q(),
    'Tachikoma::Nodes::HTTP_Route'       => q(),
    'Tachikoma::Nodes::HTTP_Store'       => q(),
    'Tachikoma::Nodes::HTTP_Timeout'     => q(),
    'Tachikoma::Nodes::HTTP_Trigger'     => q(),
    'Tachikoma::Nodes::Index'            => q(),
    'Tachikoma::Nodes::IndexByField'     => q(),
    'Tachikoma::Nodes::IndexByStream'    => q(),
    'Tachikoma::Nodes::IndexByTimestamp' => q(),
    'Tachikoma::Nodes::JobController'    => q(),
    'Tachikoma::Nodes::JobFarmer'        => q(0 Echo),
    'Tachikoma::Nodes::Join'             => q(),
    'Tachikoma::Nodes::JSONvisualizer'   => q(),
    'Tachikoma::Nodes::List'             => q(),
    'Tachikoma::Nodes::LoadBalancer'     => q(),
    'Tachikoma::Nodes::LoadController'   => q(),
    'Tachikoma::Nodes::Log'              => qq($t/log),
    'Tachikoma::Nodes::LogPrefix'        => q(),
    'Tachikoma::Nodes::Lookup'           => q(foo:table),
    'Tachikoma::Nodes::LWP'              => q(),
    'Tachikoma::Nodes::MemorySieve'      => q(),
    'Tachikoma::Nodes::Null'             => q(),
    'Tachikoma::Nodes::Partition'        => qq(--filename=$t/partition),
    'Tachikoma::Nodes::PayloadTimeout'   => qq(),
    'Tachikoma::Nodes::PidWatcher'       => q(),
    'Tachikoma::Nodes::QueryEngine'      => q(),
    'Tachikoma::Nodes::Queue'            => qq($t/queue.q),
    'Tachikoma::Nodes::RandomSieve'      => q(),
    'Tachikoma::Nodes::RateSieve'        => q(),
    'Tachikoma::Nodes::Reducer'          => q(),
    'Tachikoma::Nodes::RegexTee'         => q(),
    'Tachikoma::Nodes::Responder'        => q(),
    'Tachikoma::Nodes::Rewrite'          => q((.*) $1),
    'Tachikoma::Nodes::Ruleset'          => q(),
    'Tachikoma::Nodes::Scheduler'        => qq($t/scheduler.db),
    'Tachikoma::Nodes::SetStream'        => q(),
    'Tachikoma::Nodes::SetType'          => q(),
    'Tachikoma::Nodes::Shutdown'         => q(),
    'Tachikoma::Nodes::Sieve'            => q(),
    'Tachikoma::Nodes::Split'            => q(),
    'Tachikoma::Nodes::StdErr'           => q(),
    'Tachikoma::Nodes::StorableToBytestream' => q(),
    'Tachikoma::Nodes::Substr'               => q((.*)),
    'Tachikoma::Nodes::SudoFarmer'           => undef,
    'Tachikoma::Nodes::Sum'                  => q(),
    'Tachikoma::Nodes::Table'                => q(),
    'Tachikoma::Nodes::Tail'                 => q(/etc/hosts),
    'Tachikoma::Nodes::TailProbe'            => q(),
    'Tachikoma::Nodes::Tee'                  => q(),
    'Tachikoma::Nodes::TimedList'            => q(),
    'Tachikoma::Nodes::Timeout'              => q(),
    'Tachikoma::Nodes::Timer'                => q(),
    'Tachikoma::Nodes::Timestamp'            => q(),
    'Tachikoma::Nodes::Topic'                => q(broker),
    'Tachikoma::Nodes::TopicProbe'           => q(1),
    'Tachikoma::Nodes::Watchdog'             => q(),
    'Accessories::Nodes::Bucket'             => qq($t/bucket),
    'Accessories::Nodes::Clock'              => q(),
    'Accessories::Nodes::HexDump'            => q(),
    'Accessories::Nodes::IndexByHostname'    => q(),
    'Accessories::Nodes::IndexByProcess'     => q(),
    'Accessories::Nodes::LogColor'           => q(),
    'Accessories::Nodes::SFE4DigitLED'       => q(),
    'Accessories::Nodes::SFESerLCD'          => q(),
    'Accessories::Nodes::Spool'              => qq($t/spool),
    'Accessories::Nodes::TestStream'         => qq(),
    'Accessories::Nodes::Transform'          => qq(- 1),
    'Accessories::Nodes::Uniq'               => q(),
);

my %skip_owner_test = (
    'Tachikoma::Nodes::CommandInterpreter' => 1,
    'Tachikoma::Nodes::JobController'      => 1,
    'Tachikoma::Nodes::HTTP_Route'         => 1,
    'Tachikoma::Nodes::LoadBalancer'       => 1,
    'Tachikoma::Nodes::Queue'              => 1,
    'Tachikoma::Nodes::RegexTee'           => 1,
    'Tachikoma::Nodes::Tee'                => 1,
);

my %skip_error_test = ( 'Tachikoma::Nodes::MemorySieve' => 1, );

my %skip_eof_test = (
    'Tachikoma::Nodes::FileReceiver' => 1,
    'Tachikoma::Nodes::MemorySieve'  => 1,
);

my %skip_all_tests = (
    'Tachikoma::Nodes::BufferTop'      => 1,
    'Tachikoma::Nodes::JSONtoStorable' => 1,
    'Tachikoma::Nodes::SerialPort'     => 1,
    'Tachikoma::Nodes::Shell'          => 1,
    'Tachikoma::Nodes::Shell2'         => 1,
    'Tachikoma::Nodes::SQL'            => 1,
    'Tachikoma::Nodes::StorableToJSON' => 1,
    'Tachikoma::Nodes::TailTop'        => 1,
    'Tachikoma::Nodes::TopicTop'       => 1,
);

my $router    = test_construction('Tachikoma::Nodes::Router');
my $responder = test_construction('Tachikoma::Nodes::Responder');
my $shell     = test_construction('Tachikoma::Nodes::Shell2');
my $trap      = test_construction('Tachikoma::Nodes::Callback');

$router->name('_router');
$responder->name('_responder');
$responder->shell($shell);

my $taint = undef;
{
    local $/ = undef;
    open my $fh, '<', '/dev/null' or die "couldn't open /dev/null: $!";
    $taint = <$fh>;
    close $fh or die "couldn't close /dev/null: $!";
}

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
        if ( not $skip_owner_test{$class} ) {
            is( $node->owner('foo'), 'foo', "$class->owner can be set" );
            is( $node->owner, 'foo', "$class->owner is set correctly" );
        }
        test_fire( $class, $node );
        test_fill_error( $class, $node ) if ( not $skip_error_test{$class} );
        test_fill_eof( $class, $node )   if ( not $skip_eof_test{$class} );
    }
    is( $node->remove_node, undef, "$class->remove_node returns undef" );
    while ( my $close_cb = shift @Tachikoma::Closing ) {
        &{$close_cb}();
    }
    is( $Tachikoma::Nodes{$name}, undef, "$class->remove_node is ok" );
    return;
}

sub test_fire {
    my $class = shift;
    my $node  = shift;
    if ( $node->can('fire') ) {
        $trap->callback( sub {return} );
        $Tachikoma::Now       = time;
        $Tachikoma::Right_Now = time;
        is( $node->fire, undef, "$class->fire returns undef" );
    }
    return;
}

sub test_fill_error {
    my $class = shift;
    my $node  = shift;
    $trap->callback(
        sub {
            my $message = shift;
            is( $message->from, $class,
                "$class->fill does not stamp TM_ERROR" );
            my %valid = ( $class => 1, 'foo' => 1, q() => 1 );
            my $okay  = $valid{ $message->to };
            is( $okay, 1, "$class->fill routes TM_ERROR correctly" );
            is( $message->payload, "NOT_AVAILABLE\n",
                "$class->fill does not rewrite TM_ERROR" );
            return;
        }
    );
    my $message = Tachikoma::Message->new;
    $message->type(TM_ERROR);
    $message->from($class);
    $message->payload( "NOT_AVAILABLE\n" . $taint );
    is( $node->fill($message), undef, "$class->fill TM_ERROR returns undef" );
    return;
}

sub test_fill_eof {
    my $class = shift;
    my $node  = shift;
    $trap->callback(
        sub {
            my $message = shift;
            is( $message->from, $class,
                "$class->fill does not stamp TM_EOF" );
            my %valid = ( $class => 1, 'foo' => 1, q() => 1 );
            my $okay  = $valid{ $message->to };
            is( $okay, 1, "$class->fill routes TM_EOF correctly" );
            is( $message->payload, q(),
                "$class->fill does not rewrite TM_EOF" );
            return;
        }
    );
    my $message = Tachikoma::Message->new;
    $message->type(TM_EOF);
    $message->from($class);
    $message->payload($taint);
    is( $node->fill($message), undef, "$class->fill TM_EOF returns undef" );
    return;
}

for my $class ( sort keys %nodes ) {
    test_node( test_construction($class), $nodes{$class} );
}
for my $class ( sort keys %nodes ) {
    test_node( test_construction($class), $nodes{$class} );
}
local %ENV = ();
system '/bin/rm', '-rf', $t;
