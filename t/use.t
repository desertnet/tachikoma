#!/usr/bin/env perl
# ----------------------------------------------------------------------
# tachikoma tests
# ----------------------------------------------------------------------
#
# $Id$
#
use strict;
use warnings;
use Test::More tests => 1;

use Accessories::Forks;
use Accessories::Jobs::AfPlay;
use Accessories::Jobs::CozmoAlert;
use Accessories::Jobs::ExecFork;
use Accessories::Jobs::Lucky;
use Accessories::Jobs::Reactor;
use Accessories::Nodes::ByteSplit;
use Accessories::Nodes::HexDump;
use Accessories::Nodes::Panel;
#use Accessories::Nodes::SFE4DigitLED;
use Accessories::Nodes::SFESerLCD;
use Accessories::Nodes::SilentDeFlapper;
use Accessories::Nodes::Smooth;

use Tachikoma::Command;
use Tachikoma::Config qw( %Tachikoma );
# use Tachikoma::EventFrameworks::Epoll;
# use Tachikoma::EventFrameworks::KQueue;
use Tachikoma::EventFrameworks::Select;

use Tachikoma::Job;
use Tachikoma::Jobs::CGI;
use Tachikoma::Jobs::CommandInterpreter;
use Tachikoma::Jobs::Delay;
use Tachikoma::Jobs::DNS;
use Tachikoma::Jobs::Echo;
use Tachikoma::Jobs::FileReceiver;
use Tachikoma::Jobs::FileRemover;
use Tachikoma::Jobs::FileSender;
use Tachikoma::Jobs::Fortune;
use Tachikoma::Jobs::Inet_AtoN;
use Tachikoma::Jobs::Log;
use Tachikoma::Jobs::LWP;
use Tachikoma::Jobs::Shell;
use Tachikoma::Jobs::SQL;
use Tachikoma::Jobs::Tail;
use Tachikoma::Jobs::TailFork;
use Tachikoma::Jobs::TailForks;
use Tachikoma::Jobs::Transform;

use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD

    TM_BYTESTREAM TM_EOF TM_PING
    TM_COMMAND TM_RESPONSE TM_ERROR
    TM_INFO TM_PERSIST TM_STORABLE
    TM_COMPLETION TM_BATCH TM_KILLME
    TM_NOREPLY TM_HEARTBEAT
);

use Tachikoma::Node;
use Tachikoma::Nodes::Router;
use Tachikoma::Nodes::FileHandle qw( TK_R TK_W TK_SYNC setsockopts );
use Tachikoma::Nodes::Socket     qw( TK_R TK_W TK_SYNC setsockopts );
use Tachikoma::Nodes::STDIO      qw( TK_R TK_W TK_SYNC setsockopts );
use Tachikoma::Nodes::TTY;
use Tachikoma::Nodes::AgeSieve;
use Tachikoma::Nodes::Atom;
use Tachikoma::Nodes::Block;
use Tachikoma::Nodes::Broker;
use Tachikoma::Nodes::Bucket;
use Tachikoma::Nodes::Buffer;
use Tachikoma::Nodes::BufferDumper;
use Tachikoma::Nodes::BufferMonitor;
use Tachikoma::Nodes::BufferProbe;
use Tachikoma::Nodes::BufferProbeToGraphite;
use Tachikoma::Nodes::BufferTop;
use Tachikoma::Nodes::Callback;
use Tachikoma::Nodes::CGI;
use Tachikoma::Nodes::CircuitTester;
use Tachikoma::Nodes::ClientConnector;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Nodes::Consumer;
use Tachikoma::Nodes::ConsumerBroker;
use Tachikoma::Nodes::ConsumerGroup;
use Tachikoma::Nodes::Counter;
use Tachikoma::Nodes::Date;
use Tachikoma::Nodes::Dumper;
use Tachikoma::Nodes::Echo;
use Tachikoma::Nodes::Edge;
use Tachikoma::Nodes::FileController;
use Tachikoma::Nodes::Function;
use Tachikoma::Nodes::Gate;
use Tachikoma::Nodes::Grep;
use Tachikoma::Nodes::Hopper;
use Tachikoma::Nodes::HTTP_Auth;
use Tachikoma::Nodes::HTTP_File;
use Tachikoma::Nodes::HTTP_Responder qw( log_entry );
use Tachikoma::Nodes::HTTP_Route;
use Tachikoma::Nodes::HTTP_Timeout;
use Tachikoma::Nodes::Index;
use Tachikoma::Nodes::IndexByField;
use Tachikoma::Nodes::IndexByStream;
use Tachikoma::Nodes::IndexByTimestamp;
use Tachikoma::Nodes::JobController;
use Tachikoma::Nodes::JobFarmer;
use Tachikoma::Nodes::Join;
# use Tachikoma::Nodes::JSONtoStorable;
use Tachikoma::Nodes::List;
use Tachikoma::Nodes::LoadBalancer;
use Tachikoma::Nodes::LoadController;
use Tachikoma::Nodes::Log;
use Tachikoma::Nodes::LogPrefix;
use Tachikoma::Nodes::Lookup;
use Tachikoma::Nodes::LWP;
use Tachikoma::Nodes::MemorySieve;
# use Tachikoma::Nodes::Mon;
use Tachikoma::Nodes::Null;
use Tachikoma::Nodes::Number;
use Tachikoma::Nodes::Partition;
use Tachikoma::Nodes::PidWatcher;
use Tachikoma::Nodes::QueryEngine;
use Tachikoma::Nodes::Queue;
use Tachikoma::Nodes::RandomSieve;
use Tachikoma::Nodes::RateSieve;
use Tachikoma::Nodes::Reducer;
use Tachikoma::Nodes::RegexTee;
use Tachikoma::Nodes::Responder;
use Tachikoma::Nodes::RestoreMon;
use Tachikoma::Nodes::Rewrite;
use Tachikoma::Nodes::Ruleset;
use Tachikoma::Nodes::Scheduler;
use Tachikoma::Nodes::SetStream;
use Tachikoma::Nodes::SetType;
use Tachikoma::Nodes::Shell;
use Tachikoma::Nodes::Shell2;
use Tachikoma::Nodes::Shutdown;
use Tachikoma::Nodes::Sieve;
use Tachikoma::Nodes::Split;
# use Tachikoma::Nodes::SQL;
use Tachikoma::Nodes::StdErr;
use Tachikoma::Nodes::Substr;
# use Tachikoma::Nodes::StorableToJSON;
use Tachikoma::Nodes::Table;
use Tachikoma::Nodes::Tail;
use Tachikoma::Nodes::Tee;
use Tachikoma::Nodes::TimedList;
use Tachikoma::Nodes::Timeout;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::Timestamp;
use Tachikoma::Nodes::Topic;
use Tachikoma::Nodes::TopicTop;
use Tachikoma::Nodes::Transform;
use Tachikoma::Nodes::Uniq;
use Tachikoma::Nodes::Watchdog;
# use Tachikoma::Nodes::Watcher;
use Tachikoma;

is('ok', 'ok', 'is ok');
