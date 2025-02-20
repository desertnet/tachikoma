#!/usr/bin/env perl -T
# ----------------------------------------------------------------------
# tachikoma aggregate tests
# ----------------------------------------------------------------------
#

use strict;
use warnings;
use Test::More tests => 11;
use Tachikoma::Message
    qw( TYPE TIMESTAMP STREAM PAYLOAD TM_STORABLE TM_BYTESTREAM );
use Tachikoma::Nodes::Callback;

# Load module under test
use_ok('Tachikoma::Nodes::Aggregate');

# Constructor test
my $node = new_ok('Tachikoma::Nodes::Aggregate');
is( $node->num_partitions, 1, 'Default num_partitions is 1' );

# Setup callback sink
my $output;
my $callback = Tachikoma::Nodes::Callback->new( sub { $output = shift } );
$node->sink($callback);

# Single partition test - should store but not trigger aggregation
$node->arguments('--num_partitions=2');    # Set this BEFORE first message
my $msg = Tachikoma::Message->new;
$msg->[TYPE]      = TM_STORABLE;
$msg->[TIMESTAMP] = 123456;
$msg->[STREAM]    = 1;
$msg->[PAYLOAD]   = { 'key1' => 10 };
$node->fill($msg);
is_deeply(
    $node->partitions->{1},
    { 'key1' => 10 },
    'Single partition stored'
);

# Second partition triggers aggregation
$msg->[STREAM]  = 2;
$msg->[PAYLOAD] = { 'key1' => 20 };
$node->fill($msg);
is_deeply(
    $output->[PAYLOAD],
    { 'key1' => 30 },
    'Aggregates multiple partitions'
);
is_deeply( $node->partitions, {}, 'Partitions cleared after aggregation' );

# Timestamp change test
$msg->[TIMESTAMP] = 123457;
$node->fill($msg);
is( $node->last_timestamp, 123457, 'Updates timestamp' );
is( scalar( keys %{ $node->partitions } ),
    1, 'Resets partitions on timestamp change' );

# Error condition tests
my $bad_msg = Tachikoma::Message->new;
$bad_msg->[TYPE] = TM_BYTESTREAM;
$node->fill($bad_msg);
is( scalar( keys %{ $node->partitions } ),
    1, 'Ignores non-storable messages' );

# Empty payload test
$msg->[PAYLOAD] = {};
$node->fill($msg);
is_deeply( $node->partitions->{2}, {}, 'Handles empty payload' );

# Invalid num_partitions test
eval { $node->arguments('--num_partitions=0') };
like( $@, qr/must be greater than or equal to 1/,
    'Validates num_partitions' );
