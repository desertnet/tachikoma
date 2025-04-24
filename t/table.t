#!/usr/bin/perl
use strict;
use warnings;
use Test::More tests => 5;    # Number of subtests
use Tachikoma::Message qw( 
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD 
    TM_BYTESTREAM TM_STORABLE TM_ERROR TM_EOF 
);

my $node;

sub setup {
    $Tachikoma::Now = 123456;
    $node = Tachikoma::Nodes::Table->new;
    $node->arguments("--num_partitions=2 --window_size=300 --num_buckets=3");
    $node->fire();
}

sub teardown {
    $node = undef;
}

sub store_value {
    my ($key, $value, $time_offset) = @_;
    $Tachikoma::Now += $time_offset if $time_offset;
    my $msg = Tachikoma::Message->new;
    $msg->[TYPE] = TM_BYTESTREAM;
    $msg->[TIMESTAMP] = $Tachikoma::Now;
    $msg->[STREAM] = $key;
    $msg->[PAYLOAD] = $value;
    $node->fill($msg);
    $node->fire();
}

# Load module
use_ok('Tachikoma::Nodes::Table');

# Basic functionality
subtest 'Basic functionality' => sub {
    plan tests => 3;
    setup();
    
    # Constructor test
    isa_ok($node, 'Tachikoma::Nodes::Table');
    
    # Store and retrieve
    store_value("key1", "value1");
    is($node->lookup("key1"), "value1", "Stores and retrieves value");
    
    # Empty key returns key list
    my $keys = $node->lookup("");
    ok(ref $keys eq 'ARRAY', "Empty key returns array reference");
};

# Window management
subtest 'Window management' => sub {
    plan tests => 3;
    
    # Value exists in current window
    store_value("key2", "value2");
    is($node->lookup("key2"), "value2", "Value in current window");
    
    # Value expires after window
    $Tachikoma::Now += 1000;
    $node->fire();
    is($node->lookup("key2"), undef, "Value expired after window");
    
    # New value in new window
    store_value("key3", "value3");
    is($node->lookup("key3"), "value3", "New value in new window");
};

# Partition distribution
subtest 'Partition handling' => sub {
    plan tests => 2;
    
    # Values distributed across partitions
    store_value("key4", "value4");
    store_value("key5", "value5");
    my $p1 = $node->get_partition_id("key4");
    my $p2 = $node->get_partition_id("key5");
    ok($p1 >= 0 && $p1 < 2, "Partition 1 in range");
    ok($p2 >= 0 && $p2 < 2, "Partition 2 in range");
};

# Bucket rotation
subtest 'Bucket rotation' => sub {
    plan tests => 3;
    
    # Fill multiple buckets
    store_value("key6", "value6");
    store_value("key7", "value7", 100);
    store_value("key8", "value8", 100);
    
    is($node->lookup("key6"), "value6", "First bucket value");
    is($node->lookup("key7"), "value7", "Second bucket value");
    is($node->lookup("key8"), "value8", "Third bucket value");
};

teardown();