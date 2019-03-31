#!/usr/bin/perl
use strict;
use warnings;
require 'workstation/config.pl';

workstation_header();
workstation_benchmarks();
workstation_partitions();
workstation_services();
workstation_sound_effects();
workstation_hosts();
print "command tails add_tail /var/log/system.log local_system_log:ruleset\n";
workstation_footer();

fsync_source(
    path       => '<home>/Documents',
    pedantic   => 1,
    count      => 0,
    broadcasts => [],
);
fsync_destination(
    path    => '<home>/Documents',
    sources => [ 'misa' ],
    mode    => 'validate',
);

fsync_source(
    name       => 'fsync2',
    path       => '/Volumes/Data',
    count      => 4,
    pedantic   => 1,
    broadcasts => [],
);
fsync_destination(
    name    => 'fsync2',
    path    => '/Volumes/Data2',
    sources => [ 'localhost' ],
    count   => 4,
);

insecure();
