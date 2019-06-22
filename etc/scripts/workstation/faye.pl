#!/usr/bin/perl
use strict;
use warnings;
require './workstation/config.pl';

workstation_header();
workstation_benchmarks();
workstation_partitions();
workstation_services();
# workstation_sound_effects();
# workstation_hosts();
print "command tails add_tail /var/log/syslog local_system_log:ruleset\n";
workstation_footer();

fsync_source(
    path       => '<home>/Documents',
    pedantic   => 1,
    count      => 0,
    broadcasts => [],
    probe      => 0
);
fsync_destination(
    path    => '<home>/Documents',
    sources => [
        'misa.local',
        'nyx.local',
    ],
    mode => 'validate',
);

insecure();
