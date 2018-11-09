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
print <<'EOF';
command jobs start_job Tail local_system_log /var/log/system.log
connect_node local_system_log local_system_log:ruleset

EOF
fsync_source(
    path       => '<home>/Documents',
    pedantic   => 1,
    count      => 0,
    broadcasts => [],
    probe      => 0
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
    probe      => 0
);
fsync_destination(
    name    => 'fsync2',
    path    => '/Volumes/Data2',
    sources => [ 'localhost' ],
    count   => 4,
);

insecure();
