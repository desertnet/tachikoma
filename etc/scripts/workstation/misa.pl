#!/usr/bin/perl
use strict;
use warnings;
require 'workstation/config.pl';

workstation_header();
workstation_hosts();
print <<EOF;
command jobs start_job Tail local_system_log /var/log/system.log
connect_node local_system_log local_system_log:ruleset

EOF
workstation_benchmarks();
workstation_partitions();
workstation_services();
workstation_topic_top();
workstation_sound_effects();
workstation_http_server();
fsync_source(
    path       => '<home>/Documents',
    pedantic   => 1,
    count      => 0,
    broadcasts => [],
    no_probe   => 1
);
fsync_destination(
    path    => '<home>/Documents',
    sources => [ 'nyx' ],
    mode    => 'validate',
);

insecure();
