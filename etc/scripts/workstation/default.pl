#!/usr/bin/perl
use strict;
use warnings;
require 'workstation/config.pl';

workstation_header();
workstation_benchmarks();
workstation_partitions();
workstation_services();
workstation_topic_top();
workstation_sound_effects();
workstation_http_server();

insecure();
