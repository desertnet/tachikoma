#!/usr/bin/perl
use strict;
use warnings;
require 'workstation/config.pl';

workstation_header();
workstation_benchmarks();
workstation_partitions();
workstation_services();
workstation_sound_effects();

insecure();
