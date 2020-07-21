#!/usr/bin/perl
use strict;
use warnings;
require './workstation/config.pl';

workstation_header();
workstation_services();
workstation_footer();

insecure();
