#!/usr/bin/perl
use strict;
use warnings;
our %workstation_keys;
require 'config.pl';

add_keys(\%workstation_keys);
authorize($_, qw( meta command client )) for (keys %workstation_keys);
generate_authorized_keys();
