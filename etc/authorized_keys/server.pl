#!/usr/bin/perl
use strict;
use warnings;
our %server_keys;
our %workstation_keys;
require './config.pl';

add_keys(\%server_keys);
add_keys(\%workstation_keys);
authorize($_, qw(              client server )) for (keys %server_keys);
authorize($_, qw( meta command client        )) for (keys %workstation_keys);
generate_authorized_keys();
