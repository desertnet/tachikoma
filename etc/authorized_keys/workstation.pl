#!/usr/bin/perl
use strict;
use warnings;
our %server_keys;
require 'config.pl';

add_keys(\%server_keys);
authorize($_, qw( server )) for (keys %server_keys);
generate_authorized_keys();
