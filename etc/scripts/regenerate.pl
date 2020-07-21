#!/usr/bin/perl
use strict;
use warnings;
chdir('etc');
chdir('scripts');
system("./workstation/default.pl > workstation/default.tsl");
