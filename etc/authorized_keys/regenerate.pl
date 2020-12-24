#!/usr/bin/perl
use strict;
use warnings;
chdir('etc');
chdir('authorized_keys');

system("./default.pl     > default.keys");
system("./server.pl      > server.keys");
system("./workstation.pl > workstation.keys");
