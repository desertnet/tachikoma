#!/usr/bin/perl
use strict;
use warnings;
chdir('etc');
chdir('scripts');
# system("./server.pl            > server.tsl");
system("./workstation/default.pl > workstation/default.tsl");
system("./workstation/misa.pl    > workstation/misa.tsl");
system("./workstation/nyx.pl     > workstation/nyx.tsl");
system("./workstation/fae.pl     > workstation/fae.tsl");
