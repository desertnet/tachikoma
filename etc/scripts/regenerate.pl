#!/usr/bin/perl
use strict;
use warnings;
chdir('etc');
chdir('scripts');
# system("./server.pl            > server.tsl");
system("./workstation/default.pl > workstation/default.tsl");
