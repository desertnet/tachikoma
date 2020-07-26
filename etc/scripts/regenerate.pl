#!/usr/bin/perl
use strict;
use warnings;
chdir('etc');
chdir('scripts');
system("./docker/default.pl > docker/default.tsl");
