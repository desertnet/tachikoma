#!/usr/bin/env perl -T
# ----------------------------------------------------------------------
# tachikoma external tests
# ----------------------------------------------------------------------
#

use strict;
use warnings;
use Test::More;
my $count = 1;
is($count, 1, 'no tests run yet'); 
# Rewrite test output from multiple test scripts to look like a single test script
if ( -d './t/ext' ) {
    $ENV{PATH} = '/bin:/usr/bin:/usr/local/bin';
    # print the plan for unknown number of tests
    for my $file ( glob('./t/ext/*.t') ) {
        # untaint $file
        $file = ( $file =~ m{(.*)} )[0];
        # run test script and capture output
        my $output = qx{perl -Ilib $file};
        # remove the first line of the output
        $output =~ s/.*\n//;
        # remove the last line of the output
        $output =~ s/\n.*\z//;
        # print the output, counting and renumbering each line
        for my $line ( split /\n/, $output ) {
            # strip the test number from the line
            if ($line =~ s/^ok \d+ - //) {
                ok($count, $line);
                $count++;
            }
            elsif ($line =~ s/^not ok \d+ - //) {
                fail($line);
                $count++;
            }
            else {
                print "$line\n";
            }
        }
    }
}
# print the number of tests run
done_testing($count);
