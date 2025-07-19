#!/usr/bin/env perl
# ----------------------------------------------------------------------
# Makefile.PL
# ----------------------------------------------------------------------
#

use strict;
use warnings;
use ExtUtils::MakeMaker;
my $version = '2.0';
if ( -f 'version' ) {
    open( FH, '<version' );
    $version = <FH>;
    chomp($version);
    close(FH);
}
WriteMakefile(
    NAME      => 'Tachikoma',
    DISTNAME  => 'Tachikoma',
    VERSION   => $version,
    PREREQ_PM => {
        'Crypt::OpenSSL::RSA' => '0.10',

        # these are only necessary to pass tests:
        'File::Temp' => '0.10',
        'BerkeleyDB' => '0.10',
        'DBI'        => '1.00',
        'LWP'        => '1.00',
    },
    INSTALLDIRS => 'site',
    EXE_FILES   => [
        qw(
            bin/buffer_top
            bin/consume_topic
            bin/drain_node
            bin/fill_node
            bin/produce_topic
            bin/set_tachikoma_hz
            bin/tachikoma
            bin/tachikoma-client
            bin/tachikoma-fsync
            bin/tachikoma-gui
            bin/tachikoma-job
            bin/tachikoma-server
            bin/tail_top
            bin/topic_top
            )
    ],
);
