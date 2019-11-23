#!/usr/bin/env perl
#
# Makefile.PL
#
# Makefile for tachikoma
#
# Copyright (c) 2019 DesertNet
#
# $Id: Makefile.PL 38086 2019-11-07 15:16:47Z chris $
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
        'Net::SSLeay'         => '1.00',
        'IO::Socket::SSL'     => '1.00',

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
            bin/drain_node
            bin/fill_node
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
