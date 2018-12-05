#!/usr/bin/env perl
#
# Makefile.PL
#
# Makefile for tachikoma
#
# Copyright (c) 2018 DesertNet
#
# $Id: Makefile.PL 35876 2018-11-18 21:33:05Z chris $
#

use strict;
use warnings;
use ExtUtils::MakeMaker;
my $version = 'trunk';
if (-f 'version') {
    open(FH, '<version');
    $version = <FH>;
    chomp($version);
    close(FH);
}
if ($version eq 'trunk') {
    my $svn_version = `svnversion`;
    chomp($svn_version);
    $svn_version =~ s(\D.*)();
    $version = join('', $version, '-r', $svn_version);
}
WriteMakefile(
    NAME        => 'Tachikoma',
    DISTNAME    => 'Tachikoma',
    VERSION     => $version,
    PREREQ_PM    => {
        'Crypt::OpenSSL::Random'    => '0.01',
        'Crypt::OpenSSL::RSA'       => '0.10',
        'Digest::HMAC'              => '1.01',
        'Digest::HMAC_MD5'          => '1.01',
        'Net::SSLeay'               => '1.00',
        'IO::Socket::SSL'           => '1.00',
        # these are only necessary to pass tests:
        'File::Temp'                => '0.10',
        'BerkeleyDB'                => '0.10',
        'DBI'                       => '1.00',
        'LWP'                       => '1.00',
    },
    INSTALLDIRS => 'site',
    EXE_FILES   => [qw(
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
        bin/topic_top
    )],
);
