#!/usr/bin/perl
# ----------------------------------------------------------------------
# $Id: Config.pm 35512 2018-10-22 08:27:21Z chris $
# ----------------------------------------------------------------------

package Tachikoma::Config;
use strict;
use warnings;
use Exporter;
use vars qw( @EXPORT_OK );
use parent qw( Exporter );
@EXPORT_OK = qw(
    %Tachikoma $ID $Private_Key $Private_Ed25519_Key %Keys %SSL_Config
    %Forbidden $Secure_Level %Help %Functions %Var $Wire_Version %Aliases
    load_module include_conf new_func
);

use version; our $VERSION = qv('v2.0.165');

my $username = ( getpwuid $< )[0];

our $Wire_Version = '2.0.27';
our %Tachikoma    = (
    Listen        => [ { Socket => '/tmp/tachikoma.socket' } ],
    Prefix        => '/usr/local/bin',
    Log_Dir       => '/tmp',
    Pid_Dir       => '/tmp',
    Include_Nodes => ['Accessories::Nodes'],
    Include_Jobs  => ['Accessories::Jobs'],
    Buffer_Size   => 1048576,
);
our $ID                  = q();
our $Private_key         = q();
our $Private_Ed25519_Key = q();
our %Keys                = ();
our %SSL_Config          = ();
our %Forbidden           = ();
our $Secure_Level        = undef;
our %Help                = ();
our %Functions           = ();
our %Var                 = ();
our %Aliases             = ();

sub load_module {
    my $module_name = shift;
    my $module_path = $module_name;
    $module_path =~ s{::}{/}g;
    $module_path .= '.pm';
    require $module_path;
    return;
}

sub include_conf {
    my $script_path = shift;
    my $package     = $script_path;
    $package =~ s{[^\w\d]+}{_}g;
    $package =~ s{^(\d)}{_$1};
    $Forbidden{$script_path} = 1;
    my $fh;
    local $/ = undef;
    open $fh, '<', $script_path or die "couldn't open $script_path: $!";
    my $script = <$fh>;
    close $fh or die $!;
    ## no critic (ProhibitStringyEval)
    my $rv = eval join q(),
        'package ', $package, ";\n",
        ( $script =~ m{^(.*?)(?:__END__.*)?$}s )[0], "\n";
    ## use critic
    die $@ if ( not $rv );
    return;
}

1;
