#!/usr/bin/perl
# ----------------------------------------------------------------------
# $Id: Config.pm 35629 2018-10-26 12:29:10Z chris $
# ----------------------------------------------------------------------

package Tachikoma::Config;
use strict;
use warnings;
use Exporter;
use vars qw( @EXPORT_OK );
use parent qw( Exporter );
@EXPORT_OK = qw(
    %Tachikoma $Scheme $ID $Private_Key $Private_Ed25519_Key %Keys
    %SSL_Config %Forbidden $Secure_Level %Help %Functions %Var
    $Wire_Version %Aliases load_module include_conf
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
our $Scheme              = 'rsa';
our $ID                  = q();
our $Private_Key         = q();
our $Private_Ed25519_Key = q();
our %Keys                = ();
our %SSL_Config          = ();
our %Forbidden           = ();
our $Secure_Level        = undef;
our %Help                = ();
our %Functions           = ();
our %Var                 = ();
our %Aliases             = ();

sub new {
    my $class = shift;
    my $self  = {
        wire_version        => $Wire_Version,
        listen              => $Tachikoma{Listen},
        prefix              => $Tachikoma{Prefix},
        log_dir             => $Tachikoma{Log_Dir},
        pid_dir             => $Tachikoma{Pid_Dir},
        include_nodes       => $Tachikoma{Include_Nodes},
        include_jobs        => $Tachikoma{Include_Jobs},
        buffer_size         => $Tachikoma{Buffer_Size},
        low_water_mark      => $Tachikoma{Low_Water_Mark},
        keep_alive          => $Tachikoma{Keep_Alive},
        scheme              => $Scheme,
        id                  => q(),
        private_key         => q(),
        private_ed25519_key => q(),
        public_keys         => {},
        ssl_config          => {},
        forbidden           => {},
        secure_level        => undef,
        help                => {},
        functions           => {},
        var                 => {},
        hz                  => undef,
    };
    bless $self, $class;
    return $self;
}

sub load_legacy {
    my $self        = shift;
    my $config_file = shift;
    include_conf($config_file) if ( $config_file and -f $config_file );
    $Tachikoma{Config}           = $config_file;
    $self->{wire_version}        = $Wire_Version;
    $self->{config}              = $Tachikoma{Config};
    $self->{listen}              = $Tachikoma{Listen};
    $self->{prefix}              = $Tachikoma{Prefix};
    $self->{log_dir}             = $Tachikoma{Log_Dir};
    $self->{pid_dir}             = $Tachikoma{Pid_Dir};
    $self->{include_nodes}       = $Tachikoma{Include_Nodes};
    $self->{include_jobs}        = $Tachikoma{Include_Jobs};
    $self->{buffer_size}         = $Tachikoma{Buffer_Size};
    $self->{low_water_mark}      = $Tachikoma{Low_Water_Mark};
    $self->{keep_alive}          = $Tachikoma{Keep_Alive};
    $self->{scheme}              = $Scheme;
    $self->{id}                  = $ID;
    $self->{private_key}         = $Private_Key;
    $self->{private_ed25519_key} = $Private_Ed25519_Key;
    $self->{public_keys}         = \%Keys;
    $self->{ssl_config}          = \%SSL_Config;
    $self->{forbidden}           = \%Forbidden;
    $self->{secure_level}        = $Secure_Level;
    $self->{help}                = \%Help;
    $self->{functions}           = \%Functions;
    $self->{var}                 = \%Var;
    $self->{hz}                  = $Tachikoma{Hz};
    return $self;
}

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
