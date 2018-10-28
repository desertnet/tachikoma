#!/usr/bin/perl
# ----------------------------------------------------------------------
# $Id: Config.pm 35698 2018-10-28 02:54:48Z chris $
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
    load_module include_conf
);

use version; our $VERSION = qv('v2.0.165');

my $username = ( getpwuid $< )[0];
my $home     = ( getpwuid $< )[7];

our $Wire_Version = undef;
our %Tachikoma    = (
    Listen        => [ { Socket => '/tmp/tachikoma.socket' } ],
    Prefix        => '/usr/local/bin',
    Log_Dir       => '/tmp',
    Log_File      => undef,
    Pid_Dir       => '/tmp',
    Pid_File      => undef,
    Home          => $home,
    Include_Nodes => ['Accessories::Nodes'],
    Include_Jobs  => ['Accessories::Jobs'],
    Buffer_Size   => 1048576,
    Keep_Alive    => undef,
    Hz            => undef,
);
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
        wire_version        => '2.0.27',
        config_file         => undef,
        help                => {},
        functions           => {},
        var                 => {},
        secure_level        => undef,
        scheme              => 'rsa',
        listen_sockets      => $Tachikoma{Listen},
        prefix              => $Tachikoma{Prefix},
        log_dir             => $Tachikoma{Log_Dir},
        log_file            => $Tachikoma{Log_File},
        pid_dir             => $Tachikoma{Pid_Dir},
        pid_file            => $Tachikoma{Pid_File},
        home                => $Tachikoma{Home},
        include_nodes       => $Tachikoma{Include_Nodes},
        include_jobs        => $Tachikoma{Include_Jobs},
        buffer_size         => $Tachikoma{Buffer_Size},
        low_water_mark      => $Tachikoma{Low_Water_Mark},
        keep_alive          => $Tachikoma{Keep_Alive},
        hz                  => $Tachikoma{Hz},
        id                  => q(),
        private_key         => q(),
        private_ed25519_key => q(),
        public_keys         => {},
        ssl_config          => {},
        forbidden           => {},
    };
    bless $self, $class;
    return $self;
}

sub load_config_file {
    my $self        = shift;
    my $config_file = shift;
    include_conf($config_file) if ( $config_file and -f $config_file );
    $self->{config_file} = $config_file;
    return $self;
}

sub set_legacy {
    my $self = shift;
    $Tachikoma{Listen}         = $self->{listen_sockets};
    $Tachikoma{Prefix}         = $self->{prefix};
    $Tachikoma{Log_Dir}        = $self->{log_dir};
    $Tachikoma{Log_File}       = $self->{log_file};
    $Tachikoma{Pid_Dir}        = $self->{pid_dir};
    $Tachikoma{Pid_File}       = $self->{pid_file};
    $Tachikoma{Home}           = $self->{home};
    $Tachikoma{Include_Nodes}  = $self->{include_nodes};
    $Tachikoma{Include_Jobs}   = $self->{include_jobs};
    $Tachikoma{Buffer_Size}    = $self->{buffer_size};
    $Tachikoma{Low_Water_Mark} = $self->{low_water_mark};
    $Tachikoma{Keep_Alive}     = $self->{keep_alive};
    $Tachikoma{Hz}             = $self->{hz};
    $ID                        = $self->{id};
    $Private_Key               = $self->{private_key};
    $Private_Ed25519_Key       = $self->{private_ed25519_key};
    %Keys                      = %{ $self->{public_keys} };
    %SSL_Config                = %{ $self->{ssl_config} };
    %Forbidden                 = %{ $self->{forbidden} };
    return $self;
}

sub load_legacy {
    my $self = shift;
    $self->{listen_sockets}      = $Tachikoma{Listen};
    $self->{prefix}              = $Tachikoma{Prefix};
    $self->{log_dir}             = $Tachikoma{Log_Dir};
    $self->{log_file}            = $Tachikoma{Log_File};
    $self->{pid_dir}             = $Tachikoma{Pid_Dir};
    $self->{pid_file}            = $Tachikoma{Pid_File};
    $self->{home}                = $Tachikoma{Home};
    $self->{include_nodes}       = $Tachikoma{Include_Nodes};
    $self->{include_jobs}        = $Tachikoma{Include_Jobs};
    $self->{buffer_size}         = $Tachikoma{Buffer_Size};
    $self->{low_water_mark}      = $Tachikoma{Low_Water_Mark};
    $self->{keep_alive}          = $Tachikoma{Keep_Alive};
    $self->{hz}                  = $Tachikoma{Hz};
    $self->{id}                  = $ID if ($ID);
    $self->{private_key}         = $Private_Key if ($Private_Key);
    $self->{private_ed25519_key} = $Private_Ed25519_Key
        if ($Private_Ed25519_Key);
    $self->{public_keys}->{$_} = $Keys{$_}       for ( keys %Keys );
    $self->{ssl_config}->{$_}  = $SSL_Config{$_} for ( keys %SSL_Config );
    $self->{forbidden}->{$_}   = $Forbidden{$_}  for ( keys %Forbidden );
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

sub wire_version {
    my $self = shift;
    if (@_) {
        $self->{wire_version} = shift;
    }
    return $self->{wire_version};
}

sub config_file {
    my $self = shift;
    if (@_) {
        $self->{config_file} = shift;
    }
    return $self->{config_file};
}

sub help {
    my $self = shift;
    if (@_) {
        $self->{help} = shift;
    }
    return $self->{help};
}

sub functions {
    my $self = shift;
    if (@_) {
        $self->{functions} = shift;
    }
    return $self->{functions};
}

sub var {
    my $self = shift;
    if (@_) {
        $self->{var} = shift;
    }
    return $self->{var};
}

sub secure_level {
    my $self = shift;
    if (@_) {
        $self->{secure_level} = shift;
    }
    return $self->{secure_level};
}

sub scheme {
    my $self = shift;
    if (@_) {
        $self->{scheme} = shift;
    }
    return $self->{scheme};
}

sub listen_sockets {
    my $self = shift;
    if (@_) {
        $self->{listen_sockets} = shift;
    }
    return $self->{listen_sockets};
}

sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
}

sub log_dir {
    my $self = shift;
    if (@_) {
        $self->{log_dir} = shift;
    }
    return $self->{log_dir};
}

sub log_file {
    my $self = shift;
    if (@_) {
        $self->{log_file} = shift;
    }
    return $self->{log_file};
}

sub pid_dir {
    my $self = shift;
    if (@_) {
        $self->{pid_dir} = shift;
    }
    return $self->{pid_dir};
}

sub pid_file {
    my $self = shift;
    if (@_) {
        $self->{pid_file} = shift;
    }
    return $self->{pid_file};
}

sub home {
    my $self = shift;
    if (@_) {
        $self->{home} = shift;
    }
    return $self->{home};
}

sub include_nodes {
    my $self = shift;
    if (@_) {
        $self->{include_nodes} = shift;
    }
    return $self->{include_nodes};
}

sub include_jobs {
    my $self = shift;
    if (@_) {
        $self->{include_jobs} = shift;
    }
    return $self->{include_jobs};
}

sub buffer_size {
    my $self = shift;
    if (@_) {
        $self->{buffer_size} = shift;
    }
    return $self->{buffer_size};
}

sub low_water_mark {
    my $self = shift;
    if (@_) {
        $self->{low_water_mark} = shift;
    }
    return $self->{low_water_mark};
}

sub keep_alive {
    my $self = shift;
    if (@_) {
        $self->{keep_alive} = shift;
    }
    return $self->{keep_alive};
}

sub id {
    my $self = shift;
    if (@_) {
        $self->{id} = shift;
    }
    return $self->{id};
}

sub private_key {
    my $self = shift;
    if (@_) {
        $self->{private_key} = shift;
    }
    return $self->{private_key};
}

sub private_ed25519_key {
    my $self = shift;
    if (@_) {
        $self->{private_ed25519_key} = shift;
    }
    return $self->{private_ed25519_key};
}

sub public_keys {
    my $self = shift;
    if (@_) {
        $self->{public_keys} = shift;
    }
    return $self->{public_keys};
}

sub ssl_config {
    my $self = shift;
    if (@_) {
        $self->{ssl_config} = shift;
    }
    return $self->{ssl_config};
}

sub forbidden {
    my $self = shift;
    if (@_) {
        $self->{forbidden} = shift;
    }
    return $self->{forbidden};
}

sub hz {
    my $self = shift;
    if (@_) {
        $self->{hz} = shift;
    }
    return $self->{hz};
}

1;
