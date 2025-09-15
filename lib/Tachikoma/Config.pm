#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Config
# ----------------------------------------------------------------------
#

package Tachikoma::Config;
use strict;
use warnings;
use Exporter;
use vars   qw( @EXPORT_OK );
use parent qw( Exporter );
@EXPORT_OK = qw(
    %Tachikoma $ID $Private_Key $Private_Ed25519_Key %Keys %SSL_Config
    %Help %Var %Aliases
    include_conf load_module
);

use version; our $VERSION = qv('v2.0.165');

our $Wire_Version        = undef;
our %Tachikoma           = ();
our $ID                  = q();
our $Private_Key         = q();
our $Private_Ed25519_Key = q();
our %Keys                = ();
our %SSL_Config          = ();
our %Help                = ();
our %Var                 = ();
our %Aliases             = ();

my $CONFIGURATION = undef;
my %FORBIDDEN     = ();
my %LEGACY_MAP    = (
    scheme         => 'scheme',
    Listen         => 'listen_sockets',
    Prefix         => 'prefix',
    Log_Dir        => 'log_dir',
    Log_File       => 'log_file',
    Pid_Dir        => 'pid_dir',
    Pid_File       => 'pid_file',
    Home           => 'home',
    Include_Nodes  => 'include_nodes',
    Include_Jobs   => 'include_jobs',
    Buffer_Size    => 'buffer_size',
    Low_Water_Mark => 'low_water_mark',
    Keep_Alive     => 'keep_alive',
    Hz             => 'hz',
);

my %LEGACY_SSL_MAP = (
    'SSL_client_ca_file'   => 'ssl_client_ca_file',
    'SSL_client_cert_file' => 'ssl_client_cert_file',
    'SSL_client_key_file'  => 'ssl_client_key_file',
    'SSL_server_ca_file'   => 'ssl_server_ca_file',
    'SSL_server_cert_file' => 'ssl_server_cert_file',
    'SSL_server_key_file'  => 'ssl_server_key_file',
);

sub new {
    my $class = shift;
    my $self  = {
        wire_version         => '2.0.27',
        config_file          => undef,
        help                 => {},
        functions            => {},
        var                  => {},
        debug_level          => undef,
        secure_level         => undef,
        scheme               => 'rsa',
        listen_sockets       => undef,
        prefix               => undef,
        log_dir              => undef,
        log_file             => undef,
        pid_dir              => undef,
        pid_file             => undef,
        home                 => undef,
        include_nodes        => undef,
        include_jobs         => undef,
        buffer_size          => undef,
        low_water_mark       => undef,
        keep_alive           => undef,
        hz                   => undef,
        id                   => q(),
        private_key          => q(),
        private_ed25519_key  => q(),
        public_keys          => {},
        ssl_client_ca_file   => undef,
        ssl_client_cert_file => undef,
        ssl_client_key_file  => undef,
        ssl_server_ca_file   => undef,
        ssl_server_cert_file => undef,
        ssl_server_key_file  => undef,
        ssl_version          => undef,
        forbidden            => \%FORBIDDEN,
    };
    bless $self, $class;
    return $self;
}

sub load_config_file {
    my ( $self, @config_files ) = @_;
    for my $config_file (@config_files) {
        next if ( not -f $config_file );
        $self->include_config($config_file);
        $self->{config_file} = $config_file;
        last;
    }
    return $self;
}

sub include_config {
    my $self        = shift;
    my $script_path = shift;
    include_conf($script_path);
    return;
}

sub include_conf {
    my $script_path = shift;
    my $package     = $script_path;
    return if ( not -f $script_path );
    $package =~ s{\W+}{_}g;
    $package =~ s{^(\d)}{_$1};
    $FORBIDDEN{$script_path} = 1;
    my $fh;
    local $/ = undef;
    open $fh, '<', $script_path or die "couldn't open $script_path: $!";
    my $script = <$fh>;
    close $fh or die $!;
    my $config = global();
    ## no critic (ProhibitStringyEval)
    my $okay = eval join q(),
        'package ', $package, ";\n",
        ( $script =~ m{^(.*?)(?:__END__.*)?$}s )[0], "\n";
    ## use critic
    if ( not $okay ) {
        my $error = $@ // 'unknown error';
        die "couldn't include_conf $script_path: $error\n";
    }
    $config->load_legacy;
    return;
}

sub load_legacy {
    my $self = shift;
    for my $legacy_key ( keys %LEGACY_MAP ) {
        my $modern_key = $LEGACY_MAP{$legacy_key};
        if ( exists $Tachikoma{$legacy_key} ) {
            $self->{$modern_key} = $Tachikoma{$legacy_key};
        }
    }
    for my $legacy_key ( keys %LEGACY_SSL_MAP ) {
        my $modern_key = $LEGACY_SSL_MAP{$legacy_key};
        if ( exists $SSL_Config{$legacy_key} ) {
            $self->{$modern_key} = $SSL_Config{$legacy_key};
        }
    }
    if ( length $ID ) {
        $self->{id} = $ID;
    }
    if ( length $Private_Key ) {
        $self->{private_key} = $Private_Key;
    }
    if ( length $Private_Ed25519_Key ) {
        $self->{private_ed25519_key} = $Private_Ed25519_Key;
    }
    for my $legacy_key ( keys %Keys ) {
        $self->{public_keys}->{$legacy_key} = $Keys{$legacy_key};
    }
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

sub debug_level {
    my $self = shift;
    if (@_) {
        $self->{debug_level} = shift;
    }
    return $self->{debug_level};
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
        my $scheme = shift;
        die "invalid scheme: $scheme\n"
            if ($scheme ne 'rsa'
            and $scheme ne 'rsa-sha256'
            and $scheme ne 'ed25519' );
        $self->{scheme} = $scheme;
    }
    return $self->{scheme};
}

sub listen_sockets {
    my $self = shift;
    if (@_) {
        $self->{listen_sockets} = shift;
    }
    elsif ( not defined $self->{listen_sockets} ) {
        $self->{listen_sockets} = [ { Socket => '/tmp/tachikoma.socket' } ];
    }
    return $self->{listen_sockets};
}

sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    elsif ( not defined $self->{prefix} ) {
        $self->{prefix} = '/usr/local/bin';
    }
    return $self->{prefix};
}

sub log_dir {
    my $self = shift;
    if (@_) {
        $self->{log_dir} = shift;
    }
    elsif ( not defined $self->{log_dir} ) {
        $self->{log_dir} = '/var/log/tachikoma';
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
    elsif ( not defined $self->{pid_dir} ) {
        $self->{pid_dir} = '/var/run/tachikoma';
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
    elsif ( not defined $self->{home} ) {
        $self->{home} = ( getpwuid $< )[7];
    }
    return $self->{home};
}

sub include_nodes {
    my $self = shift;
    if (@_) {
        $self->{include_nodes} = shift;
    }
    elsif ( not defined $self->{include_nodes} ) {
        $self->{include_nodes} = ['Accessories::Nodes'];
    }
    return $self->{include_nodes};
}

sub include_jobs {
    my $self = shift;
    if (@_) {
        $self->{include_jobs} = shift;
    }
    elsif ( not defined $self->{include_jobs} ) {
        $self->{include_jobs} = ['Accessories::Jobs'];
    }
    return $self->{include_jobs};
}

sub buffer_size {
    my $self = shift;
    if (@_) {
        $self->{buffer_size} = shift;
    }
    elsif ( not defined $self->{buffer_size} ) {
        $self->{buffer_size} = 1048576;
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

sub ssl_client_ca_file {
    my $self = shift;
    if (@_) {
        $self->{ssl_client_ca_file} = shift;
    }
    return $self->{ssl_client_ca_file};
}

sub ssl_client_cert_file {
    my $self = shift;
    if (@_) {
        $self->{ssl_client_cert_file} = shift;
    }
    return $self->{ssl_client_cert_file};
}

sub ssl_client_key_file {
    my $self = shift;
    if (@_) {
        $self->{ssl_client_key_file} = shift;
    }
    return $self->{ssl_client_key_file};
}

sub ssl_server_ca_file {
    my $self = shift;
    if (@_) {
        $self->{ssl_server_ca_file} = shift;
    }
    return $self->{ssl_server_ca_file};
}

sub ssl_server_cert_file {
    my $self = shift;
    if (@_) {
        $self->{ssl_server_cert_file} = shift;
    }
    return $self->{ssl_server_cert_file};
}

sub ssl_server_key_file {
    my $self = shift;
    if (@_) {
        $self->{ssl_server_key_file} = shift;
    }
    return $self->{ssl_server_key_file};
}

sub ssl_version {
    my $self = shift;
    if (@_) {
        $self->{ssl_version} = shift;
    }
    return $self->{ssl_version};
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

sub global {
    my $self = shift;
    if ( not defined $CONFIGURATION ) {
        $CONFIGURATION = Tachikoma::Config->new;
    }
    return $CONFIGURATION;
}

1;
