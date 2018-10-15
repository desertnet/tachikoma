#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Job
# ----------------------------------------------------------------------
#
# $Id: Job.pm 35220 2018-10-15 06:55:10Z chris $
#

package Tachikoma::Job;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO PAYLOAD
    TM_EOF TM_ERROR
);
use Tachikoma::EventFrameworks::Select;
use Tachikoma::Nodes::FileHandle qw( TK_R setsockopts );
use Tachikoma::Nodes::Callback;
use Tachikoma::Config qw( %Tachikoma );
use POSIX qw( F_SETFD dup2 );
use Socket;
use Scalar::Util qw( blessed );
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.101');

my $SUDO = undef;
if ( -f '/usr/bin/sudo' ) {
    $SUDO = '/usr/bin/sudo';
}
elsif ( -f '/usr/local/bin/sudo' ) {
    $SUDO = '/usr/local/bin/sudo';
}

use constant {
    MAX_RECENT_LOG => 100,
    FD_5           => 5,
};

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{type}           = undef;
    $self->{pid}            = undef;
    $self->{should_restart} = undef;
    $self->{lazy}           = undef;
    $self->{username}       = undef;
    $self->{config}         = undef;
    $self->{original_name}  = undef;
    $self->{router}         = undef;
    $self->{connector}      = undef;
    bless $self, $class;
    return $self;
}

sub prepare {
    my $self           = shift;
    my $type           = shift;
    my $name           = shift;
    my $arguments      = shift // q{};
    my $owner          = shift // q{};
    my $should_restart = shift // q{};
    my $username       = shift || q{};
    my $config         = shift || q{};
    $self->{arguments}      = $arguments;
    $self->{type}           = $type;
    $self->{pid}            = q{-};
    $self->{should_restart} = $should_restart;
    $self->{lazy}           = 1;
    $self->{username}       = $username;
    $self->{config}         = $config;
    $self->{original_name}  = $name;
    $self->{connector}      = Tachikoma::Nodes::Callback->new;
    $self->{connector}->name($name);
    $self->{connector}->{owner} = $owner;
    $self->{connector}->callback(
        sub {
            my $message = shift;
            my $sink    = $self->{connector}->{sink};
            $owner = $self->{connector}->{owner};
            $self->spawn( $type, $name, $arguments, $owner, $should_restart,
                $username, $config );
            if ( $self->{connector}->{type} ) {
                $self->{connector}->sink($sink);
                $self->{connector}->fill($message);
            }
            return;
        }
    );
    return;
}

sub spawn {
    my $self           = shift;
    my $type           = shift;
    my $name           = shift;
    my $arguments      = shift // q{};
    my $owner          = shift // q{};
    my $should_restart = shift // q{};
    my $username       = shift || q{};
    my $config         = shift || q{};
    my $filehandles    = {
        parent => {
            stdout => undef,
            stderr => undef,
            socket => undef,
        },
        child => {
            stdout => undef,
            stderr => undef,
            socket => undef,
        },
    };
    $self->init_filehandles($filehandles);
    my $pid = fork;
    return $self->stderr("ERROR: couldn't fork: $!") if ( not defined $pid );

    if ($pid) {

        # connect parent filehandles
        $self->connect_parent( $filehandles, $name, $owner );
        $self->{arguments}      = $arguments;
        $self->{type}           = $type;
        $self->{pid}            = $pid;
        $self->{should_restart} = $should_restart;
        $self->{username}       = $username;
        $self->{config}         = $config;
        $self->{original_name}  = $name;
        return;
    }
    else {
        my $location = $Tachikoma{Prefix} || '/usr/local/bin';
        my $tachikoma_job = join q{}, $location, '/tachikoma-job';
        $type           = ( $type =~ m{^([\w:]+)$} )[0];
        $username       = ( $username =~ m{^(\S*)$} )[0];
        $config         = ( $config =~ m{^(\S*)$} )[0];
        $name           = ( $name =~ m{^(\S*)$} )[0];
        $arguments      = ( $arguments =~ m{^(.*)$}s )[0];
        $owner          = ( $owner =~ m{^(\S*)$} )[0];
        $should_restart = ( $should_restart =~ m{^(\S*)$} )[0];

        # search for module here in case we're sudoing a job without config
        my $class = $self->determine_class($type) or exit 1;

        # connect child filehandles
        $self->connect_child($filehandles);

        # bin/tachikoma-job will handle the rest
        my @command = ();
        if ( $username and $username ne ( getpwuid $< )[0] ) {
            push @command, $SUDO, '-u', $username, '-C', FD_5 + 1;
        }
        push @command,
            $tachikoma_job, $config, $class,
            $name // q{},
            $arguments // q{},
            $owner // q{},
            $should_restart // q{};
        exec @command or $self->stderr("ERROR: couldn't exec: $!");
        exit 1;
    }
    return;
}

sub init_filehandles {
    my $self        = shift;
    my $filehandles = shift;
    pipe $filehandles->{parent}->{stdout}, $filehandles->{child}->{stdout}
        or die $!;
    pipe $filehandles->{parent}->{stderr}, $filehandles->{child}->{stderr}
        or die $!;
    $filehandles->{child}->{stdout}->autoflush(1);
    $filehandles->{child}->{stderr}->autoflush(1);
    socketpair $filehandles->{child}->{socket},
        $filehandles->{parent}->{socket}, AF_UNIX, SOCK_STREAM, PF_UNSPEC
        or die $!;
    setsockopts( $filehandles->{child}->{socket} );
    setsockopts( $filehandles->{parent}->{socket} );
    return;
}

sub connect_parent {
    my ( $self, $filehandles, $name, $owner ) = @_;

    # make a FileHandle to send/receive messages over the socketpair()
    close $filehandles->{child}->{socket} or die $!;
    $self->make_socketpair( $filehandles->{parent}->{socket}, $name, $owner );

    # create STDIO and Callback nodes to log the child's STDOUT and STDERR
    close $filehandles->{child}->{stdout} or die $!;
    close $filehandles->{child}->{stderr} or die $!;
    $self->make_stdio( '_stdout', $filehandles->{parent}->{stdout}, $name );
    $self->make_stdio( '_stderr', $filehandles->{parent}->{stderr}, $name );
    return;
}

sub connect_child {
    my ( $self, $filehandles ) = @_;

    # redirect STDOUT/STDERR to our pipes
    close $filehandles->{parent}->{stdout} or die $!;
    close $filehandles->{parent}->{stderr} or die $!;
    dup2( fileno( $filehandles->{child}->{stdout} ), 1 );
    dup2( fileno( $filehandles->{child}->{stderr} ), 2 );

    # make the socketpair() available on FD 5
    fcntl $filehandles->{child}->{socket}, F_SETFD, 0
        or die $!;    # clear close-on-exec
    dup2( fileno( $filehandles->{child}->{socket} ), FD_5 );

    return;
}

sub make_socketpair {
    my ( $self, $parent, $name, $owner ) = @_;
    $self->{connector}->remove_node if ( $self->{connector} );
    $self->{connector} =
        Tachikoma::Nodes::FileHandle->filehandle( $parent, TK_R );
    $self->{connector}->name($name);
    $self->{connector}->{owner} = $owner;
    $self->{connector}->{type}  = 'job';
    return;
}

sub make_stdio {
    my ( $self, $type, $parent_stdio, $name ) = @_;
    my $node = Tachikoma::Nodes::STDIO->filehandle( $parent_stdio, TK_R );

    # give them names, but don't register them in %Tachikoma::Nodes:
    $node->{name} = join q{:}, $name, $type;
    $node->buffer_mode('line-buffered');
    $node->sink( Tachikoma::Nodes::Callback->new );
    $node->{sink}->callback(
        sub {
            my $message = shift;
            if ( not $message->[TYPE] & TM_EOF ) {
                if ( $message->[PAYLOAD] =~ m{^\d{4}-\d\d-\d\d} ) {
                    print {*STDERR} $message->[PAYLOAD];
                }
                else {
                    $self->{connector}->stderr( $message->[PAYLOAD] );
                }
            }
            return;
        }
    );
    $self->{$type} = $node;
    return;
}

sub determine_class {
    my ( $self, $type ) = @_;
    if ( not $type ) {
        $self->stderr("invalid type specified\n");
        return;
    }
    my $path = $type;
    $path =~ s{::}{/}g;
    my $class = undef;
    my $rv    = undef;
    my $error = undef;
    for my $prefix ( @{ $Tachikoma{Include_Jobs} }, 'Tachikoma::Jobs' ) {
        next if ( not $prefix );
        $class = join q{::}, $prefix, $type;
        my $class_path = $class;
        $class_path =~ s{::}{/}g;
        $class_path .= '.pm';
        $rv = eval { require $class_path };
        if ( not $rv
            and ( not $error or $error =~ m{^Can't locate \S*$path} ) )
        {
            $error = $@;
        }
        last if ($rv);
    }
    if ( not $rv ) {
        $self->stderr($error);
        return;
    }
    return $class;
}

sub initialize_graph {
    my $self = shift;
    $self->connector->sink($self);
    $self->sink( $self->router );
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $rv      = undef;
    $message->[TO] = '_parent' if ( not $message->[TO] );
    $self->{counter}++;
    $rv = $self->{router}->fill($message)
        if ( not $message->[TYPE] & TM_EOF
        or $message->[FROM] ne '_parent' );
    return $rv;
}

sub execute {
    my $self        = shift;
    my $safe_part   = shift || q{};
    my $unsafe_part = shift || q{};
    my $now_safe = ( $unsafe_part =~ m{^([\d\s\w@%"'/:,.=_-]*)$} )[0] || q{};
    if ( $unsafe_part eq $now_safe ) {
        my $command = join q{ }, $safe_part, $now_safe;
        local $ENV{ENV}  = q{};
        local $ENV{PATH} = '/bin:/usr/bin:/sbin:/usr/sbin:/usr/local/bin';
        local $/         = undef;
        my $rv = `$command 2>&1`;    ## no critic (ProhibitBacktickOperators)
        return $rv;
    }
    return qq(bad command: "$safe_part $unsafe_part"\n);
}

sub execute_unsafe {
    my $self        = shift;
    my $safe_part   = shift || q{};
    my $unsafe_part = shift || q{};
    my $now_safe    = ( $unsafe_part =~ m{^(.*)$} )[0] || q{};
    if ( $unsafe_part eq $now_safe ) {
        my $command = join q{ }, $safe_part, $now_safe;
        local $ENV{ENV}  = q{};
        local $ENV{PATH} = '/bin:/usr/bin:/sbin:/usr/sbin:/usr/local/bin';
        local $/         = undef;
        my $rv = `$command 2>&1`;    ## no critic (ProhibitBacktickOperators)
        return $rv;
    }
    return qq(bad command: "$safe_part $unsafe_part"\n);
}

sub remove_node {
    my $self = shift;
    if ( $self->{connector} ) {
        $self->{connector}->remove_node;
    }
    $self->SUPER::remove_node;
    return;
}

sub close_stdio {
    my $self = shift;
    POSIX::close(1);
    POSIX::close(2);
    return;
}

sub type {
    my $self = shift;
    if (@_) {
        $self->{type} = shift;
    }
    return $self->{type};
}

sub pid {
    my $self = shift;
    if (@_) {
        $self->{pid} = shift;
    }
    return $self->{pid};
}

sub should_restart {
    my $self = shift;
    if (@_) {
        $self->{should_restart} = shift;
    }
    return $self->{should_restart};
}

sub lazy {
    my $self = shift;
    if (@_) {
        $self->{lazy} = shift;
    }
    return $self->{lazy};
}

sub username {
    my $self = shift;
    if (@_) {
        $self->{username} = shift;
    }
    return $self->{username};
}

sub original_name {
    my $self = shift;
    if (@_) {
        $self->{original_name} = shift;
    }
    return $self->{original_name};
}

sub router {
    my $self = shift;
    if (@_) {
        $self->{router} = shift;
    }
    return $self->{router};
}

sub connector {
    my $self = shift;
    if (@_) {
        $self->{connector} = shift;
    }
    return $self->{connector};
}

1;
