#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Job
# ----------------------------------------------------------------------
#

package Tachikoma::Job;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO PAYLOAD
    TM_ERROR TM_EOF
);
use Tachikoma::Nodes::FileHandle qw( TK_R setsockopts );
use Tachikoma::Nodes::STDIO;
use Tachikoma::Nodes::Callback;
use Tachikoma::Nodes::JobSpawnTimer;
use POSIX qw( F_SETFD dup2 );
use Socket;
use Scalar::Util qw( blessed );
use parent       qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.101');

my $SUDO = undef;
if ( -f '/usr/bin/sudo' ) {
    $SUDO = '/usr/bin/sudo';
}
elsif ( -f '/usr/local/bin/sudo' ) {
    $SUDO = '/usr/local/bin/sudo';
}

my @SPAWN_QUEUE = ();

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
    $self->{last_restart}   = undef;
    $self->{lazy}           = undef;
    $self->{username}       = undef;
    $self->{config_file}    = undef;
    $self->{original_name}  = undef;
    $self->{router}         = undef;
    $self->{spawn_buffer}   = undef;
    $self->{connector}      = undef;
    bless $self, $class;
    return $self;
}

sub prepare {
    my $self    = shift;
    my $options = shift;
    $self->{arguments}      = $options->{arguments};
    $self->{type}           = $options->{type};
    $self->{pid}            = q(-);
    $self->{lazy}           = 1;
    $self->{username}       = $options->{username};
    $self->{config_file}    = $options->{config_file};
    $self->{original_name}  = $options->{name};
    $self->{should_restart} = $options->{should_restart};
    $self->{connector}      = Tachikoma::Nodes::Callback->new;
    $self->{connector}->name( $options->{name} );
    $self->{connector}->callback(
        sub {
            my $message = shift;
            my $sink    = $self->{connector}->{sink};
            my $owner   = $self->{connector}->{owner};
            $self->_spawn($options);
            if ( $self->{connector}->{type} ) {
                $self->{connector}->sink($sink);
                $self->{connector}->owner($owner) if ( length $owner );
                $self->{connector}->fill($message);
            }
            return;
        }
    );
    return;
}

sub spawn {
    my $self    = shift;
    my $options = shift;
    $self->{arguments}      = $options->{arguments};
    $self->{type}           = $options->{type};
    $self->{pid}            = q(-);
    $self->{username}       = $options->{username};
    $self->{config_file}    = $options->{config_file};
    $self->{original_name}  = $options->{name};
    $self->{arguments}      = $options->{arguments};
    $self->{owner}          = $options->{owner};
    $self->{should_restart} = $options->{should_restart};
    $self->{spawn_buffer}   = [];
    $self->{connector}      = Tachikoma::Nodes::Callback->new;
    $self->{connector}->name( $options->{name} );
    $self->{connector}->callback(
        sub {
            my $message = shift;
            push @{ $self->{spawn_buffer} }, $message;
            return;
        }
    );
    if ( not Tachikoma->nodes->{'_spawn_timer'} ) {
        my $spawn_timer = Tachikoma::Nodes::JobSpawnTimer->new;
        $spawn_timer->name('_spawn_timer');
        $spawn_timer->set_timer(250);
        $spawn_timer->sink($self);
        Tachikoma->nodes->{'_spawn_timer'} = $spawn_timer;
    }
    push @SPAWN_QUEUE, sub {
        my $sink  = $self->{connector}->{sink};
        my $owner = $self->{connector}->{owner};
        $self->_spawn($options);
        if ( $self->{connector}->{type} ) {
            $self->{connector}->sink($sink);
            $self->{connector}->owner($owner) if ( length $owner );
            $self->{connector}->fill($_) for ( @{ $self->{spawn_buffer} } );
            $self->{spawn_buffer} = undef;
        }
        return;
    };
    return;
}

sub _spawn {
    my $self        = shift;
    my $options     = shift;
    my $filehandles = {
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
        $self->connect_parent( $filehandles, $options->{name} );
        $self->{pid}          = $pid;
        $self->{last_restart} = $Tachikoma::Now;
        return;
    }
    else {
        my $location      = $self->configuration->prefix || '/usr/local/bin';
        my $tachikoma_job = join q(), $location, '/tachikoma-job';
        my $type          = ( $options->{type}        =~ m{^([\w:]+)$} )[0];
        my $username      = ( $options->{username}    =~ m{^(\S*)$} )[0];
        my $config_file   = ( $options->{config_file} =~ m{^(\S*)$} )[0];
        my $name          = ( $options->{name}        =~ m{^(\S*)$} )[0];
        my $arguments     = ( $options->{arguments}   =~ m{^(.*)$}s )[0];

        # search for module here in case we're sudoing a job without config
        my $class = $self->determine_class($type) or exit 1;

        # connect child filehandles
        $self->connect_child($filehandles);

        # bin/tachikoma-job will handle the rest
        my @command = ();
        if ( $username and $username ne ( getpwuid $< )[0] ) {
            push @command, $SUDO, '-u', $username, '-C', FD_5 + 1;
        }
        push @command, $tachikoma_job, $config_file, $class, $name,
            $arguments;
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
    my ( $self, $filehandles, $name ) = @_;

    # make a FileHandle to send/receive messages over the socketpair()
    close $filehandles->{child}->{socket} or die $!;
    $self->make_socketpair( $filehandles->{parent}->{socket}, $name );

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
    my ( $self, $parent, $name ) = @_;
    $self->{connector}->remove_node if ( $self->{connector} );
    $self->{connector} =
        Tachikoma::Nodes::FileHandle->filehandle( $parent, TK_R );
    $self->{connector}->name($name);
    $self->{connector}->{type} = 'job';
    return;
}

sub make_stdio {
    my ( $self, $type, $parent_stdio, $name ) = @_;
    my $node = Tachikoma::Nodes::STDIO->filehandle( $parent_stdio, TK_R );

    # give them names, but don't register them in %Tachikoma::Nodes:
    $node->{name} = join q(:), $name, $type;
    $node->buffer_mode('line-buffered');
    $node->sink( Tachikoma::Nodes::Callback->new );
    $node->{sink}->callback(
        sub {
            my $message = shift;
            if ( not $message->[TYPE] & TM_EOF ) {
                $self->{connector}->stderr( $message->[PAYLOAD] );
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
    for my $prefix ( @{ $self->configuration->include_jobs },
        'Tachikoma::Jobs' )
    {
        next if ( not $prefix );
        $class = join q(::), $prefix, $type;
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
    $message->[TO] = '_parent' if ( not length $message->[TO] );
    $self->{counter}++;
    $rv = $self->{sink}->fill($message)
        if ( not $message->[TYPE] & TM_EOF
        or $message->[FROM] ne '_parent' );
    return $rv;
}

sub execute {
    my $self        = shift;
    my $safe_part   = shift                                           || q();
    my $unsafe_part = shift                                           || q();
    my $now_safe = ( $unsafe_part =~ m{^([\d\s\w@%"'/:,.=_-]*)$} )[0] || q();
    if ( $unsafe_part eq $now_safe ) {
        my $command = join q( ), $safe_part, $now_safe;
        local $ENV{ENV}  = q();
        local $ENV{PATH} = '/bin:/usr/bin:/sbin:/usr/sbin:/usr/local/bin';
        local $/         = undef;
        my $rv = `$command 2>&1`;    ## no critic (ProhibitBacktickOperators)
        return $rv;
    }
    return qq(bad command: "$safe_part $unsafe_part"\n);
}

sub execute_unsafe {
    my $self        = shift;
    my $safe_part   = shift                            || q();
    my $unsafe_part = shift                            || q();
    my $now_safe    = ( $unsafe_part =~ m{^(.*)$} )[0] || q();
    if ( $unsafe_part eq $now_safe ) {
        my $command = join q( ), $safe_part, $now_safe;
        local $ENV{ENV}  = q();
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

sub last_restart {
    my $self = shift;
    if (@_) {
        $self->{last_restart} = shift;
    }
    return $self->{last_restart};
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

sub spawn_buffer {
    my $self = shift;
    if (@_) {
        $self->{spawn_buffer} = shift;
    }
    return $self->{spawn_buffer};
}

sub connector {
    my $self = shift;
    if (@_) {
        $self->{connector} = shift;
    }
    return $self->{connector};
}

sub spawn_queue {
    my $self = shift;
    return \@SPAWN_QUEUE;
}

1;
