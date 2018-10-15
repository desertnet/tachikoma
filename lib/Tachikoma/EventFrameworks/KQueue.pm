#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::EventFrameworks::KQueue
# ----------------------------------------------------------------------
#
# $Id: KQueue.pm 35226 2018-10-15 10:24:26Z chris $
#

package Tachikoma::EventFrameworks::KQueue;
use strict;
use warnings;
use Tachikoma::Config qw( %Tachikoma );
use IO::KQueue;
use POSIX qw( :sys_wait_h SIGCHLD SIGHUP SIGUSR1 SIGINT SIGTERM );
use Time::HiRes;

use version; our $VERSION = 'v2.0.227';

use constant {
    INTERVAL  => 0,
    ONESHOT   => 1,
    LAST_FIRE => 2,
};

my $KQueue        = undef;
my %Timers        = ();
my $EvFilt_Read   = EVFILT_READ;
my $EvFilt_Write  = EVFILT_WRITE;
my $EvFilt_Timer  = EVFILT_TIMER;
my $EvFilt_Vnode  = EVFILT_VNODE;
my $EvFilt_Proc   = EVFILT_PROC;
my $EvFilt_Signal = EVFILT_SIGNAL;

sub new {
    my $class = shift;
    my $self  = {};
    $KQueue = IO::KQueue->new;
    bless $self, $class;
    return $self;
}

sub register_router_node {
    my ( $self, $this ) = @_;
    $KQueue->EV_SET( SIGINT,  EVFILT_SIGNAL, EV_ADD );
    $KQueue->EV_SET( SIGTERM, EVFILT_SIGNAL, EV_ADD );
    $KQueue->EV_SET( SIGHUP,  EVFILT_SIGNAL, EV_ADD );
    $KQueue->EV_SET( SIGUSR1, EVFILT_SIGNAL, EV_ADD );
    return $this;
}

sub register_server_node {
    my ( $self, $this ) = @_;
    $KQueue->EV_SET( $this->{fd}, EVFILT_READ, EV_ADD );
    return $this;
}

sub accept_connections {
    my ( $self, $this, $kev ) = @_;
    $this->accept_connection while ( $kev->[KQ_DATA]-- > 0 );
    return;
}

sub register_reader_node {
    my ( $self, $this ) = @_;
    $KQueue->EV_SET( $this->{fd}, EVFILT_READ, EV_ADD )
        if ( defined $this->{fd} );
    return $this;
}

sub register_writer_node {
    my ( $self, $this ) = @_;
    return if ( not defined $this->{fd} );
    my $okay = eval {
        $KQueue->EV_SET( $this->{fd}, EVFILT_WRITE, EV_ADD );
        return 1;
    };
    if ( not $okay ) {
        my $error = $@;
        $error =~ s{ at \S+/KQueue[.]pm line \d+[.]$}{};
        $this->stderr("WARNING: register_writer_node failed: $@");
        return $this->handle_EOF;
    }
    return $this;
}

sub register_watcher_node {
    my ( $self, $this, @args ) = @_;
    my %watch = map { $_ => 1 } @args;
    my $notes = 0;
    $notes |= NOTE_DELETE if ( $watch{delete} );
    $notes |= NOTE_RENAME if ( $watch{rename} );
    if ($notes) {
        $KQueue->EV_SET( $this->{fd}, EVFILT_VNODE, EV_ADD, $notes );
    }
    else {
        die "ERROR: no watches specified\n";
    }
    return $this;
}

sub drain {
    my ( $self, $this, $connector ) = @_;
    my %index = (
        $EvFilt_Read  => $Tachikoma::Nodes_By_FD,
        $EvFilt_Write => $Tachikoma::Nodes_By_FD,
        $EvFilt_Timer => $Tachikoma::Nodes_By_ID,
        $EvFilt_Vnode => $Tachikoma::Nodes_By_FD,
        $EvFilt_Proc  => $Tachikoma::Nodes_By_PID,
    );
    my %methods = (
        $EvFilt_Read  => 'drain_fh',
        $EvFilt_Write => 'fill_fh',
        $EvFilt_Timer => 'fire',
        $EvFilt_Vnode => 'note_fh',
        $EvFilt_Proc  => 'note_fh',
    );
    my $timeout = undef;
    my @events  = ();
    while ( $connector ? $connector->{fh} : $this->{name} ) {
        $timeout = keys %Timers ? 1000 / ( $Tachikoma{Hz} || 10 ) : 60000;
        @events = $KQueue->kevent($timeout);
        $Tachikoma::Right_Now = Time::HiRes::time;
        $Tachikoma::Now       = int $Tachikoma::Right_Now;
        for my $kev (@events) {
            if ( $kev->[KQ_FILTER] == EVFILT_SIGNAL ) {
                $self->handle_signal( $this, $kev->[KQ_IDENT] );
                next;
            }
            my $method = $methods{ $kev->[KQ_FILTER] } or next;
            my $node = $index{ $kev->[KQ_FILTER] }->{ $kev->[KQ_IDENT] };
            if ( $kev->[KQ_FLAGS] == EV_ERROR or not $node ) {
                $self->handle_error( $this, \%methods, \%index, $kev );
                next;
            }
            elsif ( $kev->[KQ_FILTER] == EVFILT_TIMER ) {
                $node->{timer_is_active} = undef
                    if ( ( $node->{timer_is_active} // q{} ) ne 'forever' );
                $node->fire;
                next;
            }
            &{ $node->{$method} }( $node, $kev );
        }
        while ( my $close_cb = shift @Tachikoma::Closing ) {
            &{$close_cb}();
        }
        for my $id ( keys %Timers ) {
            my $timer = $Timers{$id} or next;
            if ( $Tachikoma::Right_Now - $timer->[LAST_FIRE]
                >= $timer->[INTERVAL] / 1000 )
            {
                my $node = $Tachikoma::Nodes_By_ID->{$id};
                if ( not $node ) {
                    delete $Timers{$id};
                    next;
                }
                elsif ( $timer->[ONESHOT] ) {
                    $node->{timer_is_active} = undef;
                    delete $Timers{$id};
                }
                else {
                    $timer->[LAST_FIRE] = $Tachikoma::Right_Now;
                }
                $node->fire;
            }
        }
    }
    return;
}

sub handle_signal {
    my ( $self, $this, $id ) = @_;
    if ( $id == SIGCHLD ) {
        do { } while ( waitpid( -1, WNOHANG ) > 0 );
    }
    elsif ( $id == SIGHUP ) {
        Tachikoma->touch_log_file if ( $$ == Tachikoma->my_pid );
        $this->stderr('got SIGHUP - sending SIGUSR1');
        my $usr1 = SIGUSR1;
        kill -$usr1, $$ or die q{FAILURE: couldn't signal self};
    }
    elsif ( $id == SIGUSR1 ) {
        $this->stderr('got SIGUSR1 - reloading config');
        Tachikoma->reload_config;
    }
    else {
        $this->stderr('shutting down - received signal');
        $this->shutdown_all_nodes;
    }
    return;
}

sub handle_error {
    my ( $self, $this, $methods, $index, $kev ) = @_;
    my $method = $methods->{ $kev->[KQ_FILTER] };
    my $node   = $index->{ $kev->[KQ_FILTER] }->{ $kev->[KQ_IDENT] };
    if ( $kev->[KQ_FLAGS] == EV_ERROR ) {
        my $error = $kev->[KQ_DATA];
        $this->stderr(
            "WARNING: $method() error kevent for ",
            ( $node ? $node->name : $kev->[KQ_IDENT] ),
            $error ? q{: } . $error : q{}
        );
    }
    POSIX::close( $kev->[KQ_IDENT] )
        if ( $index->{ $kev->[KQ_FILTER] } eq $Tachikoma::Nodes_By_FD );
    return;
}

sub close_filehandle {
    my ( $self, $this ) = @_;
    delete $Timers{ $this->{id} } if ( defined $this->{id} );
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQueue->EV_SET( $this->{pid}, EVFILT_PROC, EV_DELETE ) }
        if ( defined $this->{pid} );
    return;
}

sub unregister_reader_node {
    my ( $self, $this ) = @_;
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQueue->EV_SET( $this->{fd}, EVFILT_READ, EV_DELETE ) }
        if ( defined $this->{fd} );
    return;
}

sub unregister_writer_node {
    my ( $self, $this ) = @_;
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQueue->EV_SET( $this->{fd}, EVFILT_WRITE, EV_DELETE ) }
        if ( defined $this->{fd} );
    return;
}

sub unregister_watcher_node {
    my ( $self, $this ) = @_;
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQueue->EV_SET( $this->{fd}, EVFILT_VNODE, EV_DELETE ) }
        if ( defined $this->{fd} );
    return;
}

sub watch_for_signal {
    my ( $self, $signal ) = @_;
    $KQueue->EV_SET( $signal, EVFILT_SIGNAL, EV_ADD );
    return;
}

sub set_timer {
    my ( $self, $this, $interval, $oneshot ) = @_;
    $interval ||= 0;
    my $id = $this->{id};
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQueue->EV_SET( $id, EVFILT_TIMER, EV_DELETE ) };
    if ( $interval >= 1 ) {
        delete $Timers{$id};
        my $okay = eval {
            $KQueue->EV_SET( $id, EVFILT_TIMER,
                $oneshot ? EV_ADD | EV_ONESHOT : EV_ADD,
                0, $interval );
            return 1;
        };
        if ( not $okay ) {
            my $error = $@;
            chomp $error;
            $this->print_less_often(
                "WARNING: $error - falling back to internal timer");
            $Timers{$id} = [
                $interval, $oneshot,
                $Tachikoma::Right_Now || Time::HiRes::time
            ];
        }
    }
    else {
        $Timers{$id} = [
            $interval, $oneshot,
            $Tachikoma::Right_Now || Time::HiRes::time
        ];
    }
    return;
}

sub stop_timer {
    my ( $self, $this ) = @_;
    my $id = $this->{id};
    return if ( not defined $id );
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQueue->EV_SET( $id, EVFILT_TIMER, EV_DELETE ) };
    delete $Timers{$id};
    return;
}

sub queue {
    return $KQueue;
}

1;
