#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::EventFrameworks::KQueue
# ----------------------------------------------------------------------
#
# $Id: KQueue.pm 35769 2018-11-02 08:37:19Z chris $
#

package Tachikoma::EventFrameworks::KQueue;
use strict;
use warnings;
use IO::KQueue;
use POSIX qw( :sys_wait_h SIGCHLD SIGHUP SIGUSR1 SIGINT SIGTERM );
use Time::HiRes;

use version; our $VERSION = qv('v2.0.227');

use constant {
    INTERVAL  => 0,
    ONESHOT   => 1,
    LAST_FIRE => 2,
};

my $KQUEUE        = undef;
my %TIMERS        = ();
my $EVFILT_READ   = EVFILT_READ;
my $EVFILT_WRITE  = EVFILT_WRITE;
my $EVFILT_TIMER  = EVFILT_TIMER;
my $EVFILT_VNODE  = EVFILT_VNODE;
my $EVFILT_PROC   = EVFILT_PROC;
my $EVFILT_SIGNAL = EVFILT_SIGNAL;

sub new {
    my $class = shift;
    my $self = { handle_signal => \&handle_signal, };
    $KQUEUE = IO::KQueue->new;
    bless $self, $class;
    return $self;
}

sub register_router_node {
    my ( $self, $this ) = @_;
    $KQUEUE->EV_SET( SIGINT,  EVFILT_SIGNAL, EV_ADD );
    $KQUEUE->EV_SET( SIGTERM, EVFILT_SIGNAL, EV_ADD );
    $KQUEUE->EV_SET( SIGHUP,  EVFILT_SIGNAL, EV_ADD );
    $KQUEUE->EV_SET( SIGUSR1, EVFILT_SIGNAL, EV_ADD );
    return $this;
}

sub register_server_node {
    my ( $self, $this ) = @_;
    $KQUEUE->EV_SET( $this->{fd}, EVFILT_READ, EV_ADD );
    return $this;
}

sub accept_connections {
    my ( $self, $this, $kev ) = @_;
    $this->accept_connection while ( $kev->[KQ_DATA]-- > 0 );
    return;
}

sub register_reader_node {
    my ( $self, $this ) = @_;
    $KQUEUE->EV_SET( $this->{fd}, EVFILT_READ, EV_ADD )
        if ( defined $this->{fd} );
    return $this;
}

sub register_writer_node {
    my ( $self, $this ) = @_;
    return if ( not defined $this->{fd} );
    my $okay = eval {
        $KQUEUE->EV_SET( $this->{fd}, EVFILT_WRITE, EV_ADD );
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ || 'unknown error';
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
        $KQUEUE->EV_SET( $this->{fd}, EVFILT_VNODE, EV_ADD, $notes );
    }
    else {
        die "ERROR: no watches specified\n";
    }
    return $this;
}

sub drain {
    my ( $self, $this, $connector ) = @_;
    my %index = (
        $EVFILT_READ   => $Tachikoma::Nodes_By_FD,
        $EVFILT_WRITE  => $Tachikoma::Nodes_By_FD,
        $EVFILT_TIMER  => $Tachikoma::Nodes_By_ID,
        $EVFILT_VNODE  => $Tachikoma::Nodes_By_FD,
        $EVFILT_PROC   => $Tachikoma::Nodes_By_PID,
        $EVFILT_SIGNAL => { map { $_ => $self } 1 .. 31 },
    );
    my %methods = (
        $EVFILT_READ   => 'drain_fh',
        $EVFILT_WRITE  => 'fill_fh',
        $EVFILT_TIMER  => 'fire_cb',
        $EVFILT_VNODE  => 'note_fh',
        $EVFILT_PROC   => 'note_fh',
        $EVFILT_SIGNAL => 'handle_signal',
    );
    my $configuration = $this->configuration;
    while ( $connector ? $connector->{fh} : $this->{name} ) {
        my @events = $KQUEUE->kevent( keys %TIMERS ? 100 : 60000 );
        $Tachikoma::Right_Now = Time::HiRes::time;
        $Tachikoma::Now       = int $Tachikoma::Right_Now;
        for (@events) {
            my $node = $index{ $_->[KQ_FILTER] }->{ $_->[KQ_IDENT] };
            &{ $node->{ $methods{ $_->[KQ_FILTER] } } }( $node, $_ );
        }
        &{$_}() while ( $_ = shift @Tachikoma::Closing );
        for ( keys %TIMERS ) {
            my $timer = $TIMERS{$_} or next;
            next
                if ( $Tachikoma::Right_Now - $timer->[LAST_FIRE]
                < $timer->[INTERVAL] / 1000 );
            my $node = $Tachikoma::Nodes_By_ID->{$_};
            if ( not $node ) {
                delete $TIMERS{$_};
                next;
            }
            elsif ( $timer->[ONESHOT] ) {
                delete $TIMERS{$_};
            }
            else {
                $timer->[LAST_FIRE] = $Tachikoma::Right_Now;
            }
            &{ $node->{fire_cb} }($node);
        }
    }
    return;
}

sub handle_signal {
    my ( $self, $kev ) = @_;
    my $id = $kev->[KQ_IDENT];
    if ( $id == SIGCHLD ) {
        do { } while ( waitpid( -1, WNOHANG ) > 0 );
    }
    elsif ( $id == SIGHUP ) {
        Tachikoma->touch_log_file if ( $$ == Tachikoma->my_pid );
        print {*STDERR} "got SIGHUP - sending SIGUSR1\n";
        my $usr1 = SIGUSR1;
        kill -$usr1, $$ or die q(FAILURE: couldn't signal self);
    }
    elsif ( $id == SIGUSR1 ) {
        print {*STDERR} "got SIGUSR1 - reloading config\n";
        Tachikoma->reload_config;
    }
    else {
        print {*STDERR} "shutting down - received signal\n";
        Tachikoma->shutdown_all_nodes;
    }
    return;
}

sub close_filehandle {
    my ( $self, $this ) = @_;
    delete $TIMERS{ $this->{id} } if ( defined $this->{id} );
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQUEUE->EV_SET( $this->{pid}, EVFILT_PROC, EV_DELETE ) }
        if ( defined $this->{pid} );
    return;
}

sub unregister_reader_node {
    my ( $self, $this ) = @_;
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQUEUE->EV_SET( $this->{fd}, EVFILT_READ, EV_DELETE ) }
        if ( defined $this->{fd} );
    return;
}

sub unregister_writer_node {
    my ( $self, $this ) = @_;
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQUEUE->EV_SET( $this->{fd}, EVFILT_WRITE, EV_DELETE ) }
        if ( defined $this->{fd} );
    return;
}

sub unregister_watcher_node {
    my ( $self, $this ) = @_;
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQUEUE->EV_SET( $this->{fd}, EVFILT_VNODE, EV_DELETE ) }
        if ( defined $this->{fd} );
    return;
}

sub watch_for_signal {
    my ( $self, $signal ) = @_;
    $KQUEUE->EV_SET( $signal, EVFILT_SIGNAL, EV_ADD );
    return;
}

sub set_timer {
    my ( $self, $this, $interval, $oneshot ) = @_;
    $interval ||= 0;
    my $id = $this->{id};
    ## no critic (RequireCheckingReturnValueOfEval)
    eval { $KQUEUE->EV_SET( $id, EVFILT_TIMER, EV_DELETE ) };
    if ( $interval >= 1 ) {
        delete $TIMERS{$id};
        my $okay = eval {
            $KQUEUE->EV_SET( $id, EVFILT_TIMER,
                $oneshot ? EV_ADD | EV_ONESHOT : EV_ADD,
                0, $interval );
            return 1;
        };
        if ( not $okay ) {
            my $error = $@ || 'unknown error';
            chomp $error;
            $this->print_less_often(
                "WARNING: $error - falling back to internal timer");
            $TIMERS{$id} = [
                $interval, $oneshot,
                $Tachikoma::Right_Now || Time::HiRes::time
            ];
        }
    }
    else {
        $TIMERS{$id} = [
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
    eval { $KQUEUE->EV_SET( $id, EVFILT_TIMER, EV_DELETE ) };
    delete $TIMERS{$id};
    return;
}

sub queue {
    return $KQUEUE;
}

1;
