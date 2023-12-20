#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::EventFrameworks::KQueue
# ----------------------------------------------------------------------
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

my $KQUEUE = undef;
my %TIMERS = ();

sub new {
    my $class = shift;
    my $self  = { handle_signal => \&handle_signal, };
    $KQUEUE = IO::KQueue->new;
    bless $self, $class;
    return $self;
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
    my ( $self, $router ) = @_;
    my $evfilt_read   = EVFILT_READ;
    my $evfilt_write  = EVFILT_WRITE;
    my $evfilt_timer  = EVFILT_TIMER;
    my $evfilt_vnode  = EVFILT_VNODE;
    my $evfilt_signal = EVFILT_SIGNAL;
    my %index         = (
        $evfilt_read   => $Tachikoma::Nodes_By_FD,
        $evfilt_write  => $Tachikoma::Nodes_By_FD,
        $evfilt_timer  => $Tachikoma::Nodes_By_ID,
        $evfilt_vnode  => $Tachikoma::Nodes_By_FD,
        $evfilt_signal => { map { $_ => $self } 1 .. 31 },
    );
    my %methods = (
        $evfilt_read   => 'drain_fh',
        $evfilt_write  => 'fill_fh',
        $evfilt_timer  => 'fire_cb',
        $evfilt_vnode  => 'note_fh',
        $evfilt_signal => 'handle_signal',
    );
    $KQUEUE->EV_SET( SIGINT,  EVFILT_SIGNAL, EV_ADD );
    $KQUEUE->EV_SET( SIGTERM, EVFILT_SIGNAL, EV_ADD );
    $KQUEUE->EV_SET( SIGHUP,  EVFILT_SIGNAL, EV_ADD );
    $KQUEUE->EV_SET( SIGUSR1, EVFILT_SIGNAL, EV_ADD );

    while ( $router->{is_active} ) {
        my @events = $KQUEUE->kevent( keys %TIMERS ? 0 : 60000 );
        $Tachikoma::Right_Now = Time::HiRes::time;
        $Tachikoma::Now       = int $Tachikoma::Right_Now;
        for (@events) {
            my $node = $index{ $_->[KQ_FILTER] }->{ $_->[KQ_IDENT] } or next;
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
        Tachikoma->stderr('got SIGHUP - sending SIGUSR1');
        my $usr1 = SIGUSR1;
        kill -$usr1, $$ or die q(FAILURE: couldn't signal self);
    }
    elsif ( $id == SIGUSR1 ) {
        Tachikoma->stderr('got SIGUSR1 - reloading config');
        Tachikoma->reload_config;
    }
    else {
        Tachikoma->stderr('received signal');
        Tachikoma->shutdown_all_nodes;
    }
    return;
}

sub close_filehandle {
    my ( $self, $this ) = @_;
    delete $TIMERS{ $this->{id} } if ( defined $this->{id} );
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
