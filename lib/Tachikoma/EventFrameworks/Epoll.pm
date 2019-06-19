#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::EventFrameworks::Epoll
# ----------------------------------------------------------------------
#
# $Id: Epoll.pm 37661 2019-06-19 00:33:01Z chris $
#

package Tachikoma::EventFrameworks::Epoll;
use strict;
use warnings;
use Tachikoma::Nodes::FileHandle qw( TK_R TK_W TK_EPOLLED );
use IO::Epoll;
use IO::Select;
use Errno;
use POSIX qw( :sys_wait_h SIGUSR1 );
use Time::HiRes;

use version; our $VERSION = qv('v2.0.227');

use constant {
    INTERVAL   => 0,
    ONESHOT    => 1,
    LAST_FIRE  => 2,
    EVENT_FD   => 0,
    EVENT_TYPE => 1,
};

my $EPOLL         = undef;
my $READS         = undef;
my $WRITES        = undef;
my %TIMERS        = ();
my $LAST_WAIT     = 0;
my $GOT_SIGNAL    = undef;
my $SHUTDOWN      = undef;
my $GOT_HUP       = undef;
my $RELOAD_CONFIG = undef;

sub new {
    my $class = shift;
    my $self  = {};
    $EPOLL  = epoll_create(16);
    $READS  = IO::Select->new;
    $WRITES = IO::Select->new;
    bless $self, $class;
    return $self;
}

sub register_router_node {
    my ( $self, $this ) = @_;
    ## no critic (RequireLocalizedPunctuationVars)
    $SIG{INT}  = sub { $GOT_SIGNAL = $SHUTDOWN      = 1 };
    $SIG{TERM} = sub { $GOT_SIGNAL = $SHUTDOWN      = 1 };
    $SIG{HUP}  = sub { $GOT_SIGNAL = $GOT_HUP       = 1 };
    $SIG{USR1} = sub { $GOT_SIGNAL = $RELOAD_CONFIG = 1 };
    return $this;
}

sub register_server_node {
    my ( $self, $this ) = @_;
    $self->set_epoll_flags( $this, EPOLLIN );
    return $this;
}

sub accept_connections {
    my ( $self, $this ) = @_;
    $this->accept_connection;
    return;
}

sub register_reader_node {
    my ( $self, $this ) = @_;
    return if ( not $this->{fh} );
    if ( $this->{type} eq 'regular_file' ) {
        $READS->add( $this->{fh} );
    }
    else {
        my $eflags = EPOLLIN;
        $eflags |= EPOLLOUT if ( $this->{flags} & TK_W );
        $self->set_epoll_flags( $this, $eflags );
    }
    return $this;
}

sub register_writer_node {
    my ( $self, $this ) = @_;
    return if ( not defined $this->{fd} );
    if ( $this->{type} eq 'regular_file' ) {
        $WRITES->add( $this->{fh} );
    }
    else {
        my $eflags = EPOLLOUT;
        $eflags |= EPOLLIN if ( $this->{flags} & TK_R );
        $self->set_epoll_flags( $this, $eflags );
    }
    return $this;
}

sub register_watcher_node {
    my ( $self, $this ) = @_;
    return $this;
}

sub drain {    ## no critic (ProhibitExcessComplexity)
    my ( $self, $this, $connector ) = @_;
    my $configuration = $this->configuration;
    while ( $connector ? $connector->{fh} : $this->{name} ) {
        my $check_select = $READS->count or $WRITES->count;
        my $events = epoll_wait( $EPOLL, 256, $check_select
            ? 0
            : 1000 / ( $configuration->{hz} || 10 ) );
        if ( not $events ) {
            die "ERROR: epoll_wait: $!"
                if ( $! and $! != Errno::EINTR and $! != Errno::EINVAL );
            $this->stderr("WARNING: epoll_wait: $!") if ($!);
        }
        if ($check_select) {
            my ( $reads, $writes, $errors ) =
                IO::Select->select( $READS, $WRITES, $READS, 0 );
            push @{$events}, [ fileno $_ => EPOLLIN ]
                for ( @{$reads}, @{$errors} );
            push @{$events}, [ fileno $_ => EPOLLOUT ] for ( @{$writes} );
        }
        $Tachikoma::Right_Now = Time::HiRes::time;
        $Tachikoma::Now       = int $Tachikoma::Right_Now;
        for ( @{$events} ) {
            my $node = $Tachikoma::Nodes_By_FD->{ $_->[EVENT_FD] };
            &{ $node->{drain_fh} }($node) if ( $_->[EVENT_TYPE] & EPOLLIN );
            &{ $node->{fill_fh} }($node)  if ( $_->[EVENT_TYPE] & EPOLLOUT );
        }
        $self->handle_signal($this) if ($GOT_SIGNAL);
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
        if ( $Tachikoma::Right_Now - $LAST_WAIT > 5 ) {
            $LAST_WAIT = $Tachikoma::Right_Now;
            undef $!;
            do { } while ( waitpid( -1, WNOHANG ) > 0 );
        }
    }
    return;
}

sub handle_signal {
    my ( $self, $this ) = @_;
    if ($SHUTDOWN) {
        $this->stderr('shutting down - received signal');
        $this->shutdown_all_nodes;
    }
    if ($GOT_HUP) {
        Tachikoma->touch_log_file if ( $$ == Tachikoma->my_pid );
        $this->stderr('got SIGHUP - sending SIGUSR1');
        my $usr1 = SIGUSR1;
        kill -$usr1, $$ or die q(FAILURE: couldn't signal self);
        $GOT_HUP = undef;
    }
    if ($RELOAD_CONFIG) {
        $this->stderr('got SIGUSR1 - reloading config');
        Tachikoma->reload_config;
        $this->register_router_node;
        $RELOAD_CONFIG = undef;
    }
    return;
}

sub close_filehandle {
    my ( $self, $this ) = @_;
    if ( defined $this->{fd} ) {
        epoll_ctl( $EPOLL, EPOLL_CTL_DEL, $this->{fd}, 0 );
        $READS->remove( $this->{fh} )  if ( $READS->exists( $this->{fh} ) );
        $WRITES->remove( $this->{fh} ) if ( $WRITES->exists( $this->{fh} ) );
    }
    delete $TIMERS{ $this->{id} } if ( defined $this->{id} );
    return;
}

sub unregister_reader_node {
    my ( $self, $this ) = @_;
    if ( $this->{type} eq 'regular_file' ) {
        $READS->remove( $this->{fh} )
            if ( defined $this->{fh} and $READS->exists( $this->{fh} ) );
    }
    elsif ( defined $this->{fd} ) {
        my $eflags = $this->{flags} & TK_W ? EPOLLOUT : 0;
        $self->set_epoll_flags( $this, $eflags );
    }
    return;
}

sub unregister_writer_node {
    my ( $self, $this ) = @_;
    if ( $this->{type} eq 'regular_file' ) {
        $WRITES->remove( $this->{fh} )
            if ( defined $this->{fh} and $WRITES->exists( $this->{fh} ) );
    }
    elsif ( defined $this->{fd} ) {
        my $eflags = $this->{flags} & TK_R ? EPOLLIN : 0;
        $self->set_epoll_flags( $this, $eflags );
    }
    return;
}

sub unregister_watcher_node {
    my ( $self, $this ) = @_;
    return;
}

sub set_epoll_flags {
    my ( $self, $this, $eflags ) = @_;
    if ( not $this->{flags} & TK_EPOLLED ) {
        epoll_ctl( $EPOLL, EPOLL_CTL_ADD, $this->{fd}, $eflags );
        $this->{flags} |= TK_EPOLLED;
    }
    else {
        epoll_ctl( $EPOLL, EPOLL_CTL_MOD, $this->{fd}, $eflags );
    }
    return;
}

sub watch_for_signal {
    my ( $self, $signal ) = @_;
    return;
}

sub set_timer {
    my ( $self, $this, $interval, $oneshot ) = @_;
    $interval ||= 0;
    $TIMERS{ $this->{id} } =
        [ $interval, $oneshot, $Tachikoma::Right_Now || Time::HiRes::time ];
    return;
}

sub stop_timer {
    my ( $self, $this ) = @_;
    delete $TIMERS{ $this->{id} } if ( defined $this->{id} );
    return;
}

sub queue {
    return $EPOLL, $READS, $WRITES;
}

1;
