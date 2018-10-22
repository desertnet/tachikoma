#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::EventFrameworks::Epoll
# ----------------------------------------------------------------------
#
# $Id: Epoll.pm 35512 2018-10-22 08:27:21Z chris $
#

package Tachikoma::EventFrameworks::Epoll;
use strict;
use warnings;
use Tachikoma::Nodes::FileHandle qw( TK_R TK_W TK_EPOLLED );
use Tachikoma::Config qw( %Tachikoma );
use IO::Epoll;
use Errno;
use IO::Select;
use POSIX qw( :sys_wait_h SIGUSR1 );
use Time::HiRes;

use version; our $VERSION = qv('v2.0.227');

use constant {
    INTERVAL  => 0,
    ONESHOT   => 1,
    LAST_FIRE => 2,
};

my $Epoll         = undef;
my $Reads         = undef;
my $Writes        = undef;
my %Timers        = ();
my $Last_Wait     = 0;
my $Got_Signal    = undef;
my $Shutdown      = undef;
my $Got_HUP       = undef;
my $Reload_Config = undef;

sub new {
    my $class = shift;
    my $self  = {};
    $Epoll  = epoll_create(16);
    $Reads  = IO::Select->new;
    $Writes = IO::Select->new;
    bless $self, $class;
    return $self;
}

sub register_router_node {
    my ( $self, $this ) = @_;
    ## no critic (RequireLocalizedPunctuationVars)
    $SIG{INT}  = sub { $Got_Signal = $Shutdown      = 1 };
    $SIG{TERM} = sub { $Got_Signal = $Shutdown      = 1 };
    $SIG{HUP}  = sub { $Got_Signal = $Got_HUP       = 1 };
    $SIG{USR1} = sub { $Got_Signal = $Reload_Config = 1 };
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
        $Reads->add( $this->{fh} );
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
        $Writes->add( $this->{fh} );
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
    while ( $connector ? $connector->{fh} : $this->{name} ) {
        my $check_select = $Reads->count or $Writes->count;
        my $events = epoll_wait( $Epoll, 256, $check_select
            ? 0
            : 1000 / ( $Tachikoma{Hz} || 10 ) );
        if ( not $events ) {
            die "ERROR: epoll_wait: $!"
                if ( $! and $! != Errno::EINTR and $! != Errno::EINVAL );
            $this->stderr("WARNING: epoll_wait: $!") if ($!);
        }
        if ($check_select) {
            my ( $reads, $writes, $errors ) =
                IO::Select->select( $Reads, $Writes, $Reads, 0 );
            push @{$events}, [ fileno $_ => EPOLLIN ]
                for ( @{$reads}, @{$errors} );
            push @{$events}, [ fileno $_ => EPOLLOUT ] for ( @{$writes} );
        }
        $Tachikoma::Right_Now = Time::HiRes::time;
        $Tachikoma::Now       = int $Tachikoma::Right_Now;
        for my $event ( @{$events} ) {
            my ( $fd, $types ) = @{$event};
            my $node = $Tachikoma::Nodes_By_FD->{$fd};
            if ( not $node ) {
                POSIX::close($fd);
                next;
            }
            &{ $node->{drain_fh} }($node) if ( $types & EPOLLIN );
            &{ $node->{fill_fh} }($node)  if ( $types & EPOLLOUT );
        }
        $self->handle_signal($this) if ($Got_Signal);
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
                if ( $timer->[ONESHOT] ) {
                    $node->{timer_is_active} = undef;
                    delete $Timers{$id};
                }
                else {
                    $timer->[LAST_FIRE] = $Tachikoma::Right_Now;
                }
                $node->fire;
            }
        }
        if ( $Tachikoma::Right_Now - $Last_Wait > 5 ) {
            $Last_Wait = $Tachikoma::Right_Now;
            undef $!;
            do { } while ( waitpid( -1, WNOHANG ) > 0 );
        }
    }
    return;
}

sub handle_signal {
    my ( $self, $this ) = @_;
    if ($Shutdown) {
        $this->stderr('shutting down - received signal');
        $this->shutdown_all_nodes;
    }
    if ($Got_HUP) {
        Tachikoma->touch_log_file if ( $$ == Tachikoma->my_pid );
        $this->stderr('got SIGHUP - sending SIGUSR1');
        my $usr1 = SIGUSR1;
        kill -$usr1, $$ or die q(FAILURE: couldn't signal self);
        $Got_HUP = undef;
    }
    if ($Reload_Config) {
        $this->stderr('got SIGUSR1 - reloading config');
        Tachikoma->reload_config;
        $this->register_router_node;
        $Reload_Config = undef;
    }
    return;
}

sub close_filehandle {
    my ( $self, $this ) = @_;
    if ( defined $this->{fd} ) {
        epoll_ctl( $Epoll, EPOLL_CTL_DEL, $this->{fd}, 0 );
        $Reads->remove( $this->{fh} )  if ( $Reads->exists( $this->{fh} ) );
        $Writes->remove( $this->{fh} ) if ( $Writes->exists( $this->{fh} ) );
    }
    delete $Timers{ $this->{id} } if ( defined $this->{id} );
    return;
}

sub unregister_reader_node {
    my ( $self, $this ) = @_;
    if ( $this->{type} eq 'regular_file' ) {
        $Reads->remove( $this->{fh} )
            if ( defined $this->{fh} and $Reads->exists( $this->{fh} ) );
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
        $Writes->remove( $this->{fh} )
            if ( defined $this->{fh} and $Writes->exists( $this->{fh} ) );
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
        epoll_ctl( $Epoll, EPOLL_CTL_ADD, $this->{fd}, $eflags );
        $this->{flags} |= TK_EPOLLED;
    }
    else {
        epoll_ctl( $Epoll, EPOLL_CTL_MOD, $this->{fd}, $eflags );
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
    $Timers{ $this->{id} } =
        [ $interval, $oneshot, $Tachikoma::Right_Now || Time::HiRes::time ];
    return;
}

sub stop_timer {
    my ( $self, $this ) = @_;
    delete $Timers{ $this->{id} } if ( defined $this->{id} );
    return;
}

sub timers {
    return \%Timers;
}

sub queue {
    return $Epoll, $Reads, $Writes;
}

1;
