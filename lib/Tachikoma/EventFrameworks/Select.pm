#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::EventFrameworks::Select
# ----------------------------------------------------------------------
#
# $Id: Select.pm 35051 2018-10-10 23:03:53Z chris $
#

package Tachikoma::EventFrameworks::Select;
use strict;
use warnings;
use Tachikoma::Nodes::FileHandle qw( TK_W );
use Tachikoma::Config qw( %Tachikoma );
use IO::Select;
use POSIX qw( :sys_wait_h SIGUSR1 );
use Time::HiRes;

use version; our $VERSION = 'v2.0.227';

use constant {
    INTERVAL  => 0,
    ONESHOT   => 1,
    LAST_FIRE => 2,
};

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
    $Reads->add( $this->{fh} );
    return $this;
}

sub accept_connections {
    my ( $self, $this ) = @_;
    $this->accept_connection;
    return;
}

sub register_reader_node {
    my ( $self, $this ) = @_;
    $Reads->add( $this->{fh} ) if ( $this->{fh} );
    return $this;
}

sub register_writer_node {
    my ( $self, $this ) = @_;
    $Writes->add( $this->{fh} ) if ( $this->{fh} );
    return $this;
}

sub register_watcher_node {
    my ( $self, $this ) = @_;
    return $this;
}

sub drain {    ## no critic (ProhibitExcessComplexity)
    my ( $self, $this, $connector ) = @_;
    while ( $connector ? $connector->{fh} : $this->{name} ) {
        my ( $reads, $writes, $errors ) =
            IO::Select->select( $Reads, $Writes, $Reads,
            1 / ( $Tachikoma{Hz} || 10 ) );
        $Tachikoma::Right_Now = Time::HiRes::time;
        $Tachikoma::Now       = int $Tachikoma::Right_Now;
        for my $fh ( @{$reads}, @{$errors} ) {
            my $fd = fileno $fh;
            next if ( not defined $fd );
            my $node = $Tachikoma::Nodes_By_FD->{$fd};
            if ( not $node ) {
                POSIX::close($fd);
                next;
            }
            $node->{last_drain} = $Tachikoma::Right_Now;
            &{ $node->{drain_fh} }($node);
        }
        for my $fh ( @{$writes} ) {
            my $fd = fileno $fh;
            next if ( not defined $fd );
            my $node = $Tachikoma::Nodes_By_FD->{$fd};
            if ( not $node ) {
                POSIX::close($fd);
                next;
            }
            &{ $node->{fill_fh} }($node);
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
        Tachikoma->close_log_file;
        Tachikoma->open_log_file;
        $this->stderr('got SIGHUP - sending SIGUSR1');
        my $usr1 = SIGUSR1;
        kill -$usr1, $$ or die q{FAILURE: couldn't signal self};
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
    $Reads->remove( $this->{fh} )  if ( $Reads->exists( $this->{fh} ) );
    $Writes->remove( $this->{fh} ) if ( $Writes->exists( $this->{fh} ) );
    delete $Timers{ $this->{id} }  if ( defined $this->{id} );
    return;
}

sub unregister_reader_node {
    my ( $self, $this ) = @_;
    $Reads->remove( $this->{fh} ) if ( $Reads->exists( $this->{fh} ) );
    return;
}

sub unregister_writer_node {
    my ( $self, $this ) = @_;
    $Writes->remove( $this->{fh} ) if ( $Writes->exists( $this->{fh} ) );
    return;
}

sub unregister_watcher_node {
    my ( $self, $this ) = @_;
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

sub queue {
    return $Reads, $Writes;
}

1;
