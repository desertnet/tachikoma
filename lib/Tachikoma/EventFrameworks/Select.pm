#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::EventFrameworks::Select
# ----------------------------------------------------------------------
#
# $Id: Select.pm 35752 2018-11-01 09:37:37Z chris $
#

package Tachikoma::EventFrameworks::Select;
use strict;
use warnings;
use Tachikoma::Nodes::FileHandle qw( TK_W );
use IO::Select;
use POSIX qw( :sys_wait_h SIGUSR1 );
use Time::HiRes;

use version; our $VERSION = qv('v2.0.227');

use constant {
    INTERVAL  => 0,
    ONESHOT   => 1,
    LAST_FIRE => 2,
};

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
    $READS->add( $this->{fh} );
    return $this;
}

sub accept_connections {
    my ( $self, $this ) = @_;
    $this->accept_connection;
    return;
}

sub register_reader_node {
    my ( $self, $this ) = @_;
    $READS->add( $this->{fh} ) if ( $this->{fh} );
    return $this;
}

sub register_writer_node {
    my ( $self, $this ) = @_;
    $WRITES->add( $this->{fh} ) if ( $this->{fh} );
    return $this;
}

sub register_watcher_node {
    my ( $self, $this ) = @_;
    return $this;
}

sub drain {
    my ( $self, $this, $connector ) = @_;
    my $configuration = $this->configuration;
    while ( $connector ? $connector->{fh} : $this->{name} ) {
        my ( $reads, $writes, $errors ) =
            IO::Select->select( $READS, $WRITES, $READS,
            1 / ( $configuration->{hz} || 10 ) );
        $Tachikoma::Right_Now = Time::HiRes::time;
        $Tachikoma::Now       = int $Tachikoma::Right_Now;
        for ( @{$reads}, @{$errors} ) {
            my $fd = fileno $_;
            next if ( not defined $fd );
            my $node = $Tachikoma::Nodes_By_FD->{$fd};
            &{ $node->{drain_fh} }($node);
        }
        for ( @{$writes} ) {
            my $fd = fileno $_;
            next if ( not defined $fd );
            my $node = $Tachikoma::Nodes_By_FD->{$fd};
            &{ $node->{fill_fh} }($node);
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
                $node->{timer_is_active} = undef;
                delete $TIMERS{$_};
            }
            else {
                $timer->[LAST_FIRE] = $Tachikoma::Right_Now;
            }
            $node->fire;
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
    $READS->remove( $this->{fh} )  if ( $READS->exists( $this->{fh} ) );
    $WRITES->remove( $this->{fh} ) if ( $WRITES->exists( $this->{fh} ) );
    delete $TIMERS{ $this->{id} }  if ( defined $this->{id} );
    return;
}

sub unregister_reader_node {
    my ( $self, $this ) = @_;
    $READS->remove( $this->{fh} ) if ( $READS->exists( $this->{fh} ) );
    return;
}

sub unregister_writer_node {
    my ( $self, $this ) = @_;
    $WRITES->remove( $this->{fh} ) if ( $WRITES->exists( $this->{fh} ) );
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
    return $READS, $WRITES;
}

1;
