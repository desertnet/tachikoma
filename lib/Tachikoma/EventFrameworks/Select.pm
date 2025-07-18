#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::EventFrameworks::Select
# ----------------------------------------------------------------------
#

package Tachikoma::EventFrameworks::Select;
use strict;
use warnings;
use IO::Select;
use POSIX qw( SIGUSR1 );
use Time::HiRes;

use version; our $VERSION = qv('v2.0.227');

use constant {
    INTERVAL  => 0,
    ONESHOT   => 1,
    LAST_FIRE => 2,
};

sub new {
    my $class = shift;
    my $self  = {};
    $self->{reads}  = IO::Select->new;
    $self->{writes} = IO::Select->new;
    $self->{timers} = {};
    bless $self, $class;
    return $self;
}

sub register_server_node {
    my ( $self, $this ) = @_;
    $self->{reads}->add( $this->{fh} );
    return $this;
}

sub accept_connections {
    my ( $self, $this ) = @_;
    $this->accept_connection;
    return;
}

sub register_reader_node {
    my ( $self, $this ) = @_;
    $self->{reads}->add( $this->{fh} ) if ( $this->{fh} );
    return $this;
}

sub register_writer_node {
    my ( $self, $this ) = @_;
    $self->{writes}->add( $this->{fh} ) if ( $this->{fh} );
    return $this;
}

sub register_watcher_node {
    my ( $self, $this ) = @_;
    return $this;
}

sub drain {
    my ( $self, $router ) = @_;
    my $configuration = $router->configuration;
    my $got_signal    = undef;
    my $reads         = $self->{reads};
    my $writes        = $self->{writes};
    my $timers        = $self->{timers};
    local $SIG{INT}  = sub { $got_signal = 'SHUTDOWN' };
    local $SIG{TERM} = sub { $got_signal = 'SHUTDOWN' };
    local $SIG{HUP}  = sub { $got_signal = 'GOT_HUP' };
    local $SIG{USR1} = sub { $got_signal = 'RELOAD_CONFIG' };

    while ( $router->{is_active} ) {
        my ( $reads_ready, $writes_ready, $errors ) =
            IO::Select->select( $reads, $writes, $reads,
            1 / ( $configuration->{hz} || 10 ) );
        $Tachikoma::Right_Now = Time::HiRes::time;
        $Tachikoma::Now       = int $Tachikoma::Right_Now;
        for ( @{$reads_ready}, @{$errors} ) {
            my $fd = fileno($_);
            if ( not defined $fd ) {
                $reads->remove($_);
                next;
            }
            my $node = $Tachikoma::Nodes_By_FD->{ $fd };
            if ( not defined $node ) {
                $reads->remove($_);
                next;
            }
            &{ $node->{drain_fh} }($node);
        }
        for ( @{$writes_ready} ) {
            my $fd = fileno($_);
            if ( not defined $fd ) {
                $writes->remove($_);
                next;
            }
            my $node = $Tachikoma::Nodes_By_FD->{ $fd };
            if ( not defined $node ) {
                $writes->remove($_);
                next;
            }
            &{ $node->{fill_fh} }($node);
        }
        if ($got_signal) {
            $self->handle_signal( $router, $got_signal );
            $got_signal = undef;
        }
        &{$_}() while ( $_ = shift @Tachikoma::Closing );
        for ( keys %{$timers} ) {
            my $timer = $timers->{$_} or next;
            next
                if ( $Tachikoma::Right_Now - $timer->[LAST_FIRE]
                < $timer->[INTERVAL] );
            my $node = $Tachikoma::Nodes_By_ID->{$_};
            if ( not $node ) {
                delete $timers->{$_};
                next;
            }
            elsif ( $timer->[ONESHOT] ) {
                delete $timers->{$_};
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
    my ( $self, $router, $got_signal ) = @_;
    if ( $got_signal eq 'SHUTDOWN' ) {
        $router->stderr('received signal');
        $router->shutdown_all_nodes;
        $router->stop if ( $router->is_active );
    }
    elsif ( $got_signal eq 'GOT_HUP' ) {
        Tachikoma->touch_log_file if ( $$ == Tachikoma->my_pid );
        $router->stderr('got SIGHUP - sending SIGUSR1');
        my $usr1 = SIGUSR1;
        kill -$usr1, $$ or die q(FAILURE: couldn't signal self);
    }
    elsif ( $got_signal eq 'RELOAD_CONFIG' ) {
        $router->stderr('got SIGUSR1 - reloading config');
        Tachikoma->reload_config;
    }
    return;
}

sub close_filehandle {
    my ( $self, $this ) = @_;
    $self->{reads}->remove( $this->{fh} )
        if ( $self->{reads}->exists( $this->{fh} ) );
    $self->{writes}->remove( $this->{fh} )
        if ( $self->{writes}->exists( $this->{fh} ) );
    delete $self->{timers}->{ $this->{id} } if ( defined $this->{id} );
    return;
}

sub unregister_reader_node {
    my ( $self, $this ) = @_;
    $self->{reads}->remove( $this->{fh} )
        if ( $self->{reads}->exists( $this->{fh} ) );
    return;
}

sub unregister_writer_node {
    my ( $self, $this ) = @_;
    $self->{writes}->remove( $this->{fh} )
        if ( $self->{writes}->exists( $this->{fh} ) );
    return;
}

sub unregister_watcher_node {
    my ( $self, $this ) = @_;
    return;
}

sub set_timer {
    my ( $self, $this, $interval, $oneshot ) = @_;
    $interval ||= 0;
    $self->{timers}->{ $this->{id} } = [
        $interval / 1000,
        $oneshot, $Tachikoma::Right_Now || Time::HiRes::time
    ];
    return;
}

sub stop_timer {
    my ( $self, $this ) = @_;
    delete $self->{timers}->{ $this->{id} } if ( defined $this->{id} );
    return;
}

sub queue {
    my ($self) = @_;
    return $self->{reads}, $self->{writes};
}

sub reads {
    my $self = shift;
    if (@_) {
        $self->{reads} = shift;
    }
    return $self->{reads};
}

sub writes {
    my $self = shift;
    if (@_) {
        $self->{reads} = shift;
    }
    return $self->{reads};
}

sub timers {
    my $self = shift;
    if (@_) {
        $self->{timers} = shift;
    }
    return $self->{timers};
}

1;
