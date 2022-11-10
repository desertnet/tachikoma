#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Router
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Router;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_HEARTBEAT TM_COMPLETION TM_ERROR
);
use POSIX qw( :sys_wait_h );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.195');

my $PROFILES           = undef;
my @STACK              = ();
my $LAST_UTIME         = 0;
my $HEARTBEAT_INTERVAL = 15;      # seconds

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{type}                   = 'router';
    $self->{fire_cb}                = \&fire_cb;
    $self->{is_active}              = undef;
    $self->{handling_error}         = undef;
    $self->{registrations}->{TIMER} = {};
    bless $self, $class;
    $self->set_timer( $HEARTBEAT_INTERVAL * 1000 );
    return $self;
}

sub drain {
    my $self = shift;
    if ( not Tachikoma->shutting_down ) {
        Tachikoma->event_framework->drain( $self->start );
        while ( my $close_cb = shift @Tachikoma::Closing ) {
            &{$close_cb}();
        }
    }
    if ( $self->type eq 'root' ) {
        $self->stderr('waiting for child processes...');
        alarm 300;
        local $SIG{ALRM} = sub { die "timeout\n" };
        my $okay = eval {
            do { }
                while ( wait >= 0 );
            return 1;
        };
        if ( not $okay ) {
            my $error = $@ || 'unknown error';
            $self->stderr("WARNING: forcing shutdown - $error");
        }
        $self->stderr('removing pid file');
        Tachikoma->remove_pid;
        $self->stderr('shutdown complete');
        kill -9, $$ or die if ( not $okay );
        alarm 0;
    }
    else {
        do { } while ( wait >= 0 );
    }
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->drop_message( $message, 'message not addressed' )
        if ( not length $message->[TO] );
    return $self->drop_message( $message, 'path exceeded 1024 bytes' )
        if ( ( length $message->[FROM] // 0 ) > 1024 );
    my ( $name, $path ) = split m{/}, $message->[TO], 2;
    return $self->send_error( $message, "NOT_AVAILABLE\n" )
        if ( not $Tachikoma::Nodes{$name} );
    $message->[TO] = $path;
    if ($PROFILES) {
        my $before = $self->push_profile($name);
        my $rv     = $Tachikoma::Nodes{$name}->fill($message);
        $self->pop_profile($before);
        return $rv;
    }
    return $Tachikoma::Nodes{$name}->fill($message);
}

sub send_error {
    my $self    = shift;
    my $message = shift;
    my $error   = shift;
    if ( not $message->[TYPE] & TM_ERROR ) {
        chomp $error;
        return
            if ($error eq 'NOT_AVAILABLE'
            and $message->[TYPE] & TM_COMPLETION );
        if ( length $message->[FROM] ) {
            return $self->drop_message( $message, 'breaking recursion' )
                if ( $self->handling_error );
            my $response = Tachikoma::Message->new;
            $response->[TYPE]    = TM_ERROR;
            $response->[FROM]    = $message->[TO];
            $response->[TO]      = $message->[FROM];
            $response->[ID]      = $message->[ID];
            $response->[STREAM]  = $message->[STREAM];
            $response->[PAYLOAD] = "$error\n";
            $self->handling_error(1);
            $self->fill($response);
            $self->handling_error(undef);
        }
        $self->drop_message( $message, $error );
    }
    return;
}

sub fire_cb {
    my $self   = shift;
    my $config = $self->configuration;
    if ( $config->secure_level ) {
        my @again        = ();
        my $reconnecting = Tachikoma->nodes_to_reconnect;
        $self->stderr( 'DEBUG: FIRE ', $self->{timer_interval}, ' ms' )
            if ( $self->{debug_state} and $self->{debug_state} >= 3 );
        while ( my $node = shift @{$reconnecting} ) {
            my $okay = eval {
                push @again, $node if ( $node->reconnect );
                return 1;
            };
            if ( not $okay ) {
                my $error = $@ || 'unknown error';
                $node->stderr("ERROR: reconnect failed: $error");
                $node->remove_node;
            }
        }
        @{$reconnecting} = @again;
    }
    elsif ( defined $config->secure_level and $self->type ne 'router' ) {
        $self->print_less_often('WARNING: process is insecure');
    }
    $self->heartbeat( $config->var );
    $self->update_logs;
    $self->expire_callbacks;
    $self->notify_timer;
    if ( defined $PROFILES ) {
        $self->trim_profiles;
    }
    do { } while ( waitpid( -1, WNOHANG ) > 0 );
    return;
}

sub heartbeat {
    my $self  = shift;
    my $var   = shift;
    my $stale = $var->{stale_connector_threshold} || 900;
    my $slow  = $var->{slow_connector_threshold} || 900;
    for my $name ( keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        if ( not $node ) {
            $self->stderr("WARNING: clearing undefined node: $name");
            delete $Tachikoma::Nodes{$name};
            next;
        }
        next if ( not $node->isa('Tachikoma::Nodes::Socket') );
        if ( $node->isa('Tachikoma::Nodes::STDIO') ) {
            if (    $node->{last_fill}
                and @{ $node->{output_buffer} }
                and $Tachikoma::Now - $node->{last_fill} > $stale )
            {
                $self->stderr("WARNING: resetting stale connector: $name");
                $node->handle_EOF;
            }
            next;
        }
        if (    $node->{last_downbeat}
            and $Tachikoma::Now - $node->{last_downbeat} > $stale )
        {
            $self->stderr("WARNING: resetting stale connector: $name");
            $node->handle_EOF;
            next;
        }
        if ( $node->{latency_score} and $node->{latency_score} > $slow ) {
            $self->stderr("WARNING: resetting slow connector: $name");
            $node->handle_EOF;
            next;
        }
        if (    $node->{last_upbeat}
            and $Tachikoma::Now - $node->{last_upbeat} >= 60 )
        {
            my $message = Tachikoma::Message->new;
            $message->[TYPE]    = TM_HEARTBEAT;
            $message->[PAYLOAD] = $Tachikoma::Right_Now;
            $node->fill($message);
            $node->{last_upbeat} = $Tachikoma::Now;
        }
    }
    return;
}

sub update_logs {
    my $self              = shift;
    my $recent_log_timers = Tachikoma->recent_log_timers;
    for my $text ( keys %{$recent_log_timers} ) {
        delete $recent_log_timers->{$text}
            if ( $Tachikoma::Now - $recent_log_timers->{$text}->[0] > 300 );
    }
    if ( $self->{type} eq 'root' and $Tachikoma::Now - $LAST_UTIME > 300 ) {
        Tachikoma->touch_log_file;
        $LAST_UTIME = $Tachikoma::Now;
    }
    return;
}

sub expire_callbacks {
    my $self      = shift;
    my $responder = $Tachikoma::Nodes{_responder};
    my $shell     = $responder ? $responder->shell : undef;
    if ( $shell and $shell->isa('Tachikoma::Nodes::Shell2') ) {
        my $callbacks = $shell->callbacks;
        for my $id ( sort keys %{$callbacks} ) {
            my $timestamp = ( split m{:}, $id, 2 )[0];
            last if ( $Tachikoma::Now - $timestamp < 900 );
            $self->stderr("WARNING: expiring callback $id");
            delete $callbacks->{$id};
        }
    }
    return;
}

sub notify_timer {
    my $self          = shift;
    my $registrations = $self->{registrations}->{TIMER};
    for my $name ( keys %{$registrations} ) {
        my $node = $Tachikoma::Nodes{$name};
        if ( not $node ) {
            $self->stderr("WARNING: $name forgot to unregister");
            delete $registrations->{$name};
            next;
        }
        &{ $node->{fire_cb} }($node);
    }
    return;
}

sub push_profile {
    my ( $self, $name ) = @_;
    push @STACK, $name;
    return Time::HiRes::time;
}

sub pop_profile {
    my ( $self, $before ) = @_;
    return if ( not $PROFILES );
    my $after = Time::HiRes::time;
    my $name  = pop @STACK;
    my $info  = $PROFILES->{$name} ||= {};
    $info->{time} += $after - $before;
    $info->{count}++;
    $info->{avg} = $info->{time} / $info->{count};
    $info->{oldest} ||= $before;
    $info->{timestamp} = $after;

    if (@STACK) {
        $info = $PROFILES->{ $STACK[-1] };
        $info->{time} -= $after - $before;
    }
    return;
}

sub trim_profiles {
    my $self = shift;
    for my $key ( keys %{$PROFILES} ) {
        delete $PROFILES->{$key}
            if ( $Tachikoma::Now - $PROFILES->{$key}->{timestamp} > 900 );
    }
    return;
}

sub remove_node {
    my $self = shift;
    push @Tachikoma::Closing, sub {
        $self->SUPER::remove_node;
    };
    $self->stop;
    return;
}

sub start {
    my $self = shift;
    Tachikoma->shutting_down(undef);
    if ( $self->type eq 'root' or $self->debug_state ) {
        my $class   = ref Tachikoma->event_framework;
        my $version = $self->configuration->wire_version;
        $self->stderr("starting up - $class - wire format $version");
    }
    $self->is_active(1);
    return $self;
}

sub stop {
    my $self = shift;
    $self->is_active(undef);
    if ( $self->type eq 'root' or $self->debug_state ) {
        $self->stderr('shutting down');
    }
    return $self;
}

sub is_active {
    my $self = shift;
    if (@_) {
        $self->{is_active} = shift;
    }
    return $self->{is_active};
}

sub handling_error {
    my $self = shift;
    if (@_) {
        $self->{handling_error} = shift;
    }
    return $self->{handling_error};
}

sub profiles {
    my $self = shift;
    if (@_) {
        $PROFILES = shift;
        @STACK    = ();
    }
    return $PROFILES;
}

1;
