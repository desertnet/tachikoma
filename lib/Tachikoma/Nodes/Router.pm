#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Router
# ----------------------------------------------------------------------
#
# $Id: Router.pm 12579 2012-01-11 04:10:56Z chris $
#

package Tachikoma::Nodes::Router;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_HEARTBEAT TM_PING TM_INFO TM_ERROR TM_EOF
);
use Tachikoma::Config qw( $Secure_Level %Var $Wire_Version );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = 'v2.0.195';

my $Last_UTime         = 0;
my $Heartbeat_Interval = 15;    # seconds

sub new {
    my $class = shift;
    my $self  = Tachikoma::Node->new;
    $self->{type}                   = 'router';
    $self->{handling_error}         = undef;
    $self->{last_fire}              = 0;
    $self->{registrations}->{timer} = {};
    bless $self, $class;
    $self->set_timer(1000);
    return $self;
}

sub register_router_node {
    my $self = shift;
    return $Tachikoma::Event_Framework->register_router_node($self);
}

sub drain {
    my $self      = shift;
    my $connector = shift;
    if ( $self->type eq 'root' ) {
        my $class = ref $Tachikoma::Event_Framework;
        $self->stderr("starting up - $class - wire format $Wire_Version");
    }
    $Tachikoma::Event_Framework->drain( $self, $connector );
    $self->shutdown_all_nodes;
    while ( my $close_cb = shift @Tachikoma::Closing ) {
        &{$close_cb}();
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
            my $error = $@ // 'unknown error';
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
    my $to      = $message->[TO];
    $to = $message->[FROM]
        if ( $message->[TYPE] & TM_PING and not length $to );
    return $self->drop_message( $message, 'message not addressed' )
        if ( not $to );
    return $self->drop_message( $message, 'path exceeded 1024 bytes' )
        if ( length( $message->[FROM] // q{} ) > 1024 );
    my ( $name, $path ) = split m{/}, $to, 2;
    my $node = $Tachikoma::Nodes{$name};
    return $self->send_error( $message, "NOT_AVAILABLE\n" ) if ( not $node );
    $message->[TO] = $path;

    if ($Tachikoma::Profiles) {
        my $before = $self->push_profile($name);
        my $rv     = $node->fill($message);
        $self->pop_profile($before);
        return $rv;
    }
    return $node->fill($message);
}

sub send_error {
    my $self    = shift;
    my $message = shift;
    my $error   = shift;
    if ( not $message->[TYPE] & TM_ERROR ) {
        chomp $error;
        if ( $message->[FROM] ) {
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

sub drop_message {
    my $self    = shift;
    my $message = shift;
    my $error   = shift;
    $self->print_less_often(
              "WARNING: $error - "
            . $message->type_as_string
            . ( $message->from ? ' from: ' . $message->from : q{} )
            . ( $message->to   ? ' to: ' . $message->to     : q{} )
            . (
            ( $message->type == TM_INFO or $message->type == TM_ERROR )
            ? ' payload: ' . $message->payload
            : q{}
            )
    );
    return;
}

sub fire {
    my $self  = shift;
    my @again = ();
    while ( my $node = shift @Tachikoma::Reconnect ) {
        my $okay = eval {
            push @again, $node if ($node->reconnect);
            return 1;
        };
        if ( not $okay ) {
            my $error = $@ // 'unknown error';
            $node->stderr("ERROR: reconnect failed: $error");
            $node->remove_node;
        }
    }
    @Tachikoma::Reconnect = @again;
    if ( $Tachikoma::Now - $self->{last_fire} >= $Heartbeat_Interval ) {
        $self->heartbeat;
        $self->update_logs;
        $self->expire_callbacks;
        $self->notify_timer;
        if (    defined $Secure_Level
            and $Secure_Level == 0
            and $self->type ne 'router' )
        {
            $self->print_less_often('WARNING: process is insecure');
        }
        if ( defined $Tachikoma::Profiles ) {
            $self->trim_profiles;
        }
        $self->{last_fire} = $Tachikoma::Now;
    }
    return;
}

sub heartbeat {
    my $self  = shift;
    my $stale = $Var{'Stale_Connector_Threshold'} || 900;
    my $slow  = $Var{'Slow_Connector_Threshold'} || 900;
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
            and $Tachikoma::Now - $node->{last_upbeat} > 60 )
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
    my $self = shift;
    for my $text ( keys %Tachikoma::Recent_Log_Timers ) {
        delete $Tachikoma::Recent_Log_Timers{$text}
            if (
            $Tachikoma::Now - $Tachikoma::Recent_Log_Timers{$text} > 300 );
    }
    if ( $self->{type} eq 'root' and $Tachikoma::Now - $Last_UTime > 300 ) {
        Tachikoma->touch_log_file;
        $Last_UTime = $Tachikoma::Now;
    }
    return;
}

sub expire_callbacks {
    my $self      = shift;
    my $responder = $Tachikoma::Nodes{_responder};
    if ( $responder and $responder->shell->isa('Tachikoma::Nodes::Shell2') ) {
        my $callbacks = $responder->shell->callbacks;
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
    my $registrations = $self->{registrations}->{timer};
    for my $name ( keys %{$registrations} ) {
        if ( not $Tachikoma::Nodes{$name} ) {
            $self->stderr("WARNING: $name forgot to unregister");
            delete $registrations->{$name};
            next;
        }
        $Tachikoma::Nodes{$name}->fire;
    }
    return;
}

sub trim_profiles {
    my $self = shift;
    for my $key ( keys %{$Tachikoma::Profiles} ) {
        delete $Tachikoma::Profiles->{$key}
            if (
            $Tachikoma::Now - $Tachikoma::Profiles->{$key}->{timestamp}
            > 900 );
    }
    return;
}

sub remove_node {
    my $self = shift;
    push @Tachikoma::Closing, sub {
        $self->SUPER::remove_node;
    };
    return;
}

sub handling_error {
    my $self = shift;
    if (@_) {
        $self->{handling_error} = shift;
    }
    return $self->{handling_error};
}

sub last_fire {
    my $self = shift;
    if (@_) {
        $self->{last_fire} = shift;
    }
    return $self->{last_fire};
}

1;
