#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::Shell
# ----------------------------------------------------------------------
#
# $Id: Shell.pm 36622 2019-03-11 05:23:51Z chris $
#

package Tachikoma::Jobs::Shell;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::STDIO qw( TK_R );
use Tachikoma::Message qw(
    TYPE FROM ID STREAM
    TM_BYTESTREAM TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use IPC::Open3;
use Symbol qw( gensym );
use POSIX qw( :sys_wait_h SIGINT SIGKILL );
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.280');

my $Check_Proc_Interval = 15;

sub initialize_graph {
    my $self = shift;
    ## no critic (RequireLocalizedPunctuationVars)
    $SIG{PIPE} = sub { die "got sigpipe: $!" };
    my $timer = Tachikoma::Nodes::Timer->new;
    $timer->name('Timer');
    $timer->sink($self);
    $timer->set_timer( $Check_Proc_Interval * 1000 );
    $self->initialize_shell_graph;
    $self->connector->sink($self);
    $self->sink( $self->router );
    return;
}

sub initialize_shell_graph {
    my $self = shift;
    my $args = $self->arguments;
    $args = ( $args =~ m{^(.*)$}s )[0];
    my ( $child_in, $child_out, $child_err );
    $child_err = gensym();
    $self->shell_pid( open3( $child_in, $child_out, $child_err, $args ) );
    my $shell_stdin = Tachikoma::Nodes::STDIO->filehandle($child_in);
    my $shell_stdout =
        Tachikoma::Nodes::STDIO->filehandle( $child_out, TK_R );
    my $shell_stderr =
        Tachikoma::Nodes::STDIO->filehandle( $child_err, TK_R );
    $shell_stdin->name('shell:stdin');
    $shell_stdin->sink($self);
    $shell_stdout->name('shell:stdout');
    $shell_stdout->sink($self);
    $shell_stderr->name('shell:stderr');
    $shell_stderr->buffer_mode('line-buffered');
    $shell_stderr->sink($self);
    $self->shell_stdin($shell_stdin);
    $self->shell_stdout($shell_stdout);
    $self->shell_stderr($shell_stderr);
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    my $from    = $message->[FROM];
    return if ( not $type & TM_BYTESTREAM and not $type & TM_EOF );
    if ( $from =~ m{^_parent} ) {
        if ( $type & TM_EOF ) {
            ## no critic (RequireCheckedSyscalls)
            kill SIGINT, $self->{shell_pid};
            return;
        }
        $self->shell_stdin->fill($message);

        # tell LoadBalancer we're done
        if ( not $type & TM_PERSIST ) {
            my $response = Tachikoma::Message->new;
            $response->[TYPE]   = TM_RESPONSE;
            $response->[STREAM] = $message->[STREAM];
            $response->[ID]     = $message->[ID];
            $self->{sink}->fill($response);
        }
    }
    elsif ( $from eq 'shell:stdout' ) {
        if ( $type & TM_EOF ) {
            if ( kill 0, $self->{shell_pid} ) {
                my $pid = waitpid -1, WNOHANG;
                my $rv = $? >> 8;
                $self->stderr("ERROR: shell exited with value: $rv")
                    if ( $pid > 0 and $rv );
            }
            $self->shell_pid(undef);
            return $self->shutdown_all_nodes;
        }
        $self->SUPER::fill($message);
    }
    elsif ( $from eq 'shell:stderr' ) {
        return if ( $type & TM_EOF );
        $self->stderr( $message->payload );
        $message->type(TM_ERROR);
        $self->SUPER::fill($message);
    }
    elsif ( $from eq 'Timer' ) {
        do { } while ( waitpid( -1, WNOHANG ) > 0 );
        if ( not kill 0, $self->{shell_pid}
            and $! ne 'Operation not permitted' )
        {
            $self->shell_pid(undef);
            return $self->shutdown_all_nodes;
        }
    }
    return;
}

sub remove_node {
    my $self = shift;
    ## no critic (RequireLocalizedPunctuationVars, RequireCheckedSyscalls)
    $SIG{ALRM} = sub {
        kill SIGKILL, $self->{shell_pid}
            if ( defined $self->{shell_pid} );
    };
    alarm 15;
    do { } while ( wait >= 0 );
    $self->SUPER::remove_node;
    return;
}

sub shell_pid {
    my $self = shift;
    if (@_) {
        $self->{shell_pid} = shift;
    }
    return $self->{shell_pid};
}

sub shell_stdin {
    my $self = shift;
    if (@_) {
        $self->{shell_stdin} = shift;
    }
    return $self->{shell_stdin};
}

sub shell_stdout {
    my $self = shift;
    if (@_) {
        $self->{shell_stdout} = shift;
    }
    return $self->{shell_stdout};
}

sub shell_stderr {
    my $self = shift;
    if (@_) {
        $self->{shell_stderr} = shift;
    }
    return $self->{shell_stderr};
}

1;
