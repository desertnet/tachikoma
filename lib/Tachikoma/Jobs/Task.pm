#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::Task
# ----------------------------------------------------------------------
#

package Tachikoma::Jobs::Task;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw(
    TYPE FROM STREAM PAYLOAD
    TM_BYTESTREAM TM_STORABLE
);
use IPC::Open3;
use Symbol qw( gensym );
use IO::Select;
use JSON;
use Time::HiRes;
use POSIX qw( :sys_wait_h );
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.280');

my $Check_Proc_Interval = 15;

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    if ( $message->[FROM] =~ m{^_parent} ) {
        $self->send_event(
            {   type  => 'TASK_BEGIN',
                key   => $message->[STREAM],
                value => $message->payload,
            }
        );
        $self->execute($message);
        $self->send_event(
            {   type  => 'TASK_COMPLETE',
                key   => $message->[STREAM],
                value => $message->payload,
            }
        );
        $self->cancel($message);
    }
    return;
}

sub execute {
    my $self  = shift;
    my $task  = shift;
    my $key   = $task->stream;
    my $value = $task->payload;
    local $SIG{PIPE}  = sub { die $! };
    local $ENV{KEY}   = $key;
    local $ENV{VALUE} = $value;
    my $args = $self->arguments;
    $args = ( $args =~ m{^(.*)$}s )[0];
    my ( $child_in, $child_out, $child_err );
    $child_err = gensym();
    my $failed = 0;

    open3( $child_in, $child_out, $child_err, $args ) or die $!;
    my $r_in = IO::Select->new;
    $r_in->add($child_out);
    $r_in->add($child_err);

    while ( my ($reads) = IO::Select->select( $r_in, undef, undef, undef ) ) {
        my $sent = undef;
        for my $fh ( @{$reads} ) {
            if ( $fh eq $child_out ) {
                my $output = <$child_out>;
                if ( length $output ) {
                    $output =~ s{\e\[\d+m}{}g;
                    $self->send_event(
                        {   type  => 'TASK_OUTPUT',
                            key   => $key,
                            value => $output,
                        }
                    );
                    $sent = 1;
                }
            }
            elsif ( $fh eq $child_err ) {
                my $error = <$child_err>;
                if ( length $error ) {
                    $error =~ s{\e\[\d+m}{}g;
                    $self->send_event(
                        {   type  => 'TASK_ERROR',
                            key   => $key,
                            value => $error,
                        }
                    );
                    $sent = 1;
                }
            }
        }
        last if ( not $sent );
    }
    close $child_err or die $!;
    close $child_out or die $!;
    my $pid = undef;
    do {
        $pid = waitpid -1, WNOHANG;
        my $rv = $? >> 8;
        if ( $pid > 0 and $rv ) {
            my $error = "ERROR: shell exited with value: $rv";
            $self->send_event(
                {   type  => 'TASK_ERROR',
                    key   => $key,
                    value => $error
                }
            );
        }
    } while ( $pid > 0 );
    return;
}

sub send_event {
    my $self  = shift;
    my $event = shift;
    my $note  = Tachikoma::Message->new;
    $event->{timestamp} = Time::HiRes::time;
    $note->[TYPE]       = TM_STORABLE;
    $note->[STREAM]     = $event->{key};
    $note->[PAYLOAD]    = $event;
    $self->SUPER::fill($note);
    return;
}

1;
