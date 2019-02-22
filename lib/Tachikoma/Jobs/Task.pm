#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::Task
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Jobs::Task;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw(
    TYPE FROM STREAM PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_ERROR TM_EOF
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

sub initialize_graph {
    my $self = shift;
    my $json = JSON->new;
    $json->canonical(1);
    $json->pretty(1);
    $json->allow_blessed(1);
    $json->convert_blessed(0);
    $self->json($json);
    $self->connector->sink($self);
    $self->sink( $self->router );
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    my $from    = $message->[FROM];
    return if ( not $type & TM_BYTESTREAM and not $type & TM_EOF );
    if ( $from =~ m{^_parent} ) {
        return if ( $type & TM_EOF );
        $self->task($message);
        $self->send_event( { type => 'TASK_BEGIN' } );
        $self->execute;
        $self->send_event( { type => 'TASK_COMPLETE' } );
        $self->cancel($message);
        $self->task(undef);
    }
    return;
}

sub execute {
    my $self = shift;
    local $SIG{PIPE}  = sub { die $! };
    local $ENV{KEY}   = $self->task->stream;
    local $ENV{VALUE} = $self->task->payload;
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
                    $self->send_event(
                        { type => 'TASK_OUTPUT', payload => $output } );
                    $sent = 1;
                }
            }
            elsif ( $fh eq $child_err ) {
                my $error = <$child_err>;
                if ( length $error ) {
                    $self->send_event(
                        { type => 'TASK_ERROR', payload => $error } );
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
            $self->send_event( { type => 'TASK_ERROR', payload => $error } );
        }
    } while ( $pid > 0 );
    return;
}

sub send_event {
    my $self   = shift;
    my $event  = shift;
    my $stream = $self->{task}->[STREAM];
    my $note   = Tachikoma::Message->new;
    $event->{key}       = $stream;
    $event->{timestamp} = Time::HiRes::time;
    $note->[TYPE]       = TM_STORABLE;
    $note->[STREAM]     = "$stream\n";         # XXX: LB hack;
    $note->[PAYLOAD]    = $event;
    $self->SUPER::fill($note);
    return;
}

sub json {
    my $self = shift;
    if (@_) {
        $self->{json} = shift;
    }
    return $self->{json};
}

sub task {
    my $self = shift;
    if (@_) {
        $self->{task} = shift;
    }
    return $self->{task};
}

1;
