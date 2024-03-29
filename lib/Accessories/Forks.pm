#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Forks
# ----------------------------------------------------------------------
#

package Accessories::Forks;
use strict;
use warnings;
use Tachikoma::EventFrameworks::Select;
use Tachikoma::Nodes::Callback;
use Tachikoma::Nodes::JobController;
use Tachikoma::Nodes::STDIO qw( TK_SYNC );
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TM_BYTESTREAM TM_ERROR TM_EOF );
use Data::Dumper;

use version; our $VERSION = qv('v2.0.700');

sub new {
    my $class = shift;
    my $self  = {};
    $self->{ev}        = Tachikoma::EventFrameworks::Select->new;
    $self->{is_active} = undef;
    bless $self, $class;
    Tachikoma->event_framework( $self->ev );
    return $self;
}

sub spawn {
    my $self     = shift;
    my $count    = shift;
    my $original = shift || [];
    my $callback = shift;
    my $commands = [ @{$original} ];

    my $job_controller = Tachikoma::Nodes::JobController->new;
    my $output         = Tachikoma::Nodes::Callback->new;
    my $stdin = Tachikoma::Nodes::STDIO->filehandle( *STDIN, TK_SYNC );
    my $timer = Tachikoma::Nodes::Timer->new;

    $timer->arguments(100);
    $timer->sink($output);
    $job_controller->sink($output);
    $output->name('_parent');

    my $new_shells = sub {
        while ( keys %{ $job_controller->jobs } < $count and @{$commands} ) {
            my $command = shift @{$commands} or return;
            my $name    = join q(-), 'shell', Tachikoma->counter;
            $job_controller->start_job(
                {   type      => 'ExecFork',
                    name      => $name,
                    arguments => $command,
                    owner     => '_parent',
                }
            );
        }
        $self->is_active(
            scalar( keys %{ $job_controller->jobs } ) + @{$commands} );
        return;
    };

    $output->callback(
        sub {
            my $message = shift;

            # print Dumper($message);
            # print "nodes: ", join(q(, ), sort keys %Tachikoma::Nodes), "\n";
            &{$callback}( $message->stream, $message->payload )
                if ( not $message->type & TM_EOF and $message->from );
            &{$new_shells};
            return;
        }
    );

    &{$new_shells};

    $self->ev->drain($self);

    delete $Tachikoma::Nodes{_parent};
    return;
}

sub ev {
    my $self = shift;
    if (@_) {
        $self->{ev} = shift;
    }
    return $self->{ev};
}

sub is_active {
    my $self = shift;
    if (@_) {
        $self->{is_active} = shift;
    }
    return $self->{is_active};
}

sub configuration {
    return {};
}

1;
