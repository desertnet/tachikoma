#!/usr/bin/perl
# ----------------------------------------------------------------------
# $Id: Config.pm 23999 2015-11-19 02:34:42Z chris $
# ----------------------------------------------------------------------

package Accessories::Forks;
use strict;
use warnings;
use Tachikoma;
use Tachikoma::EventFrameworks::Select;
use Tachikoma::Nodes::Callback;
use Tachikoma::Nodes::JobController;
use Tachikoma::Nodes::JobFarmer;
use Tachikoma::Nodes::Router;
use Tachikoma::Nodes::STDIO qw( TK_SYNC );
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TM_BYTESTREAM TM_ERROR TM_EOF );
use Tachikoma::Config qw( %Tachikoma );
use Data::Dumper;

sub new {
    my $class = shift;
    my $self = { ev => Tachikoma::EventFrameworks::Select->new };
    bless $self, $class;
    Tachikoma->event_framework( $self->ev );
    return $self;
}

sub spawn {
    my $self     = shift;
    my $count    = shift;
    my $original = shift || [];
    my $callback = shift;
    my $commands = [@$original];

    my $job_controller = Tachikoma::Nodes::JobController->new;
    my $output         = Tachikoma::Nodes::Callback->new;
    my $stdin = Tachikoma::Nodes::STDIO->filehandle( *STDIN, TK_SYNC );
    my $timer = Tachikoma::Nodes::Timer->new;

    $timer->arguments(100);
    $timer->sink($output);
    $job_controller->sink($output);
    $output->name('_parent');

    my $new_shells = sub {
        while ( keys %{ $job_controller->jobs } < $count and @$commands ) {
            my $command = shift(@$commands) or return;
            my $name = join( '-', 'shell', Tachikoma->counter );
            $job_controller->start_job( 'ExecFork', $name, $command, '_parent' );
        }
        $stdin->{fh} = undef if ( keys %Tachikoma::Nodes <= 1 );
        return;
    };

    $output->callback(
        sub {
            my $message = shift;

            # print Dumper($message);
            # print "nodes: ", join(', ', sort keys %Tachikoma::Nodes), "\n";
            &$callback( $message->stream, $message->payload )
                if ( not $message->type & TM_EOF and $message->from );
            &$new_shells;
            return;
        }
    );

    &$new_shells;

    $self->ev->drain( $stdin, $stdin );

    delete $Tachikoma::Nodes{_parent};
    return;
}

sub farm {
    my $self     = shift;
    my $count    = shift;
    my $commands = shift || [];
    my $map      = shift;
    my $reduce   = shift;

    my $router     = Tachikoma::Nodes::Router->new;
    my $job_farmer = Tachikoma::Nodes::JobFarmer->new;
    my $output     = Tachikoma::Nodes::Callback->new;
    my $collector  = Tachikoma::Nodes::Callback->new;
    my $stdin      = Tachikoma::Nodes::STDIO->filehandle( *STDIN, TK_SYNC );
    my $timer      = Tachikoma::Nodes::Timer->new;

    $router->name('_router');
    $timer->arguments(100);
    $timer->sink($output);
    $job_farmer->name('Transform');
    $job_farmer->arguments( join( ' ', $count, 'Transform', '-', $map ) );
    $job_farmer->sink($output);
    $job_farmer->load_balancer->sink($collector);
    $job_farmer->owner('_parent');
    $job_farmer->fire;
    $output->name('_parent');

    $collector->callback(
        sub {
            my $message = shift;
            return $Tachikoma::Nodes{ $message->to }->fill($message);
        }
    );

    my $waiting = 0;
    for my $command (@$commands) {
        my $message = Tachikoma::Message->new;
        $message->type(TM_BYTESTREAM);
        $message->from('_parent');
        $message->stream($command);
        $message->payload($command);
        $job_farmer->fill($message);
        $waiting++;
    }

    $output->callback(
        sub {
            my $message = shift;

            # print Dumper($message);
            # print "nodes: ", join(', ', sort keys %Tachikoma::Nodes), "\n",
            #       "waiting: $waiting\n\n";
            if ( $message->type & TM_ERROR ) {
                print STDERR $message->payload;
            }
            elsif ( not $message->type & TM_EOF and $message->from ) {
                &$reduce( $message->stream, $message->payload );
                $waiting--;
            }
            $job_farmer->remove_node if ( $waiting < 1 );
            $stdin->{fh} = undef if ( keys %Tachikoma::Nodes <= 2 );
            return;
        }
    );

    $self->ev->drain( $stdin, $stdin );

    delete $Tachikoma::Nodes{_parent};
    delete $Tachikoma::Nodes{_router};
    return;
}

sub ev {
    my $self = shift;
    if (@_) {
        $self->{ev} = shift;
    }
    return $self->{ev};
}

1;
