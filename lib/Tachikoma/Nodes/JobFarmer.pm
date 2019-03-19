#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::JobFarmer
# ----------------------------------------------------------------------
#
# $Id: JobFarmer.pm 36781 2019-03-19 06:28:17Z chris $
#

package Tachikoma::Nodes::JobFarmer;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD
    TM_COMMAND TM_STORABLE TM_RESPONSE TM_EOF TM_KILLME TM_ERROR
);
use Tachikoma::Command;
use Tachikoma::Nodes::JobController;
use Tachikoma::Nodes::LoadBalancer;
use Tachikoma::Nodes::Tee;
use Data::Dumper;
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.280');

my %C = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{job_count}      = 0;
    $self->{job_type}       = undef;
    $self->{job_arguments}  = undef;
    $self->{job_controller} = Tachikoma::Nodes::JobController->new;
    $self->{load_balancer}  = Tachikoma::Nodes::LoadBalancer->new;
    $self->{tee}            = undef;
    $self->{autokill}       = undef;
    $self->{lazy}           = undef;
    $self->{interpreter}    = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    $self->{registrations}->{READY} = {};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node JobFarmer <node name> <count> <type> [ <arguments> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ( $job_count, $job_type, $job_arguments ) =
            ( split q( ), $arguments, 3 );
        my $name = $self->{name};
        die "ERROR: bad arguments for JobFarmer\n"
            if ( not defined $job_count
            or $job_count =~ m{\D}
            or not $job_type );
        die "can't make $name, $name:job_controller exists\n"
            if ( exists $Tachikoma::Nodes{"$name:job_controller"} );
        die "can't make $name, $name:load_balancer exists\n"
            if ( exists $Tachikoma::Nodes{"$name:load_balancer"} );
        die "can't make $name, $name:all exists\n"
            if ( exists $Tachikoma::Nodes{"$name:all"} );
        $self->{arguments}     = $arguments;
        $self->{job_count}     = $job_count;
        $self->{job_type}      = $job_type;
        $self->{job_arguments} = $job_arguments;
        $self->{job_controller}->name( $name . ':job_controller' );
        $self->{load_balancer}->name( $name . ':load_balancer' );

        if ( $job_type eq 'FileReceiver' ) {
            $self->{load_balancer}->method('hash');
        }
        else {
            $self->{load_balancer}->method('preferred');
        }
        $self->{load_balancer}->mode('all');
        if ( not $self->{timer_is_active} ) {
            $self->set_timer;
        }
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my ( $name, $next, $from ) = split m{/}, $message->[FROM], 3;
    my $job =
          $name
        ? $self->{job_controller}->{jobs}->{$name}
        : undef;
    if ( $job and $message->[TYPE] & TM_KILLME ) {
        $self->disconnect_node( $self->{load_balancer}->name, $name );
        $self->disconnect_node( $self->{tee}->name,           $name )
            if ( $self->{tee} );
        $self->fire;
    }
    elsif ($job) {
        $self->handle_response( $message, $next, $from );
    }
    elsif ( $message->[TYPE] & TM_EOF ) {
        $self->handle_EOF( $message, $job, $next );
    }
    elsif ( $message->[TYPE] & TM_COMMAND ) {
        $self->{interpreter}->fill($message);
    }
    else {
        $self->{counter}++;
        if (    $message->[TYPE] == TM_ERROR
            and $message->[PAYLOAD] eq "NOT_AVAILABLE\n" )
        {
            if ( $self->{autokill} ) {
                $self->job_controller->kill_job( $message->[TO] );
                $self->fire;
            }
        }
        else {
            $self->{load_balancer}->fill($message);
        }
    }
    return;
}

sub fire {
    my $self   = shift;
    my $jobs   = $self->{job_controller}->{jobs};
    my $owners = $self->{load_balancer}->{owner};
    my @keep   = ();
    while ( @{$owners} < $self->{job_count} ) {
        $self->start_job( $self->{job_type}, $self->{job_arguments} );
    }
    for my $name ( @{$owners} ) {
        push @keep, $name if ( $jobs->{$name} );
    }
    @{ $self->{tee}->{owner} } = @keep if ( $self->{tee} );
    @{$owners} = @keep;
    $self->set_state('READY') if ( not $self->{set_state}->{READY} );
    return;
}

sub handle_response {
    my ( $self, $message, $next, $from ) = @_;
    if ( $message->[TYPE] & TM_RESPONSE ) {
        $self->{load_balancer}->handle_response($message);
    }
    if ( $self->{autokill} ) {
        $self->stamp_message( $message, $self->{name} );
    }
    else {
        $message->[FROM] = $from if ( $next and $next eq '_parent' );
    }
    $message->[TO] = $self->{owner} if ( not length $message->[TO] );
    $self->{sink}->fill($message) if ( length $message->[TO] );
    return;
}

sub handle_EOF {
    my ( $self, $message, $next ) = @_;
    if ( $message->[STREAM] ) {

        # some EOF being delivered to job
        $self->{counter}++;
        $self->{load_balancer}->fill($message);
    }
    elsif ($next) {

        # echo EOF back to command-line
        $self->{interpreter}->fill($message);
    }

    # else: EOF is from job shutdown
    return;
}

sub lookup {
    my ( $self, $key ) = @_;
    my $value = undef;
    if ( length $key ) {
        $value = $key;
    }
    else {
        $value = [ keys %{ $self->{job_controller}->{jobs} } ];
    }
    return $value;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list_jobs\n"
            . "          dump_job <job name>\n"
            . "          restart_job <job name>\n"
            . "          kill_job <job name>\n"
            . "          set_count <job count>\n"
            . "          tee [ 'on' | 'off' ]\n"
            . "          autokill [ 'on' | 'off' ]\n"
            . "          lazy [ 'on' | 'off' ]\n" );
};

$C{list_jobs} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $response = q();
    my $jobs     = $self->patron->job_controller->jobs;
    if ( $command->arguments eq '-a' ) {
        $response = join( "\n", sort keys %{$jobs} ) . "\n";
    }
    else {
        for my $name ( sort keys %{$jobs} ) {
            $response .= sprintf "%-5s  %s\n", $jobs->{$name}->pid, $name;
        }
    }
    return $self->response( $envelope, $response );
};

$C{jobs} = $C{list_jobs};

$C{ls} = $C{list_jobs};

$C{dump_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $name     = $command->arguments;
    my $jobs     = $self->patron->job_controller->jobs;
    my $node     = $jobs->{$name};
    if ( not $node ) {
        return $self->error( $envelope, qq(can't find node "$name"\n) );
    }
    my $copy = bless { %{$node} }, ref $node;
    $copy->{sink} = $copy->{sink}->name if ( $copy->{sink} );
    delete $copy->{connector};
    return $self->response( $envelope, Dumper $copy);
};

$C{dump} = $C{dump_job};

$C{restart_job} = sub {
    my $self          = shift;
    my $command       = shift;
    my $envelope      = shift;
    my $name          = $command->arguments;
    my $load_balancer = $self->patron->load_balancer;
    my $tee           = $self->patron->tee;
    my $jobc          = $self->patron->job_controller;
    my $jobs          = $jobc->jobs;
    if ( $name eq q(*) ) {

        for my $name ( keys %{$jobs} ) {
            $self->disconnect_node( $load_balancer->name, $name );
            $self->disconnect_node( $tee->name, $name ) if ($tee);
            $jobc->stop_job($name);
        }
    }
    elsif ( $jobs->{$name} ) {
        $self->disconnect_node( $load_balancer->name, $name );
        $self->disconnect_node( $tee->name, $name ) if ($tee);
        $jobc->stop_job($name);
    }
    else {
        return $self->error( $envelope, qq(can't find node "$name"\n) );
    }
    $self->patron->fire;
    return $self->okay($envelope);
};

$C{restart} = $C{restart_job};

$C{kill_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $name     = $command->arguments;
    my $jobc     = $self->patron->job_controller;
    my $jobs     = $jobc->jobs;
    if ( $name eq q(*) ) {
        for my $name ( keys %{$jobs} ) {
            $jobc->kill_job($name);
        }
    }
    elsif ( $jobs->{$name} ) {
        $jobc->kill_job($name);
    }
    else {
        return $self->error( $envelope, qq(can't find node "$name"\n) );
    }
    $self->patron->fire;
    return $self->okay($envelope);
};

$C{kill} = $C{kill_job};

$C{cut_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $name     = $command->arguments;
    my $jobc     = $self->patron->job_controller;
    my $jobs     = $jobc->jobs;
    my $error    = q();
    if ( $name eq q(*) ) {

        for my $name ( keys %{$jobs} ) {
            my $rv = $jobc->cut_job($name);
            $error .= $rv if ( defined $rv );
        }
    }
    elsif ( $jobs->{$name} ) {
        $jobc->cut_job($name);
    }
    else {
        $error = qq(can't find node "$name"\n);
    }
    return $self->error( $envelope, $error ) if ($error);
    return $self->okay($envelope);
};

$C{cut} = $C{cut_job};

$C{set_count} = sub {
    my $self          = shift;
    my $command       = shift;
    my $envelope      = shift;
    my $count         = $command->arguments;
    my $load_balancer = $self->patron->load_balancer;
    my $tee           = $self->patron->tee;
    my $jobc          = $self->patron->job_controller;
    my $jobs          = $jobc->jobs;
    my @names         = sort keys %{$jobs};
    if ( $command->arguments =~ m{\D} ) {
        return $self->error( $envelope, "count must be an integer\n" );
    }
    if ( $count < $self->patron->job_count ) {
        my $i = $self->patron->job_count;
        while ( $i > $count ) {
            my $name = $names[ $i - 1 ];
            next if ( not defined $name );
            my $job = $jobs->{$name};
            $self->disconnect_node( $load_balancer->name, $name );
            $self->disconnect_node( $tee->name, $name ) if ($tee);
            $job->{connector}->handle_EOF if ($job);
            $i--;
        }
    }
    $self->patron->job_count($count);
    $self->patron->fire;
    return $self->okay($envelope);
};

$C{tee} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $value    = $command->arguments;
    my $patron   = $self->patron;
    if ( $value and $value eq 'off' ) {
        $patron->tee->remove_node;
        $patron->tee(undef);
    }
    elsif ( not $patron->tee ) {
        my $tee = Tachikoma::Nodes::Tee->new;
        $tee->name( $patron->name . ':all' );
        $tee->sink( $patron->sink );
        $tee->owner( [ @{ $patron->load_balancer->owner } ] );
        $patron->tee($tee);
    }
    return $self->okay($envelope);
};

$C{autokill} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $value    = $command->arguments;
    my $patron   = $self->patron;
    if ( $value and $value eq 'off' ) {
        $patron->autokill(undef);
    }
    elsif ( not $patron->autokill ) {
        $patron->autokill($value);
    }
    return $self->okay($envelope);
};

$C{lazy} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $value    = $command->arguments;
    my $patron   = $self->patron;
    if ( $value and $value eq 'off' ) {
        $patron->lazy(undef);
    }
    elsif ( not $patron->lazy ) {
        $patron->lazy($value);
    }
    return $self->okay($envelope);
};

sub start_job {
    my $self          = shift;
    my $job_type      = shift;
    my $job_arguments = shift;
    my $job_name      = sprintf '%s-%06d',
        $self->{name}, $self->{job_controller}->job_counter;
    $self->{job_controller}->start_job(
        {   type      => $job_type,
            name      => $job_name,
            arguments => $job_arguments,
            lazy      => $self->{lazy}
        }
    );
    push @{ $self->{load_balancer}->{owner} }, $job_name;
    push @{ $self->{tee}->{owner} }, $job_name if ( $self->{tee} );
    return;
}

sub remove_node {
    my $self = shift;
    $self->{job_controller}->remove_node;
    $self->{load_balancer}->remove_node;
    $self->{tee}->remove_node if ( $self->{tee} );
    $self->SUPER::remove_node;
    return;
}

sub sink {
    my $self = shift;
    if (@_) {
        my $sink = $self->SUPER::sink(@_);
        $self->{job_controller}->sink($self);
        $self->{load_balancer}->sink($sink);
        $self->{tee}->sink($sink) if ( $self->{tee} );
    }
    return $self->{sink};
}

sub job_count {
    my $self = shift;
    if (@_) {
        $self->{job_count} = shift;
    }
    return $self->{job_count};
}

sub job_type {
    my $self = shift;
    if (@_) {
        $self->{job_type} = shift;
    }
    return $self->{job_type};
}

sub job_arguments {
    my $self = shift;
    if (@_) {
        $self->{job_arguments} = shift;
    }
    return $self->{job_arguments};
}

sub job_controller {
    my $self = shift;
    if (@_) {
        $self->{job_controller} = shift;
    }
    return $self->{job_controller};
}

sub load_balancer {
    my $self = shift;
    if (@_) {
        $self->{load_balancer} = shift;
    }
    return $self->{load_balancer};
}

sub tee {
    my $self = shift;
    if (@_) {
        $self->{tee} = shift;
    }
    return $self->{tee};
}

sub autokill {
    my $self = shift;
    if (@_) {
        $self->{autokill} = shift;
    }
    return $self->{autokill};
}

sub lazy {
    my $self = shift;
    if (@_) {
        $self->{lazy} = shift;
    }
    return $self->{lazy};
}

1;
