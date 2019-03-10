#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::JobController
# ----------------------------------------------------------------------
#
# $Id: JobController.pm 36600 2019-03-10 19:26:19Z chris $
#

package Tachikoma::Nodes::JobController;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Job;
use Tachikoma::Message qw(
    TYPE FROM TO PAYLOAD
    TM_COMMAND TM_PERSIST TM_RESPONSE TM_EOF TM_KILLME
);
use Data::Dumper;
use POSIX qw( SIGCHLD SIGKILL );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.280');

my $Throttle_Delay = 60;
my $Job_Counter    = 0;
my %C              = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{commands}      = \%C;
    $self->{jobs}          = {};
    $self->{bytes_read}    = 0;
    $self->{bytes_written} = 0;
    $self->{restart}       = {};
    $self->{username}      = undef;
    $self->{shutdown_mode} = 'wait';
    $self->{shutting_down} = undef;
    $self->{interpreter}   = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    Tachikoma->event_framework->watch_for_signal(SIGCHLD);
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node JobController <node name>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    return if ( $self->{shutting_down} );
    if ( $type & TM_KILLME ) {
        my $name = ( split m{/}, $message->[FROM], 2 )[0];
        my $job  = $self->{jobs}->{$name};
        return $self->stderr("ERROR: TM_KILLME from unknown $name")
            if ( not $job );
        $job->{connector}->fill($message);
        if ( not $self->{sink}->isa('Tachikoma::Nodes::JobFarmer') ) {
            my $old_name = $job->{connector}->name;
            my $new_name = sprintf '%s-%06d', $old_name, $self->job_counter;
            $self->rename_job( $old_name, $new_name );
            return 1;
        }
    }
    return $self->{interpreter}->fill($message) if ( $type & TM_COMMAND );
    my ( $name, $next, $from ) = split m{/}, $message->[FROM], 3;
    my $job = $self->{jobs}->{$name};
    if ($job) {
        my $to        = $message->[TO]             // q();
        my $job_owner = $job->{connector}->{owner} // q();
        my $job_edge  = $job->{connector}->{edge};
        $message->[FROM] = $from
            if ($next
            and $next eq '_parent'
            and not $self->{sink}->isa('Tachikoma::Nodes::JobFarmer') );
        return $self->handle_EOF( $message, $name, $job )
            if ( not $next and $to eq $job_owner and $type & TM_EOF );
        return $job_edge->activate( $message->[PAYLOAD] ) if ($job_edge);
    }
    return $self->{interpreter}->fill($message);
}

sub fire {
    my $self    = shift;
    my $jobs    = $self->{jobs};
    my $restart = $self->{restart};
    return if ( $self->{shutting_down} );
    for my $name ( keys %{$restart} ) {
        my $job = $jobs->{$name} or next;
        if ( not $job->{should_restart} ) {
            $self->stderr("WARNING: not restarting $name");
            delete $jobs->{$name};
            $job->remove_node;
            next;
        }
        $self->stderr( 'restarting ', $job->{original_name} )
            if ( not $job->{lazy} );
        $self->restart_job($job);
    }
    %{$restart} = ();
    return;
}

sub handle_EOF {
    my ( $self, $message, $name, $job ) = @_;
    my $connector = $job->{connector};
    $self->{bytes_read}    += $connector->{bytes_read};
    $self->{bytes_written} += $connector->{bytes_written};
    if (    $job->{should_restart}
        and not $self->{sink}->isa('Tachikoma::Nodes::JobFarmer')
        and not Tachikoma->shutting_down )
    {
        my $since_last = $Tachikoma::Now - $job->{should_restart};
        my $delay      = $Throttle_Delay - $since_last;
        $delay = 0 if ( $delay < 0 );
        $self->{restart}->{$name} = 'true';
        $self->set_timer( $delay * 1000, 'oneshot' );
    }
    else {
        delete $self->{jobs}->{$name};

        # Sometimes the parent side of the socketpair connector fails
        # to generate an EOF.  To mitigate this, jobs send an EOF
        # when Tachikoma::Node::shutdown_all_nodes() is called.
        #
        # This call is redundant if the EOF is from the socketpair,
        # but if it's from shutdown, we need to remove the connector:
        $job->remove_node;
    }

    # This is mainly to notify custom jobs like TailForks.
    # CommandInterpreter will drop this message:
    return $self->{sink}->fill($message);
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list_jobs\n"
            . "          run_job <job type> [ <arguments> ]\n"
            . "          start_job <job type> [ <job name> [ <arguments> ] ]\n"
            . "          maintain_job <job type> [ <job name> [ <arguments> ] ]\n"
            . "          lazy_job <job type> [ <job name> [ <arguments> ] ]\n"
            . "          restart_job <job name>\n"
            . "          rename_job <old job name> <new job name>\n"
            . "          stop_job <job name>\n"
            . "          kill_job <job name>\n"
            . "          dump_job <job name>\n"
            . "          username [ <username> ]\n"
            . "          config [ <path> ]\n"
            . "          shutdown_mode [ wait | kill ]\n" );
};

$C{list_jobs} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $response = undef;
    my $jobs     = $self->patron->jobs;
    if ( $command->arguments eq '-a' ) {
        $response = join( "\n", sort keys %{$jobs} ) . "\n";
    }
    else {
        $response = sprintf "%-5s  %s\n", 'PID', 'NAME';
        for my $name ( sort keys %{$jobs} ) {
            $response .= sprintf "%-5s  %s\n", $jobs->{$name}->pid, $name;
        }
    }
    return $self->response( $envelope, $response );
};

$C{jobs} = $C{list_jobs};

$C{ls} = $C{list_jobs};

$C{run_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $type, $arguments ) =
        split q( ), ( $command->arguments =~ m{(.*)}s )[0], 2;
    die qq(no type specified\n) if ( not $type );
    $self->patron->start_job( $type,
        sprintf( '%s-%06d', $type, $self->patron->job_counter ),
        $arguments, $envelope->from );
    return;
};

$C{run} = $C{run_job};

$C{start_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $type, $name, $arguments ) =
        split q( ), ( $command->arguments =~ m{(.*)}s )[0], 3;
    $name ||= $type;
    $self->patron->start_job( $type, $name, $arguments );
    return $self->okay($envelope);
};

$C{start} = $C{start_job};

$C{maintain_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $type, $name, $arguments ) =
        split q( ), ( $command->arguments =~ m{(.*)}s )[0], 3;
    $name ||= $type;
    $self->patron->start_job( $type, $name, $arguments, undef,
        $Tachikoma::Now );
    return $self->okay($envelope);
};

$C{maintain} = $C{maintain_job};

$C{lazy_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $type, $name, $arguments ) =
        split q( ), ( $command->arguments =~ m{(.*)}s )[0], 3;
    $name ||= $type;
    $self->patron->start_job( $type, $name, $arguments, undef,
        $Tachikoma::Now, 'lazy' );
    return $self->okay($envelope);
};

$C{lazy} = $C{lazy_job};

$C{restart_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $old_name = $command->arguments;
    my $job      = $self->patron->jobs->{$old_name};
    die qq(no such job "$old_name"\n) if ( not $job );
    if ( $job->{type} eq 'CommandInterpreter'
        and not length( $job->{arguments} ) )
    {
        $self->verify_key( $envelope, ['meta'], 'make_node' )
            or return $self->error("verification failed\n");
    }
    my $new_name = sprintf '%s-%06d', $old_name, $self->patron->job_counter;
    my $owner    = $job->owner;
    my $should_restart = $job->should_restart;
    $job->owner(undef);
    $job->should_restart(undef);
    $self->patron->rename_job( $old_name, $new_name );
    $job->connector->handle_EOF;
    $job->should_restart($should_restart);
    $self->patron->restart_job($job);
    $job->owner($owner);
    return $self->response( $envelope, qq(job "$old_name" restarted.\n) );
};

$C{restart} = $C{restart_job};

$C{stop_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my $name = $command->arguments;
    $self->patron->stop_job($name);
    return $self->response( $envelope, qq(job "$name" stopped.\n) );
};

$C{stop} = $C{stop_job};

$C{kill_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my $name = $command->arguments;
    $self->patron->kill_job($name);
    return $self->response( $envelope, qq(job "$name" killed.\n) );
};

$C{kill} = $C{kill_job};

$C{rename_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my ( $old_name, $new_name ) = split q( ), $command->arguments, 2;
    my $job = $self->patron->jobs->{$old_name};
    $self->patron->rename_job( $old_name, $new_name );
    $job->original_name($new_name);
    return $self->okay($envelope);
};

$C{rename} = $C{rename_job};
$C{mv}     = $C{rename_job};

$C{cut_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my $name  = $command->arguments;
    my $error = $self->patron->cut_job($name);
    return $self->error( $envelope, $error ) if ($error);
    return $self->response( $envelope, qq(job "$name" cut.\n) );
};

$C{cut} = $C{cut_job};

$C{dump_job} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $name, @keys ) = split q( ), $command->arguments;
    my %want = map { $_ => 1 } @keys;
    my $node = $self->patron->jobs->{$name};
    if ( not $node ) {
        return $self->error(qq(can't find node "$name"\n));
    }
    my $copy = bless { %{$node} }, ref $node;
    if (@keys) {
        for my $key ( keys %{$copy} ) {
            delete $copy->{$key} if ( not $want{$key} );
        }
    }
    $copy->{connector} = $copy->{connector}->{name};
    $copy->{_stdout}   = $copy->{_stdout}->{name};
    $copy->{_stderr}   = $copy->{_stderr}->{name};
    return $self->response( $envelope, Dumper $copy);
};

$C{dump} = $C{dump_job};

$C{username} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my $username = $command->arguments;
    if ($username) {
        $self->patron->username($username);
        return $self->okay($envelope);
    }
    $username = $self->patron->username;
    return $self->response( $envelope, "$username\n" );
};

$C{config} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my $config_file = $command->arguments;
    if ($config_file) {
        die "ERROR: no such config file\n"
            if ( not -f $config_file );
        $self->patron->config_file($config_file);
        return $self->okay($envelope);
    }
    $config_file = $self->patron->config_file;
    return $self->response( $envelope, "$config_file\n" );
};

$C{shutdown_mode} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'make_node' )
        or return $self->error("verification failed\n");
    my $mode = $command->arguments;
    if ($mode) {
        die "ERROR: invalid mode\n"
            if ( $mode ne 'wait' and $mode ne 'kill' );
        $self->patron->shutdown_mode($mode);
        return $self->okay($envelope);
    }
    $mode = $self->patron->shutdown_mode;
    return $self->response( $envelope, "$mode\n" );
};

sub start_job {
    my $self           = shift;
    my $type           = shift or die qq(no type specified\n);
    my $name           = shift;
    my $arguments      = shift;
    my $owner          = shift;
    my $should_restart = shift;
    my $lazy           = shift;
    my $username       = $self->username;
    my $config_file    = $self->config_file;
    $type =~ s{[^\w\d:]}{}g;
    $name ||= $type;

    if ( $self->{jobs}->{$name} ) {
        die qq(job "$name" already running\n);
    }
    elsif ( $Tachikoma::Nodes{$name} ) {
        die qq(node "$name" exists\n);
    }
    my $job = Tachikoma::Job->new;
    if ($lazy) {
        $job->prepare( $type, $name, $arguments, $owner, $should_restart,
            $username, $config_file );
    }
    else {
        $job->spawn( $type, $name, $arguments, $owner, $should_restart,
            $username, $config_file );
    }
    $job->{connector}->sink($self);
    $self->{jobs}->{ $job->{connector}->name } = $job;
    return $job->{connector};
}

sub restart_job {
    my $self        = shift;
    my $old_job     = shift or die qq(no job specified\n);
    my $type        = $old_job->{type};
    my $name        = $old_job->{original_name};
    my $tmp_name    = $old_job->{name};
    my $arguments   = $old_job->{arguments};
    my $username    = $old_job->{username};
    my $config_file = $old_job->{config_file};
    die qq(node "$name" exists\n) if ( $Tachikoma::Nodes{$name} );
    my $owner   = $old_job->{connector}->owner;
    my $new_job = Tachikoma::Job->new;
    delete $self->{jobs}->{$tmp_name} if ( defined $tmp_name );
    $old_job->remove_node;

    if ( $old_job->{lazy} ) {
        $new_job->prepare( $type, $name, $arguments, $owner, $Tachikoma::Now,
            $username, $config_file );
    }
    else {
        $new_job->spawn( $type, $name, $arguments, $owner, $Tachikoma::Now,
            $username, $config_file );
    }
    $new_job->{connector}->sink($self);
    $self->{jobs}->{$name} = $new_job;
    return;
}

sub stop_job {
    my $self = shift;
    my $name = shift;
    my $job  = $self->{jobs}->{$name} or die qq(no such job "$name"\n);
    $job->{should_restart} = undef;
    delete $self->{jobs}->{$name};
    $job->remove_node;
    return;
}

sub kill_job {
    my $self = shift;
    my $name = shift;
    my $job  = $self->{jobs}->{$name} or die qq(no such job "$name"\n);
    if ( $job->{pid} ne q(-) and not kill SIGKILL => $job->{pid} ) {
        $self->stderr("ERROR: kill_job failed: $!");
    }
    $job->{should_restart} = undef;
    delete $self->{jobs}->{$name};
    $job->remove_node;
    return;
}

sub rename_job {
    my $self     = shift;
    my $old_name = shift;
    my $new_name = shift;
    my $jobs     = $self->{jobs};
    my $job      = $jobs->{$old_name} or die qq(no such job "$old_name"\n);
    $job->{connector}->name($new_name);
    $job->{name} = $new_name;
    $jobs->{$new_name} = $job;
    delete $jobs->{$old_name};
    return;
}

sub cut_job {
    my $self = shift;
    my $name = shift;
    my $job  = $self->{jobs}->{$name} or die qq(no such job "$name"\n);
    if ( not exists $Tachikoma::Nodes{$name} ) {
        if ( not defined $job->{connector}->{sink} ) {
            if ( not $job->{should_restart} ) {
                delete $self->{jobs}->{$name};
                $job->remove_node;
                return;
            }
            else {
                return "did not cut: job set to restart\n";
            }
        }
        else {
            return "did not cut: sink still set\n";
        }
    }
    else {
        return "did not cut: node $name still exists\n";
    }
}

sub owner {
    my $self = shift;
    if (@_) {
        die "ERROR: setting owner on a JobController is disabled.\n";
    }
    return $self->{owner};
}

sub remove_node {
    my $self = shift;
    my $mode = $self->{shutdown_mode};
    $self->{shutting_down} = 'true';
    for my $name ( keys %{ $self->{jobs} } ) {
        my $job = $self->{jobs}->{$name};
        if (    $mode eq 'kill'
            and $job->{pid} ne q(-)
            and not kill SIGKILL => $job->{pid} )
        {
            $self->stderr("ERROR: remove_node couldn't kill $name: $!");
        }
        delete $self->{jobs}->{$name};
        $job->remove_node;
    }
    $self->SUPER::remove_node;
    return;
}

sub dump_config {
    my $self     = shift;
    my $response = q();
    if ( not $self->{sink}->isa('Tachikoma::Nodes::JobFarmer') ) {
        $response = $self->SUPER::dump_config;
        my $jobs = $self->{jobs};
        for my $name ( sort keys %{$jobs} ) {
            my $job   = $jobs->{$name};
            my $start = $job->{should_restart} ? 'maintain_job' : 'start_job';
            my $line  = "command $self->{name} $start $job->{type} $name";
            if ( $job->{arguments} ) {
                my $arguments = $job->{arguments};
                $arguments =~ s{'}{\\'}g;
                $line .= " '$arguments'";
            }
            $response .= "$line\n";
        }
        for my $name ( sort keys %{$jobs} ) {
            my $job   = $jobs->{$name};
            my $owner = $job->{connector}->{owner} or next;
            $response .= ("connect_node $name $owner\n");
        }
    }
    return $response;
}

sub job_counter {
    my $self = shift;
    if (@_) {
        $Job_Counter = shift;
    }
    return $Job_Counter++;
}

sub jobs {
    my $self = shift;
    if (@_) {
        $self->{jobs} = shift;
    }
    return $self->{jobs};
}

sub bytes_read {
    my $self = shift;
    if (@_) {
        $self->{bytes_read} = shift;
    }
    return $self->{bytes_read};
}

sub bytes_written {
    my $self = shift;
    if (@_) {
        $self->{bytes_written} = shift;
    }
    return $self->{bytes_written};
}

sub restart {
    my $self = shift;
    if (@_) {
        $self->{restart} = shift;
    }
    return $self->{restart};
}

sub username {
    my $self     = shift;
    my $username = $self->{username};
    if (@_) {
        $username = shift;
        $self->{username} = $username;
    }
    if ( not $username ) {
        $username = ( getpwuid $< )[0];
    }
    return $username;
}

sub config_file {
    my $self        = shift;
    my $config_file = $self->{config_file};
    if (@_) {
        $config_file = shift;
        $self->{config_file} = $config_file;
    }
    if ( not $config_file ) {
        if ( $self->{username} ) {
            $config_file = join q(),
                '/usr/local/etc/tachikoma-', $self->{username}, '.conf';
        }
        else {
            $config_file = $self->configuration->config_file;
        }
    }
    return $config_file;
}

sub shutdown_mode {
    my $self = shift;
    if (@_) {
        $self->{shutdown_mode} = shift;
    }
    return $self->{shutdown_mode};
}

sub shutting_down {
    my $self = shift;
    if (@_) {
        $self->{shutting_down} = shift;
    }
    return $self->{shutting_down};
}

1;
