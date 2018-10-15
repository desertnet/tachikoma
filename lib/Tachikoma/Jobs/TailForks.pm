#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::TailForks
# ----------------------------------------------------------------------
#
# $Id: TailForks.pm 3651 2009-10-20 18:23:28Z chris $
#

package Tachikoma::Jobs::TailForks;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Nodes::JobController;
use Tachikoma::Nodes::LoadController;
use Tachikoma::Nodes::Watcher;
use Tachikoma::Nodes::Tee;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_BYTESTREAM TM_EOF
);
use Tachikoma::Config qw( %Tachikoma );
use BerkeleyDB;
use Digest::MD5 qw( md5 );
use parent qw( Tachikoma::Job );

use version; our $VERSION = 'v2.0.368';

my $Home          = $Tachikoma{Home} || ( getpwuid $< )[7];
my $DB_Dir        = "$Home/.tachikoma/Tails";
my $Max_Forking   = 8;
my $Scan_Interval = 30;
my $Last_Cache    = 0;

sub initialize_graph {
    my $self            = shift;
    my $name            = $self->name;
    my $interpreter     = Tachikoma::Nodes::CommandInterpreter->new;
    my $job_controller  = Tachikoma::Nodes::JobController->new;
    my $load_controller = Tachikoma::Nodes::LoadController->new;
    my $collector       = Tachikoma::Nodes::Tee->new;
    my $arguments       = $self->arguments;
    my @destinations    = split q{ }, $arguments;
    $self->connector->sink($interpreter);
    $interpreter->name('command_interpreter');
    $interpreter->sink($self);
    $self->interpreter($interpreter);
    $job_controller->name('jobs');
    $job_controller->sink($self);
    $self->job_controller($job_controller);
    $load_controller->name('LoadController');
    $load_controller->sink($self);
    $self->load_controller($load_controller);
    $collector->name('collector');
    $collector->sink($interpreter);
    $self->collector($collector);
    $self->connect_list( \@destinations );
    $self->tails(   {} );
    $self->files(   {} );
    $self->forking( {} );
    $self->sink( $self->router );
    $interpreter->commands->{'add_tail'} = sub {
        my $this     = shift;
        my $command  = shift;
        my $envelope = shift;
        my ( $file, $node_path ) = split q{ }, $command->arguments, 2;
        for my $expanded ( glob $file ) {
            my $quoted = $expanded;
            $quoted =~ s{/}{:}g;
            $self->files->{$quoted} = [ $expanded => $node_path ];
        }
        $self->timer->set_timer(0) if ( $self->{timer} );
        return $this->okay($envelope);
    };
    $interpreter->commands->{'add'} = $interpreter->commands->{'add_tail'};
    $interpreter->commands->{'rm_tail'} = sub {
        my $this     = shift;
        my $command  = shift;
        my $envelope = shift;
        my $regex    = $command->arguments;
        my $files    = $self->files;
        my $found    = undef;
        for my $file ( keys %{$files} ) {
            next if ( $file !~ m{$regex} );
            delete $self->files->{$file};
            $self->finish_file( $file, "delete\n" );
            my $watcher = $Tachikoma::Nodes{"$file:watcher"};
            $watcher->remove_node if ($watcher);
            $found = 1;
        }
        die "no files matching: $regex\n" if ( not $found );
        return $this->okay($envelope);
    };
    $interpreter->commands->{'start_tail'} = sub {
        my $this     = shift;
        my $command  = shift;
        my $envelope = shift;
        $self->timer->set_timer(0);
        return $this->okay($envelope);
    };
    $interpreter->commands->{'stop_tail'} = sub {
        my $this     = shift;
        my $command  = shift;
        my $envelope = shift;
        for my $file ( keys %{ $self->tails } ) {
            $self->close_tail($file);
            my $watcher = $Tachikoma::Nodes{"$file:watcher"};
            $watcher->remove_node if ($watcher);
        }
        $self->timer->remove_node if ( $self->{timer} );
        $self->{timer} = undef;
        return $this->okay($envelope);
    };
    $interpreter->commands->{'list_files'} = sub {
        my $this     = shift;
        my $command  = shift;
        my $envelope = shift;
        my $files    = $self->files;
        my $response = [ [ [ 'FILE' => 'right' ], [ 'NODE' => 'left' ] ] ];
        for my $file ( sort keys %{$files} ) {
            push @{$response}, $files->{$file};
        }
        return $this->response( $envelope, $this->tabulate($response) );
    };
    $interpreter->commands->{'list_tails'} = sub {
        my $this     = shift;
        my $command  = shift;
        my $envelope = shift;
        my $response = q{};
        for my $file ( sort keys %{ $self->tails } ) {
            $response .= "$file\n";
        }
        return $this->response( $envelope, $response );
    };
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    $self->{counter}++;
    if ( $message->[FROM] eq 'Timer' ) {
        $self->rescan_files;
        return;
    }
    elsif ( $message->[FROM] =~ m{^(.*):watcher$} ) {
        my $file = $1;
        if ( $message->[PAYLOAD] =~ m{file .* (delete|rename)} ) {
            my $event = $1;
            $self->finish_file( $file, $event );
        }
        $self->rescan_files if ( $message->[PAYLOAD] !~ m{file .* missing} );
        return $self->{collector}->fill($message);
    }
    elsif ( $message->[FROM] =~ m{^(.*):tail$} ) {
        my $file = $1;
        return $self->{sink}->fill($message) if ( $message->[TO] );
        my $forking = $self->{forking};
        $self->position( $file, $message->[ID] ) if ( $message->[ID] );
        $message->[STREAM] = $file;
        if ( $type & TM_EOF ) {
            delete $self->{tails}->{$file};
            return;
        }
        elsif ( keys( %{$forking} ) >= $Max_Forking ) {
            $self->timer->set_timer(10);
        }
        delete $forking->{$file};
        return $self->{collector}->fill($message);
    }
    elsif ( $message->[FROM] =~ m{^(.*):tail-\d+$} ) {
        my $file = $1;
        $message->[STREAM] = $file;
        return if ( $type & TM_EOF );
        return $self->{collector}->fill($message);
    }
    return $self->{sink}->fill($message);
}

sub rescan_files {
    my $self = shift;
    return if ( not $self->{timer} );
    my $tails   = $self->{tails};
    my $files   = $self->{files};
    my $forking = $self->{forking};
    for my $file ( keys %{$files} ) {
        my $path = $files->{$file}->[0];
        next if ( not -e $path );
        if ( not $Tachikoma::Nodes{"$file:watcher"} ) {
            my $file_watcher = Tachikoma::Nodes::Watcher->new;
            $file_watcher->name("$file:watcher");
            $file_watcher->arguments("vnode $path delete rename");
            $file_watcher->sink($self);
        }
        if ( not $Tachikoma::Nodes{"$file:tail"} ) {
            next if ( keys( %{$forking} ) >= $Max_Forking );
            my $tiedhash    = $self->tiedhash;
            my $filepos     = $tiedhash->{$file} || 0;
            my $destination = $self->get_destination($file);
            my $node_path   = $files->{$file}->[1];
            next if ( not $destination );
            $tiedhash->{$file} = 0 if ( not $filepos );
            $tails->{$file}    = 1;
            $forking->{$file}  = 1;
            $self->{job_controller}->start_job( 'TailFork', "$file:tail",
                "$destination $node_path $path $filepos" );
        }
    }
    my $tiedhash = $self->tiedhash;
    for my $file ( keys %{$tiedhash} ) {
        delete $tiedhash->{$file} if ( not $files->{$file} );
    }
    $self->timer->set_timer( $Scan_Interval * 1000 );
    return;
}

sub connect_list {
    my $self = shift;
    if (@_) {
        $self->{connect_list} = shift;
        for my $host_port ( @{ $self->{connect_list} } ) {
            my ( $host, $port, $use_SSL ) = split m{:}, $host_port, 3;
            my $connection =
                Tachikoma::Nodes::Socket->inet_client( $host, $port, undef,
                $use_SSL );
            $connection->on_EOF('reconnect');
            $connection->sink($self);
            $self->load_controller->add_connector( id => $connection->name );
        }
    }
    return $self->{connect_list};
}

sub get_destination {
    my $self         = shift;
    my $file         = shift;
    my $connect_list = $self->{connect_list};
    my $offline      = $self->{load_controller}->{offline};
    my @destinations = grep not( $offline->{$_} ), @{$connect_list};
    return if ( not @destinations );
    my $i = 0;
    $i += $_ for ( unpack 'C*', md5($file) );
    return $destinations[ $i % @destinations ];
}

sub position {
    my $self     = shift;
    my $file     = shift;
    my $position = shift;
    my $tiedhash = $self->tiedhash;
    $tiedhash->{$file} = $position;
    return;
}

sub close_tail {
    my $self           = shift;
    my $file           = shift;
    my $tails          = $self->tails;
    my $job_controller = $self->job_controller;
    if ( $tails->{$file} ) {
        $job_controller->stop_job("$file:tail");
        delete $tails->{$file};
    }

    # $self->stderr("closed $file");
    return;
}

sub finish_file {
    my $self  = shift;
    my $file  = shift;
    my $event = shift;
    my $tails = $self->tails;

    # $self->stderr((caller(1))[3] . " called finish");
    if ( $tails->{$file} ) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_BYTESTREAM;
        $message->[TO]      = "$file:tail";
        $message->[PAYLOAD] = "$event\n";
        $self->{sink}->fill($message);
        delete $tails->{$file};
        my $old_name = "$file:tail";
        my $new_name = sprintf '%s-%06d',
            $old_name, $self->job_controller->job_counter;
        my $okay = eval {
            $self->job_controller->rename_job( $old_name, $new_name );
            return 1;
        };
        $self->stderr("ERROR: $@") if ( not $okay );
        delete $self->tiedhash->{$file};
    }
    return;
}

sub job_controller {
    my $self = shift;
    if (@_) {
        $self->{job_controller} = shift;
    }
    return $self->{job_controller};
}

sub load_controller {
    my $self = shift;
    if (@_) {
        $self->{load_controller} = shift;
    }
    return $self->{load_controller};
}

sub tails {
    my $self = shift;
    if (@_) {
        $self->{tails} = shift;
    }
    return $self->{tails};
}

sub files {
    my $self = shift;
    if (@_) {
        $self->{files} = shift;
    }
    return $self->{files};
}

sub forking {
    my $self = shift;
    if (@_) {
        $self->{forking} = shift;
    }
    return $self->{forking};
}

sub tiedhash {
    my $self = shift;
    if (@_) {
        $self->{tiedhash} = shift;
    }
    if ( not defined $self->{tiedhash} ) {
        ## no critic (ProhibitTie)
        my %h    = ();
        my %copy = ();
        my $path = $self->db_dir . q{/} . $self->filename;
        $self->make_parent_dirs($path);
        if ( -f $path ) {

            # defunk
            tie %h, 'BerkeleyDB::Btree',
                -Filename => $path,
                -Mode     => 0600
                or warn "couldn't tie $path: $!";
            %copy = %h;
            untie %h or warn "couldn't untie $path: $!";
            unlink $path or warn "couldn't unlink $path: $!";
        }
        tie %h, 'BerkeleyDB::Btree',
            -Filename => $path,
            -Flags    => DB_CREATE,
            -Mode     => 0600
            or die "couldn't tie $path: $!\n";
        %h = %copy if ( keys %copy );
        return $self->{tiedhash} = \%h;
    }
    return $self->{tiedhash};
}

sub db_dir {
    my $self = shift;
    if (@_) {
        $DB_Dir = shift;
    }
    return $DB_Dir;
}

sub filename {
    my $self = shift;
    if (@_) {
        $self->{filename} = shift;
    }
    if ( not defined $self->{filename} ) {
        $self->{filename} = $self->{name} . '.db';
    }
    return $self->{filename};
}

sub collector {
    my $self = shift;
    if (@_) {
        $self->{collector} = shift;
    }
    return $self->{collector};
}

sub timer {
    my $self = shift;
    if (@_) {
        $self->{timer} = shift;
    }
    if ( not defined $self->{timer} ) {
        my $timer = Tachikoma::Nodes::Timer->new;
        $timer->name('Timer');
        $timer->sink($self);
        $self->{timer} = $timer;
    }
    return $self->{timer};
}

1;
