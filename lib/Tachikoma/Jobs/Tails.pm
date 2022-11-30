#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::Tails
# ----------------------------------------------------------------------
#

package Tachikoma::Jobs::Tails;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Nodes::Shell2;
use Tachikoma::Nodes::Responder;
use Tachikoma::Nodes::FileWatcher;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::Tail;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_BYTESTREAM TM_EOF
);
use BerkeleyDB;
use Digest::MD5 qw( md5 );
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.368');

my $Home   = Tachikoma->configuration->home || ( getpwuid $< )[7];
my $DB_Dir = "$Home/.tachikoma/tails";
my $Offset_Interval = 1;      # write offsets this often
my $Scan_Interval   = 15;     # check files this often
my $Startup_Delay   = -10;    # offset from scan interval
my $Default_Timeout = 300;    # message timeout for tails

sub initialize_graph {
    my $self         = shift;
    my $name         = $self->name;
    my $interpreter  = Tachikoma::Nodes::CommandInterpreter->new;
    my $shell        = Tachikoma::Nodes::Shell2->new;
    my $responder    = Tachikoma::Nodes::Responder->new;
    my $file_watcher = Tachikoma::Nodes::FileWatcher->new;
    $self->connector->sink($interpreter);
    $shell->sink($interpreter);
    $interpreter->name('_command_interpreter');
    $responder->name('_responder');
    $responder->shell($shell);
    $responder->sink( $self->router );
    $self->interpreter($interpreter);
    $file_watcher->name('_file_watcher');
    $file_watcher->sink($self);
    $self->file_watcher($file_watcher);
    $self->tails( {} );
    $self->files( {} );
    $self->sink( $self->router );
    $self->timer->set_timer( $Offset_Interval * 1000 );
    $self->last_scan( $Tachikoma::Now + $Startup_Delay );
    $interpreter->sink($self);
    $interpreter->commands->{'add_tail'} = sub {
        my $this     = shift;
        my $command  = shift;
        my $envelope = shift;
        my ( $file, $node_path, $stream ) = split q( ), $command->arguments,
            3;
        $stream //= q();
        for my $expanded ( glob $file ) {
            my $quoted = $expanded;
            $quoted =~ s{/}{:}g;
            $self->files->{$quoted} = [ $expanded, $node_path, $stream ];
        }
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
            my $path = $files->{$file}->[0];
            next if ( $path !~ m{$regex} );
            delete $self->files->{$file};
            $self->finish_file($file);
            delete $self->file_watcher->files->{$path};
            $found = 1;
        }
        die "no files matching: $regex\n" if ( not $found );
        return $this->okay($envelope);
    };
    $interpreter->commands->{'start_tail'} = sub {
        my $this     = shift;
        my $command  = shift;
        my $envelope = shift;
        $self->timer->set_timer( $Offset_Interval * 1000 );
        $self->last_scan( $Tachikoma::Now + $Startup_Delay );
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
        $self->timer->stop_timer;
        return $this->okay($envelope);
    };
    $interpreter->commands->{'list_files'} = sub {
        my $this     = shift;
        my $command  = shift;
        my $envelope = shift;
        my $files    = $self->files;
        my $response = [
            [   [ 'FILE'   => 'right' ],
                [ 'NODE'   => 'left' ],
                [ 'STREAM' => 'left' ]
            ]
        ];
        for my $file ( sort keys %{$files} ) {
            push @{$response}, $files->{$file};
        }
        return $this->response( $envelope, $this->tabulate($response) );
    };
    $interpreter->commands->{'list_tails'} = sub {
        my $this     = shift;
        my $command  = shift;
        my $envelope = shift;
        my $response = q();
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
    if ( $message->from eq '_file_watcher' ) {
        my $events = qr{(created|inode changed|missing)};
        my $event  = undef;
        if ( $message->[PAYLOAD] =~ m{^_file_watcher: file (.*) $events$} ) {
            my $file = $1;
            $event = $2;
            $file =~ s{/}{:}g;
            $self->finish_file($file);
        }
        else {
            $self->stderr( 'WARNING: unexpected message from ',
                $message->from, ': ', $message->payload );
        }
        $self->rescan_files if ( $event and $event ne 'missing' );
        return;
    }
    elsif ( $message->from eq '_timer' ) {
        my $tiedhash = $self->tiedhash;
        my $files    = $self->{files};
        if ( $Tachikoma::Now - $self->{last_scan} >= $Scan_Interval ) {
            $self->rescan_files;
        }
        for my $file ( keys %{$files} ) {
            next if ( not $files->{$file} );
            my $node = $Tachikoma::Nodes{"$file:tail"};
            if ( $node and $node->{bytes_answered} != $tiedhash->{$file} ) {
                $tiedhash->{$file} = $node->{bytes_answered};
            }
        }
        return;
    }
    return $self->{sink}->fill($message);
}

sub rescan_files {
    my $self = shift;
    return if ( not $self->{timer} );
    my $tails = $self->{tails};
    my $files = $self->{files};
    for my $file ( keys %{$files} ) {
        my $path = $files->{$file}->[0];
        next if ( not -e $path );
        if ( not $Tachikoma::Nodes{"$file:tail"} ) {
            my $tiedhash  = $self->tiedhash;
            my $filepos   = $tiedhash->{$file} || 0;
            my $node_path = $files->{$file}->[1];
            my $stream    = $files->{$file}->[2];
            $tiedhash->{$file} = 0 if ( not $filepos );
            $tails->{$file}    = 1;
            my $arguments = "--filename=$path --offset=$filepos";
            $arguments .= " --stream=$stream" if ( length $stream );
            my $tail = Tachikoma::Nodes::Tail->new;
            $tail->name("$file:tail");
            $tail->arguments($arguments);
            $tail->buffer_mode('line-buffered');
            $tail->max_unanswered(256);
            $tail->timeout($Default_Timeout);
            $tail->owner($node_path);
            $tail->sink( $self->router );
            my $message = Tachikoma::Message->new;
            $message->type(TM_BYTESTREAM);
            $message->payload($path);
            $self->{file_watcher}->fill($message);
        }
    }
    my $tiedhash = $self->tiedhash;
    for my $file ( keys %{$tiedhash} ) {
        delete $tiedhash->{$file} if ( not $files->{$file} );
    }
    tied( %{$tiedhash} )->db_sync;
    return;
}

sub close_tail {
    my $self  = shift;
    my $file  = shift;
    my $tails = $self->tails;
    if ( $tails->{$file} ) {
        my $node = $Tachikoma::Nodes{"$file:tail"};
        $node->remove_node if ($node);
        delete $tails->{$file};
    }
    return;
}

sub finish_file {
    my $self  = shift;
    my $file  = shift;
    my $tails = $self->tails;
    if ( $tails->{$file} ) {
        my $node = $Tachikoma::Nodes{"$file:tail"};
        $node->on_EOF('close') if ($node);
        delete $tails->{$file};
        delete $self->tiedhash->{$file};
    }
    return;
}

sub file_watcher {
    my $self = shift;
    if (@_) {
        $self->{file_watcher} = shift;
    }
    return $self->{file_watcher};
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

sub tiedhash {
    my $self = shift;
    if (@_) {
        $self->{tiedhash} = shift;
    }
    if ( not defined $self->{tiedhash} ) {
        ## no critic (ProhibitTie)
        my %h    = ();
        my %copy = ();
        my $path = $self->db_dir . q(/) . $self->filename;
        $self->make_parent_dirs($path);
        if ( -f $path ) {

            # defunk
            tie %h, 'BerkeleyDB::Btree',
                -Filename => $path,
                -Mode     => 0600
                or warn "couldn't tie $path: $!";
            %copy = %h;
            untie %h     or warn "couldn't untie $path: $!";
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

sub timer {
    my $self = shift;
    if (@_) {
        $self->{timer} = shift;
    }
    if ( not defined $self->{timer} ) {
        my $timer = Tachikoma::Nodes::Timer->new;
        $timer->name('_timer');
        $timer->sink($self);
        $self->{timer} = $timer;
    }
    return $self->{timer};
}

sub last_scan {
    my $self = shift;
    if (@_) {
        $self->{last_scan} = shift;
    }
    return $self->{last_scan};
}

1;
