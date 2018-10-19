#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Watcher
# ----------------------------------------------------------------------
#
# $Id: Watcher.pm 35385 2018-10-19 03:38:23Z chris $
#

package Tachikoma::Nodes::Watcher;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE FROM TO TM_BYTESTREAM );
use IO::KQueue;
use Config;
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = 'v2.0.367';

my $Check_Proc_Interval = 10;
my $Can_KQueue          = 1;
if ( $Config{osname} eq 'freebsd' ) {
    ## no critic (ProhibitBacktickOperators)
    local %ENV = ();
    my $version = `/usr/bin/uname -r`;
    my ( $major, $minor ) = ( $version =~ m{^(\d+)[.](\d+)} );
    $Can_KQueue = undef
        if ( $major >= 10
        or ( $major == 8 and $minor >= 4 ) );
}

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{drain_fh} = \&drain_fh;
    $self->{note_fh}  = \&note_fh;
    $self->{fd}       = undef;
    $self->{fh}       = undef;
    $self->{pid}      = undef;
    $self->{filter}   = undef;
    $self->{mapping}  = undef;
    $self->{notes}    = undef;
    $self->{orly}     = undef;
    $self->{queue}    = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Watcher <node name> <filter> <id> <notes>
    valid filters:
        proc  - id is a process id
              - valid notes: exit child fork exec track trackerr
        vnode - id is a path to a file
              - valid notes: delete write extend attrib link rename revoke
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ( $filter, $id, @notes ) = split q( ), $arguments;
        $self->{arguments} = $arguments;
        if ( $filter eq 'proc' ) {
            die "bad pid: $id" if ( $id !~ m{^\d+$} );
            $self->{filter}  = EVFILT_PROC;
            $self->{mapping} = {
                'exit'     => NOTE_EXIT,
                'child'    => NOTE_CHILD,
                'fork'     => NOTE_FORK,
                'exec'     => NOTE_EXEC,
                'track'    => NOTE_TRACK,
                'trackerr' => NOTE_TRACKERR
            };
            $self->{notes} = [ map { $self->{mapping}->{$_} } @notes ];
            $self->{pid}   = $id;
            $self->{orly}  = undef;
            die "already watching $id\n"
                if ( $Tachikoma::Nodes_By_PID->{$id} );
            $Tachikoma::Nodes_By_PID->{$id} = $self;
            $self->register_reader_node;
        }
        elsif ( $filter eq 'vnode' ) {
            my $fh;
            $self->{filter}  = EVFILT_VNODE;
            $self->{mapping} = {
                'delete' => NOTE_DELETE,
                'write'  => NOTE_WRITE,
                'extend' => NOTE_EXTEND,
                'attrib' => NOTE_ATTRIB,
                'link'   => NOTE_LINK,
                'rename' => NOTE_RENAME,
                'revoke' => NOTE_REVOKE
            };
            $self->{notes} = [ map { $self->{mapping}->{$_} } @notes ];
            $self->{filename} = $id;
            if ( open $fh, '<', $id ) {
                $self->fh($fh);
                $self->register_reader_node;
            }
            else {
                $self->reattempt;
            }
        }
        else {
            die "bad filter: $filter";
        }
    }
    return $self->{arguments};
}

sub register_reader_node {
    my $self  = shift;
    my $notes = 0;
    $notes |= $_ for ( @{ $self->{notes} } );
    if ( $self->{filter} == EVFILT_VNODE ) {
        $self->queue->EV_SET( $self->{fd}, $self->{filter}, EV_ADD | EV_CLEAR,
            $notes );
    }
    else {
        $self->queue->EV_SET( $self->{pid}, $self->{filter}, EV_ADD, $notes );
        $self->set_timer( $Check_Proc_Interval * 1000 );
    }
    return $self;
}

sub drain_fh {
    my $self = shift;
    die 'unexpected drain_fh';
}

sub fire {
    my $self   = shift;
    my $kevent = [];
    if ( $self->{filter} == EVFILT_VNODE ) {
        $kevent->[KQ_FFLAGS] = NOTE_DELETE;
        return $self->note_fh($kevent);
    }
    elsif ( not kill 0, $self->{pid} and $! eq 'No such process' ) {
        if ( $self->{orly} ) {
            $kevent->[KQ_FFLAGS] = NOTE_EXIT;
            $kevent->[KQ_DATA]   = q();
            $self->stderr(
                "WARNING: KQueue missed process exit, working around...\n")
                if ($Can_KQueue);
            return $self->note_fh($kevent);
        }
        else {
            $self->{orly} = 'yarly';
        }
    }
    return;
}

sub note_fh {
    my $self    = shift;
    my $kevent  = shift;
    my $message = Tachikoma::Message->new;
    my $mapping = $self->{mapping};
    my @notes   = ();
    $message->[TYPE] = TM_BYTESTREAM;
    $message->[FROM] = $self->{name};
    $message->[TO]   = $self->{owner};
    if ( $self->{filter} == EVFILT_PROC ) {

        # wtf, kqueue? wtf?
        # translation: NOTE_EXIT is the only one seeming to work, and
        #              even then it isn't getting set in the KQ_FFLAGS
        $kevent->[KQ_FFLAGS] = NOTE_EXIT;
    }
    for my $note ( keys %{$mapping} ) {
        push @notes, $note if ( $kevent->[KQ_FFLAGS] & $mapping->{$note} );
    }
    if ( $self->{filter} == EVFILT_PROC ) {
        $self->stderr("NOTICE: KQueue caught process exit!\n")
            if ($kevent->[KQ_FFLAGS] & NOTE_EXIT
            and not $Can_KQueue
            and not $self->{orly} );
        $message->payload( "$self->{name}: process $self->{pid} "
                . join( q(, ), @notes ) . q( )
                . $kevent->[KQ_DATA]
                . "\n" );
        $self->{counter}++;
        $self->{sink}->fill($message);
        $self->remove_node if ( $kevent->[KQ_FFLAGS] & NOTE_EXIT );
    }
    else {
        if ( $kevent->[KQ_IDENT] ) {
            $message->payload( "$self->{name}: file $self->{filename} "
                    . join( q(, ), @notes ) . q( )
                    . $kevent->[KQ_DATA]
                    . "\n" );
            $self->{counter}++;
            $self->{sink}->fill($message);
        }
        if (   $kevent->[KQ_FFLAGS] & NOTE_DELETE
            or $kevent->[KQ_FFLAGS] & NOTE_RENAME )
        {
            if ( defined $self->{fd} ) {
                my $okay = eval {
                    $self->queue->EV_SET( $self->{fd}, EVFILT_VNODE,
                        EV_DELETE );
                    return 1;
                };
                if ( not $okay ) {
                    my $error = $@ || 'unknown error';
                    $self->stderr("ERROR: EV_DELETE failed: $error");
                }
                close $self->{fh}
                    or $self->stderr("ERROR: couldn't close: $!");
                delete Tachikoma->nodes_by_fd->{ $self->{fd} };
                $self->{fd} = undef;
            }
            my $fh;
            $message         = Tachikoma::Message->new;
            $message->[TYPE] = TM_BYTESTREAM;
            $message->[FROM] = $self->{name};
            $message->[TO]   = $self->{owner};
            if ( open $fh, '<', $self->{filename} ) {
                $self->fh($fh);
                $self->register_reader_node;
                $message->payload(
                    "$self->{name}: file $self->{filename} created\n");
            }
            else {
                $message->payload(
                    "$self->{name}: file $self->{filename} missing\n");
                $self->reattempt;
            }
            $self->{counter}++;
            $self->{sink}->fill($message);
        }
    }
    return;
}

sub remove_node {
    my $self = shift;
    $self->stop_timer if ( $self->{timer_is_active} );
    push @Tachikoma::Closing, sub {
        $self->close_filehandle;
    };
    return;
}

sub reattempt {
    my $self = shift;
    $self->set_timer( 1000, 'oneshot' );
    return;
}

sub fd {
    my $self = shift;
    if (@_) {
        $self->{fd} = shift;
    }
    return $self->{fd};
}

sub fh {
    my $self = shift;
    if (@_) {
        my $fh = shift;
        my $fd = fileno $fh;
        $self->{name} ||= $fd;
        $self->{fd}                    = $fd;
        $self->{fh}                    = $fh;
        $Tachikoma::Nodes_By_FD->{$fd} = $self;
    }
    return $self->{fh};
}

sub close_filehandle {
    my $self = shift;
    $Tachikoma::Event_Framework->close_filehandle($self);
    delete( $Tachikoma::Nodes_By_FD->{ $self->{fd} } )
        if ( defined $self->{fd} );
    delete( $Tachikoma::Nodes_By_PID->{ $self->{pid} } )
        if ( defined $self->{pid} );
    undef $!;
    close $self->{fh}
        or $self->stderr("WARNING: couldn't close: $!")
        if ( $self->{fh} and fileno $self->{fh} );
    POSIX::close( $self->{fd} ) if ( defined $self->{fd} );
    $self->{fd}  = undef;
    $self->{fh}  = undef;
    $self->{pid} = undef;
    return;
}

sub pid {
    my $self = shift;
    if (@_) {
        $self->{pid} = shift;
    }
    return $self->{pid};
}

sub filter {
    my $self = shift;
    if (@_) {
        $self->{filter} = shift;
    }
    return $self->{filter};
}

sub mapping {
    my $self = shift;
    if (@_) {
        $self->{mapping} = shift;
    }
    return $self->{mapping};
}

sub notes {
    my $self = shift;
    if (@_) {
        $self->{notes} = shift;
    }
    return $self->{notes};
}

sub orly {
    my $self = shift;
    if (@_) {
        $self->{orly} = shift;
    }
    return $self->{orly};
}

sub queue {
    my $self = shift;
    return Tachikoma->event_framework->queue;
}

1;
