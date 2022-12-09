#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Tail
# ----------------------------------------------------------------------
#
#   - on_EOF: close, send, ignore, delete
#

package Tachikoma::Nodes::Tail;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::FileHandle;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_BYTESTREAM TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use Fcntl qw( SEEK_SET SEEK_CUR SEEK_END );
use Getopt::Long qw( GetOptionsFromString );
use Sys::Hostname qw( hostname );
use parent qw( Tachikoma::Nodes::FileHandle );

use version; our $VERSION = qv('v2.0.280');

my $DEFAULT_TIMEOUT = 900;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{type}            = 'regular_file';
    $self->{stream}          = q();
    $self->{drain_fh}        = \&drain_fh;
    $self->{note_fh}         = \&note_fh;
    $self->{drain_buffer}    = \&drain_buffer_normal;
    $self->{line_buffer}     = q();
    $self->{buffer_mode}     = 'binary';
    $self->{inflight}        = [];
    $self->{inode}           = 0;
    $self->{size}            = undef;
    $self->{msg_unanswered}  = 0;
    $self->{max_unanswered}  = 0;
    $self->{bytes_answered}  = 0;
    $self->{on_EOF}          = 'ignore';
    $self->{on_ENOENT}       = 'retry';
    $self->{on_timeout}      = 'expire';
    $self->{timeout}         = $DEFAULT_TIMEOUT;
    $self->{sent_EOF}        = undef;
    $self->{reattempt}       = undef;
    $self->{msg_timer}       = undef;
    $self->{poll_timer}      = undef;
    $self->{reattempt_timer} = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Tail <node name> <filename> [ <offset> [ <max unanswered> ] ]
make_node Tail <node name> --filename=<filename>             \
                           --stream=<stream>                 \
                           --offset=<offset>                 \
                           --buffer_mode=<buffer mode>       \
                           --max_unanswered=<max unanswered> \
                           --on_eof=<on_EOF>                 \
                           --on_enoent=<on_ENOENT>           \
                           --timeout=<seconds>
    # buffer modes: line-buffered, block-buffered, binary
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments      = shift;
        my $filename       = undef;
        my $stream         = undef;
        my $offset         = undef;
        my $buffer_mode    = undef;
        my $max_unanswered = undef;
        my $on_eof         = undef;
        my $on_enoent      = undef;
        my $timeout        = undef;

        if ( not ref $arguments ) {
            my ( $r, $argv ) = GetOptionsFromString(
                $arguments,
                'filename=s'       => \$filename,
                'stream=s'         => \$stream,
                'offset=i'         => \$offset,
                'buffer_mode=s'    => \$buffer_mode,
                'max_unanswered=i' => \$max_unanswered,
                'on_eof=s'         => \$on_eof,
                'on_enoent=s'      => \$on_enoent,
                'timeout=i'        => \$timeout
            );
            die "ERROR: bad arguments for Tail\n" if ( not $r );
            $filename       //= $argv->[0];
            $offset         //= $argv->[1];
            $max_unanswered //= $argv->[2];
            $buffer_mode    //= 'line-buffered';
        }
        else {
            $filename       = $arguments->{filename};
            $offset         = $arguments->{offset};
            $stream         = $arguments->{stream};
            $buffer_mode    = $arguments->{buffer_mode} // 'binary';
            $max_unanswered = $arguments->{max_unanswered};
            $on_eof         = $arguments->{on_EOF};
            $on_enoent      = $arguments->{on_ENOENT};
            $timeout        = $arguments->{timeout};
        }
        my $fh;
        my $path = $self->check_path($filename);
        $stream //= join q(:), hostname(), $path;
        $on_enoent //= 'remove' if ( defined $offset );
        $self->close_filehandle if ( $self->{fh} );
        $self->{arguments}      = $arguments;
        $self->{filename}       = $path;
        $self->{size}           = undef;
        $self->{stream}         = $stream // q();
        $self->{line_buffer}    = q();
        $self->{buffer_mode}    = $buffer_mode;
        $self->{inflight}       = [];
        $self->{msg_unanswered} = 0;
        $self->{max_unanswered} = $max_unanswered || 0;
        $self->{on_ENOENT}      = $on_enoent if ($on_enoent);
        $self->{timeout}        = $timeout if ($timeout);

        if ( not open $fh, '<', $path ) {
            $self->{on_EOF} = $on_eof if ($on_eof);
            $self->process_enoent;
            return $self->{arguments};
        }
        my $inode = ( stat $fh )[1];
        my $size  = ( stat _ )[7];
        if ( not defined $offset or $offset < 0 ) {
            $offset = sysseek $fh, 0, SEEK_END;
        }
        elsif ( $offset > 0 and $offset <= $size ) {
            $offset = sysseek $fh, $offset, SEEK_SET;
        }
        else {
            $offset = 0;
        }
        $self->{inode}          = $inode // 0;
        $self->{bytes_read}     = $offset;
        $self->{bytes_answered} = $offset;
        $self->fh($fh);
        $self->on_EOF($on_eof) if ($on_eof);
        $self->set_drain_buffer;
        $self->register_reader_node;
        $self->register_watcher_node(qw( delete rename ))
            if ( $self->{on_EOF} eq 'ignore' );
    }
    return $self->{arguments};
}

sub check_path {
    my $self     = shift;
    my $filename = shift;
    die "ERROR: bad arguments for Tail\n" if ( not $filename );
    my $path = ( $filename =~ m{^(/.*)$} )[0];
    die "ERROR: invalid path: $filename\n" if ( not defined $path );
    my $forbidden = $self->configuration->forbidden;
    $path =~ s{/[.]/}{/}g while ( $path =~ m{/[.]/} );
    $path =~ s{(?:^|/)[.][.](?=/)}{}g;
    $path =~ s{/+}{/}g;
    my $link_path = undef;
    $link_path = readlink $path if ( -l $path );
    die "ERROR: forbidden file: $path\n"
        if ( $forbidden->{$path}
        or ( $link_path and $forbidden->{$link_path} ) );
    return $path;
}

sub drain_fh {
    my $self   = shift;
    my $fh     = $self->{fh} or return;
    my $buffer = q();
    my $read   = sysread $fh, $buffer, 65536;
    $self->print_less_often("WARNING: couldn't read: $!")
        if ( not defined $read );
    $self->stderr( 'DEBUG: READ ', $read // 'undef', ' bytes' )
        if ( $self->{debug_state} and $self->{debug_state} >= 3 );
    &{ $self->{drain_buffer} }( $self, \$buffer )
        if ( $read and $self->{sink} );

    # Tachikoma::EventFrameworks::KQueue only calls drain_fh() when we are
    # not already at the end of the file.  This means it relies on
    # register_watcher_node() to watch for deletes and renames and calls
    # note_fh() directly.
    # This end-of-file condition will only occur using
    # Tachikoma::EventFrameworks::Select:
    $self->handle_soft_EOF
        if ( defined $read and $read < 1 );

    $self->handle_EOF
        if (
        not defined $read
        or (    $read < 1
            and $self->{on_EOF} ne 'ignore'
            and $self->finished )
        or (    $read
            and defined $self->{size}
            and not $self->{sent_EOF}
            and $self->finished )
        );
    return $read;
}

sub drain_buffer_normal {
    my ( $self, $buffer ) = @_;
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[FROM]    = $self->{name};
    $message->[TO]      = $self->{owner};
    $message->[STREAM]  = $self->{stream};
    $message->[PAYLOAD] = ${$buffer};

    # We want the offset at the end of the data we just read
    # because when handling responses, this becomes "bytes_answered"
    # which is our restart point on timeout.
    $self->{bytes_read} += length ${$buffer};
    $message->[ID] = $self->{bytes_read};
    my $max_unanswered = $self->{max_unanswered};

    if ($max_unanswered) {
        $message->[TYPE] |= TM_PERSIST;
        push @{ $self->{inflight} }, [ $message->[ID] => $Tachikoma::Now ];
        $self->{msg_unanswered}++;
        $self->unregister_reader_node
            if ( $self->{msg_unanswered} >= $max_unanswered );
    }
    $self->{counter}++;
    $self->{sink}->fill($message);
    $self->{msg_timer}->set_timer( $self->{timeout} * 1000 )
        if ( $self->{msg_unanswered}
        and not $self->msg_timer->{timer_is_active} );
    return;
}

sub drain_buffer_blocks {
    my ( $self, $buffer ) = @_;
    my $payload = $self->{line_buffer} . ${$buffer};
    my $part    = q();
    if ( substr( $payload, -1, 1 ) ne "\n" ) {
        if ( $payload =~ s{\n(.+)$}{\n}i ) {
            $part = $1;
        }
        else {
            $self->{line_buffer} = $payload;
            return;
        }
    }
    my $message = Tachikoma::Message->new;
    $message->[TYPE]     = TM_BYTESTREAM;
    $message->[FROM]     = $self->{name};
    $message->[TO]       = $self->{owner};
    $message->[STREAM]   = $self->{stream};
    $message->[PAYLOAD]  = $payload;
    $self->{line_buffer} = $part;
    $self->{bytes_read} += length $payload;
    $message->[ID] = $self->{bytes_read};
    my $max_unanswered = $self->{max_unanswered};

    if ($max_unanswered) {
        $message->[TYPE] |= TM_PERSIST;
        push @{ $self->{inflight} }, [ $message->[ID] => $Tachikoma::Now ];
        $self->{msg_unanswered}++;
        $self->unregister_reader_node
            if ( $self->{msg_unanswered} >= $max_unanswered );
    }
    $self->{counter}++;
    $self->{sink}->fill($message);
    $self->{msg_timer}->set_timer( $self->{timeout} * 1000 )
        if ( $self->{msg_unanswered}
        and not $self->msg_timer->{timer_is_active} );
    return;
}

sub drain_buffer_lines {
    my ( $self, $buffer ) = @_;
    my $name           = $self->{name};
    my $sink           = $self->{sink};
    my $owner          = $self->{owner};
    my $stream         = $self->{stream};
    my $max_unanswered = $self->{max_unanswered};
    for my $line ( split m{^}, ${$buffer} ) {
        if ( substr( $line, -1, 1 ) ne "\n" ) {
            $self->{line_buffer} .= $line;
            next;    # also last
        }
        my $message = Tachikoma::Message->new;
        $message->[TYPE]     = TM_BYTESTREAM;
        $message->[FROM]     = $name;
        $message->[TO]       = $owner;
        $message->[STREAM]   = $stream;
        $message->[PAYLOAD]  = $self->{line_buffer} . $line;
        $self->{line_buffer} = q();
        $self->{bytes_read} += length $message->[PAYLOAD];
        $message->[ID] = $self->{bytes_read};

        if ($max_unanswered) {
            $message->[TYPE] |= TM_PERSIST;
            push @{ $self->{inflight} },
                [ $message->[ID] => $Tachikoma::Now ];
            $self->{msg_unanswered}++;
            $self->unregister_reader_node
                if ( $self->{msg_unanswered} >= $max_unanswered );
        }
        $self->{counter}++;
        $sink->fill($message);
    }
    $self->{msg_timer}->set_timer( $self->{timeout} * 1000 )
        if ( $self->{msg_unanswered}
        and not $self->msg_timer->{timer_is_active} );
    return;
}

sub fill {
    my $self           = shift;
    my $message        = shift;
    my $msg_unanswered = $self->{msg_unanswered};
    my $offset         = $message->[ID];
    return if ( $message->[TYPE] & TM_ERROR or $message->[TYPE] & TM_EOF );
    return $self->check_timers($message) if ( not length $message->[FROM] );
    return $self->print_less_often( 'WARNING: unexpected type from ',
        $message->[FROM] )
        if ( $message->[TYPE] != ( TM_PERSIST | TM_RESPONSE ) );
    return if ( $message->[PAYLOAD] eq 'answer' );
    return $self->print_less_often( 'WARNING: unexpected payload from ',
        $message->[FROM] )
        if ( $message->[PAYLOAD] ne 'cancel' );
    return $self->print_less_often( 'WARNING: unexpected response from ',
        $message->[FROM] )
        if ( $msg_unanswered < 1 );

    if ( length $offset ) {
        my $lowest = $self->{inflight}->[0];
        if ( $lowest and $lowest->[0] == $offset ) {
            $self->{bytes_answered} = $lowest->[0];
            shift @{ $self->{inflight} };
        }
        elsif ( not $self->cancel_offset($offset) ) {
            return $self->print_less_often(
                'WARNING: unexpected response offset ',
                "$offset from ",
                $message->[FROM]
            );
        }
    }
    $msg_unanswered--;
    $self->register_reader_node
        if ( $msg_unanswered < $self->{max_unanswered} );
    if ($msg_unanswered) {
        $self->{msg_timer}->set_timer( $self->{timeout} * 1000 )
            if ( not $self->msg_timer->{timer_is_active} );
    }
    else {
        $self->{bytes_answered} = $self->{bytes_read};
        $self->{msg_timer}->stop_timer
            if ( $self->msg_timer->{timer_is_active} );
    }
    $self->{msg_unanswered} = $msg_unanswered;
    $self->handle_EOF if ( $self->{on_EOF} ne 'ignore' and $self->finished );
    return 1;
}

sub cancel_offset {
    my $self     = shift;
    my $offset   = shift;
    my $inflight = $self->{inflight};
    my $match    = undef;
    my $i        = 0;
    for my $record ( @{$inflight} ) {
        if ( $record->[0] == $offset ) {
            $match = $i;
            last;
        }
        $i++;
    }
    splice @{$inflight}, $match, 1 if ( defined $match );
    return $match;
}

sub note_fh {
    my $self   = shift;
    my $on_eof = $self->{on_EOF};
    $self->unregister_watcher_node;
    return if ( $on_eof ne 'ignore' );
    if ( defined $self->{fd} ) {
        return $self->reattempt if ( not $self->finished );
        $self->unregister_reader_node;
        close $self->{fh} or $self->stderr("ERROR: couldn't close: $!");
        delete Tachikoma->nodes_by_fd->{ $self->{fd} };
        $self->{fd}        = undef;
        $self->{reattempt} = undef;
    }
    my $fh;
    if ( not open $fh, '<', $self->{filename} ) {
        $self->process_enoent;
        return;
    }
    $self->fh($fh);
    $self->set_drain_buffer;
    $self->register_reader_node
        if ( not $self->{max_unanswered}
        or $self->{msg_unanswered} < $self->{max_unanswered} );
    $self->register_watcher_node(qw( delete rename ))
        if ( $self->{on_EOF} eq 'ignore' );
    $self->poll_timer->stop_timer;
    $self->reattempt_timer->stop_timer;
    $self->{bytes_read}     = 0;
    $self->{bytes_answered} = 0;
    $self->{inode}          = ( stat $fh )[1] // 0;
    $self->{size}           = undef;
    $self->{line_buffer}    = q();
    $self->{reattempt}      = undef;
    return;
}

sub check_timers {
    my $self    = shift;
    my $message = shift;
    if ( $message->[STREAM] eq 'msg_timer' ) {
        $self->expire_messages;
    }
    elsif ( $message->[STREAM] eq 'poll_timer' ) {
        $self->register_reader_node;
    }
    elsif ( $message->[STREAM] eq 'reattempt_timer' ) {
        $self->note_fh;
    }
    else {
        $self->stderr( 'WARNING: unexpected ', $message->type_as_string );
    }
    return;
}

sub expire_messages {
    my $self = shift;
    my $timestamp =
        @{ $self->{inflight} } ? $self->{inflight}->[0]->[1] : undef;
    if ( defined $timestamp
        and $Tachikoma::Now - $timestamp >= $self->{timeout} )
    {
        die "WARNING: timeout waiting for response\n"
            if ( $self->{on_timeout} eq 'die' );
        $self->stderr('WARNING: timeout waiting for response, trying again');
        my $fh     = $self->{fh};
        my $offset = $self->{bytes_answered};
        if ( length $self->{filename} ) {
            my $size = ( stat $fh )[7];
            if ( not defined $offset or $offset < 0 ) {
                $offset = sysseek $fh, 0, SEEK_END;
            }
            elsif ( $offset > 0 and $offset <= $size ) {
                $offset = sysseek $fh, $offset, SEEK_SET;
            }
            else {
                $offset = sysseek $fh, 0, SEEK_SET;
            }
        }
        $self->{line_buffer}    = q();
        $self->{bytes_read}     = $offset;
        $self->{bytes_answered} = $offset;
        $self->{inflight}       = [];
        $self->{msg_unanswered} = 0;
        $self->register_reader_node;
        $self->msg_timer->stop_timer;
    }
    return;
}

sub process_enoent {
    my $self     = shift;
    my $filename = $self->{filename};
    if ( $self->{on_ENOENT} eq 'remove' ) {
        $self->remove_node;
    }
    elsif ( $self->{on_ENOENT} eq 'die' ) {
        die "ERROR: couldn't open $self->{filename}: $!\n";
    }
    elsif ( $self->{on_ENOENT} eq 'retry' ) {
        $self->print_less_often(
            "WARNING: can't open $self->{filename}: $! - retrying")
            if ( $self->reattempt > 10 );
    }
    else {
        $self->stderr("WARNING: can't open $self->{filename}: $!");
    }
    return;
}

sub set_drain_buffer {
    my $self = shift;
    $self->{drain_buffer} =
          $self->{buffer_mode} eq 'binary'         ? \&drain_buffer_normal
        : $self->{buffer_mode} eq 'block-buffered' ? \&drain_buffer_blocks
        :                                            \&drain_buffer_lines;
    return;
}

sub owner {
    my $self = shift;
    if (@_) {
        $self->{owner}          = shift;
        $self->{msg_unanswered} = 0;
        $self->register_reader_node if ( $self->{fh} );
    }
    return $self->{owner};
}

sub filename {
    my $self = shift;
    if (@_) {
        $self->{filename} = shift;
    }
    return $self->{filename};
}

sub inode {
    my $self = shift;
    if (@_) {
        $self->{inode} = shift;
    }
    return $self->{inode};
}

sub size {
    my $self = shift;
    if (@_) {
        $self->{size} = shift;
    }
    return $self->{size};
}

sub stream {
    my $self = shift;
    if (@_) {
        $self->{stream} = shift;
    }
    return $self->{stream};
}

sub line_buffer {
    my $self = shift;
    if (@_) {
        $self->{line_buffer} = shift;
    }
    return $self->{line_buffer};
}

sub buffer_mode {
    my $self = shift;
    if (@_) {
        $self->{buffer_mode} = shift;
        $self->set_drain_buffer;
    }
    return $self->{buffer_mode};
}

sub msg_unanswered {
    my $self = shift;
    if (@_) {
        $self->{msg_unanswered} = shift;
    }
    return $self->{msg_unanswered};
}

sub max_unanswered {
    my $self = shift;
    if (@_) {
        $self->{max_unanswered} = shift;
    }
    return $self->{max_unanswered};
}

sub bytes_answered {
    my $self = shift;
    if (@_) {
        $self->{bytes_answered} = shift;
    }
    return $self->{bytes_answered};
}

sub on_EOF {
    my $self = shift;
    if (@_) {
        my $on_eof = shift;
        $self->{on_EOF} = $on_eof;
        $self->handle_EOF
            if ($on_eof ne 'ignore'
            and $self->{sink}
            and $self->finished );
    }
    return $self->{on_EOF};
}

sub on_ENOENT {
    my $self = shift;
    if (@_) {
        $self->{on_ENOENT} = shift;
    }
    return $self->{on_ENOENT};
}

sub on_timeout {
    my $self = shift;
    if (@_) {
        $self->{on_timeout} = shift;
    }
    return $self->{on_timeout};
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

sub finished {
    my $self = shift;
    my $size = $self->{size};
    my $pos =
          $self->{max_unanswered}
        ? $self->{bytes_answered}
        : $self->{bytes_read};
    return 'true' if ( not defined $self->{fh} );
    if ( $self->{filename} and not defined $size ) {
        $size = ( stat $self->{fh} )[7];
        return 'true' if ( not defined $size );
        $self->{size} = $size;
    }
    $self->stderr("DEBUG: POS $pos >= SIZE $size")
        if ($self->{debug_state}
        and $self->{debug_state} >= 4
        and defined $size );
    return ( defined $size and $pos >= $size );
}

sub handle_soft_EOF {
    my $self = shift;
    $self->stderr( 'DEBUG: SOFT_EOF ', $self->{msg_unanswered}, ' in flight' )
        if ( $self->{debug_state} and $self->{debug_state} >= 3 );

    # Watch for delete, rename and truncation.
    if ( $self->{on_EOF} eq 'ignore' ) {
        my $inode = 0;
        my $size  = undef;
        if ( $self->{filename} ) {
            $inode = ( stat $self->{filename} )[1];
            $size  = ( stat _ )[7];
        }
        return $self->reattempt
            if ( not $inode
            or $inode != $self->{inode}
            or not defined $size
            or $size < $self->{bytes_read} );
    }

    # Only poll the file every so often.
    my $hz = $self->{configuration}->{hz} || 10;
    $self->unregister_reader_node;
    $self->poll_timer->set_timer( 1000 / $hz, 'oneshot' );

    # Support STDIN--defining size() will enable finished() to return
    # true once all of the messages have been acknowledged.
    if ( not length $self->{filename} ) {
        $self->{size} = $self->{bytes_read};
    }
    return;
}

sub reattempt {
    my $self      = shift;
    my $reattempt = $self->reattempt_timer;
    $reattempt->set_timer( 1000, 'oneshot' )
        if ( not $reattempt->timer_is_active );
    $self->{reattempt} ||= 0;
    return $self->{reattempt}++;
}

sub handle_EOF {
    my $self   = shift;
    my $on_eof = $self->{on_EOF};
    $self->stderr('DEBUG: EOF')
        if ( $self->{debug_state} and $self->{debug_state} >= 2 );
    if ( $on_eof eq 'delete' ) {
        $self->delete_EOF;
    }
    $self->SUPER::handle_EOF;
    return;
}

sub delete_EOF {
    my $self = shift;
    $self->send_EOF;
    $self->remove_node;
    unlink $self->{filename}
        or $self->stderr("WARNING: couldn't unlink $self->{filename}: $!");
    return;
}

sub send_EOF {
    my $self = shift;
    $self->{sent_EOF} = 'true';
    return $self->SUPER::send_EOF(@_);
}

sub sent_EOF {
    my $self = shift;
    if (@_) {
        $self->{sent_EOF} = shift;
    }
    return $self->{sent_EOF};
}

sub msg_timer {
    my $self = shift;
    if (@_) {
        $self->{msg_timer} = shift;
    }
    if ( not defined $self->{msg_timer} ) {
        $self->{msg_timer} = Tachikoma::Nodes::Timer->new;
        $self->{msg_timer}->stream('msg_timer');
        $self->{msg_timer}->sink($self);
    }
    return $self->{msg_timer};
}

sub poll_timer {
    my $self = shift;
    if (@_) {
        $self->{poll_timer} = shift;
    }
    if ( not defined $self->{poll_timer} ) {
        $self->{poll_timer} = Tachikoma::Nodes::Timer->new;
        $self->{poll_timer}->stream('poll_timer');
        $self->{poll_timer}->sink($self);
    }
    return $self->{poll_timer};
}

sub reattempt_timer {
    my $self = shift;
    if (@_) {
        $self->{reattempt_timer} = shift;
    }
    if ( not defined $self->{reattempt_timer} ) {
        $self->{reattempt_timer} = Tachikoma::Nodes::Timer->new;
        $self->{reattempt_timer}->stream('reattempt_timer');
        $self->{reattempt_timer}->sink($self);
    }
    return $self->{reattempt_timer};
}

sub remove_node {
    my $self = shift;
    $self->{msg_timer}->remove_node       if ( $self->{msg_timer} );
    $self->{poll_timer}->remove_node      if ( $self->{poll_timer} );
    $self->{reattempt_timer}->remove_node if ( $self->{reattempt_timer} );
    $self->SUPER::remove_node;
    return;
}

1;
