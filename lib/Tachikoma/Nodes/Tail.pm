#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Tail
# ----------------------------------------------------------------------
#
#   - on_EOF: close, send, ignore, reopen, send_and_wait,
#             wait_to_send, wait_to_close, wait_to_delete,
#             wait_for_delete, wait_for_a_while
#
# $Id: Tail.pm 35265 2018-10-16 06:42:47Z chris $
#

package Tachikoma::Nodes::Tail;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::FileHandle;
use Tachikoma::Nodes::STDIO;
use Tachikoma::Message qw(
    TYPE FROM ID STREAM PAYLOAD
    TM_BYTESTREAM TM_PERSIST TM_RESPONSE TM_ERROR
);
use Tachikoma::Config qw( %Tachikoma %Forbidden );
use Fcntl qw( SEEK_SET SEEK_CUR SEEK_END );
use Getopt::Long qw( GetOptionsFromString );
use Sys::Hostname qw( hostname );
use parent qw( Tachikoma::Nodes::FileHandle );

use version; our $VERSION = qv('v2.0.280');

my $Default_Timeout = 900;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{type}            = 'regular_file';
    $self->{on_EOF}          = 'reopen';
    $self->{on_ENOENT}       = 'retry';
    $self->{on_timeout}      = 'expire';
    $self->{drain_fh}        = \&drain_fh;
    $self->{note_fh}         = \&note_fh;
    $self->{line_buffer}     = q();
    $self->{buffer_mode}     = 'binary';
    $self->{msg_unanswered}  = 0;
    $self->{max_unanswered}  = 0;
    $self->{bytes_answered}  = 0;
    $self->{timeout}         = $Default_Timeout;
    $self->{msg_timer}       = undef;
    $self->{poll_timer}      = undef;
    $self->{reattempt_timer} = undef;
    $self->{wait_timer}      = undef;
    $self->{sent_EOF}        = undef;
    $self->{reattempt}       = undef;
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
                           --on-eof=<on_EOF>                 \
                           --on-enoent=<on_ENOENT>           \
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
                'on-eof=s'         => \$on_eof,
                'on-enoent=s'      => \$on_enoent,
                'timeout=i'        => \$timeout
            );
            die "invalid option\n" if ( not $r );
            $filename ||= $argv->[0];
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
        $on_enoent = 'die' if ( defined $offset );
        $self->close_filehandle if ( $self->{fh} );
        $self->{arguments}      = $arguments;
        $self->{filename}       = $path;
        $self->{size}           = undef;
        $self->{stream}         = $stream;
        $self->{line_buffer}    = q();
        $self->{buffer_mode}    = $buffer_mode;
        $self->{msg_unanswered} = 0;
        $self->{max_unanswered} = $max_unanswered || 0;
        $self->{on_ENOENT}      = $on_enoent if ($on_enoent);
        $self->{timeout}        = $timeout if ($timeout);

        if ( not open $fh, '<', $path ) {
            $self->{on_EOF} = $on_eof if ($on_eof);
            $self->process_enoent;
            return $self->{arguments};
        }
        my $size = ( stat $fh )[7];
        if ( not defined $offset or $offset < 0 ) {
            $offset = sysseek $fh, 0, SEEK_END;
        }
        elsif ( $offset > 0 and $offset <= $size ) {
            $offset = sysseek $fh, $offset, SEEK_SET;
        }
        else {
            $offset = 0;
        }
        $self->{bytes_read}     = $offset;
        $self->{bytes_answered} = $offset;
        $self->fh($fh);
        $self->on_EOF($on_eof) if ($on_eof);
        $self->set_drain_buffer;
        $self->register_reader_node;
        $self->register_watcher_node(qw( delete rename ))
            if ( $self->{on_EOF} eq 'reopen' );
    }
    return $self->{arguments};
}

sub check_path {
    my $self     = shift;
    my $filename = shift;
    my $path     = ( $filename =~ m{^(/.*)$} )[0];
    die "ERROR: invalid path: $filename\n" if ( not defined $path );
    $path =~ s{/[.]/}{/}g while ( $path =~ m{/[.]/} );
    $path =~ s{(?:^|/)[.][.](?=/)}{}g;
    $path =~ s{/+}{/}g;
    my $link_path = undef;
    $link_path = readlink $path if ( -l $path );
    die "ERROR: forbidden file: $path\n"
        if ( $Forbidden{$path}
        or ( $link_path and $Forbidden{$link_path} ) );
    return $path;
}

sub drain_fh {
    my $self = shift;
    my $kev  = shift;
    my $fh   = $self->{fh} or return;
    $self->file_shrank if ( $kev and $kev->[4] < 0 );
    my $buffer = q();
    my $read = sysread $fh, $buffer, 65536;
    $self->print_less_often("WARNING: couldn't read: $!")
        if ( not defined $read );
    &{ $self->{drain_buffer} }( $self, \$buffer, $self->{stream} )
        if ( $read and $self->{sink} );
    $self->handle_soft_EOF
        if ( defined $read and $read < 1 );    # select and epoll
    $self->handle_EOF
        if (
        not defined $read
        or (    $read < 1
            and $self->{on_EOF} ne 'reopen'
            and $self->{on_EOF} ne 'ignore'
            and $self->finished )
        or (    $read
            and $self->{size}
            and not $self->{sent_EOF}
            and $self->finished )
        );
    return $read;
}

sub fill {
    my $self           = shift;
    my $message        = shift;
    my $msg_unanswered = $self->{msg_unanswered};
    my $max_unanswered = $self->{max_unanswered};
    return $self->check_timers($message) if ( not length $message->[FROM] );
    return $self->stderr( 'WARNING: unexpected response from ',
        $message->[FROM] )
        if ( not $msg_unanswered and not $message->[TYPE] & TM_ERROR );
    return
        if ( not $max_unanswered
        or not $message->[TYPE] & TM_PERSIST
        or not $message->[TYPE] & TM_RESPONSE );
    $self->{bytes_answered} = $message->[ID]
        if ($message->[ID] =~ m{^\d}
        and $message->[ID] > $self->{bytes_answered} );
    my $on_eof = $self->{on_EOF};
    $msg_unanswered-- if ( $msg_unanswered > 0 );
    $self->register_reader_node if ( $msg_unanswered < $max_unanswered );
    $self->{msg_unanswered} = $msg_unanswered;

    if ($msg_unanswered) {
        $self->msg_timer->set_timer( $self->{timeout} * 1000, 'oneshot' );
    }
    else {
        $self->msg_timer->stop_timer;
    }
    $self->handle_EOF
        if ($on_eof ne 'reopen'
        and $on_eof ne 'ignore'
        and $self->finished );
    return 1;
}

sub note_fh {
    my $self   = shift;
    my $on_eof = $self->{on_EOF};
    $self->unregister_watcher_node;
    if ( $on_eof eq 'wait_for_delete' ) {
        $self->wait_timer->stop_timer;
        $self->{on_EOF} = 'wait_to_close';
        $self->handle_EOF if ( $self->finished );
        return;
    }
    return if ( $on_eof ne 'reopen' and $on_eof ne 'ignore' );

    # $self->stderr("reopening $self->{filename}");
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
        if ( $self->{on_EOF} eq 'reopen' );
    $self->poll_timer->stop_timer;
    $self->reattempt_timer->stop_timer;
    $self->{bytes_read}     = 0;
    $self->{bytes_answered} = 0;
    $self->{size}           = undef;
    $self->{line_buffer}    = q();
    $self->{reattempt}      = undef;
    return;
}

sub file_shrank {
    my $self = shift;
    if ( $self->{on_EOF} ne 'reopen' ) {
        my $filename = $self->{filename};
        $self->stderr("ERROR: file $filename shrank unexpectedly");
        $self->{on_EOF} = 'close';
        $self->handle_EOF;
        return;
    }

    # $self->stderr("WARNING: $self->{filename} has shrunk");
    sysseek $self->{fh}, 0, SEEK_SET or die "ERROR: couldn't seek: $!";
    $self->{bytes_read}     = 0;
    $self->{bytes_answered} = 0;
    return;
}

sub check_timers {
    my $self    = shift;
    my $message = shift;
    if ( $message->[STREAM] eq 'msg_timer' ) {
        die "WARNING: timeout waiting for response\n"
            if ( $self->{on_timeout} eq 'die' );
        $self->stderr('WARNING: timeout waiting for response, trying again');
        $self->expire;
    }
    elsif ( $message->[STREAM] eq 'poll_timer' ) {
        $self->register_reader_node;
    }
    elsif ( $message->[STREAM] eq 'reattempt_timer' ) {
        $self->note_fh;
    }
    elsif ( $message->[STREAM] eq 'wait_timer' ) {
        if ( $self->{on_EOF} eq 'wait_for_delete' ) {
            $self->stderr('WARNING: timeout waiting for delete event');
            $self->{on_EOF} = 'close';
            $self->handle_EOF;
        }
        elsif ( $self->{on_EOF} eq 'wait_for_a_while' ) {
            $self->{on_EOF} = 'close';
            $self->handle_EOF;
        }
        else {
            $self->stderr('ERROR: unexpected wait timer');
        }
    }
    else {
        $self->stderr( 'WARNING: unexpected ', $message->type_as_string );
    }
    return;
}

sub expire {
    my ( $self, @args ) = @_;
    my $fh     = $self->{fh};
    my $offset = $self->{bytes_answered};
    my $size   = ( stat $fh )[7];
    if ( not defined $offset or $offset < 0 ) {
        $offset = sysseek $fh, 0, SEEK_END;
    }
    elsif ( $offset > 0 and $offset <= $size ) {
        $offset = sysseek $fh, $offset, SEEK_SET;
    }
    else {
        $offset = sysseek $fh, 0, SEEK_SET;
    }
    $self->{bytes_read} = $offset;
    return Tachikoma::Nodes::STDIO::expire( $self, @args );
}

sub process_enoent {
    my $self     = shift;
    my $filename = $self->{filename};
    if (    $self->{on_EOF} eq 'reopen'
        and $self->{on_ENOENT} eq 'retry' )
    {
        $self->print_less_often(
            "WARNING: can't open $self->{filename}: $! - retrying")
            if ( $self->reattempt > 10 );
        return;
    }
    else {
        die "ERROR: couldn't open $self->{filename}: $!\n";
    }
    return;
}

sub handle_soft_EOF {
    my $self = shift;
    if ( $self->{on_EOF} eq 'reopen' ) {
        my $size = ( stat $self->{filename} )[7];
        return $self->reattempt
            if ( not defined $size or $size < $self->{bytes_read} );
    }

    # only poll the file every so often
    # also throttles overzealous fifos in kqueue
    $self->unregister_reader_node;
    $self->poll_timer->set_timer( 1000 / ( $Tachikoma{Hz} || 10 ),
        'oneshot' );
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
    if ( $on_eof eq 'send_and_wait' ) {
        $self->send_EOF;
        $self->unregister_reader_node;
    }
    elsif ( $on_eof eq 'wait_to_send' ) {
        $self->wait_to_send_EOF;
        $self->unregister_reader_node;
    }
    elsif ( $on_eof eq 'wait_to_close' ) {
        $self->wait_to_close_EOF;
        $self->unregister_reader_node;
    }
    elsif ( $on_eof eq 'wait_to_delete' ) {
        $self->wait_to_delete_EOF;
        $self->unregister_reader_node;
    }
    else {
        $self->SUPER::handle_EOF;
    }
    return;
}

sub wait_to_send_EOF {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::wait_to_send_EOF(@args);
}

sub wait_to_close_EOF {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::wait_to_close_EOF(@args);
}

sub wait_to_delete_EOF {
    my $self = shift;
    if ( not $self->{msg_unanswered} ) {
        $self->send_EOF;
        $self->remove_node;
        unlink $self->{filename}
            or
            $self->stderr("WARNING: couldn't unlink $self->{filename}: $!");
    }
    return;
}

sub send_EOF {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::send_EOF(@args);
}

sub edge {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::edge(@args);
}

sub set_drain_buffer {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::set_drain_buffer(@args);
}

sub filename {
    my $self = shift;
    if (@_) {
        $self->{filename} = shift;
    }
    return $self->{filename};
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
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::line_buffer(@args);
}

sub buffer_mode {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::buffer_mode(@args);
}

sub msg_unanswered {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::msg_unanswered(@args);
}

sub max_unanswered {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::max_unanswered(@args);
}

sub bytes_answered {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::bytes_answered(@args);
}

sub on_EOF {
    my $self = shift;
    if (@_) {
        my $on_eof = shift;
        $self->{on_EOF} = $on_eof;
        if ( $on_eof eq 'wait_for_delete' ) {
            $self->register_watcher_node(qw( delete ));
            $self->wait_timer->set_timer( 3600 * 1000, 'oneshot' );
        }
        elsif ( $on_eof eq 'wait_for_a_while' ) {
            $self->wait_timer->set_timer( 900 * 1000, 'oneshot' );
        }
        $self->handle_EOF
            if ($on_eof ne 'reopen'
            and $on_eof ne 'ignore'
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
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::on_timeout(@args);
}

sub timeout {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::timeout(@args);
}

sub finished {
    my $self = shift;
    my $size = $self->{size};
    my $bytes_finished =
          $self->{max_unanswered}
        ? $self->{bytes_answered}
        : $self->{bytes_read};
    my $pos = $bytes_finished + length $self->{line_buffer};
    return 'true' if ( not defined $self->{fh} );
    return if ( $self->{on_EOF} eq 'wait_for_delete' );
    if ( not defined $size ) {
        $size = ( stat $self->{fh} )[7];
        return 'true' if ( not defined $size );
        $self->{size} = $size;
    }
    return $pos >= $size;
}

sub msg_timer {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::msg_timer(@args);
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

sub wait_timer {
    my $self = shift;
    if (@_) {
        $self->{wait_timer} = shift;
    }
    if ( not defined $self->{wait_timer} ) {
        $self->{wait_timer} = Tachikoma::Nodes::Timer->new;
        $self->{wait_timer}->stream('wait_timer');
        $self->{wait_timer}->sink($self);
    }
    return $self->{wait_timer};
}

sub sent_EOF {
    my (@args) = @_;
    return Tachikoma::Nodes::STDIO::sent_EOF(@args);
}

sub remove_node {
    my $self = shift;
    $self->{msg_timer}->remove_node       if ( $self->{msg_timer} );
    $self->{poll_timer}->remove_node      if ( $self->{poll_timer} );
    $self->{reattempt_timer}->remove_node if ( $self->{reattempt_timer} );
    $self->{wait_timer}->remove_node      if ( $self->{wait_timer} );
    $self->SUPER::remove_node;
    return;
}

1;
