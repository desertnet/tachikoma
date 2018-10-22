#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::STDIO
# ----------------------------------------------------------------------
#
# Tachikoma's connections to the outside world.
# If Tachikoma had a single edge, this module would be it.
#   - Efficient line buffering support for tails, etc
#   - max_unanswered support for easy throttling
#   - on_EOF: close, send, ignore, reconnect,
#             wait_to_send, wait_to_close
#
# $Id: STDIO.pm 35512 2018-10-22 08:27:21Z chris $
#

package Tachikoma::Nodes::STDIO;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::Socket qw( TK_R TK_W TK_SYNC setsockopts );
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_BYTESTREAM TM_EOF TM_PERSIST TM_RESPONSE
);
use Tachikoma::Config qw( %Tachikoma );
use POSIX qw( EAGAIN );
use vars qw( @EXPORT_OK );
use parent qw( Tachikoma::Nodes::Socket );
@EXPORT_OK = qw( TK_R TK_W TK_SYNC setsockopts );

use version; our $VERSION = qv('v2.0.195');

my $Default_Timeout = 900;

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $flags = shift || 0;
    my $self  = $class->SUPER::new($flags);
    $self->{on_timeout}     = 'expire';
    $self->{line_buffer}    = q();
    $self->{buffer_mode}    = 'binary';
    $self->{msg_unanswered} = 0;
    $self->{max_unanswered} = 0;
    $self->{bytes_answered} = 0;
    $self->{last_fill}      = undef;
    $self->{timeout}        = $Default_Timeout;
    $self->{msg_timer}      = undef;
    $self->{got_EOF}        = undef;
    $self->{sent_EOF}       = undef;
    $self->{drain_buffer}   = \&drain_buffer_normal;
    $self->{fill_fh}        = \&fill_fh;
    $self->{fill_modes}->{fill} =
        $flags & TK_SYNC
        ? \&fill_fh_sync
        : \&fill_buffer;
    $self->{fill_modes}->{unauthenticated} = $self->{fill_modes}->{fill};
    $self->{fill} = $self->{fill_modes}->{fill};
    bless $self, $class;
    return $self;
}

sub init_connect {
    my $self = shift;
    $self->{fill_fh}   = \&fill_fh;
    $self->{drain_fh}  = \&drain_fh;
    $self->{last_fill} = 0;
    return;
}

sub init_accept {
    my $self = shift;
    $self->{fill_fh}   = \&fill_fh;
    $self->{drain_fh}  = \&drain_fh;
    $self->{last_fill} = 0;
    return;
}

sub drain_fh {
    my $self   = shift;
    my $fh     = $self->{fh} or return;
    my $buffer = $self->{input_buffer};
    my $got    = length ${$buffer};
    my $read   = sysread $fh, ${$buffer}, 65536, $got;
    my $again  = $! == EAGAIN;
    $read = 0 if ( not defined $read and $again and $self->{use_SSL} );
    if ( not defined $read or ( $read < 1 and not $again ) ) {
        $self->print_less_often("WARNING: couldn't read: $!")
            if ( not defined $read and $! ne 'Connection reset by peer' );
        return $self->handle_EOF;
    }
    $got += $read;
    &{ $self->{drain_buffer} }( $self, $buffer )
        if ( $got > 0 and $self->{sink} );
    my $new_buffer = q();
    $self->{input_buffer} = \$new_buffer;
    return $read;
}

sub drain_buffer {    ## no critic (RequireArgUnpacking)
    my $self = shift;
    return (
        $self->{sink}
        ? &{ $self->{drain_buffer} }( $self, @_ )
        : undef
    );
}

sub drain_buffer_normal {
    my ( $self, $buffer, $stream ) = @_;
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[FROM]    = $self->{name};
    $message->[TO]      = $self->{owner};
    $message->[STREAM]  = $stream if ( defined $stream );
    $message->[PAYLOAD] = ${$buffer};
    $self->{bytes_read} += length ${$buffer};
    $message->[ID] = $self->{bytes_read};
    my $max_unanswered = $self->{max_unanswered};

    if ($max_unanswered) {
        $message->[TYPE] |= TM_PERSIST;
        $self->{msg_unanswered}++;
        $self->unregister_reader_node
            if ( $self->{msg_unanswered} >= $max_unanswered );
    }
    $self->{counter}++;
    $self->{sink}->fill($message);
    $self->msg_timer->set_timer( $self->{timeout} * 1000, 'oneshot' )
        if ( $self->{msg_unanswered} );
    return;
}

sub drain_buffer_blocks {
    my ( $self, $buffer, $stream ) = @_;
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
    $message->[STREAM]   = $stream if ( defined $stream );
    $message->[PAYLOAD]  = $payload;
    $self->{line_buffer} = $part;
    $self->{bytes_read} += length $payload;
    $message->[ID] = $self->{bytes_read};
    my $max_unanswered = $self->{max_unanswered};

    if ($max_unanswered) {
        $message->[TYPE] |= TM_PERSIST;
        $self->{msg_unanswered}++;
        $self->unregister_reader_node
            if ( $self->{msg_unanswered} >= $max_unanswered );
    }
    $self->{counter}++;
    $self->{sink}->fill($message);
    $self->msg_timer->set_timer( $self->{timeout} * 1000, 'oneshot' )
        if ( $self->{msg_unanswered} );
    return;
}

sub drain_buffer_lines {
    my ( $self, $buffer, $stream ) = @_;
    my $name           = $self->{name};
    my $sink           = $self->{sink};
    my $owner          = $self->{owner};
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
        $message->[STREAM]   = $stream if ( defined $stream );
        $message->[PAYLOAD]  = $self->{line_buffer} . $line;
        $self->{line_buffer} = q();
        $self->{bytes_read} += length $message->[PAYLOAD];
        $message->[ID] = $self->{bytes_read};

        if ($max_unanswered) {
            $message->[TYPE] |= TM_PERSIST;
            $self->{msg_unanswered}++;
            $self->unregister_reader_node
                if ( $self->{msg_unanswered} >= $max_unanswered );
        }
        $self->{counter}++;
        $sink->fill($message);
    }
    $self->msg_timer->set_timer( $self->{timeout} * 1000, 'oneshot' )
        if ( $self->{msg_unanswered} );
    return;
}

sub drain_buffer_edge {
    my ( $self, $buffer ) = @_;
    $self->{bytes_read} += length ${$buffer};
    return $self->{edge}->activate($buffer);
}

sub drain_buffer_edge_blocks {
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
    $self->{line_buffer} = $part;
    $self->{bytes_read} += length $payload;
    return $self->{edge}->activate( \$payload );
}

sub drain_buffer_edge_lines {
    my ( $self, $buffer ) = @_;
    for my $line ( split m{^}, ${$buffer} ) {
        if ( substr( $line, -1, 1 ) ne "\n" ) {
            $self->{line_buffer} .= $line;
            next;    # also last
        }
        my $payload = $self->{line_buffer} . $line;
        $self->{line_buffer} = q();
        $self->{bytes_read} += length $payload;
        $self->{edge}->activate( \$payload );
    }
    return;
}

sub fill {
    my $self           = shift;
    my $message        = shift;
    my $max_unanswered = $self->{max_unanswered};
    my $type           = $message->[TYPE];
    if ( $type & TM_PERSIST and $type & TM_RESPONSE ) {
        my $msg_unanswered = $self->{msg_unanswered};
        return $self->stderr( 'WARNING: unexpected response from ',
            $message->from )
            if ( not $max_unanswered or not $msg_unanswered );
        $self->{bytes_answered} = $message->[ID]
            if ($message->[ID] =~ m{^\d}
            and $message->[ID] > $self->{bytes_answered} );
        $msg_unanswered-- if ( $msg_unanswered > 0 );
        $self->register_reader_node if ( $msg_unanswered < $max_unanswered );
        $self->{msg_unanswered} = $msg_unanswered;
        if ($msg_unanswered) {
            $self->msg_timer->set_timer( $self->{timeout} * 1000, 'oneshot' );
        }
        else {
            $self->msg_timer->stop_timer;
        }
    }
    elsif ( $type & TM_EOF ) {

        # upstream is done sending, we can close once we catch up
        $self->{got_EOF} = 'true';
        $self->handle_EOF if ( not @{ $self->{output_buffer} } );
    }
    elsif ( not $message->[FROM] and $message->[STREAM] eq 'msg_timer' ) {
        if ( $self->{on_timeout} eq 'close' ) {
            $self->stderr('WARNING: timeout waiting for response');
            $self->remove_node;
        }
        else {
            $self->stderr(
                'WARNING: timeout waiting for response, trying again');
            $self->expire;
        }
    }
    else {
        $self->SUPER::fill($message);
    }
    return;
}

sub activate {    ## no critic (RequireArgUnpacking, RequireFinalReturn)
    push @{ $_[0]->{output_buffer} }, $_[1];
    $_[0]->register_writer_node if ( not $_[0]->{flags} & TK_W );
}

sub fill_buffer {
    my $self    = shift;
    my $message = shift;
    my $size    = length $message->[PAYLOAD];
    return if ( not $message->[TYPE] & TM_BYTESTREAM or not $size );
    $self->{counter}++;
    my $buffer = $self->{output_buffer};
    push @{$buffer}, $message;
    $self->{largest_msg_sent} = $size
        if ( $size > $self->{largest_msg_sent} );
    $self->{high_water_mark} = @{$buffer}
        if ( @{$buffer} > $self->{high_water_mark} );
    $self->{last_fill} ||= $Tachikoma::Now
        if ( defined $self->{last_fill} );
    $self->register_writer_node if ( not $self->{flags} & TK_W );
    return;
}

sub fill_fh_sync {
    my $self        = shift;
    my $message     = shift;
    my $fh          = $self->{fh} or return;
    my $packed      = \$message->[PAYLOAD];
    my $packed_size = length ${$packed};
    my $wrote       = 0;
    while ( $wrote < $packed_size ) {
        my $rv = syswrite $fh, ${$packed}, $packed_size - $wrote, $wrote;
        $rv = 0 if ( not defined $rv );
        last if ( not $rv );
        $wrote += $rv;
    }
    die "ERROR: wrote $wrote < $packed_size; $!\n"
        if ( $wrote != $packed_size );
    $self->{counter}++;
    $self->{largest_msg_sent} = $packed_size
        if ( $packed_size > $self->{largest_msg_sent} );
    $self->{bytes_written} += $wrote;
    $self->cancel($message) if ( $message->[TYPE] & TM_PERSIST );
    return;
}

sub fill_fh {
    my $self   = shift;
    my $fh     = $self->{fh};
    my $buffer = $self->{output_buffer};
    my $cursor = $self->{output_cursor} || 0;
    my $eof    = undef;
    while ( @{$buffer} ) {
        my $message = $buffer->[0];
        my $size    = length $message->[PAYLOAD];
        my $wrote   = syswrite $fh, $message->[PAYLOAD], $size - $cursor,
            $cursor;
        if ( $wrote and $wrote > 0 ) {
            $cursor += $wrote;
            $self->{bytes_written} += $wrote;
            last if ( $cursor < $size );
            $self->cancel($message) if ( $message->[TYPE] & TM_PERSIST );
            shift @{$buffer};
            $cursor = 0;
            next;
        }
        elsif ( $! and $! != EAGAIN ) {
            $self->print_less_often("WARNING: couldn't write: $!");
            @{$buffer} = ();
            $cursor = 0;
            $eof    = 1;
        }
        else {
            last;
        }
    }
    $self->{output_cursor} = $cursor;
    $self->{last_fill} = @{$buffer} ? $Tachikoma::Now : 0
        if ( defined $self->{last_fill} );
    $self->unregister_writer_node if ( not @{$buffer} );
    $self->handle_EOF if ( $eof or ( not @{$buffer} and $self->{got_EOF} ) );
    return;
}

sub expire {
    my $self = shift;
    $self->{msg_unanswered} = 0;
    $self->register_reader_node;
    return;
}

sub handle_EOF {
    my $self   = shift;
    my $on_EOF = $self->{on_EOF};
    $self->{got_EOF} = undef;
    if ( $on_EOF eq 'wait_to_send' ) {
        $self->wait_to_send_EOF;
        $self->unregister_reader_node;
    }
    elsif ( $on_EOF eq 'wait_to_close' ) {
        $self->wait_to_close_EOF;
        $self->unregister_reader_node;
    }
    else {
        $self->SUPER::handle_EOF;
    }
    return;
}

sub wait_to_send_EOF {
    my $self = shift;
    if ( not $self->{msg_unanswered} ) {
        $self->send_EOF;
    }
    return;
}

sub wait_to_close_EOF {
    my $self = shift;
    if ( not $self->{msg_unanswered} ) {
        $self->send_EOF;
        $self->remove_node;
    }
    return;
}

sub send_EOF {
    my $self = shift;
    $self->{sent_EOF} = 'true';
    return $self->SUPER::send_EOF(@_);
}

sub remove_node {
    my $self = shift;
    $self->{msg_timer}->remove_node if ( $self->{msg_timer} );
    $self->SUPER::remove_node;
    return;
}

sub flags {
    my $self = shift;
    if (@_) {
        $self->{flags} = shift;
        $self->{fill_modes}->{fill} =
            $self->{flags} & TK_SYNC
            ? \&fill_fh_sync
            : \&fill_buffer;
    }
    return $self->{flags};
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

sub edge {
    my $self = shift;
    my $rv   = $self->SUPER::edge(@_);
    $self->set_drain_buffer if (@_);
    return $rv;
}

sub set_drain_buffer {
    my $self = shift;
    $self->{drain_buffer} = (
          $self->{edge}
        ? $self->{buffer_mode} eq 'binary'
                ? \&drain_buffer_edge
                : $self->{buffer_mode} eq 'block-buffered'
            ? \&drain_buffer_edge_blocks
            : \&drain_buffer_edge_lines
        : $self->{buffer_mode} eq 'binary'         ? \&drain_buffer_normal
        : $self->{buffer_mode} eq 'block-buffered' ? \&drain_buffer_blocks
        :                                            \&drain_buffer_lines
    );
    return;
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
        $self->{buffer_mode} = shift // 'binary';
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

sub last_fill {
    my $self = shift;
    if (@_) {
        $self->{last_fill} = shift;
    }
    return $self->{last_fill};
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

sub got_EOF {
    my $self = shift;
    if (@_) {
        $self->{got_EOF} = shift;
    }
    return $self->{got_EOF};
}

sub sent_EOF {
    my $self = shift;
    if (@_) {
        $self->{sent_EOF} = shift;
    }
    return $self->{sent_EOF};
}

1;
