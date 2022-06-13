#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::STDIO
# ----------------------------------------------------------------------
#
# Tachikoma's connections to the outside world.
# If Tachikoma had a single edge, this module would be it.
#   - on_EOF: close, send, ignore, reconnect
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
use IO::Socket::SSL qw( SSL_WANT_WRITE );
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
    $self->{last_fill}    = undef;
    $self->{got_EOF}      = undef;
    $self->{drain_buffer} = \&drain_buffer_normal;
    $self->{line_buffer}  = q();
    $self->{buffer_mode}  = 'binary';
    $self->{fill_fh}      = \&fill_fh;
    $self->{fill_modes}->{fill} =
        $flags & TK_SYNC ? \&fill_fh_sync : \&fill_buffer;
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
    $self->set_state( 'CONNECTED' => $self->{name} );
    return;
}

sub init_accept {
    my $self = shift;
    $self->{fill_fh}   = \&fill_fh;
    $self->{drain_fh}  = \&drain_fh;
    $self->{last_fill} = 0;
    $self->set_state( 'CONNECTED' => $self->{name} );
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

sub drain_buffer_normal {
    my ( $self, $buffer ) = @_;
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[FROM]    = $self->{name};
    $message->[TO]      = $self->{owner};
    $message->[PAYLOAD] = ${$buffer};
    $self->{bytes_read} += length ${$buffer};
    $message->[ID] = $self->{bytes_read};
    $self->{counter}++;
    $self->{sink}->fill($message);
    return;
}

sub drain_buffer_lines {
    my ( $self, $buffer, $stream ) = @_;
    my $name  = $self->{name};
    my $sink  = $self->{sink};
    my $owner = $self->{owner};
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
        $self->{counter}++;
        $sink->fill($message);
    }
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_EOF ) {

        # upstream is done sending, we can close once we catch up
        $self->{got_EOF} = 'true';
        $self->handle_EOF if ( not @{ $self->{output_buffer} } );
    }
    else {
        $self->SUPER::fill($message);
    }
    return;
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
    my @cancel = ();
    while ( @{$buffer} ) {
        my $message = $buffer->[0];
        my $size    = length $message->[PAYLOAD];
        my $wrote   = syswrite $fh, $message->[PAYLOAD], $size - $cursor,
            $cursor;
        if ( $wrote and $wrote > 0 ) {
            $cursor += $wrote;
            $self->{bytes_written} += $wrote;
            last if ( $cursor < $size );
            shift @{$buffer};
            push @cancel, $message if ( $message->[TYPE] & TM_PERSIST );
            $cursor = 0;
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
    $self->{last_fill}     = @{$buffer} ? $Tachikoma::Now : 0
        if ( defined $self->{last_fill} );
    $self->cancel($_) for (@cancel);
    $self->unregister_writer_node if ( not @{$buffer} );
    $self->handle_EOF if ( $eof or ( not @{$buffer} and $self->{got_EOF} ) );
    return;
}

sub handle_EOF {
    my $self   = shift;
    my $on_EOF = $self->{on_EOF};
    $self->{got_EOF} = undef;
    $self->SUPER::handle_EOF;
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

sub set_drain_buffer {
    my $self = shift;
    $self->{drain_buffer} =
        $self->{buffer_mode} eq 'binary'
        ? \&drain_buffer_normal
        : \&drain_buffer_lines;
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
        $self->{buffer_mode} = shift;
        $self->set_drain_buffer;
    }
    return $self->{buffer_mode};
}

sub last_fill {
    my $self = shift;
    if (@_) {
        $self->{last_fill} = shift;
    }
    return $self->{last_fill};
}

sub got_EOF {
    my $self = shift;
    if (@_) {
        $self->{got_EOF} = shift;
    }
    return $self->{got_EOF};
}

1;
