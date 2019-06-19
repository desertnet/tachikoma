#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::FileHandle
# ----------------------------------------------------------------------
#
# Tachikomatic IPC - send and receive messages over filehandles
#                  - on_EOF: close, send, ignore
#
# $Id: FileHandle.pm 37668 2019-06-19 21:35:08Z chris $
#

package Tachikoma::Nodes::FileHandle;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO PAYLOAD
    TM_BYTESTREAM TM_ERROR TM_EOF
    VECTOR_SIZE
);
use Socket qw( SOL_SOCKET SO_SNDBUF SO_RCVBUF SO_SNDLOWAT SO_KEEPALIVE );
use POSIX qw( F_SETFL O_NONBLOCK EAGAIN );
use vars qw( @EXPORT_OK );
use parent qw( Exporter Tachikoma::Node );
@EXPORT_OK = qw( TK_R TK_W TK_SYNC setsockopts );

use version; our $VERSION = qv('v2.0.195');

# flags for new()
use constant TK_R    => 000001;    #    1
use constant TK_W    => 000002;    #    2
use constant TK_SYNC => 000004;    #    4

sub filehandle {
    my $class = shift;
    my $fh    = shift;
    my $flags = shift || 0;
    my $self  = $class->new($flags);
    $self->fh($fh);
    die 'FAILED: invalid option: TK_W' if ( $flags & TK_W );
    if ( $flags & TK_R ) {
        $flags ^= TK_R;
        $self->flags($flags);
        $self->register_reader_node;
    }
    return $self;
}

sub new {
    my $proto        = shift;
    my $class        = ref($proto) || $proto;
    my $flags        = shift || 0;
    my $self         = $class->SUPER::new;
    my $input_buffer = q();
    $self->{type}             = 'filehandle';
    $self->{flags}            = $flags;
    $self->{on_EOF}           = 'close';
    $self->{drain_fh}         = \&drain_fh;
    $self->{drain_buffer}     = \&drain_buffer_normal;
    $self->{fill_fh}          = \&fill_fh;
    $self->{input_buffer}     = \$input_buffer;
    $self->{output_buffer}    = [];
    $self->{output_cursor}    = undef;
    $self->{id}               = undef;
    $self->{fd}               = undef;
    $self->{fh}               = undef;
    $self->{bytes_read}       = 0;
    $self->{bytes_written}    = 0;
    $self->{high_water_mark}  = 0;
    $self->{largest_msg_sent} = 0;
    $self->{fill_modes}       = {
        null => \&null_cb,
        fill => $flags & TK_SYNC ? \&fill_fh_sync : \&fill_buffer
    };
    $self->{fill} = $self->{fill_modes}->{fill};
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        die "ERROR: invalid arguments\n";
    }
    return $self->{arguments};
}

sub setsockopts {
    my $socket = shift;
    my $config = Tachikoma->configuration;
    if ( $config->{buffer_size} ) {
        setsockopt $socket, SOL_SOCKET, SO_SNDBUF, $config->{buffer_size}
            or die "FAILED: setsockopt: $!";
        setsockopt $socket, SOL_SOCKET, SO_RCVBUF, $config->{buffer_size}
            or die "FAILED: setsockopt: $!";
    }
    if ( $config->{low_water_mark} ) {
        setsockopt $socket, SOL_SOCKET, SO_SNDLOWAT, $config->{low_water_mark}
            or die "FAILED: setsockopt: $!";
    }
    if ( $config->{keep_alive} ) {
        setsockopt $socket, SOL_SOCKET, SO_KEEPALIVE, 1
            or die "FAILED: setsockopt: $!";
    }
    return;
}

sub register_reader_node {
    my $self = shift;
    $self->{flags} |= TK_R;
    return $Tachikoma::Event_Framework->register_reader_node($self);
}

sub register_writer_node {
    my $self = shift;
    $self->{flags} |= TK_W;
    return $Tachikoma::Event_Framework->register_writer_node($self);
}

sub register_watcher_node {
    my ( $self, @watch ) = @_;
    return $Tachikoma::Event_Framework->register_watcher_node( $self,
        @watch );
}

sub drain_fh {
    my $self   = shift;
    my $fh     = $self->{fh} or return;
    my $buffer = $self->{input_buffer};
    my $got    = length ${$buffer};
    my $read   = sysread $fh, ${$buffer}, 1048576, $got;
    my $again  = $! == EAGAIN;
    $read = 0 if ( $self->{use_SSL} and not defined $read and $again );
    if ( not defined $read or ( $read < 1 and not $again ) ) {
        $self->print_less_often("WARNING: couldn't read: $!")
            if ( not defined $read and $! ne 'Connection reset by peer' );
        return $self->handle_EOF;
    }
    $got += $read;
    $got = &{ $self->{drain_buffer} }( $self, $buffer ) if ( $got > 0 );
    if ( not defined $got or $got < 1 ) {
        my $new_buffer = q();
        $self->{input_buffer} = \$new_buffer;
    }
    return $read;
}

sub drain_buffer_normal {
    my $self   = shift;
    my $buffer = shift;
    my $name   = $self->{name};
    my $sink   = $self->{sink};
    my $owner  = $self->{owner};
    my $got    = length ${$buffer};

    # XXX:M
    # my $size =
    #     $got > VECTOR_SIZE
    #     ? VECTOR_SIZE + unpack 'N', ${$buffer}
    #     : 0;
    my $size = $got > VECTOR_SIZE ? unpack 'N', ${$buffer} : 0;
    while ( $got >= $size and $size > 0 ) {
        my $message =
            Tachikoma::Message->new( \substr ${$buffer}, 0, $size, q() );
        $got -= $size;
        $self->{bytes_read} += $size;
        $self->{counter}++;

        # XXX:M
        # $size =
        #     $got > VECTOR_SIZE
        #     ? VECTOR_SIZE + unpack 'N', ${$buffer}
        #     : 0;
        $size = $got > VECTOR_SIZE ? unpack 'N', ${$buffer} : 0;
        $message->[FROM] =
            length $message->[FROM]
            ? join q(/), $name, $message->[FROM]
            : $name;
        if ( length $message->[TO] and length $owner ) {
            $self->print_less_often(
                      "ERROR: message addressed to $message->[TO]"
                    . " while owner is set to $owner"
                    . " - dropping message from $message->[FROM]" )
                if ( $message->[TYPE] != TM_ERROR );
            next;
        }
        $message->[TO] = $owner if ( length $owner );
        $sink->fill($message);
    }
    return $got;
}

sub fill {    ## no critic (RequireArgUnpacking)
    my $self = shift;
    return &{ $self->{fill} }( $self, @_ );
}

sub fill_buffer {
    my $self        = shift;
    my $message     = shift;
    my $packed      = $message->packed;
    my $buffer_size = push @{ $self->{output_buffer} }, $packed;
    my $packed_size = length ${$packed};
    $self->{counter}++;
    $self->{high_water_mark} = $buffer_size
        if ( $buffer_size > $self->{high_water_mark} );
    $self->{largest_msg_sent} = $packed_size
        if ( $packed_size > $self->{largest_msg_sent} );
    $self->register_writer_node if ( not $self->{flags} & TK_W );
    return;
}

sub fill_fh_sync {
    my $self        = shift;
    my $message     = shift;
    my $fh          = $self->{fh} or return;
    my $packed      = $message->packed;
    my $packed_size = length ${$packed};
    my $wrote       = syswrite( $fh, ${$packed} ) // 0;
    die "ERROR: wrote $wrote < $packed_size; $!\n"
        if ( $wrote != $packed_size );
    $self->{counter}++;
    $self->{largest_msg_sent} = $packed_size
        if ( $packed_size > $self->{largest_msg_sent} );
    $self->{bytes_written} += $wrote;
    return;
}

sub fill_fh {
    my $self   = shift;
    my $fh     = $self->{fh};
    my $buffer = $self->{output_buffer};
    my $cursor = $self->{output_cursor} || 0;
    my $size   = 0;
    while ( @{$buffer} ) {
        $size += length ${ $buffer->[0] };
        my $wrote = syswrite $fh, ${ $buffer->[0] }, $size - $cursor, $cursor;
        if ( $wrote and $wrote > 0 ) {
            $cursor += $wrote;
            $self->{bytes_written} += $wrote;
            last if ( $cursor < $size );
            shift @{$buffer};
            $cursor = 0;
            $size   = 0;
            next;
        }
        elsif ( $! and $! != EAGAIN ) {
            $self->print_less_often("WARNING: couldn't write: $!");
            $self->handle_EOF;
            @{$buffer} = ();
            $cursor = 0;
        }
        else {
            last;
        }
    }
    $self->{output_cursor} = $cursor;
    $self->unregister_writer_node if ( not @{$buffer} );
    return;
}

sub null_cb {
    return;
}

sub handle_EOF {
    my $self   = shift;
    my $on_EOF = $self->{on_EOF};
    if ( $on_EOF eq 'close' ) {
        $self->send_EOF;
        $self->remove_node;
    }
    elsif ( $on_EOF eq 'send' ) {
        $self->send_EOF;
    }
    return;
}

sub send_EOF {
    my $self    = shift;
    my $message = Tachikoma::Message->new;
    $message->[TYPE] = TM_EOF;
    $message->[FROM] = $self->{name};
    $message->[TO]   = $self->{owner};
    $self->{sink}->fill($message) if ( $self->{sink} );
    return;
}

sub remove_node {
    my $self = shift;
    $self->{drain_fh} = \&null_cb;
    $self->{fill_fh}  = \&null_cb;
    $self->{fill}     = \&null_cb;
    $self->on_EOF('ignore');
    push @Tachikoma::Closing, sub {
        $self->close_filehandle_and_remove_node;
    };
    return;
}

sub close_filehandle_and_remove_node {
    my $self = shift;
    $self->close_filehandle;
    $self->SUPER::remove_node;
    return;
}

sub close_filehandle {
    my $self         = shift;
    my $input_buffer = q();
    $Tachikoma::Event_Framework->close_filehandle($self);
    delete $Tachikoma::Nodes_By_FD->{ $self->{fd} }
        if ( defined $self->{fd} );
    undef $!;
    $self->stderr("WARNING: couldn't close: $!")
        if ($self->{fh}
        and fileno $self->{fh}
        and not close $self->{fh}
        and $!
        and $! ne 'Connection reset by peer'
        and $! ne 'Broken pipe' );
    POSIX::close( $self->{fd} ) if ( defined $self->{fd} );

    if ( $self->{type} ne 'regular_file' ) {
        $self->{drain_fh} = \&null_cb;
        $self->{fill_fh}  = \&null_cb;
        $self->{fill}     = \&null_cb;
    }
    $self->{flags}         = 0;
    $self->{fd}            = undef;
    $self->{fh}            = undef;
    $self->{input_buffer}  = \$input_buffer;
    $self->{output_buffer} = [];
    $self->{output_cursor} = undef;
    return;
}

sub unregister_reader_node {
    my $self = shift;
    $self->{flags} ^= TK_R;
    $Tachikoma::Event_Framework->unregister_reader_node($self);
    return;
}

sub unregister_writer_node {
    my $self = shift;
    $self->{flags} ^= TK_W;
    $Tachikoma::Event_Framework->unregister_writer_node($self);
    return;
}

sub unregister_watcher_node {
    my $self = shift;
    $Tachikoma::Event_Framework->unregister_watcher_node($self);
    return;
}

sub type {
    my $self = shift;
    if (@_) {
        $self->{type} = shift;
    }
    return $self->{type};
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

sub on_EOF {
    my $self = shift;
    if (@_) {
        $self->{on_EOF} = shift;
    }
    return $self->{on_EOF};
}

sub input_buffer {
    my $self = shift;
    if (@_) {
        $self->{input_buffer} = shift;
    }
    return $self->{input_buffer};
}

sub output_buffer {
    my $self = shift;
    if (@_) {
        $self->{output_buffer} = shift;
    }
    return $self->{output_buffer};
}

sub output_cursor {
    my $self = shift;
    if (@_) {
        $self->{output_cursor} = shift;
    }
    return $self->{output_cursor};
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
        $self->{fd} = $fd;
        $self->{fh} = $fh;
        if ( $self->{flags} & TK_SYNC ) {
            ## no critic (RequireCheckedSyscalls)
            fcntl $fh, F_SETFL, 0;    # fails for /dev/null on freebsd
        }
        else {
            fcntl $fh, F_SETFL, O_NONBLOCK or die "FAILED: fcntl: $!";
        }
        $Tachikoma::Nodes_By_FD->{$fd} = $self;
    }
    return $self->{fh};
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

sub high_water_mark {
    my $self = shift;
    if (@_) {
        $self->{high_water_mark} = shift;
    }
    return $self->{high_water_mark};
}

sub largest_msg_sent {
    my $self = shift;
    if (@_) {
        $self->{largest_msg_sent} = shift;
    }
    return $self->{largest_msg_sent};
}

sub fill_modes {
    my $self = shift;
    if (@_) {
        $self->{fill_modes} = shift;
    }
    return $self->{fill_modes};
}

1;
