#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Join
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Join;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_BYTESTREAM TM_PERSIST TM_EOF
);
use Getopt::Long qw( GetOptionsFromString );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.367');

my $Default_Interval = 1000;
my $Default_Size     = 65536;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{buffer}    = undef;
    $self->{offset}    = undef;
    $self->{stream}    = undef;
    $self->{count}     = 0;
    $self->{max_size}  = $Default_Size;
    $self->{max_count} = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Join <node name> [ --interval=<microseconds>    ]
                           [ --size=<buffer size>         ]
                           [ --count=<number of messages> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ( $interval, $size, $count );
        my ( $r, $argv ) = GetOptionsFromString(
            $arguments,
            'interval=i' => \$interval,
            'size=i'     => \$size,
            'count=i'    => \$count,
        );
        if ( not $interval and not $size and not $count ) {
            ( $interval, $size, $count ) = @{$argv};
            $size ||= $Default_Size;
        }
        die "ERROR: invalid option\n" if ( not $r );
        $self->{arguments} = $arguments;
        $self->{buffer}    = undef;
        $self->{offset}    = undef;
        $self->{stream}    = undef;
        $self->{count}     = 0;
        $self->{max_size}  = $size;
        $self->{max_count} = $count;
        $self->set_timer( $interval || $Default_Interval );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_EOF ) {
        $self->fire;
        return $self->{sink}->fill($message);
    }
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    if ( not defined $self->{buffer} ) {
        my $new_buffer = q();
        $self->{buffer} = \$new_buffer;
    }
    my $buffer = $self->{buffer};
    ${$buffer} .= $message->[PAYLOAD];
    if ( not defined $self->{offset} ) {
        $self->{offset} = $message->[ID];
        $self->{stream} = $message->[STREAM];
    }
    $self->{count}++;
    $self->{counter}++;
    if (   ( $self->{max_size} and length( ${$buffer} ) >= $self->{max_size} )
        or ( $self->{max_count} and $self->{count} >= $self->{max_count} ) )
    {
        my $persist  = $message->[TYPE] & TM_PERSIST ? TM_PERSIST : 0;
        my $response = Tachikoma::Message->new;
        $response->[TYPE] = TM_BYTESTREAM | $persist;
        $response->[FROM] = $message->[FROM];
        $response->[TO]   = $self->{owner};

        # XXX:TODO
        # We should use $self->{offset}, but that ID has already
        # been cancelled.  Instead we'll just use the current ID
        # to keep things moving.
        #
        # $response->[ID] = $self->{offset};
        #
        # To fix this with the current Consumer, we need to keep
        # all messages joined under a given offset, so we can
        # cancel all of them at once when we receive a response.
        $response->[ID] = $message->[ID];

        $response->[STREAM]  = $self->{stream};
        $response->[PAYLOAD] = ${$buffer};
        $self->{buffer}      = undef;
        $self->{offset}      = undef;
        $self->{stream}      = undef;
        $self->{count}       = 0;
        $self->set_timer( $self->{timer_interval} );
        $self->{sink}->fill($response);
    }
    else {
        $self->cancel($message);
    }
    return;
}

sub fire {
    my $self = shift;
    return if ( not defined $self->{buffer} );
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[TO]      = $self->{owner};
    $message->[ID]      = $self->{offset};
    $message->[STREAM]  = $self->{stream};
    $message->[PAYLOAD] = ${ $self->{buffer} };
    $self->{buffer}     = undef;
    $self->{offset}     = undef;
    $self->{count}      = 0;
    $self->set_timer( $self->{timer_interval} );
    return $self->{sink}->fill($message);
}

sub buffer {
    my $self = shift;
    if (@_) {
        $self->{buffer} = shift;
    }
    return $self->{buffer};
}

sub offset {
    my $self = shift;
    if (@_) {
        $self->{offset} = shift;
    }
    return $self->{offset};
}

sub stream {
    my $self = shift;
    if (@_) {
        $self->{stream} = shift;
    }
    return $self->{stream};
}

sub count {
    my $self = shift;
    if (@_) {
        $self->{count} = shift;
    }
    return $self->{count};
}

sub max_size {
    my $self = shift;
    if (@_) {
        $self->{max_size} = shift;
    }
    return $self->{max_size};
}

sub max_count {
    my $self = shift;
    if (@_) {
        $self->{max_count} = shift;
    }
    return $self->{max_count};
}

1;
