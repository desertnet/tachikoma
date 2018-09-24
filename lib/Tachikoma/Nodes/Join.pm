#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Join
# ----------------------------------------------------------------------
#
# $Id: Join.pm 33259 2018-02-17 15:06:46Z chris $
#

package Tachikoma::Nodes::Join;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE TO ID PAYLOAD TM_BYTESTREAM TM_EOF );
use parent qw( Tachikoma::Nodes::Timer );

my $Default_Interval = 1000;
my $Default_Size     = 65536;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{buffer}      = undef;
    $self->{buffer_size} = $Default_Size;
    $self->{last_update} = 0;
    $self->{offset}      = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Join <node name> [ <interval> [ <buffer size> ] ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $interval, $bufsize ) = split( ' ', $self->{arguments}, 2 );
        $self->set_timer( $interval || $Default_Interval );
        $self->buffer_size($bufsize) if ($bufsize);
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_EOF ) {
        $self->{last_update} = 0;
        $self->fire;
        return $self->{sink}->fill($message);
    }
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    if ( not defined $self->{buffer} ) {
        my $new_buffer = '';
        $self->{buffer} = \$new_buffer;
    }
    my $buffer = $self->{buffer};
    $$buffer .= $message->[PAYLOAD];
    $self->{offset} = $message->[ID] if ( not defined $self->{offset} );
    $self->{counter}++;
    if ( length($$buffer) >= $self->{buffer_size} ) {
        $self->{last_update} = $Tachikoma::Right_Now;
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_BYTESTREAM;
        $response->[ID]      = $self->{offset};
        $response->[TO]      = $self->{owner};
        $response->[PAYLOAD] = $$buffer;
        $self->{buffer}      = undef;
        $self->{offset}      = undef;
        return $self->{sink}->fill($response);
    }
    return 1;
}

sub fire {
    my $self = shift;
    return
        if ( $Tachikoma::Right_Now - $self->{last_update} < 1.0
        or not defined $self->{buffer} );
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[ID]      = $self->{offset};
    $message->[TO]      = $self->{owner};
    $message->[PAYLOAD] = ${ $self->{buffer} };
    $self->{buffer}     = undef;
    $self->{offset}     = undef;
    return $self->{sink}->fill($message);
}

sub buffer {
    my $self = shift;
    if (@_) {
        $self->{buffer} = shift;
    }
    return $self->{buffer};
}

sub buffer_size {
    my $self = shift;
    if (@_) {
        $self->{buffer_size} = shift;
    }
    return $self->{buffer_size};
}

sub last_update {
    my $self = shift;
    if (@_) {
        $self->{last_update} = shift;
    }
    return $self->{last_update};
}

sub offset {
    my $self = shift;
    if (@_) {
        $self->{offset} = shift;
    }
    return $self->{offset};
}

1;
