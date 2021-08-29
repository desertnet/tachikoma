#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Join
# ----------------------------------------------------------------------
#
# $Id: Join.pm 40778 2021-08-29 02:55:46Z chris $
#

package Tachikoma::Nodes::Join;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE TO ID PAYLOAD TM_BYTESTREAM TM_EOF );
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
    $self->{offset} = $message->[ID] if ( not defined $self->{offset} );
    $self->{count}++;
    $self->{counter}++;
    if (   ( $self->{max_size} and length( ${$buffer} ) >= $self->{max_size} )
        or ( $self->{max_count} and $self->{count} >= $self->{max_count} ) )
    {
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_BYTESTREAM;
        $response->[ID]      = $self->{offset};
        $response->[TO]      = $self->{owner};
        $response->[PAYLOAD] = ${$buffer};
        $self->{buffer}      = undef;
        $self->{offset}      = undef;
        $self->{count}       = 0;
        $self->set_timer( $self->{timer_interval} );
        return $self->{sink}->fill($response);
    }
    return 1;
}

sub fire {
    my $self = shift;
    return if ( not defined $self->{buffer} );
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[ID]      = $self->{offset};
    $message->[TO]      = $self->{owner};
    $message->[PAYLOAD] = ${ $self->{buffer} };
    $self->{buffer}     = undef;
    $self->{offset}     = undef;
    $self->{count}      = 0;
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
