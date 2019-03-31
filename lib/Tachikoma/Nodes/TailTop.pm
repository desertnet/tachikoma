#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::TailTop
# ----------------------------------------------------------------------
#
# $Id: TailTop.pm 12254 2011-11-12 21:54:29Z chris $
#

package Tachikoma::Nodes::TailTop;
use strict;
use warnings;
use Tachikoma::Nodes::TopicTop qw( smart_sort );
use Tachikoma::Message qw( TYPE TIMESTAMP PAYLOAD TM_BYTESTREAM TM_EOF );
use POSIX qw( strftime );
use vars qw( @ISA );
use parent qw( Tachikoma::Nodes::TopicTop );

use version; our $VERSION = qv('v2.0.368');

my $Tail_Timeout            = 60;
my $Default_Output_Interval = 4.0;                                 # seconds
my $Numeric                 = qr{^-?(?:\d+(?:[.]\d*)?|[.]\d+)$};
my $Winch                   = undef;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{where}     = {};
    $self->{where_not} = {};
    $self->{sort_by}   = '_distance';
    $self->{height}    = undef;
    $self->{width}     = undef;
    $self->{threshold} = 1;
    $self->{delay}     = $Default_Output_Interval;
    $self->{fields}    = {
        hostname       => { label => 'HOSTNAME',  size => '16',  pad => 0 },
        tail_name      => { label => 'TAIL_NAME', size => '-16', pad => 0 },
        filename       => { label => 'FILENAME',  size => '-16', pad => 0 },
        bytes_answered => { label => 'ANSWERED',  size => '9',   pad => 0 },
        file_size      => { label => 'SIZE',      size => '9',   pad => 0 },
        distance       => { label => 'DISTANCE',  size => '9',   pad => 0 },
        recv_rate      => { label => 'RX/s',      size => '9',   pad => 0 },
        direction      => { label => q(),         size => '1',   pad => 0 },
        direction2     => { label => q(),         size => '1',   pad => 0 },
        send_rate      => { label => 'TX/s',      size => '9',   pad => 0 },
        eta            => { label => 'ETA',       size => '11',  pad => 0 },
        lag            => { label => 'LAG',       size => '5',   pad => 1 },
        age            => { label => 'AGE',       size => '5',   pad => 0 }
    };
    $self->{fields}->{tail_name}->{dynamic} = 'true';
    $self->{fields}->{filename}->{dynamic}  = 'true';
    $self->{tails}                          = {};
    bless $self, $class;
    $self->set_timer( $Default_Output_Interval * 1000 );
    return $self;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->{sink}->fill($message)
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $tails = $self->{tails};
    for my $line ( split m{^}, $message->[PAYLOAD] ) {
        my $stats   = { map { split m{:}, $_, 2 } split q( ), $line };
        my $tail_id = join q(:), $stats->{hostname}, $stats->{tail_name};
        $stats->{last_update} = $Tachikoma::Right_Now;
        $stats->{timestamp}   = $message->[TIMESTAMP];
        my $tail = $tails->{$tail_id} // {};
        $tail->{$_}        = $stats->{$_} for ( keys %{$stats} );
        $tails->{$tail_id} = $tail;
    }
    return 1;
}

sub fire {    ## no critic (ProhibitExcessComplexity)
    my $self      = shift;
    my $tails     = $self->{tails};
    my $where     = $self->{where};
    my $where_not = $self->{where_not};
    my $threshold = $self->{threshold};
    my $sort      = $self->{sort_by};
    my $sorted    = {};
    my $totals    = {};
    my $total     = 0;
    for my $tail_id ( keys %{$tails} ) {
        my $tail = $tails->{$tail_id};
        if ( not $tail->{last_timestamp} ) {
            $tail->{_recv_rate}          = 0;
            $tail->{_send_rate}          = 0;
            $tail->{eta}                 = '--:--:--';
            $tail->{last_file_size}      = $tail->{file_size};
            $tail->{last_bytes_answered} = $tail->{bytes_answered};
            $tail->{distance}            = 0;
            $tail->{_distance}           = 0;
            $tail->{last_rate}           = 0;
            $tail->{last_timestamp}      = $tail->{timestamp};
            $tail->{last_recv}           = $tail->{timestamp};
            $tail->{last_send}           = $tail->{timestamp};
            next;
        }
        $tail->{_distance} = $tail->{file_size} - $tail->{bytes_answered};

        # incoming byte rate
        my $span = $tail->{timestamp} - $tail->{last_recv};
        if ($span) {
            my $distance = $tail->{file_size} - $tail->{last_file_size};
            $tail->{_recv_rate} = $distance / $span;
        }

        # outgoing byte rate
        $span = $tail->{timestamp} - $tail->{last_send};
        if ($span) {
            my $distance =
                $tail->{bytes_answered} - $tail->{last_bytes_answered};
            $tail->{_send_rate} = $distance / $span;
        }

        # calculate ETA
        my $rate = $tail->{_send_rate} - $tail->{_recv_rate};
        if ( $rate > 0 ) {
            $tail->{last_rate} = $rate;
        }
        elsif ( $tail->{last_rate} ) {
            $rate = $tail->{last_rate};
        }
        if ( $rate > 0 ) {
            my $eta   = $tail->{_distance} / $rate;
            my $hours = sprintf '%02d', $eta / 3600;
            $tail->{eta} = strftime( "$hours:%M:%S", localtime $eta );
        }

        $tail->{last_timestamp} = $tail->{timestamp};
        $tail->{last_recv}      = $tail->{timestamp}
            if ( $tail->{file_size} > $tail->{last_file_size} );
        $tail->{last_send} = $tail->{timestamp}
            if ( $tail->{bytes_answered} > $tail->{last_bytes_answered} );
        $tail->{last_file_size}      = $tail->{file_size};
        $tail->{last_bytes_answered} = $tail->{bytes_answered};
    }
COLLECT: for my $tail_id ( keys %{$tails} ) {
        my $tail = $tails->{$tail_id};
        $tail->{lag} = sprintf '%.1f',
            $tail->{last_update} - $tail->{timestamp};
        if ( $Tachikoma::Right_Now - $tail->{last_update} > $Tail_Timeout ) {
            delete $tails->{$tail_id};
            next COLLECT;
        }
        for my $field ( keys %{$where} ) {
            my $regex = $where->{$field};
            if ( $tail->{$field} !~ m{$regex} ) {
                delete $tails->{$tail_id};
                next COLLECT;
            }
        }
        for my $field ( keys %{$where_not} ) {
            my $regex = $where_not->{$field};
            if ( $tail->{$field} =~ m{$regex} ) {
                delete $tails->{$tail_id};
                next COLLECT;
            }
        }
        for my $field (qw( file_size bytes_answered )) {
            $totals->{"_$field"} += $tail->{$field} if ( $tail->{$field} );
        }
        for my $field (qw( _distance _recv_rate _send_rate )) {
            $totals->{$field} += $tail->{$field} if ( $tail->{$field} );
        }
        my $key = $tail->{$sort};
        $sorted->{$key} ||= {};
        $sorted->{$key}->{$tail_id} = $tail;
        $total++;
    }
    my $fields   = $self->{fields};
    my $selected = $self->{selected};
    my $height   = $self->height;
    my $width    = $self->width;
    my $count    = 5;
    my $color    = "\e[90m";
    my $reset    = "\e[0m";
    my $output =
        sprintf '%3d tails; key: DISTANCE > 10M',
        $total;
    $output =
        sprintf "\e[H%3d tails; key: \e[91mDISTANCE > 10M\e[0m%s\n",
        $total, q( ) x ( $width - length $output );
    $totals->{$_} = Tachikoma::Nodes::TopicTop::human( $totals->{"_$_"} )
        for (qw( file_size bytes_answered distance recv_rate send_rate ));
    $output .= join q(),
        sprintf(
        join( q(), $color, $self->{format}, $reset, "\n" ),
        map substr( $totals->{$_} // q(), 0, abs $fields->{$_}->{size} ),
        map $_ || q(),
        @{$selected}
        ),
        $self->{header};
OUTPUT: for my $key ( sort { smart_sort( $a, $b ) } keys %{$sorted} ) {

        for my $tail_id ( sort keys %{ $sorted->{$key} } ) {
            my $tail = $sorted->{$key}->{$tail_id};
            next
                if ($sort eq '_distance'
                and $key < $threshold );
            $color = q();
            if ( $tail->{_distance} > 10485760 ) {
                $color = "\e[91m";
            }
            $tail->{hostname} =~ s{[.].*}{};
            $tail->{recv_rate} =
                Tachikoma::Nodes::TopicTop::human( $tail->{_recv_rate} );
            $tail->{send_rate} =
                Tachikoma::Nodes::TopicTop::human( $tail->{_send_rate} );
            $tail->{distance} =
                Tachikoma::Nodes::TopicTop::human( $tail->{_distance} );
            $tail->{direction} = (
                  ( $tail->{_recv_rate} == $tail->{_send_rate} ) ? q(=)
                : ( $tail->{_recv_rate} > $tail->{_send_rate} )  ? q(>)
                :                                                  q(<)
            );
            $tail->{direction2} = (
                ( $tail->{_recv_rate} > $tail->{_send_rate} )
                ? q(>)
                : q()
            );
            $output .= sprintf
                join( q(), $color, $self->{format}, $reset ),
                map
                substr( $tail->{$_} // q(), 0, abs $fields->{$_}->{size} ),
                map $_ || q(),
                @{$selected};
            last OUTPUT if ( $count++ > $height );
            $output .= "\n";
        }
    }
    $output .= join q(), q( ) x ( ( $height - ( $count - 2 ) ) * $width ),
        "\e[?25l";
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[PAYLOAD] = $output;
    $self->{sink}->fill($response);
    return;
}

sub tails {
    my $self = shift;
    if (@_) {
        $self->{tails} = shift;
    }
    return $self->{tails};
}

1;
