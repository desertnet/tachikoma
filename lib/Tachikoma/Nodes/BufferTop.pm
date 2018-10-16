#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::BufferTop
# ----------------------------------------------------------------------
#
# $Id: BufferTop.pm 12254 2011-11-12 21:54:29Z chris $
#

package Tachikoma::Nodes::BufferTop;
use strict;
use warnings;
use Tachikoma::Nodes::TopicTop qw( smart_sort );
use Tachikoma::Message qw( TYPE TIMESTAMP PAYLOAD TM_BYTESTREAM TM_EOF );
use POSIX qw( strftime );
use vars qw( @ISA );
use parent qw( Tachikoma::Nodes::TopicTop );

use version; our $VERSION = 'v2.0.368';

my $Buffer_Timeout          = 10;
my $Default_Output_Interval = 4.0;                                 # seconds
my $Numeric                 = qr{^-?(?:\d+(?:[.]\d*)?|[.]\d+)$};
my $Winch                   = undef;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{where}     = {};
    $self->{where_not} = {};
    $self->{sort_by}   = 'msg_in_buf';
    $self->{height}    = undef;
    $self->{width}     = undef;
    $self->{threshold} = 1;
    $self->{delay}     = $Default_Output_Interval;
    $self->{fields}    = {
        hostname       => { label => 'HOSTNAME',    size => '16',  pad => 0 },
        buff_name      => { label => 'BUFFER_NAME', size => '-16', pad => 0 },
        buff_fills     => { label => 'RCVD',        size => '11',  pad => 0 },
        resp_sent      => { label => 'RSP_SENT',    size => '11',  pad => 0 },
        p_msg_sent     => { label => 'SENT',        size => '11',  pad => 0 },
        err_sent       => { label => 'ERR',         size => '8',   pad => 0 },
        resp_rcvd      => { label => 'RSP_RCVD',    size => '11',  pad => 0 },
        msg_unanswered => { label => 'UNANS',       size => '5',   pad => 0 },
        max_unanswered => { label => 'MAX',         size => '-5',  pad => 0 },
        msg_in_buf     => { label => 'IN_BUF',      size => '11',  pad => 0 },
        recv_rate      => { label => 'RX/s',        size => '9',   pad => 0 },
        direction      => { label => q(),           size => '1',   pad => 0 },
        direction2     => { label => q(),           size => '1',   pad => 0 },
        send_rate      => { label => 'TX/s',        size => '9',   pad => 0 },
        eta            => { label => 'ETA',         size => '11',  pad => 0 },
        lag            => { label => 'LAG',         size => '5',   pad => 1 },
        age            => { label => 'AGE',         size => '5',   pad => 0 }
    };
    $self->{fields}->{buff_name}->{dynamic} = 'true';
    $self->{buffers} = {};
    bless $self, $class;
    $self->set_timer( $Default_Output_Interval * 1000 );
    return $self;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->{sink}->fill($message)
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $buffers = $self->{buffers};
    for my $line ( split m{^}, $message->[PAYLOAD] ) {
        my $stats = { map { split m{:}, $_, 2 } split q( ), $line };
        my $buffer_id = join q(:), $stats->{hostname}, $stats->{buff_name};
        $stats->{last_update} = $Tachikoma::Right_Now;
        $stats->{timestamp}   = $message->[TIMESTAMP];
        my $buffer = $buffers->{$buffer_id} // {};
        $buffer->{$_} = $stats->{$_} for ( keys %{$stats} );
        $buffers->{$buffer_id} = $buffer;
    }
    return 1;
}

sub fire {    ## no critic (ProhibitExcessComplexity)
    my $self      = shift;
    my $buffers   = $self->{buffers};
    my $where     = $self->{where};
    my $where_not = $self->{where_not};
    my $threshold = $self->{threshold};
    my $sort      = $self->{sort_by};
    my $sorted    = {};
    my $totals    = {};
    my $total     = 0;
    for my $buffer_id ( keys %{$buffers} ) {
        my $buffer = $buffers->{$buffer_id};
        if ( not $buffer->{last_timestamp} ) {
            $buffer->{_recv_rate}      = 0;
            $buffer->{_send_rate}      = 0;
            $buffer->{eta}             = '--:--:--';
            $buffer->{last_buff_fills} = $buffer->{buff_fills};
            $buffer->{last_p_msg_sent} = $buffer->{p_msg_sent};
            $buffer->{last_resp_rcvd}  = $buffer->{resp_rcvd};
            $buffer->{last_rate}       = 0;
            $buffer->{last_timestamp}  = $buffer->{timestamp};
            $buffer->{last_recv}       = $buffer->{timestamp};
            $buffer->{last_send}       = $buffer->{timestamp};
            next;
        }

        # incoming message rate
        my $span = $buffer->{timestamp} - $buffer->{last_recv};
        if ($span) {
            $buffer->{_recv_rate} =
                ( $buffer->{buff_fills} - $buffer->{last_buff_fills} )
                / $span;
        }

        # outgoing message rate
        $span = $buffer->{timestamp} - $buffer->{last_send};
        if ($span) {
            $buffer->{_send_rate} =
                ( $buffer->{p_msg_sent} - $buffer->{last_p_msg_sent} )
                / $span;
        }

        # calculate ETA
        my $rate = $buffer->{_send_rate} - $buffer->{_recv_rate};
        if ( $rate > 0 ) {
            $buffer->{last_rate} = $rate;
        }
        elsif ( $buffer->{last_rate} ) {
            $rate = $buffer->{last_rate};
        }
        if ( $rate > 0 ) {
            my $eta = $buffer->{msg_in_buf} / $rate;
            my $hours = sprintf '%02d', $eta / 3600;
            $buffer->{eta} = strftime( "$hours:%M:%S", localtime $eta );
        }

        $buffer->{last_timestamp} = $buffer->{timestamp};
        $buffer->{last_recv}      = $buffer->{timestamp}
            if ( $buffer->{buff_fills} > $buffer->{last_buff_fills} );
        $buffer->{last_send} = $buffer->{timestamp}
            if ( $buffer->{p_msg_sent} > $buffer->{last_p_msg_sent} );
        $buffer->{last_buff_fills} = $buffer->{buff_fills};
        $buffer->{last_p_msg_sent} = $buffer->{p_msg_sent};
        $buffer->{last_resp_rcvd}  = $buffer->{resp_rcvd};
    }
COLLECT: for my $buffer_id ( keys %{$buffers} ) {
        my $buffer = $buffers->{$buffer_id};
        $buffer->{lag} = sprintf '%.1f',
            $buffer->{last_update} - $buffer->{timestamp};
        $buffer->{age} = sprintf '%.1f',
            $Tachikoma::Right_Now - $buffer->{last_update};
        for my $field ( keys %{$where} ) {
            my $regex = $where->{$field};
            if ( $buffer->{$field} !~ m{$regex} ) {
                delete $buffers->{$buffer_id};
                next COLLECT;
            }
        }
        for my $field ( keys %{$where_not} ) {
            my $regex = $where_not->{$field};
            if ( $buffer->{$field} =~ m{$regex} ) {
                delete $buffers->{$buffer_id};
                next COLLECT;
            }
        }
        for my $field (
            qw( buff_fills p_msg_sent resp_rcvd _recv_rate _send_rate
            msg_unanswered max_unanswered msg_in_buf )
            )
        {
            $totals->{$field} += $buffer->{$field} if ( $buffer->{$field} );
        }
        my $key = $buffer->{$sort};
        $sorted->{$key} ||= {};
        $sorted->{$key}->{$buffer_id} = $buffer;
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
        sprintf '%3d buffers; key: AGE > %d IN_BUF > 1000 UNANSWERED >= MAX',
        $total, $Buffer_Timeout;
    $output =
          sprintf "\e[H%3d buffers; key:"
        . " \e[41mAGE > %d\e[0m \e[91mIN_BUF > 1000\e[0m"
        . " \e[93mUNANSWERED >= MAX\e[0m%s\n",
        $total, $Buffer_Timeout, q( ) x ( $width - length $output );
    $totals->{$_} = sprintf '%.2f', $totals->{"_$_"} // 0
        for (qw( recv_rate send_rate ));
    $output .= join q(),
        sprintf(
        join( q(), $color, $self->{format}, $reset, "\n" ),
        map substr( $totals->{$_} // q(), 0, abs $fields->{$_}->{size} ),
        map $_ || q(),
        @{$selected}
        ),
        $self->{header};
OUTPUT: for my $key ( sort { smart_sort( $a, $b ) } keys %{$sorted} ) {

        for my $buffer_id ( sort keys %{ $sorted->{$key} } ) {
            my $buffer = $sorted->{$key}->{$buffer_id};
            next
                if ($sort eq 'msg_in_buf'
                and $key < $threshold
                and $buffer->{age} < $Buffer_Timeout );
            $color = q();
            if ( $buffer->{age} > $Buffer_Timeout ) {
                $color = "\e[41m";
            }
            elsif ( $buffer->{msg_in_buf} > 1000 ) {
                $color = "\e[91m";
            }
            elsif ( $buffer->{msg_unanswered} >= $buffer->{max_unanswered} ) {
                $color = "\e[93m";
            }
            $buffer->{hostname} =~ s{[.].*}{};
            $buffer->{recv_rate} = sprintf '%.2f', $buffer->{_recv_rate};
            $buffer->{send_rate} = sprintf '%.2f', $buffer->{_send_rate};
            $buffer->{direction} = (
                  ( $buffer->{_recv_rate} == $buffer->{_send_rate} ) ? q(=)
                : ( $buffer->{_recv_rate} > $buffer->{_send_rate} )  ? q(>)
                :                                                      q(<)
            );
            $buffer->{direction2} = (
                ( $buffer->{_recv_rate} > $buffer->{_send_rate} )
                ? q(>)
                : q()
            );
            $output .= sprintf
                join( q(), $color, $self->{format}, $reset ),
                map
                substr( $buffer->{$_} // q(), 0, abs $fields->{$_}->{size} ),
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

sub buffers {
    my $self = shift;
    if (@_) {
        $self->{buffers} = shift;
    }
    return $self->{buffers};
}

1;
