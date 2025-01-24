#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::TopicTop
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::TopicTop;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE TIMESTAMP PAYLOAD TM_BYTESTREAM TM_EOF );
use POSIX              qw( strftime );
use vars               qw( @EXPORT_OK );
use parent             qw( Exporter Tachikoma::Nodes::Timer );
@EXPORT_OK = qw( smart_sort );

use version; our $VERSION = qv('v2.0.367');

my $TOPIC_TIMEOUT           = 10;
my $DEFAULT_OUTPUT_INTERVAL = 4.0;                                 # seconds
my $NUMERIC                 = qr/^-?(?:\d+(?:[.]\d*)?|[.]\d+)$/;
my $WINCH                   = undef;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{where}     = {};
    $self->{where_not} = {};
    $self->{sort_by}   = '_distance';
    $self->{height}    = undef;
    $self->{width}     = undef;
    $self->{threshold} = 0;
    $self->{delay}     = $DEFAULT_OUTPUT_INTERVAL;
    $self->{fields}    = {
        hostname       => { label => 'HOSTNAME',  size => '16',  pad => 0 },
        partition      => { label => 'PARTITION', size => '-32', pad => 0 },
        consumer       => { label => 'CONSUMER',  size => '-16', pad => 0 },
        p_offset       => { label => 'P. OFFSET', size => '16',  pad => 0 },
        c_offset       => { label => 'C. OFFSET', size => '16',  pad => 0 },
        distance       => { label => 'DISTANCE',  size => '9',   pad => 0 },
        msg_sent       => { label => 'MSG_SENT',  size => '11',  pad => 0 },
        msg_unanswered => { label => 'UNANS',     size => '5',   pad => 0 },
        max_unanswered => { label => 'MAX',       size => '6',   pad => 0 },
        cache          => { label => 'CACHE',     size => '9',   pad => 0 },
        recv_rate      => { label => 'RX/s',      size => '9',   pad => 0 },
        direction      => { label => q(),         size => '1',   pad => 0 },
        direction2     => { label => q(),         size => '1',   pad => 0 },
        send_rate      => { label => 'TX/s',      size => '9',   pad => 0 },
        msg_rate       => { label => 'MSG/s',     size => '11',  pad => 0 },
        eta            => { label => 'ETA',       size => '11',  pad => 0 },
        lag            => { label => 'LAG',       size => '5',   pad => 1 },
        age            => { label => 'AGE',       size => '5',   pad => 0 }
    };
    $self->{fields}->{partition}->{dynamic} = 'true';
    $self->{fields}->{consumer}->{dynamic}  = 'true';
    $self->{partitions}                     = {};
    $self->{consumers}                      = {};
    bless $self, $class;
    $self->set_timer( $DEFAULT_OUTPUT_INTERVAL * 1000 );
    ## no critic (RequireLocalizedPunctuationVars)
    $SIG{WINCH} = sub { $WINCH = 1 };
    $SIG{INT}   = sub {
        print "\e[0m\e[?25h";
        ## no critic (RequireCheckedSyscalls)
        system '/bin/stty', 'echo', '-cbreak';
        exit 1;
    };
    return $self;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->{sink}->fill($message)
        if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $partitions = $self->{partitions};
    my $consumers  = $self->{consumers};
    for my $line ( split m{^}, $message->[PAYLOAD] ) {
        my $stats = { map { split m{:}, $_, 2 } split q( ), $line };
        $stats->{last_update} = $Tachikoma::Right_Now;
        $stats->{timestamp}   = $message->[TIMESTAMP];
        my $row = undef;
        if ( $stats->{consumer} ) {
            my $id = $stats->{consumer};
            $consumers->{$id} //= {};
            $row = $consumers->{$id};
        }
        else {
            $partitions->{ $stats->{partition} } //= {};
            $row = $partitions->{ $stats->{partition} };
        }
        $row->{$_} = $stats->{$_} for ( keys %{$stats} );
    }
    return 1;
}

sub fire {
    my $self       = shift;
    my $partitions = $self->{partitions};
    my $consumers  = $self->{consumers};
    my $threshold  = $self->{threshold};
    my $sort       = $self->{sort_by};
    my $reverse    = $sort =~ s{^-}{}o;
    my $sorted     = {};
    my $totals     = {};

    for my $id ( keys %{$partitions} ) {
        $self->calculate_partition_row( $partitions->{$id} );
        $self->calculate_partition_totals( $partitions->{$id}, $totals );
    }
    for my $id ( keys %{$consumers} ) {
        $self->calculate_row( $consumers->{$id} );
        $self->calculate_totals( $consumers->{$id}, $totals );
    }
    my $total    = $self->sort_rows($sorted);
    my $fields   = $self->{fields};
    my $selected = $self->{selected};
    my $height   = $self->height;
    my $width    = $self->width;
    my $count    = 5;
    my $color    = "\e[90m";
    my $reset    = "\e[0m";
    my $output   = sprintf
        '%3d consumers; key: AGE > %d DISTANCE > 10M UNANSWERED >= MAX',
        $total, $TOPIC_TIMEOUT;
    $output =
          sprintf "\e[H%3d consumers; key:"
        . " \e[41mAGE > %d\e[0m \e[91mDISTANCE > 10M\e[0m"
        . " \e[93mUNANSWERED >= MAX\e[0m%s\n",
        $total, $TOPIC_TIMEOUT, q( ) x ( $width - length $output );
    $totals->{$_} = human( $totals->{"_$_"} )
        for (qw( p_offset c_offset distance recv_rate send_rate cache ));
    $totals->{msg_rate} = sprintf '%.2f', $totals->{_msg_rate} // 0;
    $output .= join q(),
        sprintf(
        join( q(), $color, $self->{format}, $reset, "\n" ),
        map substr( $totals->{$_} // q(), 0, abs $fields->{$_}->{size} ),
        map $_ || q(),
        @{$selected}
        ),
        $self->{header};
OUTPUT:

    for my $key ( sort { smart_sort( $a, $b, $reverse ) } keys %{$sorted} ) {

        for my $id ( sort keys %{ $sorted->{$key} } ) {
            my $consumer = $sorted->{$key}->{$id};
            next
                if ($sort eq '_distance'
                and $key < $threshold
                and $consumer->{age} < $TOPIC_TIMEOUT );
            $color = q();
            if ( $consumer->{age} > $TOPIC_TIMEOUT ) {
                $color = "\e[41m";
            }
            elsif ( $consumer->{_distance} > 10485760 ) {
                $color = "\e[91m";
            }
            elsif ( $consumer->{msg_unanswered}
                >= ( $consumer->{max_unanswered} || 1 ) )
            {
                $color = "\e[93m";
            }
            $consumer->{hostname} =~ s{[.].*}{};
            $consumer->{cache} = human( $consumer->{cache_size} );
            $consumer->{$_} = human( $consumer->{"_$_"} )
                for (qw( distance recv_rate send_rate ));
            $consumer->{msg_rate}  = sprintf '%.2f', $consumer->{_msg_rate};
            $consumer->{direction} = (
                ( $consumer->{_recv_rate} == $consumer->{_send_rate} )  ? q(=)
                : ( $consumer->{_recv_rate} > $consumer->{_send_rate} ) ? q(>)
                :                                                         q(<)
            );
            $consumer->{direction2} = (
                ( $consumer->{_recv_rate} > $consumer->{_send_rate} )
                ? q(>)
                : q()
            );
            $output .= sprintf
                join( q(), $color, $self->{format}, $reset ),
                map substr( $consumer->{$_} // q(), 0,
                abs $fields->{$_}->{size} ),
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

sub human {
    my $value = shift // 0;
    return $value if ( $value !~ m{^-?[\d.]+$} );
    if ( abs($value) >= 1000 * 1024**4 ) {
        $value = sprintf '%0.2fP', $value / 1024**5;
    }
    elsif ( abs($value) >= 1000 * 1024**3 ) {
        $value = sprintf '%0.2fT', $value / 1024**4;
    }
    elsif ( abs($value) >= 1000 * 1024**2 ) {
        $value = sprintf '%0.2fG', $value / 1024**3;
    }
    elsif ( abs($value) >= 1000 * 1024 ) {
        $value = sprintf '%0.2fM', $value / 1024**2;
    }
    elsif ( abs($value) >= 1000 ) {
        $value = sprintf '%0.2fK', $value / 1024;
    }
    else {
        $value = sprintf '%0.2f ', $value;
    }
    return $value;
}

sub calculate_partition_row {
    my $self = shift;
    my $row  = shift;

    if ( not $row->{last_timestamp} ) {
        $row->{_recv_rate}     = 0;
        $row->{last_p_offset}  = $row->{p_offset};
        $row->{last_timestamp} = $row->{timestamp};
        $row->{last_recv}      = $row->{timestamp};
        return;
    }

    # incoming byte rate
    my $span = $row->{timestamp} - $row->{last_recv};
    if ($span) {
        $row->{_recv_rate} =
            ( $row->{p_offset} - $row->{last_p_offset} ) / $span;
    }

    $row->{last_timestamp} = $row->{timestamp};
    $row->{last_recv}      = $row->{timestamp}
        if ( $row->{p_offset} > $row->{last_p_offset} );
    $row->{last_p_offset} = $row->{p_offset};
    return;
}

sub calculate_partition_totals {
    my ( $self, $row, $totals ) = @_;
    $totals->{_p_offset}  += $row->{p_offset};
    $totals->{_recv_rate} += $row->{_recv_rate};
    return;
}

sub calculate_row {
    my $self      = shift;
    my $row       = shift;
    my $partition = $self->{partitions}->{ $row->{partition} };
    $row->{p_offset} = $partition->{p_offset} if ($partition);
    $row->{p_offset} //= 0;
    $row->{_recv_rate} = $partition->{_recv_rate} if ($partition);
    $row->{_recv_rate} //= 0;

    if ( not $row->{last_timestamp} ) {
        $row->{_send_rate}     = 0;
        $row->{_msg_rate}      = 0;
        $row->{eta}            = '--:--:--';
        $row->{last_p_offset}  = $row->{p_offset};
        $row->{last_c_offset}  = $row->{c_offset};
        $row->{last_msg_sent}  = $row->{msg_sent};
        $row->{_distance}      = 0;
        $row->{distance}       = 0;
        $row->{last_rate}      = 0;
        $row->{last_timestamp} = $row->{timestamp};
        $row->{last_send}      = $row->{timestamp};
        return;
    }

    # calculate distance
    $row->{_distance} = abs $row->{p_offset} - $row->{c_offset};

    # outgoing rates
    my $span = $row->{timestamp} - $row->{last_send};
    if ($span) {
        $row->{_send_rate} =
            ( $row->{c_offset} - $row->{last_c_offset} ) / $span;
        $row->{_msg_rate} =
            ( $row->{msg_sent} - $row->{last_msg_sent} ) / $span;
    }

    # calculate ETA
    my $rate = $row->{_send_rate} - $row->{_recv_rate};
    if ( $rate > 0 ) {
        $row->{last_rate} = $rate;
    }
    elsif ( $row->{last_rate} ) {
        $rate = $row->{last_rate};
    }
    if ( $rate > 0 ) {
        my $eta   = $row->{_distance} / $rate;
        my $hours = sprintf '%02d', $eta / 3600;
        $row->{eta} = strftime( "$hours:%M:%S", localtime $eta );
    }

    $row->{last_timestamp} = $row->{timestamp};
    $row->{last_send}      = $row->{timestamp}
        if ( $row->{c_offset} > $row->{last_c_offset} );
    $row->{last_p_offset} = $row->{p_offset};
    $row->{last_c_offset} = $row->{c_offset};
    $row->{last_msg_sent} = $row->{msg_sent};
    return;
}

sub calculate_totals {
    my ( $self, $row, $totals ) = @_;
    $totals->{_c_offset} += $row->{c_offset};
    $totals->{_cache}    += $row->{cache_size};
    for my $field (
        qw( msg_sent msg_unanswered max_unanswered
        _distance _send_rate _msg_rate )
        )
    {
        $totals->{$field} += $row->{$field} if ( $row->{$field} );
    }
    return;
}

sub sort_rows {
    my ( $self, $sorted ) = @_;
    my $sort      = $self->{sort_by};
    my $consumers = $self->{consumers};
    my $where     = $self->{where};
    my $where_not = $self->{where_not};
    my $total     = 0;
    $sort =~ s{^-}{};
COLLECT: for my $id ( keys %{$consumers} ) {
        my $consumer = $consumers->{$id};
        $consumer->{lag} = sprintf '%.1f',
            $consumer->{last_update} - $consumer->{timestamp};
        $consumer->{age} = sprintf '%.1f',
            $Tachikoma::Right_Now - $consumer->{last_update};
        for my $field ( keys %{$where} ) {
            my $regex = $where->{$field};
            if ( $consumer->{$field} !~ m{$regex} ) {
                delete $consumers->{$id};
                next COLLECT;
            }
        }
        for my $field ( keys %{$where_not} ) {
            my $regex = $where_not->{$field};
            if ( $consumer->{$field} =~ m{$regex} ) {
                delete $consumers->{$id};
                next COLLECT;
            }
        }
        my $key = $consumer->{$sort};
        $sorted->{$key} ||= {};
        $sorted->{$key}->{$id} = $consumer;
        $total++;
    }
    return $total;
}

sub smart_sort {
    my $a       = shift;
    my $b       = shift;
    my $reverse = shift;
    return $reverse ? $a <=> $b : $b <=> $a
        if ( $a =~ m{$NUMERIC} and $b =~ m{$NUMERIC} );
    return $reverse ? lc $b cmp lc $a : lc $a cmp lc $b;
}

sub select_fields {
    my $self = shift;
    if (@_) {
        my $new_fields = shift;
        $self->{select_fields} = $new_fields if ($new_fields);
        my $width    = $self->width;
        my $fields   = $self->{fields};
        my @selected = ();
        for my $field ( split m{,\s*}, $self->{select_fields} ) {
            die "no such field: $field\n" if ( not exists $fields->{$field} );
            push @selected, $field;
        }
        my $total = -1;
        $total += 2 * $fields->{$_}->{pad} + abs( $fields->{$_}->{size} ) + 1
            for (@selected);
        for ( reverse @selected ) {
            next if ( not $fields->{$_}->{dynamic} );
            my $size = $fields->{$_}->{size};
            if ( $size > 0 ) {
                $size += $width - $total;
            }
            else {
                $size -= $width - $total;
            }
            $fields->{$_}->{size} = $size;
            $total = $width;
            last;
        }
        $self->{format} = join q( ),
            map join( q(),
            q( ) x $fields->{$_}->{pad}, q(%), $fields->{$_}->{size},
            's', q( ) x $fields->{$_}->{pad} ),
            @selected;
        $self->{format} .= q( ) x ( $width - $total );
        $self->{header} = sprintf $self->{format} . "\n",
            map $fields->{$_}->{label}, @selected;
        $self->{selected} = \@selected;
    }
    return $self->{select_fields};
}

sub update_window_size {
    my $self = shift;
    $WINCH = undef;
    ## no critic (ProhibitBacktickOperators)
    my $height = `tput lines`;
    my $width  = `tput cols`;
    chomp $height;
    chomp $width;
    $self->{height} = $height;
    $self->{width}  = $width;
    $self->select_fields(undef);
    return;
}

sub remove_node {
    my $self = shift;
    print "\e[0m\e[?25h";
    ## no critic (RequireCheckedSyscalls)
    system '/bin/stty', 'echo', '-cbreak';
    $self->SUPER::remove_node;
    return;
}

sub where {
    my $self = shift;
    if (@_) {
        $self->{where} = shift;
    }
    return $self->{where};
}

sub where_not {
    my $self = shift;
    if (@_) {
        $self->{where_not} = shift;
    }
    return $self->{where_not};
}

sub sort_by {
    my $self = shift;
    if (@_) {
        $self->{sort_by} = shift;
    }
    return $self->{sort_by};
}

sub height {
    my $self = shift;
    if (@_) {
        $self->{height} = shift;
    }
    $self->update_window_size if ( $WINCH or not defined $self->{height} );
    return $self->{height};
}

sub width {
    my $self = shift;
    if (@_) {
        $self->{width} = shift;
    }
    $self->update_window_size if ( $WINCH or not defined $self->{width} );
    return $self->{width};
}

sub threshold {
    my $self = shift;
    if (@_) {
        $self->{threshold} = shift;
    }
    return $self->{threshold};
}

sub delay {
    my $self = shift;
    if (@_) {
        $self->{delay} = shift;
        $self->set_timer( $self->{delay} * 1000 );
    }
    return $self->{delay};
}

sub fields {
    my $self = shift;
    if (@_) {
        $self->{fields} = shift;
    }
    return $self->{fields};
}

sub partitions {
    my $self = shift;
    if (@_) {
        $self->{partitions} = shift;
    }
    return $self->{partitions};
}

sub consumers {
    my $self = shift;
    if (@_) {
        $self->{consumers} = shift;
    }
    return $self->{consumers};
}

1;
