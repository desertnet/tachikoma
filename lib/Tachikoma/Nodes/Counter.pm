#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Counter
# ----------------------------------------------------------------------
#
# $Id: Counter.pm 5634 2010-05-14 23:48:15Z chris $
#

package Tachikoma::Nodes::Counter;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw( TYPE TO PAYLOAD TM_BYTESTREAM TM_COMMAND );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.367');

my $Default_Interval = 60;
my @Windows          = qw( one_minute five_minute fifteen_minute hour day );
my %Count_Modes      = map { $_ => 1 } qw( simple sizes payloads );
my %Send_Modes       = map { $_ => 1 } @Windows, 'cancel', 'payloads';
my %C                = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    my ( $sec, $min, $hour, $day ) = localtime( $Tachikoma::Now // time );
    $self->{last_time}           = [ $sec, $min, $hour, $day ];
    $self->{one_second}          = 0;
    $self->{one_minute}          = [];
    $self->{five_minute}         = [];
    $self->{fifteen_minute}      = [];
    $self->{hour}                = [];
    $self->{day}                 = [];
    $self->{last_one_minute}     = [];
    $self->{last_five_minute}    = [];
    $self->{last_fifteen_minute} = [];
    $self->{last_hour}           = [];
    $self->{last_day}            = [];
    $self->{last_update}         = 0;
    $self->{count_mode}          = 'simple';
    $self->{send_mode}           = 'payloads';
    $self->{interval}            = $Default_Interval;
    $self->{interpreter}         = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    $self->set_timer(1000);
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Counter <node name> [ <count mode> <send mode> <interval> ]
#   count modes: simple, sizes, payloads
#    send modes: cancel, payloads, one_minute, etc
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $count_mode, $send_mode, $interval ) =
            split q( ), $self->{arguments}, 3;
        die 'invalid count mode'
            if ( $count_mode and not $Count_Modes{$count_mode} );
        die 'invalid send mode'
            if ( $send_mode and not $Send_Modes{$send_mode} );
        $self->{count_mode} = $count_mode if ($count_mode);
        $self->{send_mode}  = $send_mode  if ($send_mode);
        $self->{interval}   = $interval   if ($interval);
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return $self->{interpreter}->fill($message)
        if ( $message->[TYPE] & TM_COMMAND );
    $message->[TO] = join q(/), $self->{owner}, $message->[TO]
        if ( $self->{owner} and $message->[TO] );
    if ( $self->{count_mode} eq 'simple' ) {
        $self->{one_second}++;
    }
    elsif ( $self->{count_mode} eq 'sizes' ) {
        $self->{one_second} += length( $message->[PAYLOAD] );
    }
    else {
        my $value = $message->[PAYLOAD];
        chomp $value;
        $self->{one_second} += $value;
    }
    return $self->SUPER::fill($message)
        if ( $self->{send_mode} eq 'payloads' );
    return $self->cancel($message)
        if ( $self->{send_mode} eq 'cancel' );
    return 1;
}

sub activate {    ## no critic (RequireArgUnpacking, RequireFinalReturn)
    if ( $_[0]->{count_mode} eq 'simple' ) {
        $_[0]->{one_second}++;
    }
    elsif ( $_[0]->{count_mode} eq 'sizes' ) {
        $_[0]->{one_second} += length ${ $_[1] };
    }
    else {
        my $value = ${ $_[1] };
        chomp $value;
        $_[0]->{one_second} += $value;
    }
}

sub fire {
    my $self           = shift;
    my $one_second     = $self->{one_second};
    my $one_minute     = $self->{one_minute};
    my $five_minute    = $self->{five_minute};
    my $fifteen_minute = $self->{fifteen_minute};
    my $hour           = $self->{hour};
    my $day            = $self->{day};
    my ( $lsec, $lmin, $lhour, $lday ) = @{ $self->{last_time} };
    my ( $nsec, $nmin, $nhour, $nday ) = localtime $Tachikoma::Now;
    push @{$one_minute},     $one_second;
    push @{$five_minute},    $one_second;
    push @{$fifteen_minute}, $one_second;

    if ( $nmin != $lmin ) {
        my $avg = 0;
        $avg += $_ for ( @{$one_minute} );
        $avg /= @{$one_minute};
        push @{$hour}, $avg;
        $self->{last_one_minute}     = $one_minute;
        $self->{last_five_minute}    = $five_minute if ( not $nmin % 5 );
        $self->{last_fifteen_minute} = $fifteen_minute if ( not $nmin % 15 );
        $one_minute                  = [];
        $five_minute    = [] if ( not $nmin % 5 );
        $fifteen_minute = [] if ( not $nmin % 15 );
    }
    if ( $nhour != $lhour ) {
        my $avg = 0;
        $avg += $_ for ( @{$hour} );
        $avg /= @{$hour};
        push @{$day}, $avg;
        $self->{last_hour} = $hour;
        $hour = [];
    }
    if ( $nday != $lday ) {
        my $avg = 0;
        $avg += $_ for ( @{$day} );
        $avg /= @{$day};
        $self->stderr($avg);
        $self->{last_day} = $day;
        $day = [];
    }
    $self->{one_second}     = 0;
    $self->{one_minute}     = $one_minute;
    $self->{five_minute}    = $five_minute;
    $self->{fifteen_minute} = $fifteen_minute;
    $self->{hour}           = $hour;
    $self->{day}            = $day;
    @{ $self->{last_time} } = ( $nsec, $nmin, $nhour, $nday );
    my $send_mode = $self->{send_mode};
    return if ( $send_mode eq 'payloads' or $send_mode eq 'cancel' );
    return if ( $Tachikoma::Now - $self->{last_update} < $self->{interval} );
    $self->{last_update} = $Tachikoma::Now;
    my $message = Tachikoma::Message->new;
    $message->[TYPE] = TM_BYTESTREAM;

    if ( $send_mode eq 'one_minute' ) {
        $message->[PAYLOAD] = $self->calculate( 'one_minute', 60 ) . "\n";
    }
    elsif ( $send_mode eq 'five_minute' ) {
        $message->[PAYLOAD] = $self->calculate( 'five_minute', 300 ) . "\n";
    }
    elsif ( $send_mode eq 'fifteen_minute' ) {
        $message->[PAYLOAD] =
            $self->calculate( 'fifteen_minute', 900 ) . "\n";
    }
    elsif ( $send_mode eq 'hour' ) {
        $message->[PAYLOAD] = $self->calculate( 'hour', 60 ) . "\n";
    }
    elsif ( $send_mode eq 'day' ) {
        $message->[PAYLOAD] = $self->calculate( 'day', 24 ) . "\n";
    }
    return $self->SUPER::fill($message);
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response(
        $envelope,
        "commands: averages\n",
        "          set_count_mode [ simple | sizes | payloads ]\n",
        "          set_send_mode [ cancel | payloads | one_minute | etc ]\n"
    );
};

$C{averages} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $response  = q();
    my $patron    = $self->patron;
    my $calculate = sub {
        my $this     = shift;
        my $count    = shift;
        my $last_t   = join q(), 'last_', $this;
        my @averages = ( @{ $patron->{$last_t} }, @{ $patron->{$this} } );
        splice @averages, 0, @averages - $count;
        my $average = 0;
        $average += $_ for (@averages);
        $average /= @averages if (@averages);
        return sprintf '%.4f', $average;
    };
    my $one_minute_avg     = $patron->calculate( 'one_minute',     60 );
    my $five_minute_avg    = $patron->calculate( 'five_minute',    300 );
    my $fifteen_minute_avg = $patron->calculate( 'fifteen_minute', 900 );
    my $hour_avg           = $patron->calculate( 'hour',           60 );
    my $day_avg            = $patron->calculate( 'day',            24 );
    if ( $command->arguments ne '-s' ) {
        $response = join q(),
            'one minute average:     ', $one_minute_avg,     "\n",
            'five minute average:    ', $five_minute_avg,    "\n",
            'fifteen minute average: ', $fifteen_minute_avg, "\n",
            'hour average:           ', $hour_avg,           "\n",
            'day average:            ', $day_avg,            "\n";
    }
    else {
        $response = join q(),
            'one_min_avg:',     $one_minute_avg,     q( ),
            'five_min_avg:',    $five_minute_avg,    q( ),
            'fifteen_min_avg:', $fifteen_minute_avg, q( ),
            'hour_avg:',        $hour_avg,           q( ),
            'day_avg:',         $day_avg,            "\n";
    }
    return $self->response( $envelope, $response );
};

$C{set_count_mode} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $arguments = $command->arguments;
    die 'invalid count mode'
        if ( not $arguments or not $Count_Modes{$arguments} );
    $self->patron->count_mode($arguments);
    return $self->okay($envelope);
};

$C{set_send_mode} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $arguments = $command->arguments;
    die 'invalid send mode'
        if ( not $arguments or not $Send_Modes{$arguments} );
    $self->patron->send_mode($arguments);
    return $self->okay($envelope);
};

sub calculate {
    my $self     = shift;
    my $this     = shift;
    my $count    = shift;
    my $last_t   = join q(), 'last_', $this;
    my @averages = ( @{ $self->{$last_t} }, @{ $self->{$this} } );
    splice @averages, 0, @averages - $count if ( @averages > $count );
    my $average = 0;
    $average += $_ for (@averages);
    $average /= @averages if (@averages);
    return sprintf '%.4f', $average;
}

sub last_time {
    my $self = shift;
    if (@_) {
        $self->{last_time} = shift;
    }
    return $self->{last_time};
}

sub one_second {
    my $self = shift;
    if (@_) {
        $self->{one_second} = shift;
    }
    return $self->{one_second};
}

sub one_minute {
    my $self = shift;
    if (@_) {
        $self->{one_minute} = shift;
    }
    return $self->{one_minute};
}

sub five_minute {
    my $self = shift;
    if (@_) {
        $self->{five_minute} = shift;
    }
    return $self->{five_minute};
}

sub fifteen_minute {
    my $self = shift;
    if (@_) {
        $self->{fifteen_minute} = shift;
    }
    return $self->{fifteen_minute};
}

sub hour {
    my $self = shift;
    if (@_) {
        $self->{hour} = shift;
    }
    return $self->{hour};
}

sub day {
    my $self = shift;
    if (@_) {
        $self->{day} = shift;
    }
    return $self->{day};
}

sub last_one_minute {
    my $self = shift;
    if (@_) {
        $self->{last_one_minute} = shift;
    }
    return $self->{last_one_minute};
}

sub last_five_minute {
    my $self = shift;
    if (@_) {
        $self->{last_five_minute} = shift;
    }
    return $self->{last_five_minute};
}

sub last_fifteen_minute {
    my $self = shift;
    if (@_) {
        $self->{last_fifteen_minute} = shift;
    }
    return $self->{last_fifteen_minute};
}

sub last_hour {
    my $self = shift;
    if (@_) {
        $self->{last_hour} = shift;
    }
    return $self->{last_hour};
}

sub last_day {
    my $self = shift;
    if (@_) {
        $self->{last_day} = shift;
    }
    return $self->{last_day};
}

sub last_update {
    my $self = shift;
    if (@_) {
        $self->{last_update} = shift;
    }
    return $self->{last_update};
}

sub count_mode {
    my $self = shift;
    if (@_) {
        $self->{count_mode} = shift;
    }
    return $self->{count_mode};
}

sub send_mode {
    my $self = shift;
    if (@_) {
        $self->{send_mode} = shift;
    }
    return $self->{send_mode};
}

sub interval {
    my $self = shift;
    if (@_) {
        $self->{interval} = shift;
    }
    return $self->{interval};
}

1;
