#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::SFE4DigitLED
# ----------------------------------------------------------------------
#

package Accessories::Nodes::SFE4DigitLED;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TM_BYTESTREAM TM_NOREPLY );
use Data::Dumper;
use Storable qw( nfreeze );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.768');

# simple regex for what characters can be displayed
my $Chars = '[A-Za-z0-9\s-]';

my %cmds = (
    'wtf'        => 0x0A,
    'cls'        => 0x76,
    'dots'       => 0x77,
    'blank'      => 0x78,
    'cursor'     => 0x79,
    'brightness' => 0x7A,
    'dig1'       => 0x7B,
    'dig2'       => 0x7C,
    'dig3'       => 0x7D,
    'dig4'       => 0x7E,
    'baud'       => 0x7F,
    'i2caddr'    => 0x80,
    'factreset'  => 0x81,
);

my %dot_bits = (
    'decp1'  => 1,
    'decp2'  => 1 << 1,
    'decp3'  => 1 << 2,
    'decp4'  => 1 << 3,
    'colon'  => 1 << 4,
    'degree' => 1 << 5,
);

sub new {
    my $proto = shift;
    my $class = ref $proto || $proto;
    my $self  = $class->SUPER::new;
    $self->{default_brightness}   = 0;
    $self->{pending_chunks}       = [];
    $self->{last_displayed_chunk} = {};
    $self->{active_string}        = q();
    $self->{default_dwell}        = 10;
    $self->{verbose}              = 0;

    $self->{chars} = {
        'char1' => 0x20,
        'char2' => 0x20,
        'char3' => 0x20,
        'char4' => 0x20
    };

    $self->{dots} = {
        'decp1'  => 0,
        'decp2'  => 0,
        'decp3'  => 0,
        'decp4'  => 0,
        'colon'  => 0,
        'degree' => 0
    };

    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->{verbose}   = 1 if ( $self->{arguments} =~ /\bverbose\b/ );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( not $message->type & TM_BYTESTREAM ) {
        return $self->SUPER::fill($message);
    }
    my $payload = $message->payload;
    chomp $payload;
    my $string = undef;

    my $chunk = {
        push       => undef,
        expire     => ( $Tachikoma::Right_Now + $self->{default_dwell} ),
        slot       => undef,
        brightness => undef,
        char1      => undef,
        decp1      => undef,
        char2      => undef,
        decp2      => undef,
        colon      => undef,
        char3      => undef,
        decp3      => undef,
        degree     => undef,
        char4      => undef,
        decp4      => undef
    };

    $self->verbose("fill: payload: '$payload'");

    if ( $payload =~ /^(.*?)[|][|][|](.*?)$/s ) {
        my $cmds = $1;
        $string = $2;
        if ( $cmds =~ /^defaultbrightness[:=](\d+[.]\d+)$/ ) {
            my $luma = $1 * 1.0;
            $self->verbose("default brightness: $1 luma: $luma");
            $self->{default_brightness} = $luma;
            return $self->send_message(
                $self->brightness( { brightness => $luma } ) );
        }
        if ( $cmds =~ /\bforce\b/ or $cmds =~ /\bpush\b/ ) {
            $chunk->{push} = 1;
        }
        if ( $cmds =~ /\bexpire[:=](\d+[.]?\d*?)\b/ ) {
            $chunk->{expire} = $1;
        }
        elsif ( $cmds =~ /\bdwell[:=](\d+[.]?\d*?)\b/ ) {
            $self->verbose("dwell time: $1");
            $chunk->{expire} = $Tachikoma::Right_Now + $1;
        }
        if ( $cmds =~ /\bslot[:=](\w+)\b/ ) {
            $chunk->{slot} = $1;
            $self->clslot( $chunk->{slot} );
        }
        if ( $cmds =~ /\bbrightn?e?s?s?[:=](\d+[.]?\d*)\b/ ) {
            $chunk->{brightness} = $1 * 1.0;
        }
    }
    else {
        $string = $payload;
        chomp $string;
    }

    ## no critic (ProhibitComplexRegexes)
    if ($string =~ m{ ^ ($Chars) ([.]?)
                        ($Chars) ([.]?)    (:?)
                        ($Chars) ([.]?)   (['°]?)
                        ($Chars) ([.]?)           $}x
        )
    {
        $chunk->{char1}  = $1 if ( length $1 );
        $chunk->{decp1}  = $2;
        $chunk->{char2}  = $3 if ( length $3 );
        $chunk->{decp2}  = $4;
        $chunk->{colon}  = $5;
        $chunk->{char3}  = $6 if ( length $6 );
        $chunk->{decp3}  = $7;
        $chunk->{degree} = $8;
        $chunk->{char4}  = $9 if ( length $9 );
        $chunk->{decp4}  = $10;
        $self->verbose('standard message format');
    }
    elsif ( $string eq q() ) {

        # nothing
    }
    else {
        $self->stderr("unsupported message format: '$string'\n");
        return $self->cancel($message);
    }
    if ( $chunk->{push} ) {
        $self->verbose( 'prepending chunk:' . Dumper($chunk) );
        unshift @{ $self->{pending_chunks} }, $chunk;
    }
    else {
        $self->verbose( 'appending chunk' . Dumper($chunk) );
        push @{ $self->{pending_chunks} }, $chunk;
    }
    $self->fire();
    return $self->cancel($message);
}

sub fire {
    my $self = shift;
    $self->verbose( sprintf 'fire: pending chunks: %d',
        $#{ $self->{pending_chunks} } );
    my $prev = $self->{last_displayed_chunk} // {};
    my $pend = $self->{pending_chunks}->[0]  // {};
    $self->verbose( 'last chunk: ' . Dumper($prev) . "\n" );
    $self->verbose( 'pend chunk: ' . Dumper($pend) . "\n" );
    #
    if ( $pend->{expire} ) {
        if ( $pend->{expire} <= $Tachikoma::Right_Now ) {

            # expired? take it off the stack and recurse
            my $old = shift @{ $self->{pending_chunks} };
            $self->{last_displayed_chunk} = {};
            if ( $self->{pending_chunks}->[0] ) {
                return $self->fire;
            }
            else {
                $self->verbose('last pending chunk expired: clearing screen');
                $self->{active_rows} = [];
                $self->{next_crawl}  = undef;
                return $self->send_message( $self->cls );
            }
        }
        else {
            $self->set_timer(
                ( $pend->{expire} - $Tachikoma::Right_Now ) * 1000,
                'oneshot' );
        }
    }
    elsif ( not keys %{$pend} ) {
        $self->verbose("stopping timer\n");

        #return $self->stop_timer;
    }
    else {
        $self->set_timer( ( 1 / $self->{crawl_hz} ) * 1000, 'oneshot' );
    }

    if ( $self->hashes_differ( $prev, $pend ) ) {
        $self->verbose('hashes are different');
        $self->{last_displayed_chunk} = $pend;

        # current chunk hasn't been superceded
        $self->verbose("chunk valid until $pend->{expire}")
            if ( $pend->{expire} );
        $self->send_message( $self->format_chunk($pend) );
        $self->send_message( $self->brightness( $pend->{bright} ) )
            if ( $pend->{bright} );
    }
    else {
        #$self->verbose('nothing to do: clearing screen');
        #$self->send_message($self->cls);
    }
    return;
}

sub format_chunk {
    my $self   = shift;
    my $chunk  = shift;
    my $output = q();

    #$self->send_message($self->cls);
    my $dots   = $self->dots($chunk)       // q();
    my $chars  = $self->chars($chunk)      // q();
    my $bright = $self->brightness($chunk) // q();
    $output .= $self->cls . $bright . $dots . $chars;
    $self->verbose( 'output length: ' . length($output) . "; '$output'" );
    return $output;
}

sub hashes_differ {
    my $self = shift;
    my $a    = shift;
    my $b    = shift;
    my $rv   = 1;
    $Storable::canonical = 1;
    $rv                  = 0 if ( nfreeze($a) eq nfreeze($b) );
    return $rv;
}

sub send_message {
    my $self    = shift;
    my $payload = shift;
    $self->verbose( sprintf "sending '%s'\n", $payload );
    my $msg = Tachikoma::Message->new;
    $msg->type(TM_BYTESTREAM);
    $msg->payload($payload);
    return $self->SUPER::fill($msg);
}

sub brightness {
    my $self  = shift;
    my $chunk = shift;
    my $level;
    if ( $chunk->{brightness} ) {
        $level = $chunk->{brightness} * 255.0;
    }
    else {
        return;
    }
    foreach my $cmd ( keys %cmds ) {
        return if ( chr $level eq chr $cmds{$cmd} );
    }
    return chr( $cmds{brightness} ) . chr $level;
}

sub cls {
    my $self = shift;
    foreach my $char ( keys %{ $self->{chars} } ) {
        $self->{chars}->{$char} = 0x00;
    }
    foreach my $dot ( keys %{ $self->{dots} } ) {
        $self->{dots}->{$dot} = 0;
    }
    return chr( $cmds{dots} ) . chr(0x00) . chr $cmds{cls};
}

sub dots {
    my $self  = shift;
    my $chunk = shift;
    my $dots  = 0;
    foreach my $dot ( keys %dot_bits ) {
        $self->verbose($dot) if ( $chunk->{$dot} );
        $dots += $dot_bits{$dot} if ( $chunk->{$dot} );
    }
    if ( $dots > 0 ) {
        return chr( $cmds{dots} ) . chr $dots;
    }
    else {
        return;
    }
}

sub chars {
    my $self  = shift;
    my $chunk = shift;
    if (   length( $chunk->{char1} )
        || length( $chunk->{char2} )
        || length( $chunk->{char3} )
        || length( $chunk->{char4} ) )
    {
        return ( ( $chunk->{char1} // q( ) )
            . ( $chunk->{char2}     // q( ) )
                . ( $chunk->{char3} // q( ) )
                . ( $chunk->{char4} // q( ) ) );
    }
    else {
        $self->verbose('no chars');
        return;
    }
}

sub clslot {
    my $self = shift;
    my $slot = shift or return;
    ## no critic (ProhibitCStyleForLoops)
    for ( my $j = 0; $j < scalar( @{ $self->{pending_chunks} } ); $j++ ) {
        next unless ( defined $self->{pending_chunks}->[$j]->{slot} );
        if ( $self->{pending_chunks}->[$j]->{slot} eq $slot ) {
            splice @{ $self->{pending_chunks} }, $j, 1;
        }
    }
    return;
}

sub f {
    my $self = shift;
    my $val  = shift;
    return 1.0 if ( $val > 1.0 );
    return 0.0 if ( $val < 0.0 );
    return $val;
}

sub verbose {
    my $self = shift;
    my $text = shift;
    $self->stderr($text) if ( $self->{verbose} );
    return;
}

1;
