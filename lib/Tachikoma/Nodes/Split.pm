#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Split
# ----------------------------------------------------------------------
#
# $Id: Split.pm 35477 2018-10-21 13:27:41Z chris $
#

package Tachikoma::Nodes::Split;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = 'v2.0.367';

my $Default_Timeout = 840;
my $Counter         = 0;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{delimiter}   = q();
    $self->{line_buffer} = q();
    $self->{messages}    = {};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Split <node name> [ <delimiter> ]
    valid delimiters: newline (default), whitespace, <regex>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my $delimiter = ( $self->{arguments} =~ m{^(.*)$} )[0];
        $self->{delimiter} = $delimiter;
    }
    return $self->{arguments};
}

sub fill {    ## no critic (RequireArgUnpacking, ProhibitExcessComplexity)
    if ( $_[0]->{edge} ) {
        my $edge      = $_[0]->{edge};
        my $delimiter = $_[0]->{delimiter};
        $_[0]->{counter}++;
        if ( not $delimiter or $delimiter eq 'newline' ) {
            for my $line ( split m{(?<=\n)}, $_[1]->[PAYLOAD] ) {
                if ( $line !~ m{\n} ) {
                    $_[0]->{line_buffer} .= $line;
                    next;    # also last
                }
                my $payload = $_[0]->{line_buffer} . $line;
                $_[0]->{line_buffer} = q();
                $_[0]->{edge}->activate( \$payload );
            }
        }
        elsif ( $delimiter eq 'whitespace' ) {
            $_[0]->{edge}->activate( \"$_\n" )
                for ( split q( ), $_[1]->[PAYLOAD] );
        }
        else {
            $_[0]->{edge}->activate( \"$_\n" )
                for ( split m{$delimiter}, $_[1]->[PAYLOAD] );
        }
        return $_[0]->cancel( $_[1] );
    }
    my $self       = shift;
    my $message    = shift;
    my $messages   = undef;
    my $message_id = undef;
    my $persist    = undef;
    return if ( $message->[TYPE] & TM_ERROR or $message->[TYPE] & TM_EOF );
    if ( $message->[TYPE] & TM_PERSIST ) {
        $messages = $self->{messages};
        if ( $message->[TYPE] & TM_RESPONSE ) {
            $message_id = $message->[ID];
            my $info     = $messages->{$message_id} or return;
            my $original = $info->{original};
            my $type     = $message->[PAYLOAD];
            if ( $info->{$type}++ >= $info->{count} - 1 ) {
                delete $messages->{$message_id};
                return (
                      $type eq 'cancel'
                    ? $self->cancel($original)
                    : $self->answer($original)
                );
            }
            elsif ( $info->{answer} + $info->{cancel} >= $info->{count} ) {
                delete $messages->{$message_id};
                return $self->answer($original);
            }
            return;
        }
        $message_id = $self->msg_counter;
        $messages->{$message_id} = {
            original  => $message,
            count     => undef,
            answer    => 0,
            cancel    => 0,
            timestamp => $Tachikoma::Now
        };
        $persist = 'true';
        $self->set_timer if ( not $self->{timer_is_active} );
    }
    my $delimiter = $self->{delimiter};
    my $count     = 0;
    $self->{counter}++;
    if ( not $delimiter or $delimiter eq 'newline' ) {
        for my $line ( split m{(?<=\n)}, $message->[PAYLOAD] ) {
            if ( $line !~ m{\n} ) {
                $self->{line_buffer} .= $line;
                next;    # also last
            }
            my $response = Tachikoma::Message->new;
            $response->[TYPE]      = $message->[TYPE];
            $response->[FROM]      = $self->{name};
            $response->[TO]        = $self->{owner};
            $response->[ID]        = $message_id;
            $response->[TIMESTAMP] = $message->[TIMESTAMP];
            $response->[PAYLOAD]   = $self->{line_buffer} . $line;
            $self->{line_buffer}   = q();
            $self->{sink}->fill($response);
            $count++;
        }
    }
    elsif ( $delimiter eq 'whitespace' ) {
        for my $block ( split q( ), $message->[PAYLOAD] ) {
            my $response = Tachikoma::Message->new;
            $response->[TYPE]      = $message->[TYPE];
            $response->[FROM]      = $self->{name};
            $response->[TO]        = $self->{owner};
            $response->[ID]        = $message_id;
            $response->[TIMESTAMP] = $message->[TIMESTAMP];
            $response->[PAYLOAD]   = $block . "\n";
            $self->{sink}->fill($response);
            $count++;
        }
    }
    else {
        for my $block ( split m{$delimiter}, $message->[PAYLOAD] ) {
            my $response = Tachikoma::Message->new;
            $response->[TYPE]      = $message->[TYPE];
            $response->[FROM]      = $self->{name};
            $response->[TO]        = $self->{owner};
            $response->[ID]        = $message_id;
            $response->[TIMESTAMP] = $message->[TIMESTAMP];
            $response->[PAYLOAD]   = $block . "\n";
            $self->{sink}->fill($response);
            $count++;
        }
    }
    $messages->{$message_id}->{count} = $count if ($message_id);
    return;
}

sub fire {
    my $self     = shift;
    my $messages = $self->{messages};
    my $timeout  = $Default_Timeout;
    for my $message_id ( keys %{$messages} ) {
        my $timestamp = $messages->{$message_id}->{timestamp};
        delete $messages->{$message_id}
            if ( $Tachikoma::Now - $timestamp > $timeout );
    }
    $self->stop_timer if ( not keys %{$messages} );
    return;
}

sub delimiter {
    my $self = shift;
    if (@_) {
        $self->{delimiter} = shift;
    }
    return $self->{delimiter};
}

sub line_buffer {
    my $self = shift;
    if (@_) {
        $self->{line_buffer} = shift;
    }
    return $self->{line_buffer};
}

sub messages {
    my $self = shift;
    if (@_) {
        $self->{messages} = shift;
    }
    return $self->{messages};
}

sub msg_counter {
    my $self = shift;
    $Counter = ( $Counter + 1 ) % $Tachikoma::Max_Int;
    return sprintf '%d:%010d', $Tachikoma::Now, $Counter;
}

1;
