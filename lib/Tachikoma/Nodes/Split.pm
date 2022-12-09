#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Split
# ----------------------------------------------------------------------
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

use version; our $VERSION = qv('v2.0.367');

my $DEFAULT_TIMEOUT = 840;
my $COUNTER         = 0;

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

sub fill {
    my $self       = shift;
    my $message    = shift;
    my $messages   = undef;
    my $message_id = undef;
    return if ( $message->[TYPE] & TM_ERROR or $message->[TYPE] & TM_EOF );
    if ( $message->[TYPE] & TM_PERSIST ) {
        $messages = $self->{messages};
        return $self->handle_response($message)
            if ( $message->[TYPE] & TM_RESPONSE );
        $message_id = $self->msg_counter;
        $messages->{$message_id} = {
            original  => $message,
            count     => undef,
            answer    => 0,
            cancel    => 0,
            timestamp => $Tachikoma::Now
        };
        $self->set_timer if ( not $self->{timer_is_active} );
    }
    my $delimiter = $self->{delimiter};
    my $count     = 0;
    $self->{counter}++;
    my @payloads = ();
    if ( not $delimiter or $delimiter eq 'newline' ) {
        for my $line ( split m{(?<=\n)}, $message->[PAYLOAD] ) {
            if ( $line !~ m{\n} ) {
                $self->{line_buffer} .= $line;
                next;    # also last
            }
            push @payloads, $self->{line_buffer} . $line;
            $self->{line_buffer} = q();
            $count++;
        }
    }
    elsif ( $delimiter eq 'whitespace' ) {
        for my $block ( split q( ), $message->[PAYLOAD] ) {
            push @payloads, $block . "\n";
            $count++;
        }
    }
    else {
        for my $block ( split m{$delimiter}, $message->[PAYLOAD] ) {
            push @payloads, $block . "\n";
            $count++;
        }
    }
    $messages->{$message_id}->{count} = $count if ($message_id);
    for my $payload (@payloads) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE]      = $message->[TYPE];
        $response->[FROM]      = $self->{name};
        $response->[TO]        = $self->{owner};
        $response->[ID]        = $message_id if ($message_id);
        $response->[TIMESTAMP] = $message->[TIMESTAMP];
        $response->[PAYLOAD]   = $payload;
        $self->{sink}->fill($response);
    }
    return;
}

sub handle_response {
    my $self     = shift;
    my $message  = shift;
    my $info     = $self->{messages}->{ $message->[ID] } or return;
    my $original = $info->{original};
    my $type     = $message->[PAYLOAD];
    if ( $info->{$type}++ >= $info->{count} - 1 ) {
        delete $self->{messages}->{ $message->[ID] };
        return (
              $type eq 'cancel'
            ? $self->cancel($original)
            : $self->answer($original)
        );
    }
    elsif ( $info->{answer} + $info->{cancel} >= $info->{count} ) {
        delete $self->{messages}->{ $message->[ID] };
    }
    return;
}

sub fire {
    my $self     = shift;
    my $messages = $self->{messages};
    my $timeout  = $DEFAULT_TIMEOUT;
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
    $COUNTER = ( $COUNTER + 1 ) % $Tachikoma::Max_Int;
    return sprintf '%d:%010d', $Tachikoma::Now, $COUNTER;
}

1;
