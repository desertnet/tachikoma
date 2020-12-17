#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Tee
# ----------------------------------------------------------------------
#
# $Id: Tee.pm 39713 2020-12-17 19:44:33Z chris $
#

package Tachikoma::Nodes::Tee;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE TO ID PAYLOAD
    TM_PERSIST TM_RESPONSE TM_ERROR
);
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.280');

my $Default_Timeout = 3600;
my $Counter         = 0;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{owner}    = [];
    $self->{messages} = {};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Tee <node name> [ <timeout> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $total   = scalar @{ $self->{owner} };
    return $self->handle_response( $message, $total )
        if ( $message->[TYPE] == ( TM_PERSIST | TM_RESPONSE )
        or $message->[TYPE] == TM_ERROR );
    if ( $total > 1 ) {
        $self->tee($message);
    }
    elsif ($total) {
        my $owner = $self->{owner}->[0];
        my $name = ( split m{/}, $owner, 2 )[0];
        return if ( not $Tachikoma::Nodes{$name} );
        $message->[TO] = join q(/), grep length, $owner, $message->[TO];
        if ( $message->[TYPE] & TM_PERSIST ) {
            $self->stamp_message( $message, $self->{name} ) or return;
        }
        $self->SUPER::fill($message);
    }
    return;
}

sub tee {
    my $self       = shift;
    my $message    = shift;
    my $owners     = $self->{owner};
    my $message_id = undef;
    my $persist    = undef;
    my @keep       = ();
    my $packed     = $message->packed;
    if ( $message->[TYPE] & TM_PERSIST ) {
        my $copy = Tachikoma::Message->new($packed);
        $copy->[PAYLOAD]                 = q();
        $message_id                      = $self->msg_counter;
        $self->{messages}->{$message_id} = {
            original  => $copy,
            count     => scalar( @{$owners} ),
            answer    => 0,
            cancel    => 0,
            timestamp => $Tachikoma::Now
        };
        $persist = 'true';
        if ( not $self->{timer_is_active} ) {
            $self->set_timer;
        }
    }
    for my $owner ( @{$owners} ) {
        my $name = ( split m{/}, $owner, 2 )[0];
        next if ( not $Tachikoma::Nodes{$name} );
        my $copy = Tachikoma::Message->new($packed);
        $copy->[TO] = join q(/), grep length, $owner, $copy->[TO];
        if ($persist) {
            $self->stamp_message( $copy, $self->{name} ) or return;
            $copy->[ID] = $message_id;
        }
        $self->{sink}->fill($copy);
        push @keep, $owner;
    }
    if ( @keep < @{$owners} ) {
        @{$owners} = @keep;
        $self->check_messages;
    }
    $self->{counter}++;
    return;
}

sub handle_response {
    my $self    = shift;
    my $message = shift;
    my $total   = shift;
    if ( $total > 1 ) {
        my $messages = $self->{messages};
        my $info     = $messages->{ $message->[ID] } or return;
        my $original = $info->{original};
        my $type     = $message->[PAYLOAD];
        my $count    = $info->{count};
        $count = $total if ( $total < $count );

        if ( $info->{$type}++ >= $count - 1 ) {
            delete $messages->{ $message->[ID] };
            if ( $type eq 'cancel' ) {
                $self->cancel($original);
            }
            else {
                $self->answer($original);
            }
        }
        elsif ( $info->{answer} + $info->{cancel} >= $count ) {
            delete $messages->{ $message->[ID] };
            $self->answer($original);
        }
    }
    elsif ( $message->[TO] ) {
        $message->[TYPE] = TM_PERSIST | TM_RESPONSE;
        $message->[PAYLOAD] = 'answer' if ( $message->[PAYLOAD] ne 'cancel' );
        $self->{sink}->fill($message);
    }
    return;
}

sub fire {
    my $self     = shift;
    my $messages = $self->{messages};

    # check for dead links
    my $owners = $self->{owner};
    my @keep   = ();
    for my $owner ( @{$owners} ) {
        my $name = ( split m{/}, $owner, 2 )[0];
        next if ( not $Tachikoma::Nodes{$name} );
        push @keep, $owner;
    }
    if ( @keep < @{$owners} ) {
        @{$owners} = @keep;
        $self->check_messages;
    }

    # expire messages
    my $timeout = $self->{arguments} || $Default_Timeout;
    for my $message_id ( keys %{$messages} ) {
        my $timestamp = $messages->{$message_id}->{timestamp};
        delete $messages->{$message_id}
            if ( $Tachikoma::Now - $timestamp > $timeout );
    }
    if ( not keys %{$messages} ) {
        $self->stop_timer;
    }
    return;
}

sub check_messages {
    my $self     = shift;
    my $messages = $self->{messages};
    my $current  = @{ $self->{owner} } or return;
    for my $message_id ( keys %{$messages} ) {
        my $info     = $messages->{$message_id};
        my $original = $info->{original};
        my $count    = $info->{count};
        $count = $current if ( $current < $count );
        if ( $info->{cancel} >= $count ) {
            delete $messages->{$message_id};
            $self->cancel($original);
        }
        elsif ( $info->{answer} + $info->{cancel} >= $count ) {
            delete $messages->{$message_id};
            $self->answer($original);
        }
    }
    return;
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
