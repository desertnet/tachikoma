#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Tee
# ----------------------------------------------------------------------
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

my $DEFAULT_TIMEOUT = 3600;
my $COUNTER         = 0;

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
    return $self->handle_response($message)
        if ( $message->[TYPE] == ( TM_PERSIST | TM_RESPONSE )
        or $message->[TYPE] == TM_ERROR );
    my $owners     = $self->{owner};
    my $message_id = undef;
    my $persist    = undef;
    my @keep       = ();
    my $packed     = $message->packed;

    if ( $message->[TYPE] & TM_PERSIST ) {
        my $copy = Tachikoma::Message->unpacked($packed);
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
        my $copy = Tachikoma::Message->unpacked($packed);
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
    my $self     = shift;
    my $message  = shift;
    my $total    = scalar @{ $self->{owner} };
    my $messages = $self->{messages};
    my $info     = $messages->{ $message->[ID] } or return;
    my $original = $info->{original};
    my $type     = $message->[PAYLOAD] eq 'cancel' ? 'cancel' : 'answer';
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
    my $timeout = $self->{arguments} || $DEFAULT_TIMEOUT;
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
    $COUNTER = ( $COUNTER + 1 ) % $Tachikoma::Max_Int;
    return sprintf '%d:%010d', $Tachikoma::Now, $COUNTER;
}

1;
