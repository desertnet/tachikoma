#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Null
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Null;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TO TM_BYTESTREAM TM_PERSIST );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.280');

my $Default_Max_Unanswered = 50;
my $Default_Payload_Size   = 100;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{cached_message} = undef;
    $self->{msg_unanswered} = undef;
    $self->{max_unanswered} = undef;
    $self->{payload_size}   = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Null <node name>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $time, $max_unanswered, $payload_size ) = split q( ),
            $self->{arguments}, 3;
        $self->{msg_unanswered} = 0;
        $self->{max_unanswered} = $max_unanswered // $Default_Max_Unanswered;
        $self->{payload_size}   = $payload_size // $Default_Payload_Size;
        $self->{timer_interval} = $time;
        $self->cached_message(undef);
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    $self->{counter}++;
    $self->{msg_unanswered}-- if ( $self->{msg_unanswered} > 0 );
    $self->set_timer( $self->{timer_interval}, 'oneshot' )
        if ( length $self->{arguments} and not $self->{timer_is_active} );
    return;
}

sub fire {
    my $self    = shift;
    my $message = $self->{cached_message};
    while ( $self->{msg_unanswered} < $self->{max_unanswered} ) {
        $self->{msg_unanswered}++;
        $message->[TO] = $self->{owner};
        $self->{sink}->fill($message);
    }
    return;
}

sub name {
    my ( $self, @args ) = @_;
    my $rv = $self->SUPER::name(@args);
    if ( @args and $self->{cached_message} ) {
        $self->cached_message(undef);
    }
    return $rv;
}

sub owner {
    my ( $self, @args ) = @_;
    my $rv = $self->SUPER::owner(@args);
    if ( @args and $self->{cached_message} ) {
        $self->cached_message(undef);
    }
    return $rv;
}

sub cached_message {
    my $self = shift;
    if (@_) {
        $self->{cached_message} = shift;
    }
    if ( not defined $self->{cached_message} ) {
        my $message = Tachikoma::Message->new;
        $message->type( TM_BYTESTREAM | TM_PERSIST );
        $message->from( $self->{name} );
        $message->to( $self->{owner} );
        $message->payload( ( q(.) x ( $self->{payload_size} - 1 ) ) . "\n" );
        $self->{cached_message} = $message;
        $self->{msg_unanswered} = 0;
        $self->set_timer( $self->{timer_interval}, 'oneshot' )
            if ( length $self->{arguments} );
    }
    return $self->{cached_message};
}

sub msg_unanswered {
    my $self = shift;
    if (@_) {
        $self->{msg_unanswered} = shift;
    }
    return $self->{msg_unanswered};
}

sub max_unanswered {
    my $self = shift;
    if (@_) {
        $self->{max_unanswered} = shift;
    }
    return $self->{max_unanswered};
}

sub payload_size {
    my $self = shift;
    if (@_) {
        $self->{payload_size} = shift;
    }
    return $self->{payload_size};
}

1;
