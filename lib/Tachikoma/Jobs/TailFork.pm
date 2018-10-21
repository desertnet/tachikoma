#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::TailFork
# ----------------------------------------------------------------------
#
# $Id: TailFork.pm 9686 2011-01-08 07:17:51Z chris $
#

package Tachikoma::Jobs::TailFork;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::Socket qw( TK_SYNC );
use Tachikoma::Nodes::Tail;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID PAYLOAD
    TM_BYTESTREAM TM_PERSIST TM_RESPONSE TM_EOF TM_ERROR
);
use Data::Dumper;
use parent qw( Tachikoma::Job );

use version; our $VERSION = 'v2.0.368';

$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Useperl  = 1;

my $Default_Timeout = 90;
my $Update_Interval = 1;

sub initialize_graph {
    my $self = shift;
    my ( $destination_settings, $node_path, $tail_settings ) =
        split q( ), $self->arguments, 3;
    my $tail  = Tachikoma::Nodes::Tail->new;
    my $timer = Tachikoma::Nodes::Timer->new;
    my ( $host, $port, $use_SSL ) = split m{:}, $destination_settings, 3;
    $self->destination_host($host);
    $self->destination_port($port);
    $self->use_SSL($use_SSL);
    $self->destination;
    $self->last_offset(-1);
    $self->offset(0);
    $self->connector->sink($self);
    $self->tail($tail);
    $tail->name('Tail');
    $tail->on_EOF('ignore');
    $tail->on_ENOENT('die');
    $tail->on_timeout('die');
    $tail->arguments($tail_settings);
    $tail->buffer_mode('line-buffered');
    $tail->max_unanswered(256);
    $tail->timeout($Default_Timeout);
    $tail->sink($self);
    $timer->name('Timer');
    $timer->set_timer( $Update_Interval * 1000 );
    $timer->sink($self);
    $self->timer($timer);
    $self->sink( $self->router );
    $tail->owner($node_path);
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( not $self->{destination}->{fh} ) {
        $self->stderr(
            'WARNING: lost connection to ',
            $self->{destination}->{hostname},
            ' with ',
            $message->type_as_string,
            ' from ',
            $message->[FROM]
        );
        $self->shutdown_all_nodes;
    }
    elsif ( $message->[FROM] eq 'Tail' ) {
        $message->[FROM] = $self->{name};
        $self->{destination}->fill($message);
        $self->shutdown_all_nodes if ( $message->[TYPE] & TM_EOF );
    }
    elsif ( $message->[TYPE] & TM_RESPONSE ) {
        $self->{offset} = $message->[ID];
        $self->{tail}->fill($message);
    }
    elsif ( $message->[FROM] eq 'Timer' ) {
        $self->send_offset if ( $self->{offset} != $self->{last_offset} );
    }
    elsif ( $message->[TYPE] & TM_ERROR ) {
        $self->shutdown_all_nodes;
    }
    elsif ( $message->[TYPE] & TM_EOF ) {
        $self->stderr( 'WARNING: unexpected TM_EOF from ', $message->[FROM] );
        $self->shutdown_all_nodes;
    }
    elsif ( $message->[PAYLOAD] eq "rename\n" ) {
        $self->{tail}->on_EOF('wait_for_a_while');
        $self->{timer}->remove_node;
    }
    elsif ( $message->[PAYLOAD] eq "delete\n" ) {
        $self->{tail}->on_EOF('wait_to_close');
        $self->{timer}->remove_node;
    }
    elsif ( $message->[PAYLOAD] =~ m{^dump(?:\s+(\S+))?\n$} ) {
        my $name     = $1;
        my $response = Tachikoma::Message->new;
        $response->[TYPE] = TM_BYTESTREAM;
        $response->[TO]   = $message->[FROM];
        if ($name) {
            my $copy = bless { %{ $Tachikoma::Nodes{$name} } }, 'main';
            my %normal = map { $_ => 1 } qw( SCALAR ARRAY HASH );
            for my $key ( keys %{$copy} ) {
                my $value    = $copy->{$key};
                my $ref_type = ref $value;
                $copy->{$key} = $ref_type
                    if ( $ref_type and not $normal{$ref_type} );
            }
            $response->[PAYLOAD] = Dumper($copy);
        }
        else {
            $response->[PAYLOAD] =
                join( "\n", sort keys %Tachikoma::Nodes ) . "\n";
        }
        $self->SUPER::fill($response);
    }
    else {
        $self->stderr( 'WARNING: unexpected message from ',
            $message->[FROM] );
    }
    return;
}

sub send_offset {
    my $self    = shift;
    my $message = Tachikoma::Message->new;
    $message->[TYPE]     = TM_BYTESTREAM;
    $message->[ID]       = $self->{offset};
    $message->[PAYLOAD]  = join q(), $self->{offset}, "\n";
    $self->{last_offset} = $self->{offset};
    return $self->SUPER::fill($message);
}

sub tail {
    my $self = shift;
    if (@_) {
        $self->{tail} = shift;
    }
    return $self->{tail};
}

sub destination {
    my $self = shift;
    if (@_) {
        $self->{destination} = shift;
    }
    if ( not defined $self->{destination} ) {
        my $destination = Tachikoma::Nodes::Socket->inet_client(
            $self->{destination_host},
            $self->{destination_port},
            TK_SYNC, $self->{use_SSL}
        );
        $destination->sink($self);
        $self->{destination} = $destination;
    }
    return $self->{destination};
}

sub destination_host {
    my $self = shift;
    if (@_) {
        $self->{destination_host} = shift;
    }
    return $self->{destination_host};
}

sub destination_port {
    my $self = shift;
    if (@_) {
        $self->{destination_port} = shift;
    }
    return $self->{destination_port};
}

sub use_SSL {
    my $self = shift;
    if (@_) {
        $self->{use_SSL} = shift;
    }
    return $self->{use_SSL};
}

sub last_offset {
    my $self = shift;
    if (@_) {
        $self->{last_offset} = shift;
    }
    return $self->{last_offset};
}

sub offset {
    my $self = shift;
    if (@_) {
        $self->{offset} = shift;
    }
    return $self->{offset};
}

sub timer {
    my $self = shift;
    if (@_) {
        $self->{timer} = shift;
    }
    return $self->{timer};
}

1;
