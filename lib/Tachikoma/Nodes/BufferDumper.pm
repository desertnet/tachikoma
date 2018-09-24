#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::BufferDumper
# ----------------------------------------------------------------------
#
# $Id: BufferDumper.pm 15843 2013-03-14 23:15:50Z chris $
#

package Tachikoma::Nodes::BufferDumper;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE ID PAYLOAD TM_STORABLE );
use parent qw( Tachikoma::Nodes::Timer );

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{buffers} = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node BufferDumper <node name> <buffer> [ <buffer> ... ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my @buffers = split( ' ', $self->{arguments} );
        $self->{buffers} = \@buffers;
        $self->set_timer;
    }
    return $self->{arguments};
}

sub fire {
    my $self   = shift;
    my %output = ();
    for my $name ( keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        next if ( not $node->isa('Tachikoma::Nodes::Buffer') );
        for my $buffer ( @{ $self->{buffers} } ) {
            next if ( $name !~ m($buffer) );
            my $tiedhash       = $node->tiedhash;
            my $msg_unanswered = $node->{msg_unanswered};
            my @messages       = ();
            my $i              = 0;
            for my $key ( keys %$msg_unanswered ) {
                my ( $timestamp, $attempts, $packed ) =
                    ( unpack( 'F N a*', $tiedhash->{$key} ) );
                my $queued;
                eval { $queued = Tachikoma::Message->new( \$packed ) };
                if ($@) {
                    $self->stderr("WARNING: $@");
                    next;
                }
                $queued->payload if ( $queued->[TYPE] & TM_STORABLE );
                push(
                    @messages,
                    {   'next_attempt' => $timestamp,
                        'attempts'     => $attempts,
                        'message'      => [@$queued],
                        'in_flight'    => $msg_unanswered->{ $queued->[ID] }
                    }
                );
            }
            $output{$name} = {
                msg_received   => $node->{buffer_fills},
                msg_sent       => $node->{pmsg_sent},
                rsp_received   => $node->{rsp_received},
                msg_unanswered => scalar( keys %$msg_unanswered ),
                max_unanswered => $node->{max_unanswered},
                buffer_size    => $node->buffer_size,
                delay          => $node->{delay},
                timeout        => $node->{timeout},
                messages       => \@messages
            };
            last;
        }
    }
    $self->{counter}++;
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_STORABLE;
    $message->[PAYLOAD] = \%output;
    return $self->SUPER::fill($message);
}

sub buffers {
    my $self = shift;
    if (@_) {
        $self->{buffers} = shift;
    }
    return $self->{buffers};
}

1;
