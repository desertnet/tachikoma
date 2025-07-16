#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::TopicProbeToGraphite
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::TopicProbeToGraphite;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE TIMESTAMP PAYLOAD TM_BYTESTREAM );
use parent             qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.368');

my $DEFAULT_INTERVAL = 60;
my @FIELDS           = qw(
    distance
    msg_sent
);
    # p_offset
    # c_offset
    # cache_size
    # msg_unanswered
    # max_unanswered

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{prefix}     = 'hosts';
    $self->{partitions} = {};
    $self->{consumers}  = {};
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->{prefix}    = $self->{arguments};
        $self->set_timer( $DEFAULT_INTERVAL * 1000 );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $prefix    = $self->{prefix};
    my $timestamp = $message->[TIMESTAMP];
    for my $line ( split m{^}, $message->[PAYLOAD] ) {
        my $topic = { map { split m{:}, $_, 2 } split q( ), $line };
        next if ( not $topic->{partition} );
        $topic->{timestamp} = $timestamp;
        if ( $topic->{consumer} ) {
            my $p_offset = $self->{partitions}->{ $topic->{partition} };
            next if ( not defined $p_offset );
            $topic->{c_offset} = $p_offset if ( $p_offset < $topic->{c_offset} );
            $self->{consumers}->{ $topic->{partition} } //= {};
            $self->{consumers}->{ $topic->{partition} }
                ->{ $topic->{consumer} } = $topic;
        }
        elsif ( exists $topic->{p_offset} ) {
            $self->{partitions}->{ $topic->{partition} } = $topic->{p_offset};
        }
    }
    return;
}

sub fire {
    my $self   = shift;
    my @output = ();
    my $prefix = $self->{prefix};
    for my $partition ( keys %{ $self->{consumers} } ) {
        my $p_offset = $self->{partitions}->{$partition} || 0;
        for my $consumer ( keys %{ $self->{consumers}->{$partition} } ) {
            my $topic    = $self->{consumers}->{$partition}->{$consumer};
            my $hostname = $topic->{hostname};
            $hostname =~ s{[.].*}{};
            $consumer =~ s{[^\w\d]+}{_}g;
            $topic->{p_offset} = $p_offset;
            $topic->{distance} = $p_offset - $topic->{c_offset};
            for my $field (@FIELDS) {
                my $key = join q(.),
                    $prefix, $hostname, 'tachikoma',
                    'topics', $consumer, $field;
                push @output, "$key $topic->{$field} $topic->{timestamp}\n";
            }
        }
    }
    while (@output) {
        my (@seg)    = splice @output, 0, 16;
        my $response = Tachikoma::Message->new;
        $response->type(TM_BYTESTREAM);
        $response->payload( join q(), @seg );
        $self->SUPER::fill($response);
    }
    $self->partitions( {} );
    $self->consumers( {} );
    return;
}

sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
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
