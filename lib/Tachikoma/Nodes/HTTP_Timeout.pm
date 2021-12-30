#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::HTTP_Timeout
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::HTTP_Timeout;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.367');

my $Default_Request_Timeout = 300;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{watched} = {};
    $self->{timeout} = $Default_Request_Timeout;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->{timeout} = $self->{arguments} || $Default_Request_Timeout;
        $self->set_timer;
    }
    return $self->{arguments};
}

sub fire {
    my $self    = shift;
    my $watched = $self->{watched};
    my $timeout = $self->{timeout};
    for my $name ( keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        if (    $node->isa('Tachikoma::Nodes::STDIO')
            and $node->{type} eq 'accept' )
        {
            my $status  = $watched->{$name};
            my $counter = $node->{counter};
            if ($status) {
                if ( $counter > $status->{counter} ) {
                    $status->{counter}     = $counter;
                    $status->{last_update} = $Tachikoma::Now;
                }
                elsif ( $Tachikoma::Now - $status->{last_update} > $timeout )
                {
                    $self->print_less_often(
                        "WARNING: idle for $timeout seconds, closing ",
                        $name );
                    $node->remove_node;
                    delete $watched->{$name};
                }
            }
            else {
                $watched->{$name} = {
                    counter     => $counter,
                    last_update => $Tachikoma::Now
                };
            }
        }
    }
    for my $name ( keys %{$watched} ) {
        delete $watched->{$name} if ( not $Tachikoma::Nodes{$name} );
    }
    return;
}

sub watched {
    my $self = shift;
    if (@_) {
        $self->{watched} = shift;
    }
    return $self->{watched};
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

1;
