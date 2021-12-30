#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::FileController
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::FileController;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM STREAM PAYLOAD
    TM_BYTESTREAM TM_PERSIST TM_RESPONSE
);
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.349');

my $Default_Timeout = 840;
my %Exclusive = map { $_ => 1 } qw(
    rmdir
    rename
);

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{op}    = q();
    $self->{pools} = [];
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->{op}        = q();
        $self->{pools}     = [];
    }
    return $self->{arguments};
}

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    my $pools   = $self->{pools};
    if ( $type & TM_BYTESTREAM ) {
        my $payload = $message->[PAYLOAD];
        chomp $payload;
        my ( $op, $path ) = split m{:}, $payload, 2;
        return if ( not $op );
        if ( not $path ) {
            $path    = $op;
            $op      = 'update';
            $payload = join q(:), $op, $path;
        }
        my $last_op = $self->{op};
        $self->{op} = $op;
        if ( not $last_op or ( $op eq $last_op and not $Exclusive{$op} ) ) {
            $pools->[0] ||= {};
            if ( @{$pools} == 1 ) {

                # still filling the first pool
                if ( not $pools->[0]->{$payload} ) {

                    # first event
                    $pools->[0]->{$payload} = $message;
                    $self->send_payload($payload);
                }
                else {
                    # another event for the same payload requires a new pool
                    push @{$pools}, { $payload => $message };
                }
            }
            elsif ( $pools->[-1]->{$payload} ) {

                # future pools can coalesce events
                $self->cancel($message);
            }
            else {
                # fill the last pool
                $pools->[-1]->{$payload} = $message;
            }
        }
        else {
            # different or exclusive operations require new pools
            push @{$pools}, { $payload => $message };
        }
        $self->set_timer( $Default_Timeout * 1000 )
            if ( not $self->{timer_is_active} );
    }
    elsif ( $type & TM_RESPONSE ) {
        my $payload    = $message->[STREAM];
        my $first_pool = $pools->[0];
        return if ( not $first_pool );
        $self->cancel( $first_pool->{$payload} )
            if ( $first_pool->{$payload} );
        delete $first_pool->{$payload};
        if ( not keys %{$first_pool} ) {
            shift @{$pools};
            if ( not @{$pools} ) {
                $self->{op} = q();
                $self->stop_timer;
                return 1;
            }
            $self->send_payload($_) for ( keys %{ $pools->[0] } );
        }
        $self->set_timer( $Default_Timeout * 1000 );
    }
    return;
}

sub fire {
    my $self = shift;

    # empty all the pools so the buffer can refill them
    $self->stderr('WARNING: emptying event pools') if ( @{ $self->{pools} } );
    $self->{op}    = q();
    $self->{pools} = [];
    $self->stop_timer;
    return;
}

sub send_payload {
    my $self    = shift;
    my $payload = shift;
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM | TM_PERSIST;
    $message->[FROM]    = $self->{name};
    $message->[STREAM]  = $payload;
    $message->[PAYLOAD] = "$payload\n";
    $self->SUPER::fill($message);
    return;
}

sub op {
    my $self = shift;
    if (@_) {
        $self->{op} = shift;
    }
    return $self->{op};
}

sub pools {
    my $self = shift;
    if (@_) {
        $self->{pools} = shift;
    }
    return $self->{pools};
}

1;
