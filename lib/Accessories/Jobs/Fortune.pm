#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Jobs::Fortune
# ----------------------------------------------------------------------
#

package Accessories::Jobs::Fortune;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw( TM_BYTESTREAM );
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.349');

my $Fortune = undef;
if ( -f '/opt/local/bin/fortune' ) {
    $Fortune = '/opt/local/bin/fortune';
}
else {
    $Fortune = '/usr/games/fortune';
}

sub initialize_graph {
    my $self = shift;
    $self->connector->sink($self);
    $self->sink( $self->router );
    if ( $self->owner ) {
        my $message = Tachikoma::Message->new;
        $message->type(TM_BYTESTREAM);
        $message->payload( $self->arguments );
        $self->fill($message);
        $self->remove_node;
    }
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->type & TM_BYTESTREAM );
    my $arguments = $message->payload;
    my $fortune   = $self->execute( $Fortune, $arguments );
    my @canned    = ();
    return if ( not $fortune );
    $fortune =~ s{^%% [(].*?[)]\s*}{}g;

    for my $cookie ( split m{%%\n}, $fortune ) {
        $cookie =~ s{\t}{        }g;
        push @canned, $cookie;
    }
    $message->to( $message->from );
    $message->payload( $canned[ int rand @canned ] );
    return $self->SUPER::fill($message);
}

1;
