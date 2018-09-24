#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::Fortune
# ----------------------------------------------------------------------
#
# $Id: Fortune.pm 32953 2018-02-09 10:17:30Z chris $
#

package Tachikoma::Jobs::Fortune;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw( TM_BYTESTREAM );
use parent qw( Tachikoma::Job );

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
    $message->to( $message->from );
    return if ( not $message->type & TM_BYTESTREAM );
    my $arguments = $message->payload;
    my $fortune   = $self->execute( $Fortune, $arguments );
    my @canned    = ();
    return if ( not $fortune );
    $fortune =~ s(^%% [(].*?[)]\s*)()g;

    for my $cookie ( split( "%%\n", $fortune ) ) {
        $cookie =~ s(\t)(        )g;
        push( @canned, $cookie );
    }
    $message->payload( $canned[ int rand(@canned) ] );
    return $self->SUPER::fill($message);
}

1;
