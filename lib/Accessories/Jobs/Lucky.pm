#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Jobs::Lucky
# ----------------------------------------------------------------------
#
# $Id$
#

package Accessories::Jobs::Lucky;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::LWP;
use Tachikoma::Message qw( TM_BYTESTREAM TM_EOF );
use URI::Escape;
use parent qw( Tachikoma::Job );

sub initialize_graph {
    my $self = shift;
    my $lwp  = Tachikoma::Nodes::LWP->new;
    $self->lwp($lwp);
    $lwp->name('LWP');
    $lwp->arguments('30');
    $lwp->sink($self);
    $self->collector('');
    $self->connector->sink($self);
    $self->sink( $self->router );

    if ( $self->arguments ) {
        my $message = Tachikoma::Message->new;
        $message->type(TM_BYTESTREAM);
        $message->from('_parent');
        $self->fill($message);
    }
    $self->remove_node;
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( length $message->to ) {
        if ( $message->type & TM_EOF ) {
            my $html = $self->collector;
            my $out  = '';
            if ( $html =~ m(class="r".*?a href="(.*?)") ) {
                $out = uri_unescape($1) . "\n";
                if ( $out =~ m(^/url) ) {
                    $out =~ s(^/url\?q=)();
                    $out =~ s(&amp;.*)();
                }
                elsif ( $out =~ m(^/) ) {
                    $out =~ s(^)(http://www.google.com);
                }
            }
            else {
                $out = "no match\n";
            }
            $message->type(TM_BYTESTREAM);
            $message->payload($out);
            return $self->SUPER::fill($message);
        }
        $self->{collector} .= $message->payload;
        return;
    }
    return if ( not $message->type & TM_BYTESTREAM );
    my $arguments = $self->arguments;
    my $escaped   = uri_escape($arguments);
    $message->payload("http://www.google.com/search?q=$escaped");
    return $self->lwp->fill($message);
}

sub collector {
    my $self = shift;
    if (@_) {
        $self->{collector} = shift;
    }
    return $self->{collector};
}

sub lwp {
    my $self = shift;
    if (@_) {
        $self->{lwp} = shift;
    }
    return $self->{lwp};
}

1;
