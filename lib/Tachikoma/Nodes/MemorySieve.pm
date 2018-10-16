#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::MemorySieve
# ----------------------------------------------------------------------
#
# $Id: MemorySieve.pm 5634 2010-05-14 23:48:15Z chris $
#

package Tachikoma::Nodes::MemorySieve;
use strict;
use warnings;
use Tachikoma::Nodes::Echo;
use parent qw( Tachikoma::Nodes::Echo );

use version; our $VERSION = qv('v2.0.280');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{max_size}    = undef;
    $self->{should_warn} = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node MemorySieve <node name> [ <max messages> [ should_warn ] ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $max_size, $should_warn ) = split q( ), $self->{arguments}, 2;
        $self->{max_size}    = $max_size // 1024;
        $self->{should_warn} = $should_warn;
    }
    return $self->{arguments};
}

sub fill {
    my $self     = shift;
    my $message  = shift;
    my $max_size = $self->{max_size};
    my $target   = $self->{owner} or return $self->stderr('ERROR: no owner');
    my $name     = ( split m{/}, $target, 2 )[0];
    my $node     = $Tachikoma::Nodes{$name}
        or return $self->print_less_often(q{ERROR: couldn't find node});
    if (   $node->isa('Tachikoma::Nodes::Tee')
        or $node->isa('Tachikoma::Nodes::LoadBalancer') )
    {
        for my $t_target ( @{ $node->owner } ) {
            my $t_name = ( split m{/}, $t_target, 2 )[0];
            my $t_node   = $Tachikoma::Nodes{$t_name} or next;
            my $t_buffer = $t_node->{output_buffer}   or next;
            if ( @{$t_buffer} >= $max_size ) {
                $self->print_less_often(
                    "WARNING: $t_name bufsize > $max_size - dropping messages"
                ) if ( $self->{should_warn} );
                return $self->cancel($message);
            }
        }
        return $self->SUPER::fill($message);
    }
    my $buffer = $node->{output_buffer};
    if ( $buffer and @{$buffer} >= $max_size ) {
        $self->print_less_often(
            "WARNING: $name bufsize > $max_size - dropping messages")
            if ( $self->{should_warn} );
        return $self->cancel($message);
    }
    return $self->SUPER::fill($message);
}

sub max_size {
    my $self = shift;
    if (@_) {
        $self->{max_size} = shift;
    }
    return $self->{max_size};
}

sub should_warn {
    my $self = shift;
    if (@_) {
        $self->{should_warn} = shift;
    }
    return $self->{should_warn};
}

1;
