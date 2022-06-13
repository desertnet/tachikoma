#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::MemorySieve
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::MemorySieve;
use strict;
use warnings;
use Tachikoma::Node;
use parent qw( Tachikoma::Node );

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
    my $target   = $self->{owner} or return;
    my $name     = ( split m{/}, $target, 2 )[0];
    my $node     = $Tachikoma::Nodes{$name}
        or return $self->print_less_often(q(ERROR: couldn't find node));
    my $size = 0;
    if (   $node->isa('Tachikoma::Nodes::Tee')
        or $node->isa('Tachikoma::Nodes::LoadBalancer') )
    {

        for my $t_target ( @{ $node->owner } ) {
            my $t_name   = ( split m{/}, $t_target, 2 )[0];
            my $t_node   = $Tachikoma::Nodes{$t_name} or next;
            my $t_buffer = $t_node->{output_buffer} or next;
            $size = scalar @{$t_buffer} if ( scalar @{$t_buffer} > $size );
        }
    }
    elsif ( $node->isa('Tachikoma::Nodes::Topic') ) {
        for my $i ( keys %{ $node->{batch} } ) {
            my $batch = $node->{batch}->{$i} or next;
            $size = scalar @{$batch} if ( scalar @{$batch} > $size );
        }
    }
    elsif ( $node->{output_buffer} ) {
        $size = scalar @{ $node->{output_buffer} };
    }
    if ( $size >= $max_size ) {
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
