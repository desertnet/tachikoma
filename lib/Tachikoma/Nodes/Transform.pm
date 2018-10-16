#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Transform
# ----------------------------------------------------------------------
#
# $Id: Transform.pm 2915 2009-09-09 03:38:21Z chris $
#

package Tachikoma::Nodes::Transform;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM ID STREAM PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_INFO TM_EOF TM_PERSIST
);
use parent qw( Tachikoma::Node );

use version; our $VERSION = 'v2.0.367';

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{function} = undef;
    bless $self, $class;
    die "ERROR: Transform nodes are not allowed in the main server process\n"
        if ( $$ == Tachikoma->my_pid );
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Transform <node name> <package> <transform>
    package   - is a module or a path to a module, or '-' for none
    transform - is perl: given \@_ of (\$self, \$message, \$payload),
                return values are sent as messages
    e.g: make_node Transform uc - return uc(\$_[2])
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $package, $transform ) =
            ( $self->{arguments} =~ m{^([\w':./-]+)\s+(.+)$}s );
        die "no package specified\n"   if ( not $package );
        die "no transform specified\n" if ( not $transform );
        $package =~ s{^|$}{'}g         if ( $package =~ m{^/} );

        ## no critic (ProhibitStringyEval)
        eval "require $package"
            or die "couldn't require $package: $@"
            if ( $package ne q(-) );
        $self->{function} = eval "sub { $transform }";
        die $@ if ($@);
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    return if ( $type & TM_EOF );
    return $self->SUPER::fill($message)
        if (not $type & TM_BYTESTREAM
        and not $type & TM_STORABLE
        and not $type & TM_INFO );
    my $persist = $type & TM_PERSIST ? TM_PERSIST : 0;
    my @rv = &{ $self->{function} }( $self, $message, $message->payload );
    for my $transformed (@rv) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE] = ref($transformed) ? TM_STORABLE : TM_BYTESTREAM;
        $response->[TYPE] |= $persist if ($persist);
        $response->[FROM]    = $message->[FROM];
        $response->[ID]      = $message->[ID];
        $response->[STREAM]  = $message->[STREAM];
        $response->[PAYLOAD] = $transformed;
        $self->SUPER::fill($response);
    }
    return 1;
}

sub function {
    my $self = shift;
    if (@_) {
        $self->{function} = shift;
    }
    return $self->{function};
}

1;
