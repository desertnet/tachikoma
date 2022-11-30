#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Function
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Function;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_INFO TM_REQUEST TM_STORABLE TM_PERSIST
);
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.349');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{parse_tree} = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Function <node name> '{ <commands> }'
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        if ( $self->{arguments} ) {
            my $name     = $self->{name};
            my $commands = $self->{arguments};
            my $shell    = $self->shell;
            $self->{parse_tree} = $shell->parse($commands);
        }
        else {
            die "ERROR: bad arguments for Function\n";
        }
    }
    return $self->{arguments};
}

sub fill {
    my $self      = shift;
    my $message   = shift;
    my $arguments = {};
    my $rv        = [];
    my $payload   = $message->payload;
    my $copy      = {
        type      => $message->[TYPE],
        from      => $message->[FROM],
        to        => $message->[TO],
        id        => $message->[ID],
        stream    => $message->[STREAM],
        timestamp => $message->[TIMESTAMP],
        payload   => $payload
    };
    $self->climb( 'message', $copy, $arguments );
    if (   $message->[TYPE] & TM_BYTESTREAM
        or $message->[TYPE] & TM_INFO
        or $message->[TYPE] & TM_REQUEST )
    {
        my $name = $self->{name};
        $arguments->{q(@)}  = join q( ), $name, $payload;
        $arguments->{q(0)}  = $name;
        $arguments->{q(1)}  = $payload;
        $arguments->{q(_C)} = 1;
    }
    my $shell     = $self->shell;
    my $old_local = $shell->set_local($arguments);
    my $okay      = eval {
        $shell->send_command( $self->{parse_tree} );
        return 1;
    };
    $shell->restore_local($old_local);
    if ( not $okay ) {
        my $trap = $@ || 'unknown error';
        chomp $trap;
        my ( $type, $value ) = split m{:}, $trap, 2;
        if ( $type and $type eq 'RV' ) {
            $rv = [$value] if ( defined $value );
        }
        else {
            return $self->stderr("ERROR: send_command failed: $trap");
        }
    }
    return                         if ( not $self->{owner} );
    return $self->cancel($message) if ( not @{$rv} );
    my $persist  = $message->[TYPE] & TM_PERSIST ? TM_PERSIST : 0;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]      = TM_BYTESTREAM | $persist;
    $response->[FROM]      = $message->[FROM];
    $response->[ID]        = $message->[ID];
    $response->[STREAM]    = $message->[STREAM];
    $response->[TIMESTAMP] = $message->[TIMESTAMP];
    $response->[PAYLOAD]   = join q(), @{$rv};
    return $self->SUPER::fill($response);
}

sub climb {
    my ( $self, $prefix, $branch, $paths ) = @_;
    if ( ref $branch eq 'HASH' ) {
        $self->climb( "$prefix.$_", $branch->{$_}, $paths )
            for ( keys %{$branch} );
    }
    elsif ( ref $branch eq 'ARRAY' ) {
        $self->climb( "$prefix.$_", $branch->[$_], $paths )
            for ( 0 .. $#{$branch} );
    }
    elsif ( ref $branch ) {
        $self->stderr( "ERROR: $prefix is a ", ref $branch );
    }
    else {
        $paths->{$prefix} = $branch;
    }
    return $paths;
}

sub shell {
    my $self      = shift;
    my $responder = $Tachikoma::Nodes{_responder};
    die "ERROR: couldn't find _responder\n" if ( not $responder );
    die "ERROR: Shell v1 does not support functions\n"
        if ( not $responder->shell->isa('Tachikoma::Nodes::Shell2') );
    return $responder->shell;
}

sub parse_tree {
    my $self = shift;
    if (@_) {
        $self->{parse_tree} = shift;
    }
    return $self->{parse_tree};
}

1;
