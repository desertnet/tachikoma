#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Function
# ----------------------------------------------------------------------
#
# $Id: Function.pm 2915 2009-09-09 03:38:21Z chris $
#

package Tachikoma::Nodes::Function;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_INFO TM_STORABLE TM_PERSIST
);
use Tachikoma::Config qw( %Functions );
use parent qw( Tachikoma::Node );

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
            die "ERROR: missing arguments\n";
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
    if ( $message->[TYPE] & TM_BYTESTREAM or $message->[TYPE] & TM_INFO ) {
        my $name = $self->{name};
        $arguments->{'@'}  = join( ' ', $name, $payload );
        $arguments->{'0'}  = $name;
        $arguments->{'1'}  = $payload;
        $arguments->{'_C'} = 1;
    }
    my $shell     = $self->shell;
    my $old_local = $shell->set_local($arguments);
    eval { $rv = $shell->send_command( $self->{parse_tree} ); };
    $shell->restore_local($old_local);
    if ($@) {
        my $trap = $@;
        chomp($trap);
        my ( $type, $value ) = split( ':', $trap, 2 );
        if ( $type eq 'RV' ) {
            $rv = [$value] if ( defined $value );
        }
        else {
            return $self->stderr($@);
        }
    }
    return if ( not $self->{owner} );
    my $persist = $message->[TYPE] & TM_PERSIST ? TM_PERSIST : 0;
    my $response = Tachikoma::Message->new;
    $response->[TYPE] = TM_BYTESTREAM;
    $response->[TYPE] |= $persist if ($persist);
    $response->[FROM]    = $message->[FROM];
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join( '', @$rv );
    return $self->SUPER::fill($response);
}

sub climb {
    my ( $self, $prefix, $branch, $paths ) = @_;
    if ( ref($branch) eq 'HASH' ) {
        $self->climb( "$prefix.$_", $branch->{$_}, $paths )
            for ( keys %$branch );
    }
    elsif ( ref($branch) eq 'ARRAY' ) {
        $self->climb( "$prefix.$_", $branch->[$_], $paths )
            for ( 0 .. $#$branch );
    }
    elsif ( ref($branch) ) {
        $self->stderr( "ERROR: $prefix is a ", ref($branch) );
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
