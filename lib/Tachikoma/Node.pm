#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Node
# ----------------------------------------------------------------------
#
# $Id: Node.pm 35327 2018-10-17 04:54:45Z chris $
#

package Tachikoma::Node;
use strict;
use warnings;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_COMMAND TM_PERSIST TM_RESPONSE TM_INFO TM_EOF
);
use Tachikoma::Command;
use Tachikoma::Crypto;
use POSIX qw( strftime );
use Sys::Hostname qw( hostname );
use Time::HiRes;

use version; our $VERSION = qv('v2.0.101');

use constant MAX_FROM_SIZE => 1024;
use constant MKDIR_MASK    => oct 777;

sub new {
    my $class = shift;
    my $self  = {
        name          => q(),
        arguments     => undef,
        owner         => q(),
        sink          => undef,
        edge          => undef,
        counter       => 0,
        registrations => {}
    };
    bless $self, $class;
    return $self;
}

sub name {
    my $self = shift;
    if (@_) {
        my $name  = shift;
        my $nodes = Tachikoma->nodes;
        die qq(can't rename "$self->{name}" to "$name", node exists\n)
            if ( exists $nodes->{$name} );

        # support renaming
        delete $nodes->{ $self->{name} } if ( $self->{name} );
        $self->{name} = $name;
        $nodes->{$name} = $self if ( length $name );
    }
    return $self->{name};
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
    }
    return $self->{arguments};
}

sub fill {
    my ( $self, $message ) = @_;
    $message->[TO] ||= $self->{owner};
    $self->{counter}++;
    return $self->{sink}->fill($message);
}

sub activate {
    my ($self) = @_;
    return $self->print_less_often('ERROR: activation failed');
}

sub remove_node {
    my ($self)        = @_;
    my $name          = $self->{name};
    my $registrations = $self->{registrations};
    for my $event ( keys %{$registrations} ) {
        for ( keys %{ $registrations->{$event} } ) {
            $self->unregister( $event, $_ );
        }
    }
    if ( $self->{interpreter} ) {
        $self->{interpreter}->remove_node;
    }
    $self->sink(undef);
    $self->edge(undef);
    if ($name) {
        delete Tachikoma->nodes->{$name};
    }
    $self->{name} = undef;
    return;
}

sub dump_config {
    my ($self) = @_;
    my $name = $self->{name};
    my $type = ( ref($self) =~ m{^\w+::Nodes::(.*)} )[0] or return q();
    my $line = "make_node $type $name";
    if ( $self->{arguments} ) {
        my $arguments = $self->{arguments};
        $arguments =~ s{'}{\\'}g;
        $line .= " '$arguments'";
    }
    return "$line\n";
}

sub register {
    my ( $self, $event, $name, $is_function ) = @_;
    my $registrations = $self->{registrations};
    die "no such event: $event\n" if ( not $registrations->{$event} );
    $registrations->{$event} ||= {};
    $registrations->{$event}->{$name} = $is_function;
    return;
}

sub unregister {
    my ( $self, $event, $name ) = @_;
    my $registrations = $self->{registrations};
    die "no such event: $event\n" if ( not $registrations->{$event} );
    if ( $registrations->{$event}->{$name} ) {
        my $responder = Tachikoma->nodes->{_responder};
        my $shell = $responder ? $responder->{shell} : undef;
        if ($shell) {
            delete $shell->callbacks->{$name};
        }
    }
    delete $registrations->{$event}->{$name};
    return;
}

sub connect_node {
    my ( $self, $name, $owner ) = @_;
    die "no node specified\n" if ( not $name );
    my $node = Tachikoma->nodes->{$name};
    die qq(no such node: "$name"\n) if ( not $node );
    if ( ref( $node->owner ) eq 'ARRAY' ) {
        my @owners = grep { $_ ne $owner } @{ $node->owner };
        push @owners, $owner;
        $node->owner( [ sort @owners ] );
    }
    else {
        $node->owner($owner);
    }
    return 1;
}

sub disconnect_node {
    my ( $self, $name, $owner ) = @_;
    die "no node specified\n" if ( not $name );
    my $node = Tachikoma->nodes->{$name};
    die qq(no such node: "$name"\n) if ( not $node );
    if ( ref( $node->owner ) eq 'ARRAY' ) {
        my @keep = ();
        if ( $owner and $owner ne q(*) ) {
            for my $node ( @{ $node->owner } ) {
                if ( $node ne $owner ) {
                    push @keep, $node;
                }
            }
        }
        @{ $node->owner } = @keep;
    }
    else {
        $node->owner(q());
    }
    return 1;
}

sub command {
    my ( $self, $name, $arguments, $payload ) = @_;
    my $message = Tachikoma::Message->new;
    my $command = Tachikoma::Command->new;
    if ($arguments) {
        $arguments =~ s{^\s*|\s*$}{}g;
    }
    $command->{name}      = $name;
    $command->{arguments} = $arguments;
    $command->{payload}   = $payload;
    $command->sign( $self->scheme, $message->timestamp );
    $message->[TYPE]    = TM_COMMAND;
    $message->[PAYLOAD] = $command->packed;
    return $message;
}

sub answer {
    my ( $self, $message ) = @_;
    return if ( not $message->[TYPE] & TM_PERSIST );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_PERSIST | TM_RESPONSE;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $message->[FROM] or return;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = 'answer';
    $self->{sink}->fill($response);
    return;
}

sub cancel {
    my ( $self, $message ) = @_;
    return if ( not $message->[TYPE] & TM_PERSIST );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_PERSIST | TM_RESPONSE;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $message->[FROM] or return;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = 'cancel';
    $self->{sink}->fill($response);
    return;
}

sub notify {
    my ( $self, $event, $payload ) = @_;
    $payload ||= $event;
    chomp $payload;
    my $registrations = $self->{registrations}->{$event};
    my $responder     = $Tachikoma::Nodes{_responder};
    my $shell         = $responder ? $responder->{shell} : undef;
    for my $name ( keys %{$registrations} ) {
        if ( defined $registrations->{$name} ) {
            chomp $payload;
            if ( not $shell->callback( $name, $payload ) ) {
                delete $registrations->{$name};
            }
            next;
        }
        if ( not $Tachikoma::Nodes{$name} ) {
            $self->stderr("WARNING: $name forgot to unregister");
            delete $registrations->{$name};
            next;
        }
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_INFO;
        $message->[FROM]    = $self->{name};
        $message->[STREAM]  = $event;
        $message->[PAYLOAD] = "$payload\n";
        $Tachikoma::Nodes{$name}->fill($message);
    }
    return;
}

sub stamp_message {
    my ( $self, $message, $name ) = @_;
    my $from = $message->[FROM];
    if ( length $from ) {
        $from = join q(/), $name, $from;
        if ( length($from) > MAX_FROM_SIZE ) {
            $self->stderr( 'ERROR: path exceeded '
                    . MAX_FROM_SIZE
                    . " bytes, dropping message from: $from" );
            return;
        }
        $message->[FROM] = $from;
    }
    else {
        $message->[FROM] = $name;
    }
    return 1;
}

sub make_parent_dirs {
    my ( $self, $path_string ) = @_;
    my @path_list = grep {length} split m{/}, $path_string;
    my $path = q();
    pop @path_list;
    for my $dir (@path_list) {
        $path .= q(/) . $dir;
        if (    not -d $path
            and not( mkdir $path, MKDIR_MASK )
            and $! !~ m{File exists} )
        {
            die "couldn't mkdir $path: $!\n";
        }
    }
    return 1;
}

sub make_dirs {
    my ( $self, $path_string ) = @_;
    my @path_list = grep {length} split m{/}, $path_string;
    my $path = q();
    for my $dir (@path_list) {
        $path .= q(/) . $dir;
        if (    not -d $path
            and not( mkdir $path, MKDIR_MASK )
            and $! !~ m{File exists} )
        {
            die "couldn't mkdir $path: $!\n";
        }
    }
    return 1;
}

sub push_profile {
    my ( $self, $name ) = @_;
    push @Tachikoma::Stack, $name;
    return Time::HiRes::time;
}

sub pop_profile {
    my ( $self, $before ) = @_;
    return if ( not $Tachikoma::Profiles );
    my $after = Time::HiRes::time;
    my $name  = pop @Tachikoma::Stack;
    my $info  = $Tachikoma::Profiles->{$name} ||= {};
    $info->{time} += $after - $before;
    $info->{count}++;
    $info->{avg} = $info->{time} / $info->{count};
    $info->{oldest} ||= $before;
    $info->{timestamp} = $after;

    if (@Tachikoma::Stack) {
        $info = $Tachikoma::Profiles->{ $Tachikoma::Stack[-1] };
        $info->{time} -= $after - $before;
    }
    return;
}

sub shutdown_all_nodes {
    my ($self) = @_;
    my $nodes = Tachikoma->nodes;
    Tachikoma->shutting_down('true');

    # Let our parent JobController know that we have shut down.
    if ( $nodes->{_parent} ) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE] = TM_EOF;
        $nodes->{_parent}->fill($message);
    }
    for my $name ( keys %{$nodes} ) {
        if ( not defined $nodes->{$name} ) {
            if ( exists $nodes->{$name} ) {
                $self->stderr(
                    "WARNING: $name found undefined during shutdown");
            }
            next;
        }
        $nodes->{$name}->remove_node;
    }
    return;
}

sub print_less_often {
    my ( $self, $text, @extra ) = @_;
    my $recent = Tachikoma->recent_log_timers;
    my $key    = $self->log_midfix($text);
    if ( not exists $recent->{$key} ) {
        $self->stderr( $text, @extra );
        $recent->{$key} = Tachikoma->now // time;
    }
    return;
}

sub stderr {
    my ( $self, @raw ) = @_;
    print {*STDERR} $self->log_prefix( $self->log_midfix(@raw) );
    return;
}

sub log_prefix {
    my ( $self, @raw ) = @_;
    my $prefix = join q(),
        strftime( '%F %T %Z ', localtime time ),
        hostname(), q( ), $0, '[', $$, ']: ';
    if (@raw) {
        my $msg = join q(), grep defined, @raw;
        chomp $msg;
        my $router = $Tachikoma::Nodes{_router};
        $msg =~ s{^}{$prefix}mg
            if ( $router and $router->{type} ne 'router' );
        return $msg . "\n";
    }
    else {
        return $prefix;
    }
}

sub log_midfix {
    my ( $self, @raw ) = @_;
    my $midfix = q();
    if (    ref $self
        and $self->{name}
        and $0 !~ m{^$self->{name}\b} )
    {
        $midfix = join q(), $self->{name}, ': ';
    }
    if (@raw) {
        my $msg = join q(), grep defined, @raw;
        chomp $msg;
        $msg =~ s{^}{$midfix}mg;
        return $msg . "\n";
    }
    else {
        return $midfix;
    }
}

sub owner {
    my $self = shift;
    if (@_) {
        $self->{owner} = shift;
    }
    return $self->{owner};
}

sub sink {
    my $self = shift;
    if (@_) {
        $self->{sink} = shift;
        if ( $self->{interpreter} ) {
            $self->{interpreter}->sink( $self->{sink} );
        }
    }
    return $self->{sink};
}

sub edge {
    my $self = shift;
    if (@_) {
        $self->{edge} = shift;
    }
    return $self->{edge};
}

sub interpreter {
    my $self = shift;
    if (@_) {
        $self->{interpreter} = shift;
    }
    return $self->{interpreter};
}

sub counter {
    my $self = shift;
    if (@_) {
        $self->{counter} = shift;
    }
    return $self->{counter};
}

sub registrations {
    my $self = shift;
    if (@_) {
        $self->{registrations} = shift;
    }
    return $self->{registrations};
}

sub scheme {
    my ( $self, @args ) = @_;
    return Tachikoma::Crypto->scheme(@args);
}

1;

__END__

=head1 NAME

Tachikoma::Node

=head1 DESCRIPTION

Base class for all nodes.

=head1 CLASS METHODS

=head2 new()

Override this method to declare attributes for your node, etc. Timers should be set in arguments().

=head1 INSTANCE METHODS

=head2 name( $new_name )

If this node manages other nodes, it might useful to override this method to update their names. This is always called before arguments() by L<Tachikoma::Nodes::CommandInterpreter>.

=head2 arguments( $new_arguments )

Override this method to parse your arguments and initialize your node. This is always called by L<Tachikoma::Nodes::CommandInterpreter>. The empty string is passed if there are no arguments. Set any timers here if necessary.

=head2 fill( $message )

Override this method to do work when your node receives a message. Typically called via sink(). See also activate().

=head2 activate( $payload )

Similar to fill() but only receives message payloads. This method is typically called via edge(). Used to implement high performance processing, etc.

=head2 owner()

The implementation of "logical" message routing, this contains the path to another node. Used by fill() to address messages. Override this method if you want something to happen when someone connects to your node--i.e. reset msg_unanswered, etc. Also if this node manages other nodes, it might useful to override this method to update their owners.

Usage: $message->to( $self->owner );

=head2 sink()

The primary implementation of "physical" message routing, this contains a reference to another node. Usually the only method called on a sink() is fill(). If this node manages other nodes, it might useful to override this method to update their sinks as well.

Usage: $self->sink->fill( $message );

=head2 edge()

A secondary implementation of "physical" message routing for high performance processing, this contains a reference to another node. Usually the only method called on an edge() is activate(), but it can also be [ab]used for other purposes. If this node manages other nodes, it might useful to override this method to update their edges as well.

Usage: $self->edge->activate( \{ $message->payload } );

=head2 remove_node()

Override this method when you have work that needs to be done when your node is removed.

=head2 dump_config()

Override this method to provide any additional commands needed to recreate it with the current settings.

=head1 UTILITY METHODS

register( $event, $path, $is_function )

unregister( $event, $path )

connect_node( $name, $owner )

disconnect_node( $name, $owner )

command( $name, $arguments )

answer( $message )

cancel( $message )

notify( $event, $payload )

stamp_message( $message, $name )

make_parent_dirs( $path )

make_dirs( $path )

push_profile( $name )

pop_profile( $before )

shutdown_all_nodes()

print_less_often( $text )

stderr( @elements )

log_prefix()

log_midfix()

interpreter()

counter()

registrations()

=head1 DEPENDENCIES

L<Tachikoma::Message>

L<Tachikoma::Command>

L<Tachikoma::Crypto>

L<POSIX>

L<Sys::Hostname>

L<Time::HiRes>

=head1 SEE ALSO

L<Tachikoma>

L<Tachikoma::Nodes::Timer>

L<Tachikoma::Nodes::FileHandle>

L<Tachikoma::Nodes::Socket>

L<Tachikoma::Nodes::Shell2>

L<Tachikoma::Nodes::CommandInterpreter>

L<Tachikoma::Nodes::JobController>

=head1 AUTHOR

Christopher Reaume C<< <chris@desert.net> >>

=head1 COPYRIGHT

Copyright (c) 2018 DesertNet
