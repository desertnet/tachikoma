#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Node
# ----------------------------------------------------------------------
#

package Tachikoma::Node;
use strict;
use warnings;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_COMMAND TM_PERSIST TM_RESPONSE TM_INFO TM_REQUEST TM_ERROR TM_EOF
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
        sink          => undef,
        edge          => undef,
        owner         => q(),
        counter       => 0,
        registrations => {},
        set_state     => {},
        configuration => Tachikoma->configuration,
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
        delete $nodes->{ $self->{name} } if ( length $self->{name} );
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
    $message->[TO] = $self->{owner} if ( not length $message->[TO] );
    $self->{counter}++;
    return $self->drop_message( $message, 'no sink' )
        if ( not $self->{sink} );
    return $self->{sink}->fill($message);
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
    my $name   = $self->{name};
    my $type   = ( ref($self) =~ m{^\w+::Nodes::(.*)} )[0] or return q();
    my $line   = "make_node $type $name";
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
    if ( length $self->{set_state}->{$event} ) {
        $self->_notify_registered( $name, $event,
            $self->{set_state}->{$event} );
    }
    return;
}

sub unregister {
    my ( $self, $event, $name ) = @_;
    my $registrations = $self->{registrations};
    die "no such event: $event\n" if ( not $registrations->{$event} );
    if ( $registrations->{$event}->{$name} ) {
        my $responder = Tachikoma->nodes->{_responder};
        my $shell     = $responder ? $responder->{shell} : undef;
        if ($shell) {
            delete $shell->callbacks->{$name};
        }
    }
    delete $registrations->{$event}->{$name};
    return;
}

sub connect_node {
    my ( $self, $name, $owner ) = @_;
    die "no node specified\n" if ( not length $name );
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

sub set_state {
    my ( $self, $event, $payload ) = @_;
    $payload ||= $event;
    chomp $payload;
    $self->{set_state}->{$event} = $payload;
    $self->notify( $event, $payload );
    return;
}

sub notify {
    my ( $self, $event, $payload ) = @_;
    $payload ||= $event;
    chomp $payload;
    for my $name ( keys %{ $self->{registrations}->{$event} } ) {
        $self->_notify_registered( $name, $event, $payload );
    }
    return;
}

sub _notify_registered {
    my ( $self, $name, $event, $payload ) = @_;
    my $registered = $self->{registrations}->{$event};
    my $responder  = $Tachikoma::Nodes{_responder};
    my $shell      = $responder ? $responder->{shell} : undef;
    if ( defined $registered->{$name} ) {
        my $okay = $shell->callback(
            $name,
            {   from    => $self->{name},
                event   => $event,
                payload => $payload
            }
        );
        if ( not $okay ) {
            delete $registered->{$name};
        }
        return;
    }
    if ( not $Tachikoma::Nodes{$name} ) {
        $self->stderr("WARNING: $name forgot to unregister");
        delete $registered->{$name};
        return;
    }
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_INFO;
    $message->[FROM]    = $self->{name};
    $message->[STREAM]  = $event;
    $message->[PAYLOAD] = "$payload\n";
    $Tachikoma::Nodes{$name}->fill($message);
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

sub drop_message {
    my $self    = shift;
    my $message = shift;
    my $error   = shift;
    my $payload = undef;
    if (   $message->type == TM_INFO
        or $message->type == TM_REQUEST
        or $message->type == TM_ERROR )
    {
        $payload = ' payload: ' . $message->payload;
    }
    elsif ( $message->type & TM_COMMAND ) {
        my $command = Tachikoma::Command->new( $message->payload );
        $payload = ' payload: ' . $command->name . q( ) . $command->arguments;
    }
    my @log = (
        "WARNING: $error - ",
        $message->type_as_string,
        ( $message->from   ? ' from: ' . $message->from : q() ),
        ( $message->to     ? ' to: ' . $message->to     : q() ),
        ( defined $payload ? $payload                   : q() ),
    );
    if ( $error eq 'NOT_AVAILABLE' ) {
        $self->print_least_often(@log);
    }
    else {
        $self->print_less_often(@log);
    }
    return;
}

sub make_parent_dirs {
    my ( $self, $path_string ) = @_;
    my @path_list = grep {length} split m{/}, $path_string;
    my $path      = q();
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
    my $path      = q();
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

sub print_least_often {
    my ( $self, $text, @extra ) = @_;
    my $recent = Tachikoma->recent_log_timers;
    my $key    = $self->log_midfix($text);
    if ( exists $recent->{$key} ) {
        $recent->{$key}->[1]++;
        $self->stderr( $text, @extra ) if ( $recent->{$key}->[1] == 10 );
    }
    else {
        $recent->{$key} = [ Tachikoma->now // time, 1 ];
    }
    return;
}

sub print_less_often {
    my ( $self, $text, @extra ) = @_;
    my $recent = Tachikoma->recent_log_timers;
    my $key    = $self->log_midfix($text);
    if ( exists $recent->{$key} ) {
        $recent->{$key}->[1]++;
    }
    else {
        $self->stderr( $text, @extra );
        $recent->{$key} = [ Tachikoma->now // time, 1 ];
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
            if ( not $router or $router->{type} ne 'router' );
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

sub owner {
    my $self = shift;
    if (@_) {
        $self->{owner} = shift;
    }
    return $self->{owner};
}

sub counter {
    my $self = shift;
    if (@_) {
        $self->{counter} = shift;
    }
    return $self->{counter};
}

sub configuration {
    my $self = shift;
    if (@_) {
        $self->{configuration} = shift;
    }
    return $self->{configuration};
}

sub interpreter {
    my $self = shift;
    if (@_) {
        $self->{interpreter} = shift;
    }
    return $self->{interpreter};
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
    return $self->configuration->scheme(@args);
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

Override this method to do work when your node receives a message. Typically called via sink().

=head2 sink()

The primary implementation of "physical" message routing, this contains a reference to another node. Usually the only method called on a sink() is fill(). If this node manages other nodes, it might useful to override this method to update their sinks as well.

Usage: $self->sink->fill( $message );

=head2 edge()

A secondary implementation of "physical" message routing, this contains a reference to another node. Usually the only method called on an edge() is fill(), but it can also be [ab]used for other purposes. If this node manages other nodes, it might useful to override this method to update their edges as well.

Usage: $self->edge->fill( \{ $message->payload } );

=head2 owner()

The implementation of "logical" message routing, this contains the path to another node. Used by fill() to address messages. Override this method if you want something to happen when someone connects to your node--i.e. reset msg_unanswered, etc. Also if this node manages other nodes, it might useful to override this method to update their owners.

Usage: $message->to( $self->owner );

=head2 remove_node()

Override this method when you have work that needs to be done when your node is removed.

=head2 dump_config()

Override this method to provide any additional commands needed to recreate it with the current settings.

=head1 UTILITY METHODS

register( $event, $name, $is_function )

unregister( $event, $name )

connect_node( $name, $owner )

disconnect_node( $name, $owner )

command( $name, $arguments )

answer( $message )

cancel( $message )

notify( $event, $payload )

stamp_message( $message, $name )

make_parent_dirs( $path )

make_dirs( $path )

shutdown_all_nodes()

print_less_often( $text )

stderr( @elements )

log_prefix()

log_midfix()

counter()

configuration()

interpreter()

registrations()

scheme()

=head1 DEPENDENCIES

L<Tachikoma::Message>

L<Tachikoma::Command>

L<Tachikoma::Crypto>

L<POSIX>

L<Sys::Hostname>

L<Time::HiRes>

=head1 SEE ALSO

L<Tachikoma>

L<Tachikoma::Config>

L<Tachikoma::Nodes::Router>

L<Tachikoma::Nodes::Timer>

L<Tachikoma::Nodes::FileHandle>

L<Tachikoma::Nodes::Socket>

L<Tachikoma::Nodes::Shell2>

L<Tachikoma::Nodes::CommandInterpreter>

L<Tachikoma::Nodes::JobController>

=head1 AUTHOR

Christopher Reaume C<< <chris@desert.net> >>

=head1 COPYRIGHT

Copyright (c) 2020 DesertNet
