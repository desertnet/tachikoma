#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::FileSender
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Jobs::FileSender;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Nodes::Tail;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD
    TM_BYTESTREAM TM_EOF TM_PERSIST TM_RESPONSE TM_KILLME
);
use Data::Dumper;
use parent qw( Tachikoma::Job );

my $Request_Timeout = 600;
my $Expire_Interval = 60;
my $Respawn_Timeout = 300;
my $Shutting_Down   = undef;
# my $Separator       = chr(0);
my $Separator       = join('', chr(30), ' -> ', chr(30));

sub initialize_graph {
    my $self = shift;
    my ( $prefix, $receiver ) = split( ' ', $self->arguments );
    $self->prefix($prefix);
    $self->receiver( join( '/', '_parent', $receiver ) );
    $self->requests( {} );
    $self->timer( Tachikoma::Nodes::Timer->new );
    $self->timer->name('Timer');
    $self->timer->set_timer( $Expire_Interval * 1000 );
    $self->timer->sink($self);
    $self->connector->sink($self);
    $self->sink( $self->router );
    return;
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    my $from    = $message->[FROM];
    my $prefix  = $self->{prefix};

    # service tails
    if ( $from =~ m(^tail-\d+$) ) {
        my $tail = $Tachikoma::Nodes{$from};
        if ( $tail->{filename} !~ m(^$prefix/(.*)$) ) {
            die "ERROR: bad path: $tail->{filename}";
        }
        my $filename = $1;
        $message->[STREAM] = join( ':', 'update', $filename );
        $message->[TO] = $self->{receiver};
        if ( $type & TM_EOF ) {
            $message->[TYPE] = TM_EOF | TM_PERSIST;
            $message->[PAYLOAD] = join( ':', lstat( $tail->{filename} ) );
            $tail->{msg_unanswered}++;
            $tail->{on_EOF} = 'ignore';
        }
        return $self->{sink}->fill($message);
    }

    # handle responses
    if ( $type & TM_PERSIST and $type & TM_RESPONSE ) {
        my $to       = $message->[TO];
        my $tail     = $Tachikoma::Nodes{$to};
        my $stream   = $message->[STREAM];
        my $requests = $self->{requests};
        my $request  = $requests->{$stream};
        $request->{timestamp} = $Tachikoma::Now if ($request);
        if ($tail) {
            $tail->fill($message);
            return if ( $tail->{msg_unanswered} or not $tail->finished );
            $tail->remove_node;
            delete $requests->{$stream};
        }
        elsif ( $to ne '__SELF__' ) {
            return $self->stderr("WARNING: stale response for $stream");
        }
        return $self->stderr("ERROR: couldn't find request for $stream")
            if ( not $request );
        delete $requests->{$stream};

        return $self->SUPER::fill( $request->{message} );
    }

    # housekeeping
    return $self->expire_requests if ( $from eq 'Timer' );

    # make sure it's a bytestream and has what we're looking for
    return if ( not $type & TM_BYTESTREAM );

    # get our path and make sure it looks good
    my $payload = $message->[PAYLOAD];
    if ( $payload =~ m(^dump(?:\s+(\S+))?\n$) ) {
        my $name     = $1;
        my $response = Tachikoma::Message->new;
        $response->[TYPE] = TM_BYTESTREAM;
        $response->[TO]   = $from;
        if ($name) {
            my $copy = bless( { %{ $Tachikoma::Nodes{$name} } }, 'main' );
            my %normal = map { $_ => 1 } qw( SCALAR ARRAY HASH );
            for my $key ( keys %$copy ) {
                my $value = $copy->{$key};
                my $type  = ref($value);
                $copy->{$key} = $type if ( $type and not $normal{$type} );
            }
            $response->[PAYLOAD] = Dumper($copy);
        }
        else {
            $response->[PAYLOAD] =
                join( "\n", sort keys %Tachikoma::Nodes ) . "\n";
        }
        return $self->SUPER::fill($response);
    }

    my $op        = undef;
    my $path      = undef;
    my $arguments = '';
    if ( $payload =~ m(^\w+:) ) {
        ( $op, $path ) = split( ':', $payload, 2 );
    }
    else {
        $op   = 'update';
        $path = $payload;
    }
    chomp($path);
    my $relative = undef;
    my $stream   = undef;
    if ( $op ne 'rename' ) {
        $path =~ s(/\./)(/)g while ( $path =~ m(/\./) );
        $path =~ s((?:^|/)\.\.(?=/))()g;
        $path =~ s(/+)(/)g;
        if ( $path !~ m(^$prefix/(.*)$) ) {
            $self->stderr( "ERROR: bad path: $path from ", $message->from );
            return $self->cancel($message);
        }
        $relative = $1;
    }
    else {
        my @paths = split( ' ', $path );
        @paths = split( $Separator, $path, 2 ) if ( @paths > 2 );
        my ( $from_path, $to_path ) = @paths;
        $from_path =~ s((?:^|/)\.\.(?=/))()g if ($from_path);
        $to_path =~ s((?:^|/)\.\.(?=/))()g   if ($to_path);
        if ( $from_path !~ m(^$prefix/(.*)$) ) {
            $self->stderr( qq(ERROR: bad "from" path: $from_path from ),
                $message->from );
            return $self->cancel($message);
        }
        my $from_relative = $1;
        if ( $to_path !~ m(^$prefix/(.*)$) ) {
            $self->stderr( qq(ERROR: bad "to" path: $to_path from ),
                $message->from );
            return $self->cancel($message);
        }
        my $to_relative = $1;
        $relative = join( $Separator, $from_relative, $to_relative );
    }
    $stream = join( ':', $op, $relative );
    my $requests = $self->{requests};
    if ( $requests->{$stream} ) {
        $self->stderr("WARNING: resetting $stream");
        my $tail = $Tachikoma::Nodes{ $requests->{$stream}->{name} };
        $tail->remove_node if ($tail);
        $self->cancel( $requests->{$stream}->{message} );
        delete $requests->{$stream};
    }

    if ( $op eq 'update' ) {
        my $fh;
        if ( not lstat($path) ) {
            $self->stderr("WARNING: couldn't find $path");
            return $self->cancel($message);
        }
        elsif ( -l _ ) {
            $op        = 'symlink';
            $stream    = join( ':', $op, $relative );
            $arguments = readlink($path);
        }
        elsif ( -d _ ) {
            $op        = 'mkdir';
            $stream    = join( ':', $op, $relative );
            $arguments = join( ':', lstat(_) );
        }
        else {
            # set up tails
            my $name;
            do {
                $name = sprintf( 'tail-%016d', Tachikoma->counter );
            } while ( exists $Tachikoma::Nodes{$name} );
            my $node;
            eval {
                $node = Tachikoma::Nodes::Tail->new;
                $node->name($name);
                $node->arguments(
                    {   filename       => $path,
                        offset         => 0,
                        max_unanswered => 8,
                        on_EOF         => 'send',
                        on_ENOENT      => 'die'
                    }
                );
                $node->{sink} = $self;
            };
            if ($@) {
                my $error = $@;
                eval { $node->remove_node };
                $self->stderr("ERROR: $error");
                return $self->cancel($message);
            }
            $requests->{$stream} = {
                name      => $name,
                message   => $message,
                timestamp => $Tachikoma::Now
            };

            # send beginning-of-file message to FileReceiver
            my $event = Tachikoma::Message->new;
            $event->[TYPE]   = TM_BYTESTREAM | TM_PERSIST;
            $event->[FROM]   = $name;
            $event->[TO]     = $self->{receiver};
            $event->[STREAM] = $stream;
            $self->{sink}->fill($event);

            # send EOF now if the file is empty
            $node->on_EOF('send_and_wait');

            # wait for response to beginning-of-file message
            $node->{msg_unanswered}++;
        }
    }
    elsif ( $op eq 'chmod' ) {
        my @lstat = lstat($path);
        if ( not @lstat ) {
            $self->stderr("WARNING: couldn't find $path");
            return $self->cancel($message);
        }
        $arguments = join( ':', @lstat );
    }
    if ( $op ne 'update' ) {

        # send other event types
        my $name = '__SELF__';
        $requests->{$stream} = {
            name      => $name,
            message   => $message,
            timestamp => $Tachikoma::Now
        };
        my $event = Tachikoma::Message->new;
        $event->[TYPE]    = TM_BYTESTREAM | TM_PERSIST;
        $event->[FROM]    = $name;
        $event->[TO]      = $self->{receiver};
        $event->[STREAM]  = $stream;
        $event->[PAYLOAD] = $arguments;
        $self->{sink}->fill($event);
    }
    return;
}

sub expire_requests {
    my $self     = shift;
    my $requests = $self->{requests};
    for my $stream ( keys %$requests ) {
        if ( $Tachikoma::Now - $requests->{$stream}->{timestamp}
            > $Request_Timeout )
        {
            my $tail = $Tachikoma::Nodes{ $requests->{$stream}->{name} };
            $tail->remove_node if ($tail);
            delete $requests->{$stream};
            $self->stderr("WARNING: expired $stream from request cache");
        }
    }
    return;
}

sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
}

sub receiver {
    my $self = shift;
    if (@_) {
        $self->{receiver} = shift;
    }
    return $self->{receiver};
}

sub requests {
    my $self = shift;
    if (@_) {
        $self->{requests} = shift;
    }
    return $self->{requests};
}

sub timer {
    my $self = shift;
    if (@_) {
        $self->{timer} = shift;
    }
    return $self->{timer};
}

1;
