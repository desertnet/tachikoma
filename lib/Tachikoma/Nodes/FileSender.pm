#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::FileSender
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Nodes::FileSender;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::Tail;
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD
    TM_BYTESTREAM TM_EOF TM_COMMAND TM_PERSIST TM_RESPONSE TM_KILLME
);
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = 'v2.0.349';

my $Request_Timeout = 600;
my $Expire_Interval = 60;
my $Respawn_Timeout = 300;
my $Shutting_Down   = undef;

# my $Separator       = chr 0;
my $Separator = join q{}, chr 30, ' -> ', chr 30;

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $prefix, $receiver ) = split q( ), $self->{arguments}, 2;
        $self->set_timer( $Expire_Interval * 1000 );
        $self->prefix($prefix);
        $self->receiver($receiver);
        $self->requests( {} );
    }
    return $self->{arguments};
}

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    my $from    = $message->[FROM];
    my $prefix  = $self->{prefix};

    # service tails
    if ( $from =~ m{^tail-\d+$} ) {
        my $tail     = $Tachikoma::Nodes{$from};
        my $filename = undef;
        if ( $tail->{filename} =~ m{^$prefix/(.*)$} ) {
            $filename = $1;
        }
        else {
            return $self->stderr("ERROR: bad path: $tail->{filename}");
        }
        $message->[STREAM] = join q(:), 'update', $filename;
        $message->[TO] = $self->{receiver};
        if ( $type & TM_EOF ) {
            $message->[TYPE] = TM_EOF | TM_PERSIST;
            $message->[PAYLOAD] = join q(:), lstat $tail->{filename};
            $tail->{msg_unanswered}++;
            $tail->{on_EOF} = 'ignore';
        }
        return $self->{sink}->fill($message);
    }

    # handle responses
    if ( $type & TM_PERSIST and $type & TM_RESPONSE ) {
        my $to       = $message->[TO];
        my $stream   = $message->[STREAM];
        my $requests = $self->{requests};
        my $request  = $requests->{$stream};
        if ( $to and $to ne $self->{name} ) {
            if ( $to !~ m{^tail-\d+$} ) {
                return $self->stderr(
                    "WARNING: bad response for $stream from $from");
            }
            my $tail = $Tachikoma::Nodes{$to};
            $request->{timestamp} = $Tachikoma::Now if ($request);
            if ($tail) {
                $tail->fill($message);
                return if ( $tail->{msg_unanswered} or not $tail->finished );
                $tail->remove_node;
            }
            else {
                return $self->print_less_often(
                    'WARNING: stale response for ', $stream );
            }
            return $self->stderr("ERROR: couldn't find request for $stream")
                if ( not $request );
        }
        delete $requests->{$stream};
        $request->{message}->[TO] = $self->{owner};
        return $self->{sink}->fill( $request->{message} );
    }

    # make sure it's a bytestream and has what we're looking for
    return if ( not $type & TM_BYTESTREAM );

    # get our path and make sure it looks good
    my $payload   = $message->[PAYLOAD];
    my $op        = undef;
    my $path      = undef;
    my $arguments = q{};
    if ( $payload =~ m{^\w+:} ) {
        ( $op, $path ) = split m{:}, $payload, 2;
    }
    else {
        $op   = 'update';
        $path = $payload;
    }
    chomp $path;
    my $relative = undef;
    my $stream   = undef;
    if ( $op ne 'rename' ) {
        $path =~ s{/[.]/}{/}g while ( $path =~ m{/[.]/} );
        $path =~ s{(?:^|/)[.][.](?=/)}{}g;
        $path =~ s{/+}{/}g;
        if ( $path =~ m{^$prefix/(.*)$} ) {
            $relative = $1;
        }
        else {
            $self->stderr( "ERROR: bad path: $path from ", $message->from );
            return $self->cancel($message);
        }
    }
    else {
        my @paths = split q( ), $path;
        @paths = split $Separator, $path, 2 if ( @paths > 2 );
        my ( $from_path, $to_path ) = @paths;
        $from_path =~ s{(?:^|/)[.][.](?=/)}{}g if ($from_path);
        $to_path =~ s{(?:^|/)[.][.](?=/)}{}g   if ($to_path);
        my $from_relative = undef;
        my $to_relative   = undef;
        if ( $from_path =~ m{^$prefix/(.*)$} ) {
            $from_relative = $1;
        }
        else {
            $self->stderr( qq(ERROR: bad "from" path: $from_path from ),
                $message->from );
            return $self->cancel($message);
        }
        if ( $to_path =~ m{^$prefix/(.*)$} ) {
            $to_relative = $1;
        }
        else {
            $self->stderr( qq(ERROR: bad "to" path: $to_path from ),
                $message->from );
            return $self->cancel($message);
        }
        $relative = join $Separator, $from_relative, $to_relative;
    }
    $stream = join q(:), $op, $relative;
    my $requests = $self->{requests};
    if ( $requests->{$stream} ) {
        $self->stderr("WARNING: resetting $stream");
        my $tail = $Tachikoma::Nodes{ $requests->{$stream}->{name} };
        $tail->remove_node if ($tail);
        $self->cancel( $requests->{$stream}->{message} );
        delete $requests->{$stream};
    }

    if ( $op eq 'update' ) {
        if ( not lstat $path ) {
            $self->stderr("WARNING: couldn't find $path");
            return $self->cancel($message);
        }
        elsif ( -l _ ) {
            $op        = 'symlink';
            $stream    = join q(:), $op, $relative;
            $arguments = readlink $path;
        }
        elsif ( -d _ ) {
            $op        = 'mkdir';
            $stream    = join q(:), $op, $relative;
            $arguments = join q(:), lstat _;
        }
        else {
            # set up tails
            my $name;
            do {
                $name = sprintf 'tail-%016d', Tachikoma->counter;
            } while ( exists $Tachikoma::Nodes{$name} );
            my $node;
            my $okay = eval {
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
                return 1;
            };
            if ( not $okay ) {
                my $error = $@ // 'eval failed';
                eval { $node->remove_node; return 1 }
                    or $self->stderr("ERROR: remove_node failed: $@");
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
        my @lstat = lstat $path;
        if ( not @lstat ) {
            $self->stderr("WARNING: couldn't find $path");
            return $self->cancel($message);
        }
        $arguments = join q(:), @lstat;
    }
    if ( $op ne 'update' ) {

        # send other event types
        my $name = $self->{name};
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

sub fire {
    my $self     = shift;
    my $requests = $self->{requests};
    for my $stream ( keys %{$requests} ) {
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

1;
