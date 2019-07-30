#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Consumer
# ----------------------------------------------------------------------
#
# $Id: Consumer.pm 29406 2017-04-29 11:18:09Z chris $
#

package Tachikoma::Nodes::Consumer;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID PAYLOAD
    TM_REQUEST TM_STORABLE TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
    VECTOR_SIZE
);
use Tachikoma;
use Getopt::Long qw( GetOptionsFromString );
use Storable qw( nstore retrieve );
use Time::HiRes qw( usleep );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.256');

my $Poll_Interval   = 1;             # poll for new messages this often
my $Timeout         = 900;           # default async message timeout
my $Expire_Interval = 15;            # check message timeouts
my $Commit_Interval = 60;            # commit offsets
my $Hub_Timeout     = 60;            # timeout waiting for hub
my $Cache_Type      = 'snapshot';    # save complete state
my %Targets         = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;

    # async and sync support
    my $new_buffer = q();
    if (@_) {
        $self->{partition} = shift;
        $self->{offsetlog} = shift;
    }
    else {
        $self->{partition} = undef;
        $self->{offsetlog} = undef;
    }
    $self->{broker_id}      = undef;
    $self->{partition_id}   = undef;
    $self->{offset}         = undef;
    $self->{next_offset}    = undef;
    $self->{default_offset} = 'end';
    $self->{group}          = undef;
    $self->{buffer}         = \$new_buffer;
    $self->{poll_interval}  = $Poll_Interval;
    $self->{last_receive}   = Time::HiRes::time;
    $self->{cache}          = undef;
    $self->{cache_type}     = undef;
    $self->{cache_dir}      = undef;
    $self->{cache_size}     = undef;
    $self->{auto_commit}    = $self->{offsetlog} ? $Commit_Interval : undef;
    $self->{last_commit}    = 0;
    $self->{last_commit_offset} = -1;
    $self->{hub_timeout}        = $Hub_Timeout;

    # async support
    $self->{expecting}               = undef;
    $self->{lowest_offset}           = 0;
    $self->{saved_offset}            = undef;
    $self->{timestamps}              = {};
    $self->{last_expire}             = $Tachikoma::Now;
    $self->{msg_unanswered}          = 0;
    $self->{max_unanswered}          = undef;
    $self->{timeout}                 = $Timeout;
    $self->{status}                  = undef;
    $self->{registrations}->{ACTIVE} = {};
    $self->{registrations}->{READY}  = {};

    # sync support
    $self->{host}       = 'localhost';
    $self->{port}       = 4230;
    $self->{target}     = undef;
    $self->{eos}        = undef;
    $self->{sync_error} = undef;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Consumer <node name> --partition=<path>            \
                               --offsetlog=<path>            \
                               --max_unanswered=<int>        \
                               --timeout=<seconds>           \
                               --poll_interval=<seconds>     \
                               --cache_type=<string>         \
                               --cache_dir=<path>            \
                               --auto_commit=<seconds>       \
                               --hub_timeout=<seconds>       \
                               --default_offset=<int|string>
    # valid cache types: window, snapshot
    # valid offsets: start (0), recent (-2), end (-1)
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments = shift;
        my ($partition, $offsetlog,     $max_unanswered,
            $timeout,   $poll_interval, $cache_type,
            $cache_dir, $auto_commit,   $hub_timeout,
            $default_offset
        );
        my ( $r, $argv ) = GetOptionsFromString(
            $arguments,
            'partition=s'      => \$partition,
            'offsetlog=s'      => \$offsetlog,
            'max_unanswered=i' => \$max_unanswered,
            'timeout=i'        => \$timeout,
            'cache_type=s'     => \$cache_type,
            'cache_dir=s'      => \$cache_dir,
            'poll_interval=i'  => \$poll_interval,
            'auto_commit=i'    => \$auto_commit,
            'hub_timeout=i'    => \$hub_timeout,
            'default_offset=s' => \$default_offset,
        );
        $partition //= shift @{$argv};
        die "ERROR: bad arguments for Consumer\n"
            if ( not $r or not $partition );
        my $new_buffer = q();
        $self->{arguments}      = $arguments;
        $self->{partition}      = $partition;
        $self->{offsetlog}      = $offsetlog;
        $self->{offset}         = undef;
        $self->{next_offset}    = undef;
        $self->{default_offset} = $default_offset // 'end';
        $self->{buffer}         = \$new_buffer;
        $self->{poll_interval}  = $poll_interval || $Poll_Interval;
        $self->{last_receive}   = $Tachikoma::Now;
        $self->{cache_type}     = $cache_type // $Cache_Type;
        $self->{cache_dir}      = $cache_dir;
        $self->{cache_size}     = undef;
        $self->{auto_commit}    = $auto_commit // $Commit_Interval;
        $self->{auto_commit} = undef if ( not $offsetlog and not $cache_dir );
        $self->{last_commit} = 0;
        $self->{last_commit_offset} = -1;
        $self->{hub_timeout}        = $hub_timeout || $Hub_Timeout;
        $self->{expecting}          = undef;
        $self->{lowest_offset}      = 0;
        $self->{saved_offset}       = undef;
        $self->{timestamps}         = {};
        $self->{last_expire}        = $Tachikoma::Now;
        $self->{msg_unanswered}     = 0;
        $self->{max_unanswered}     = $max_unanswered || 1;
        $self->{timeout}            = $timeout || $Timeout;
        $self->{status}             = $offsetlog ? 'INIT' : 'ACTIVE';
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $offset  = $message->[ID];
    if ( $message->[TYPE] == ( TM_PERSIST | TM_RESPONSE ) ) {
        $self->handle_response($message);
    }
    elsif ( $message->[TYPE] & TM_ERROR ) {
        $self->handle_error($message);
    }
    else {
        if ( not $self->{expecting} ) {
            $self->stderr('WARNING: unexpected message')
                if ( not $message->[TYPE] & TM_EOF );
            return;
        }
        $self->{offset} //= $offset;
        if (    $self->{next_offset} > 0
            and $offset != $self->{next_offset} )
        {
            $self->stderr( 'WARNING: skipping from ',
                $self->{next_offset}, ' to ', $offset );
            my $new_buffer = q();
            $self->{buffer} = \$new_buffer;
            $self->{offset} = $offset;
        }
        if ( $message->[TYPE] & TM_EOF ) {
            $self->handle_EOF($offset);
        }
        else {
            $self->{next_offset} = $offset + length $message->[PAYLOAD];
            ${ $self->{buffer} } .= $message->[PAYLOAD];
            $self->set_timer(0)
                if ($self->{timer_interval}
                and $self->{msg_unanswered} < $self->{max_unanswered} );
        }
        $self->{last_receive} = $Tachikoma::Now;
        $self->{expecting}    = undef;
        $self->set_state( 'ACTIVE' => $self->{partition_id} )
            if ( not $self->{set_state}->{ACTIVE}
            and $self->{status} eq 'ACTIVE' );
    }
    return;
}

sub handle_response {
    my $self    = shift;
    my $message = shift;
    my $offset  = $message->[ID];
    if ( $message->[PAYLOAD] eq 'answer' ) {
        $self->remove_node if ( defined $self->partition_id );
    }
    elsif ( $self->{timestamps}->{$offset} ) {
        $self->{last_receive} = $Tachikoma::Now;
        delete $self->{timestamps}->{$offset};
        if ( $self->{msg_unanswered} > 0 ) {
            $self->{msg_unanswered}--;
        }
        else {
            $self->print_less_often(
                'WARNING: unexpected response from ' . $message->[FROM] );
            $self->{msg_unanswered} = 0;
        }
        $self->set_timer(0)
            if ($self->{timer_interval}
            and $self->{msg_unanswered} < $self->{max_unanswered} );
    }
    return;
}

sub handle_error {
    my $self    = shift;
    my $message = shift;
    $self->stderr( $message->[PAYLOAD] )
        if ( $message->[PAYLOAD] ne "NOT_AVAILABLE\n" );
    if ( defined $self->partition_id ) {
        $self->remove_node;
    }
    else {
        $self->arguments( $self->arguments );
    }
    return;
}

sub handle_EOF {
    my $self   = shift;
    my $offset = shift;
    if ( $self->{status} eq 'INIT' ) {
        $self->{status} = 'ACTIVE';
        $self->load_cache_complete;
        $self->next_offset( $self->{saved_offset} );
        $self->set_timer(0) if ( $self->{timer_interval} );
    }
    else {
        $self->{next_offset} = $offset;
        $self->set_state( 'READY' => $self->{partition_id} )
            if ( not $self->{set_state}->{READY} );
    }
    return;
}

sub fire {
    my $self = shift;
    if ( not $self->{msg_unanswered}
        and $Tachikoma::Now - $self->{last_receive} > $self->{hub_timeout} )
    {
        $self->stderr('WARNING: timeout waiting for hub');
        if ( defined $self->partition_id ) {
            $self->remove_node;
        }
        else {
            $self->arguments( $self->arguments );
        }
        return;
    }
    if (    $self->{status} eq 'ACTIVE'
        and $Tachikoma::Now - $self->{last_expire} >= $Expire_Interval )
    {
        $self->expire_timestamps or return;
    }
    if ( not $self->{timer_interval}
        or $self->{timer_interval} != $self->{poll_interval} * 1000 )
    {
        if ( defined $self->{partition_id} ) {
            $self->stop_timer;
            $self->{timer_interval} = $self->{poll_interval} * 1000;
        }
        else {
            $self->set_timer( $self->{poll_interval} * 1000 );
        }
    }
    if ( $self->{msg_unanswered} < $self->{max_unanswered}
        and length ${ $self->{buffer} } )
    {
        $self->drain_buffer;
    }
    if ( $self->{msg_unanswered} < $self->{max_unanswered}
        and not $self->{expecting} )
    {
        $self->get_batch_async;
    }
    return;
}

sub drain_buffer {
    my $self   = shift;
    my $offset = $self->{offset};
    my $buffer = $self->{buffer};
    my $edge   = $self->{edge};
    my $i      = $self->{partition_id};
    my $got    = length ${$buffer};

    # XXX:M
    # my $size =
    #     $got > VECTOR_SIZE
    #     ? VECTOR_SIZE + unpack 'N', ${$buffer}
    #     : 0;
    my $size = $got > VECTOR_SIZE ? unpack 'N', ${$buffer} : 0;
    while ( $got >= $size and $size > 0 ) {
        my $message =
            Tachikoma::Message->new( \substr ${$buffer}, 0, $size, q() );
        if ( $self->{status} eq 'INIT' ) {
            if ( $message->[TYPE] & TM_STORABLE ) {
                $self->load_cache( $message->payload );
            }
            else {
                $self->print_less_often( 'WARNING: unexpected ',
                    $message->type_as_string, ' in cache' );
            }
        }
        else {
            $message->[FROM] =
                defined $i
                ? join q(/), $self->{name}, $i
                : $self->{name};
            $message->[ID] = $offset;
            $self->{counter}++;
            if ($edge) {
                $edge->fill($message);
            }
            else {
                $message->[TYPE] |= TM_PERSIST;
                $message->[TO] = $self->{owner};
                $self->{timestamps}->{$offset} = $Tachikoma::Now;
                $self->{msg_unanswered}++;
                $self->{sink}->fill($message);
                $got = 0
                    if ( $self->{msg_unanswered} >= $self->{max_unanswered} );
            }
        }
        $offset += $size;
        $got -= $size;

        # XXX:M
        # $size =
        #     $got > VECTOR_SIZE
        #     ? VECTOR_SIZE + unpack 'N', ${$buffer}
        #     : 0;
        $size = $got > VECTOR_SIZE ? unpack 'N', ${$buffer} : 0;
    }
    $self->{offset} = $offset;
    return;
}

sub get_batch_async {
    my $self   = shift;
    my $offset = $self->{next_offset};
    return if ( not $self->{sink} );
    if ( not defined $offset ) {
        if ( $self->cache_dir ) {
            my $file = join q(), $self->{cache_dir}, q(/), $self->{name},
                q(.db);
            $self->load_cache( retrieve($file) ) if ( -f $file );
            $self->load_cache_complete;
            $offset = $self->{saved_offset};
        }
        if ( $self->status eq 'INIT' ) {
            $offset //= -2;
        }
        elsif ( $self->default_offset eq 'start' ) {
            $offset //= 0;
        }
        elsif ( $self->default_offset eq 'recent' ) {
            $offset //= -2;
        }
        else {
            $offset //= -1;
        }
        $self->next_offset($offset);
    }
    my $message = Tachikoma::Message->new;
    $message->[TYPE] = TM_REQUEST;
    $message->[FROM] = $self->{name};
    $message->[TO] =
          $self->{status} eq 'INIT'
        ? $self->{offsetlog}
        : $self->{partition};
    $message->[PAYLOAD] = "GET $offset\n";
    $self->{expecting} = 1;
    $self->{sink}->fill($message);
    return;
}

sub expire_timestamps {
    my $self       = shift;
    my $timestamps = $self->{timestamps};
    my $lowest     = ( sort { $a <=> $b } keys %{$timestamps} )[0];
    my $retry      = undef;
    $retry = $lowest
        if ( defined $lowest
        and $Tachikoma::Now - $timestamps->{$lowest} > $self->{timeout} );
    $lowest //= $self->{offset};
    $self->{lowest_offset} = $lowest if ( defined $lowest );
    if ( defined $retry ) {
        $self->stderr("RETRY $retry");
        if ( defined $self->partition_id ) {
            $self->remove_node;
        }
        else {
            $self->arguments( $self->arguments );
            $self->next_offset($lowest);
        }
    }
    elsif ( $self->{auto_commit}
        and $self->{last_commit}
        and $Tachikoma::Now - $self->{last_commit} >= $self->{auto_commit}
        and $self->{lowest_offset} != $self->{last_commit_offset} )
    {
        $self->commit_offset_async;
    }
    $self->{last_expire} = $Tachikoma::Now;
    return not $retry;
}

sub commit_offset_async {
    my $self      = shift;
    my $timestamp = shift;
    my $cache     = shift;
    my $stored    = {
        timestamp => $timestamp // $Tachikoma::Now,
        offset => $self->{lowest_offset},
        cache_type => $self->{cache_type},
        cache      => $cache,
    };
    return if ( not $self->{sink} );
    if ( $self->{cache_type} eq 'snapshot' ) {
        my $i = $self->{partition_id};
        $self->{edge}->on_save_snapshot( $i, $stored )
            if (defined $i
            and $self->{edge}
            and $self->{edge}->can('on_save_snapshot') );
    }
    if ( $self->{cache_dir} ) {
        my $file = join q(), $self->{cache_dir}, q(/), $self->{name}, q(.db);
        my $tmp = join q(), $file, '.tmp';
        $self->make_parent_dirs($tmp);
        nstore( $stored, $tmp );
        rename $tmp, $file
            or $self->stderr("ERROR: couldn't rename cache file $tmp: $!");
        $self->{cache_size} = ( stat $file )[7];
    }
    else {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_STORABLE;
        $message->[FROM]    = $self->{name};
        $message->[TO]      = $self->{offsetlog};
        $message->[PAYLOAD] = $stored;
        $self->{sink}->fill($message);
        $self->{cache_size} = $message->size;
    }
    $self->{last_commit}        = $Tachikoma::Now;
    $self->{last_commit_offset} = $self->{lowest_offset};
    return;
}

sub load_cache {
    my $self   = shift;
    my $stored = shift;
    if ( ref $stored ) {
        my $i = $self->{partition_id};
        $self->{saved_offset} = $stored->{offset};
        if ( $self->{edge} and defined $i ) {
            my $cache_type = $stored->{cache_type} // 'snapshot';
            if ( $cache_type eq 'snapshot' ) {
                if ( $self->{edge}->can('on_load_snapshot') ) {
                    $self->{cache} = $stored;
                }
            }
            elsif ( $cache_type eq 'window' ) {
                if ( $self->{edge}->can('on_load_window') ) {
                    $self->{edge}->on_load_window( $i, $stored );
                }
            }
        }
    }
    else {
        $self->print_less_often('WARNING: bad data in cache');
    }
    return;
}

sub load_cache_complete {
    my $self = shift;
    my $i    = $self->{partition_id};
    if ( $self->{edge} and defined $i ) {
        if ( $self->{cache_type} eq 'window' ) {
            if ( $self->{edge}->can('on_save_window') ) {
                $self->{edge}->on_save_window->[$i] = sub {
                    $self->commit_offset_async(@_);
                    return;
                };
            }
        }
        else {
            if ( $self->{edge}->can('on_load_snapshot') ) {
                $self->{edge}->on_load_snapshot( $i, $self->{cache} );
                $self->{cache} = undef;
            }
        }
    }
    if ( defined $self->{saved_offset} ) {
        $self->{lowest_offset} = $self->{saved_offset};
    }
    else {
        $self->stderr(
            'INFO: beginning at ',
            $self->{default_offset},
            ' offset'
        );
    }
    $self->{last_commit}        = $Tachikoma::Now;
    $self->{last_commit_offset} = $self->{lowest_offset};
    return;
}

sub owner {
    my $self = shift;
    if (@_) {
        $self->{owner} = shift;
        $self->last_receive($Tachikoma::Now);
        $self->set_timer(0);
    }
    return $self->{owner};
}

sub edge {
    my $self = shift;
    if (@_) {
        my $edge = shift;
        my $i    = $self->{partition_id};
        $self->{edge} = $edge;
        $edge->new_cache($i)
            if ( $edge and defined $i and $edge->can('new_cache') );
        $self->last_receive($Tachikoma::Now);
        $self->set_timer(0);
    }
    return $self->{edge};
}

sub remove_node {
    my $self = shift;
    $self->name(q());
    if ( $self->{edge} ) {
        my $edge = $self->{edge};
        my $i    = $self->{partition_id};
        $edge->new_cache($i)
            if ( $edge and defined $i and $edge->can('new_cache') );
    }
    return $self->SUPER::remove_node(@_);
}

sub dump_config {
    my $self     = shift;
    my $response = q();
    if ( not defined $self->{partition_id} ) {
        $response = $self->SUPER::dump_config;
    }
    return $response;
}

########################
# synchronous interface
########################

sub fetch {
    my $self     = shift;
    my $callback = shift;
    my $messages = [];
    my $target   = $self->target or return $messages;
    $self->{sync_error} = undef;
    $self->get_offset
        or return $messages
        if ( not defined $self->{next_offset} );
    return $messages
        if (defined $self->{auto_commit}
        and defined $self->{offset}
        and Time::HiRes::time - $self->{last_commit} >= $self->{auto_commit}
        and not $self->commit_offset );
    usleep( $self->{poll_interval} * 1000000 )
        if ( $self->{eos} and $self->{poll_interval} );
    $self->{eos} = undef;
    my $request = Tachikoma::Message->new;
    $request->[TYPE]    = TM_REQUEST;
    $request->[TO]      = $self->{partition};
    $request->[PAYLOAD] = join q(), 'GET ', $self->{next_offset}, "\n";
    $target->callback( $self->get_batch_sync );
    my $okay = eval {
        $target->fill($request);
        $target->drain;
        return 1;
    };
    if ( not $okay ) {
        my $error = $@ || 'unknown error';
        chomp $error;
        $self->sync_error("FETCH: $error\n");
    }
    elsif ( not $target->{fh} ) {
        $self->sync_error("FETCH: lost connection\n");
    }
    $self->get_messages($messages);
    if ( @{$messages} ) {
        if ($callback) {
            &{$callback}( $self, $_ ) for ( @{$messages} );
        }
        $self->{last_receive} = Time::HiRes::time;
    }
    $self->retry_offset if ( $self->{sync_error} );
    return $messages;
}

sub get_offset {
    my $self   = shift;
    my $stored = undef;
    $self->cache(undef);
    if ( $self->cache_dir ) {
        die "ERROR: no group specified\n" if ( not $self->group );
        my $name = join q(:), $self->partition, $self->group;
        my $file = join q(), $self->cache_dir, q(/), $name, q(.db);
        $stored = retrieve($file);
        $self->offset( $stored->{offset} );
        $self->cache( $stored->{cache} );
    }
    elsif ( $self->offsetlog ) {
        my $consumer = Tachikoma::Nodes::Consumer->new( $self->offsetlog );
        $consumer->next_offset(-2);
        $consumer->broker_id( $self->broker_id );
        $consumer->timeout( $self->timeout );
        $consumer->hub_timeout( $self->hub_timeout );
        while (1) {
            my $messages = $consumer->fetch;
            my $error = $consumer->sync_error // q();
            chomp $error;
            $self->sync_error("GET_OFFSET: $error\n") if ($error);
            last if ( not @{$messages} );
            $stored = $messages->[-1]->payload;
        }
        if ( $self->sync_error ) {
            $self->remove_target;
            return;
        }
    }
    if ( $stored and defined $stored->{cache} ) {
        $self->cache( $stored->{cache} );
    }
    if ( $stored and defined $stored->{offset} ) {
        $self->next_offset( $stored->{offset} );
    }
    elsif ( $self->default_offset eq 'start' ) {
        $self->next_offset(0);
    }
    elsif ( $self->default_offset eq 'recent' ) {
        $self->next_offset(-2);
    }
    else {
        $self->next_offset(-1);
    }
    return 1;
}

sub get_batch_sync {
    my $self = shift;
    return sub {
        my $response  = shift;
        my $expecting = 1;
        if ( length $response->[ID] ) {
            my $offset = $response->[ID];
            my $eof    = $response->[TYPE] & TM_EOF;
            $self->{offset} //= $offset;
            if (    $self->{next_offset} > 0
                and $offset != $self->{next_offset} )
            {
                print {*STDERR} 'WARNING: skipping from ',
                    $self->{next_offset}, ' to ', $offset, "\n";
                my $new_buffer = q();
                $self->{buffer} = \$new_buffer;
                $self->{offset} = $offset;
            }
            if ($eof) {
                $self->{next_offset} = $offset;
                $self->{eos}         = $eof;
            }
            else {
                $self->{next_offset} = $offset + length $response->[PAYLOAD];
                ${ $self->{buffer} } .= $response->[PAYLOAD];
            }
            $expecting = undef;
        }
        elsif ( $response->[PAYLOAD] ) {
            die $response->[PAYLOAD];
        }
        else {
            die $response->type_as_string . "\n";
        }
        return $expecting;
    };
}

sub get_messages {
    my $self     = shift;
    my $messages = shift;
    my $from     = $self->{partition_id};
    my $offset   = $self->{offset};
    my $buffer   = $self->{buffer};
    my $got      = length ${$buffer};

    # XXX:M
    # my $size =
    #     $got > VECTOR_SIZE
    #     ? VECTOR_SIZE + unpack 'N', ${$buffer}
    #     : 0;
    my $size = $got > VECTOR_SIZE ? unpack 'N', ${$buffer} : 0;
    while ( $got >= $size and $size > 0 ) {
        my $message =
            Tachikoma::Message->new( \substr ${$buffer}, 0, $size, q() );
        $message->[FROM] = $from;
        $message->[ID] = join q(:), $offset, $offset + $size;
        $offset += $size;
        $got -= $size;

        # XXX:M
        # $size =
        #     $got > VECTOR_SIZE
        #     ? VECTOR_SIZE + unpack 'N', ${$buffer}
        #     : 0;
        $size = $got > VECTOR_SIZE ? unpack 'N', ${$buffer} : 0;
        push @{$messages}, $message;
    }
    $self->{offset} = $offset;
    return;
}

sub commit_offset {
    my $self = shift;
    return 1 if ( $self->{last_commit} >= $self->{last_receive} );
    my $rv = undef;
    if ( $self->cache_dir ) {
        die "ERROR: no group specified\n" if ( not $self->group );
        my $name = join q(:), $self->partition, $self->group;
        my $file = join q(), $self->cache_dir, q(/), $name, q(.db);
        my $tmp = join q(), $file, '.tmp';
        $self->make_parent_dirs($tmp);
        nstore(
            {   offset => $self->offset,
                cache  => $self->cache
            },
            $tmp
        );
        rename $tmp, $file
            or die "ERROR: couldn't rename cache file $tmp: $!\n";
        $rv = 1;
    }
    else {
        my $target = $self->target or return;
        die "ERROR: no offsetlog specified\n" if ( not $self->offsetlog );
        my $message = Tachikoma::Message->new;
        $message->[TYPE] = TM_STORABLE;
        $message->[TO]   = $self->offsetlog;
        $message->payload(
            {   offset => $self->offset,
                cache  => $self->cache
            }
        );
        $rv = eval {
            $target->fill($message);
            return 1;
        };
        if ( not $rv ) {
            if ( not $target->fh ) {
                $self->sync_error("COMMIT_OFFSET: lost connection\n");
            }
            elsif ( not defined $self->sync_error ) {
                $self->sync_error("COMMIT_OFFSET: send_messages failed\n");
            }
            $self->retry_offset;
            $rv = undef;
        }
    }
    $self->last_commit( $self->last_receive );
    return $rv;
}

sub reset_offset {
    my $self  = shift;
    my $cache = shift;
    $self->next_offset(0);
    $self->cache($cache);
    $self->last_commit(0);
    return $self->commit_offset;
}

sub retry_offset {
    my $self = shift;
    $self->next_offset(undef);
    $self->cache(undef);
    $self->remove_target;
    return;
}

# async and sync support
sub partition {
    my $self = shift;
    if (@_) {
        $self->{partition} = shift;
    }
    return $self->{partition};
}

sub offsetlog {
    my $self = shift;
    if (@_) {
        $self->{offsetlog}   = shift;
        $self->{status}      = 'INIT';
        $self->{last_commit} = 0;
    }
    return $self->{offsetlog};
}

sub broker_id {
    my $self = shift;
    if (@_) {
        $self->{broker_id} = shift;
        my ( $host, $port ) = split m{:}, $self->{broker_id}, 2;
        $self->{host} = $host;
        $self->{port} = $port;
    }
    if ( not defined $self->{broker_id} ) {
        $self->{broker_id} = join q(:), $self->{host}, $self->{port};
    }
    return $self->{broker_id};
}

sub partition_id {
    my $self = shift;
    if (@_) {
        $self->{partition_id} = shift;
    }
    return $self->{partition_id};
}

sub offset {
    my $self = shift;
    if (@_) {
        $self->{offset} = shift;
    }
    return $self->{offset};
}

sub next_offset {
    my $self = shift;
    if (@_) {
        $self->{next_offset} = shift;
        my $new_buffer = q();
        $self->{buffer} = \$new_buffer;
        $self->{offset} = undef;
    }
    return $self->{next_offset};
}

sub default_offset {
    my $self = shift;
    if (@_) {
        $self->{default_offset} = shift;
        $self->next_offset(undef);
    }
    return $self->{default_offset};
}

sub group {
    my $self = shift;
    if (@_) {
        $self->{group} = shift;
    }
    return $self->{group};
}

sub buffer {
    my $self = shift;
    if (@_) {
        $self->{buffer} = shift;
    }
    return $self->{buffer};
}

sub poll_interval {
    my $self = shift;
    if (@_) {
        $self->{poll_interval} = shift;
    }
    return $self->{poll_interval};
}

sub last_receive {
    my $self = shift;
    if (@_) {
        $self->{last_receive} = shift;
    }
    return $self->{last_receive};
}

sub cache {
    my $self = shift;
    if (@_) {
        $self->{cache} = shift;
    }
    return $self->{cache};
}

sub cache_type {
    my $self = shift;
    if (@_) {
        $self->{cache_type} = shift;
    }
    return $self->{cache_type};
}

sub cache_dir {
    my $self = shift;
    if (@_) {
        $self->{cache_dir}   = shift;
        $self->{last_commit} = 0;
    }
    return $self->{cache_dir};
}

sub cache_size {
    my $self = shift;
    if (@_) {
        $self->{cache_size} = shift;
    }
    return $self->{cache_size};
}

sub auto_commit {
    my $self = shift;
    if (@_) {
        $self->{auto_commit} = shift;
    }
    return $self->{auto_commit};
}

sub last_commit {
    my $self = shift;
    if (@_) {
        $self->{last_commit} = shift;
    }
    return $self->{last_commit};
}

sub last_commit_offset {
    my $self = shift;
    if (@_) {
        $self->{last_commit_offset} = shift;
    }
    return $self->{last_commit_offset};
}

sub hub_timeout {
    my $self = shift;
    if (@_) {
        $self->{hub_timeout} = shift;
    }
    return $self->{hub_timeout};
}

# async support
sub expecting {
    my $self = shift;
    if (@_) {
        $self->{expecting} = shift;
    }
    return $self->{expecting};
}

sub lowest_offset {
    my $self = shift;
    if (@_) {
        $self->{lowest_offset} = shift;
    }
    return $self->{lowest_offset};
}

sub saved_offset {
    my $self = shift;
    if (@_) {
        $self->{saved_offset} = shift;
    }
    return $self->{saved_offset};
}

sub timestamps {
    my $self = shift;
    if (@_) {
        $self->{timestamps} = shift;
    }
    return $self->{timestamps};
}

sub last_expire {
    my $self = shift;
    if (@_) {
        $self->{last_expire} = shift;
    }
    return $self->{last_expire};
}

sub msg_unanswered {
    my $self = shift;
    if (@_) {
        $self->{msg_unanswered} = shift;
    }
    return $self->{msg_unanswered};
}

sub max_unanswered {
    my $self = shift;
    if (@_) {
        $self->{max_unanswered} = shift;
    }
    return $self->{max_unanswered};
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

sub status {
    my $self = shift;
    if (@_) {
        $self->{status} = shift;
    }
    return $self->{status};
}

# sync support
sub remove_target {
    my $self = shift;
    if ( $self->{target} ) {
        if ( $self->{target}->{fh} ) {
            close $self->{target}->{fh} or die "ERROR: couldn't close: $!";
            $self->{target}->{fh} = undef;
        }
        $self->{target} = undef;
        usleep( $self->{poll_interval} * 1000000 )
            if ( $self->{poll_interval} );
    }
    return;
}

sub host {
    my $self = shift;
    if (@_) {
        $self->{host} = shift;
    }
    return $self->{host};
}

sub port {
    my $self = shift;
    if (@_) {
        $self->{port} = shift;
    }
    return $self->{port};
}

sub target {
    my $self = shift;
    if (@_) {
        $self->{target} = shift;
    }
    if ( not defined $self->{target} ) {
        my $broker_id = $self->broker_id;
        my ( $host, $port ) = split m{:}, $broker_id, 2;
        $self->{target} = eval { Tachikoma->inet_client( $host, $port ) };
        $self->{target}->timeout( $self->{hub_timeout} )
            if ( $self->{target} );
        if ( not $self->{target} ) {
            $self->sync_error( $@ || "ERROR: connect: unknown error\n" );
            usleep( $self->{poll_interval} * 1000000 )
                if ( $self->{poll_interval} );
        }
    }
    return $self->{target};
}

sub eos {
    my $self = shift;
    if (@_) {
        $self->{eos} = shift;
    }
    return $self->{eos};
}

sub sync_error {
    my $self = shift;
    if (@_) {
        $self->{sync_error} = shift;
    }
    return $self->{sync_error};
}

1;
