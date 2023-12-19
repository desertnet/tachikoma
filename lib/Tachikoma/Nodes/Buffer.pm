#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Buffer
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Buffer;
use strict;
use warnings;
use Tachikoma::Nodes::Scheduler;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_INFO TM_PERSIST
    TM_COMMAND TM_RESPONSE TM_ERROR TM_EOF
);
use Data::Dumper;
use POSIX qw( strftime );
use parent qw( Tachikoma::Nodes::Scheduler );

use version; our $VERSION = qv('v2.0.280');

my $CLEAR_INTERVAL  = 900;
my $DEFAULT_TIMEOUT = 900;
my $HOME            = Tachikoma->configuration->home || ( getpwuid $< )[7];
my $DB_DIR          = "$HOME/.tachikoma/buffers";
my $COUNTER         = 0;
my $DEFAULT_MAX_ATTEMPTS = 10;
my %C                    = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{msg_sent}         = 0;
    $self->{buffer_fills}     = 0;
    $self->{rsp_sent}         = 0;
    $self->{pmsg_sent}        = 0;
    $self->{rsp_received}     = 0;
    $self->{errors_passed}    = 0;
    $self->{msg_unanswered}   = {};
    $self->{max_unanswered}   = 1;
    $self->{max_attempts}     = undef;
    $self->{on_max_attempts}  = 'dead_letter:buffer';
    $self->{cache}            = undef;
    $self->{buffer_size}      = undef;
    $self->{responders}       = {};
    $self->{buffer_mode}      = 'normal';
    $self->{last_fire_time}   = 0;
    $self->{last_clear_time}  = 0;
    $self->{last_expire_time} = $Tachikoma::Now;
    $self->{last_buffer}      = undef;
    $self->{delay}            = 0;
    $self->{timeout}          = $DEFAULT_TIMEOUT;
    $self->{is_active}        = undef;
    $self->{interpreter}->commands( \%C );
    $self->{registrations}->{MSG_RECEIVED} = {};
    $self->{registrations}->{MSG_SENT}     = {};
    $self->{registrations}->{MSG_CANCELED} = {};
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Buffer <node name> <filename> [ <max_unanswered> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $filename, $max_unanswered ) =
            split q( ), $self->{arguments}, 2;
        $self->is_active(undef);
        $self->msg_unanswered( {} );
        $self->untie_hash
            if ( $self->{tiedhash} and tied %{ $self->{tiedhash} } );
        $self->cache(undef);
        $self->buffer_size(undef);
        $self->filename($filename);
        $self->max_unanswered($max_unanswered) if ($max_unanswered);
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    return $self->handle_response($message)
        if ( $type == ( TM_PERSIST | TM_RESPONSE ) or $type == TM_ERROR );
    return $self->SUPER::fill($message)
        if ( $type & TM_COMMAND or $type & TM_EOF );
    my $tiedhash       = $self->{tiedhash} // $self->tiedhash;
    my $message_id     = undef;
    my $unanswered     = keys %{ $self->{msg_unanswered} };
    my $max_unanswered = $self->{max_unanswered};
    my $copy           = bless [ @{$message} ], ref $message;
    return $self->stderr( 'ERROR: unexpected ',
        $message->type_as_string, ' from ', $message->from )
        if (not $type & TM_BYTESTREAM
        and not $type & TM_STORABLE
        and not $type & TM_INFO );
    $self->{counter}++;
    return if ( $self->{buffer_mode} eq 'null' );
    do {
        $message_id = $self->msg_counter;
    } while ( exists $tiedhash->{$message_id} );
    $copy->[TYPE] = $type | TM_PERSIST;
    $copy->[FROM] = $self->{name};
    $copy->[TO]   = $self->{owner};
    $copy->[ID]   = $message_id;
    $self->get_buffer_size if ( not defined $self->{buffer_size} );
    $self->{buffer_fills}++;
    $self->{buffer_size}++;
    $tiedhash->{$message_id} = pack 'F N a*',
        $Tachikoma::Now + $self->{delay}, 0, ${ $copy->packed };
    $self->send_event(
        {   'type'  => 'MSG_RECEIVED',
            'key'   => $copy->[STREAM],
            'value' => $copy->[TYPE] & TM_BYTESTREAM ? $copy->[PAYLOAD] : q(),
        }
    );

    if ( $type & TM_ERROR ) {
        $self->{errors_passed}++;
        $self->answer($message) if ( $type & TM_PERSIST );
    }
    elsif ( $type & TM_PERSIST ) {
        $self->cancel($message);
    }
    $self->set_timer(0)
        if ( $self->{owner} and $unanswered < $max_unanswered );
    return 1;
}

sub handle_response {
    my $self       = shift;
    my $response   = shift;
    my $message_id = $response->[ID];
    my $payload    = $response->[PAYLOAD];
    return if ( not $message_id );
    my $msg_unanswered = $self->{msg_unanswered};
    my $type           = 0;
    if ( $msg_unanswered->{$message_id} ) {
        my $entry = $msg_unanswered->{$message_id};
        $type = $entry->[1];
    }
    $payload = 'cancel' if ( $payload eq 'answer' and $type & TM_ERROR );
    if ( $payload eq 'cancel' ) {
        my $tiedhash    = $self->{tiedhash}    // $self->tiedhash;
        my $buffer_size = $self->{buffer_size} // $self->get_buffer_size;
        if ( $buffer_size > 0 and $tiedhash->{$message_id} ) {
            $self->{rsp_received}++;
            $buffer_size--;
        }
        $self->{buffer_size} = $buffer_size;
        delete $tiedhash->{$message_id};
        delete $msg_unanswered->{$message_id};
        $self->send_event(
            {   'type'  => 'MSG_CANCELED',
                'key'   => $response->[STREAM],
                'value' => $buffer_size,
            }
        );
    }
    else {
        delete $msg_unanswered->{$message_id};
    }
    $self->{responders}->{ $response->[FROM] }++;
    $self->set_timer(0) if ( $self->{owner} );
    return 1;
}

sub fire {
    my $self = shift;
    $self->set_timer;

    # maintain stats
    if ( $Tachikoma::Now - $self->{last_expire_time} > 86400 ) {
        $self->{responders}       = {};
        $self->{last_expire_time} = $Tachikoma::Now;
    }

    my $max_unanswered = $self->{max_unanswered};
    my $msg_unanswered = $self->{msg_unanswered};
    my $timeout        = $self->{timeout} * 1000;

    # time out unanswered messages
    for my $key ( keys %{$msg_unanswered} ) {
        my $span =
            ( $Tachikoma::Right_Now - $msg_unanswered->{$key}->[0] ) * 1000;
        if ( $span > $timeout ) {
            delete $msg_unanswered->{$key};
            $self->{cache} = undef;
        }
        else {
            $self->set_timer( $timeout - $span )
                if ( $timeout - $span < $self->{timer_interval} );
        }
    }
    if ( not $self->{owner} ) {
        $self->stop_timer if ( not keys %{$msg_unanswered} );
        return;
    }

    # refill the run queue
    my $is_empty = undef;
    my $i        = keys %{$msg_unanswered};
    while ( $i < $max_unanswered ) {
        my $key = $self->get_next_key;
        if ( not defined $key ) {

            # buffer is empty (this is the easiest place to detect it)
            # untie and unlink and create a fresh buffer:
            if ( $i == 1 and not $self->get_buffer_size ) {
                if ( $Tachikoma::Now - $self->{last_clear_time}
                    > $CLEAR_INTERVAL )
                {
                    if ( tied %{ $self->{tiedhash} } ) {
                        untie %{ $self->{tiedhash} } or warn;
                        unlink $self->filename       or warn;
                        $self->tiedhash(undef);
                    }
                    $self->{cache}           = undef;
                    $self->{last_clear_time} = $Tachikoma::Now;
                }
                $is_empty = 'true';
            }
            last;
        }
        $i++;
        $self->refill($key);
    }
    $self->{is_active} = 1;
    $self->stop_timer if ( $is_empty and not keys %{$msg_unanswered} );
    return;
}

sub get_next_key {
    my $self  = shift;
    my $cache = $self->{cache};
    if ( not $cache ) {
        $cache = [ sort keys %{ $self->tiedhash } ];
        $self->{cache} = $cache;
    }
    $self->{cache} = undef if ( not @{$cache} );
    return shift @{$cache};
}

sub refill {
    my $self           = shift;
    my $key            = shift;
    my $tiedhash       = $self->{tiedhash};
    my $msg_unanswered = $self->{msg_unanswered};
    my $max_attempts   = $self->{max_attempts} || $DEFAULT_MAX_ATTEMPTS;
    my $value          = $tiedhash->{$key};
    return if ( $msg_unanswered->{$key} or not defined $value );
    my ( $next_attempt, $attempts, $packed ) = unpack 'F N a*', $value;
    my $span    = ( $next_attempt - $Tachikoma::Now ) * 1000;
    my $timeout = $self->{timeout};
    my $to      = $self->{owner};

    if ( $self->{is_active} and $span > 0 ) {
        $self->set_timer($span) if ( $next_attempt < $self->{timer} );
        return 'wait';
    }
    if ( $max_attempts and $attempts >= $max_attempts ) {
        my $path = $self->{on_max_attempts};
        my $name = ( split m{/}, $path, 2 )[0];
        if ( $Tachikoma::Nodes{$name} and $name ne $self->{name} ) {
            $self->print_less_often(
                "$key has failed $attempts attempts - sending to $name");
            $to = $path;
        }
        elsif ( $path eq 'drop' ) {
            $self->stderr(
                "ERROR: $key has failed $attempts attempts - dropping");
            $self->{buffer_size}-- if ( $self->{buffer_size} );
            delete $msg_unanswered->{$key};
            delete $tiedhash->{$key};
            return;
        }
        else {
            $self->print_less_often(
                "$key has failed $attempts attempts - skipping");
            return;
        }
    }
    my $message = eval { Tachikoma::Message->unpacked( \$packed ) };
    if ( $message and $message->[TYPE] ) {
        if ( $self->{is_active} ) {
            $tiedhash->{$key} = pack 'F N a*',
                $Tachikoma::Right_Now + $timeout,
                $attempts + 1, $packed;
        }
        $message->[FROM] = $self->{name};
        $message->[TO]   = $to;
        $message->[ID]   = $key;
        $self->{pmsg_sent}++;
        $msg_unanswered->{$key} = [ $Tachikoma::Right_Now, $message->[TYPE] ];
        $self->{sink}->fill($message);
        $self->send_event(
            {   'type'  => 'MSG_SENT',
                'key'   => $message->[STREAM],
                'value' => $message->[TYPE] & TM_BYTESTREAM
                ? $message->[PAYLOAD]
                : q(),
            }
        );
    }
    else {
        $self->{buffer_size}-- if ( $self->{buffer_size} > 0 );
        delete $msg_unanswered->{$key};
        delete $tiedhash->{$key};
    }
    return;
}

sub lookup {
    my ( $self, @args ) = @_;
    my $tiedhash = $self->{tiedhash};
    my $value    = undef;
    if ( length $args[0] ) {
        my $key = shift @args;
        my ( $timestamp, $attempts, $packed ) = unpack 'F N a*',
            $tiedhash->{$key};
        my $message = eval { Tachikoma::Message->unpacked( \$packed ) };
        $value = {
            next_attempt      => $timestamp,
            attempts          => $attempts,
            message_timestamp => $message->[TIMESTAMP],
            message_stream    => $message->[STREAM],
            message_payload   => $message->payload
            }
            if ($message);
    }
    else {
        $value = [];
        for my $key ( keys %{$tiedhash} ) {
            next if ( not length $key );
            push @{$value}, $self->lookup($key);
        }
    }
    return $value;
}

sub send_event {
    my $self          = shift;
    my $event         = shift;
    my $registrations = $self->{registrations}->{ $event->{type} };
    my $note          = Tachikoma::Message->new;
    $event->{queue}     = $self->{name};
    $event->{timestamp} = $Tachikoma::Right_Now;
    $note->[TYPE]       = TM_STORABLE;
    $note->[STREAM]     = $event->{key};
    $note->[PAYLOAD]    = $event;

    for my $name ( keys %{$registrations} ) {
        my $node = $Tachikoma::Nodes{$name};
        if ( not $node ) {
            $self->stderr("WARNING: $name forgot to unregister");
            delete $registrations->{$name};
            next;
        }
        $node->fill($note);
    }
    return;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list_messages [ -av ]\n"
            . "          remove_message <key>\n"
            . "          dump_message <key>\n"
            . "          set_count <max unanswered count>\n"
            . "          set_default_max_attempts <max attempts>\n"
            . "          set_max_attempts <max attempts>\n"
            . "          on_max_attempts <path> | \"keep\" | \"drop\"\n"
            . "          set_last_buffer <regex>\n"
            . "          set_mode <normal | null>\n"
            . "          set_delay <seconds>\n"
            . "          set_timeout <seconds>\n"
            . "          list_responders\n"
            . "          stats [ -s ]\n"
            . "          reset [ -r ]\n"
            . "          kick\n" );
};

$C{list_messages} = sub {
    my $self           = shift;
    my $command        = shift;
    my $envelope       = shift;
    my $arguments      = $command->arguments;
    my $tiedhash       = $self->patron->tiedhash;
    my $msg_unanswered = $self->patron->msg_unanswered;
    my $list_all       = $arguments =~ m{a};
    my $verbose        = $arguments =~ m{v};
    my $buffer_size    = $self->patron->buffer_size || 0;
    my $response       = q();
    return if ( $list_all and $buffer_size > 10000 );
    my $hash = $list_all ? $tiedhash : $msg_unanswered;

    if ($verbose) {
        $response = sprintf "%-31s %8s %8s %25s\n",
            'ID', 'ATTEMPTS', 'AGE', 'NEXT ATTEMPT';
        for my $key ( sort keys %{$hash} ) {
            my ( $timestamp, $attempts, $packed ) =
                ( unpack 'F N a*', $tiedhash->{$key} );
            my $message = Tachikoma::Message->unpacked( \$packed );
            $response .=
                sprintf "%-31s %8d %8d %25s\n", $key, $attempts,
                $Tachikoma::Now - $message->[TIMESTAMP],
                $timestamp
                ? strftime( '%F %T %Z', localtime $timestamp )
                : 'N/A';
        }
    }
    else {
        for my $key ( sort keys %{$hash} ) {
            $response .= "$key\n";
        }
    }
    return $self->response( $envelope, $response );
};

$C{ls} = $C{list_messages};

$C{remove_message} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $tiedhash = $self->patron->tiedhash;
    my $key      = $command->arguments;
    if ( $key eq q(*) ) {
        if ( $self->patron->filename ) {
            untie %{$tiedhash}             or warn;
            unlink $self->patron->filename or warn;
        }
        $self->patron->msg_unanswered( {} );
        $self->patron->tiedhash(undef);
        $self->patron->buffer_size(undef);
        $self->patron->last_clear_time($Tachikoma::Now);
        return $self->okay($envelope);
    }
    elsif ( exists $tiedhash->{$key} ) {
        delete $self->patron->msg_unanswered->{$key};
        delete $tiedhash->{$key};
        $self->patron->{buffer_size}--;
        return $self->okay($envelope);
    }
    else {
        return $self->error( $envelope, qq(couldn't find message: "$key"\n) );
    }
};

$C{rm} = $C{remove_message};

$C{dump_message} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $key      = $command->arguments;
    my $tiedhash = $self->patron->tiedhash;
    if ( $key eq q(*) ) {
        $key = each %{ $self->patron->msg_unanswered };
        if ( not $key ) {
            return $self->response( $envelope, "no messages in flight\n" );
        }
    }
    if ( exists $tiedhash->{$key} ) {
        my ( $timestamp, $attempts, $packed ) =
            ( unpack 'F N a*', $tiedhash->{$key} );
        my $message = Tachikoma::Message->unpacked( \$packed );
        $message->[FROM] = $self->patron->name;
        $message->[TO]   = $self->patron->owner;
        $message->[ID]   = $key;
        $message->payload if ( $message->[TYPE] & TM_STORABLE );
        return $self->response(
            $envelope,
            Dumper(
                {   'next_attempt' =>
                        strftime( '%F %T %Z', localtime $timestamp ),
                    'attempts' => $attempts,
                    'message'  => $message
                }
            )
        );
    }
    else {
        return $self->error( $envelope, qq(can't find message "$key"\n) );
    }
};

$C{dump} = $C{dump_message};

$C{set_count} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $command->arguments =~ m{\D} ) {
        die qq(count must be an integer\n);
    }
    $self->patron->max_unanswered( $command->arguments );
    return $self->okay($envelope);
};

$C{set_default_max_attempts} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $command->arguments =~ m{\D} ) {
        die qq(count must be an integer\n);
    }
    $DEFAULT_MAX_ATTEMPTS = $command->arguments;
    return $self->okay($envelope);
};

$C{set_max_attempts} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $command->arguments =~ m{\D} ) {
        die qq(count must be an integer\n);
    }
    $self->patron->max_attempts( $command->arguments );
    return $self->okay($envelope);
};

$C{on_max_attempts} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $command->arguments ne 'keep' and $command->arguments ne 'drop' ) {
        die qq(valid options: keep drop\n);
    }
    $self->patron->on_max_attempts( $command->arguments );
    return $self->okay($envelope);
};

$C{set_last_buffer} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $arguments = $command->arguments;
    my $foo       = 'test';
    $foo =~ m{$arguments};
    $self->patron->last_buffer($arguments);
    return $self->okay($envelope);
};

$C{set_mode} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $arguments = $command->arguments;
    my %valid     = map { $_ => 1 } qw( normal null );
    if ( not $valid{$arguments} ) {
        die qq(mode must be "normal", or "null"\n);
    }
    $self->patron->buffer_mode($arguments);
    return $self->okay($envelope);
};

$C{set_delay} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $command->arguments =~ m{\D} ) {
        die qq(seconds must be an integer\n);
    }
    $self->patron->delay( $command->arguments );
    return $self->okay($envelope);
};

$C{set_timeout} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $command->arguments =~ m{\D} ) {
        die qq(seconds must be an integer\n);
    }
    $self->patron->timeout( $command->arguments );
    return $self->okay($envelope);
};

$C{list_responders} = sub {
    my $self       = shift;
    my $command    = shift;
    my $envelope   = shift;
    my $responders = $self->patron->responders;
    my $response   = q();
    for my $key ( sort keys %{$responders} ) {
        $response .= sprintf "%-30s %9d\n", $key, $responders->{$key};
    }
    return $self->response( $envelope, $response );
};

$C{responders} = $C{list_responders};

$C{stats} = sub {
    my $self            = shift;
    my $command         = shift;
    my $envelope        = shift;
    my $patron          = $self->patron;
    my $msg_sent        = $patron->msg_sent;
    my $msg_received    = $patron->counter;
    my $fills           = $patron->buffer_fills;
    my $rsp_sent        = $patron->rsp_sent;
    my $pmsg_sent       = $patron->pmsg_sent;
    my $rsp_received    = $patron->rsp_received;
    my $errors_passed   = $patron->errors_passed;
    my $unanswered      = keys %{ $patron->msg_unanswered };
    my $max             = $patron->max_unanswered;
    my $max_attempts    = $patron->max_attempts || q(-);
    my $on_max_attempts = $patron->on_max_attempts;
    my $delay           = $patron->delay;
    my $timeout         = $patron->timeout;
    my $in_buffer       = $patron->buffer_size // $patron->get_buffer_size;
    my $response;

    if ( $command->arguments eq '-s' ) {
        $response =
            ("$unanswered unanswered / $max max; $in_buffer in buffer\n");
    }
    else {
        $response =
            (     "messages sent:              $msg_sent\n"
                . "messages received:          $msg_received\n"
                . "buffer fills:               $fills\n"
                . "responses sent:             $rsp_sent\n"
                . "persistent messages sent:   $pmsg_sent\n"
                . "responses received:         $rsp_received\n"
                . "errors passed:              $errors_passed\n"
                . "default max attempts:       $DEFAULT_MAX_ATTEMPTS\n"
                . "max attempts:               $max_attempts\n"
                . "on max attempts:            $on_max_attempts\n"
                . "delay:                      $delay\n"
                . "timeout:                    $timeout\n"
                . "messages awaiting response: $unanswered / $max\n"
                . "messages in buffer:         $in_buffer\n" );
    }
    return $self->response( $envelope, $response );
};

$C{reset} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $patron   = $self->patron;
    if ( $command->arguments eq '-r' ) {
        $patron->responders( {} );
    }
    else {
        $patron->msg_sent(0);
        $patron->counter(0);
        $patron->buffer_fills(0);
        $patron->rsp_sent(0);
        $patron->pmsg_sent(0);
        $patron->rsp_received(0);
        $patron->errors_passed(0);
    }
    return $self->okay($envelope);
};

$C{kick} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $patron   = $self->patron;
    $patron->arguments( $patron->arguments );
    $patron->fire;
    return $self->okay($envelope);
};

sub answer {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_PERSIST );
    $self->{rsp_sent}++;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_PERSIST | TM_RESPONSE;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $self->get_last_buffer($message) or return;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = 'answer';
    return $self->{sink}->fill($response);
}

sub cancel {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_PERSIST );
    $self->{rsp_sent}++;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_PERSIST | TM_RESPONSE;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $self->get_last_buffer($message) or return;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = 'cancel';
    return $self->{sink}->fill($response);
}

sub get_last_buffer {
    my $self        = shift;
    my $message     = shift;
    my $last_buffer = $self->{last_buffer};
    my $from        = $message->[FROM];
    $from = $last_buffer
        if ( $last_buffer and $from !~ s{^.*?($last_buffer)}{$1}s );
    return $from;
}

sub dump_config {
    my $self            = shift;
    my $response        = undef;
    my $settings        = undef;
    my $name            = $self->{name};
    my $filename        = $self->{filename} // q();
    my $max_unanswered  = $self->{max_unanswered};
    my $max_attempts    = $self->{max_attempts};
    my $on_max_attempts = $self->{on_max_attempts};
    my $buffer_mode     = $self->{buffer_mode};
    my $last_buffer     = $self->{last_buffer};
    my $delay           = $self->{delay};
    my $timeout         = $self->{timeout};
    $response = "make_node Buffer $self->{name}";
    $response .= " $filename"
        if ( $filename ne $name or $max_unanswered > 1 );
    $response .= " $max_unanswered" if ( $max_unanswered > 1 );
    $response .= "\n";
    $settings = "  on_max_attempts $on_max_attempts\n" if ($on_max_attempts);
    $settings = "  set_max_attempts $max_attempts\n"   if ($max_attempts);
    $settings .= "  set_last_buffer $last_buffer\n" if ($last_buffer);
    $settings .= "  set_mode $buffer_mode\n"
        if ( $buffer_mode ne 'normal' );
    $settings .= "  set_delay $delay\n" if ($delay);
    $settings .= "  set_timeout $timeout\n"
        if ( $timeout ne $DEFAULT_TIMEOUT );
    $response .= "cd $self->{name}\n" . $settings . "cd ..\n" if ($settings);
    return $response;
}

sub get_buffer_size {
    my $self     = shift;
    my $tiedhash = $self->tiedhash;
    $self->{buffer_size} = keys %{$tiedhash};
    return $self->{buffer_size};
}

sub msg_counter {
    my $self = shift;
    $COUNTER = ( $COUNTER + 1 ) % $Tachikoma::Max_Int;
    return sprintf '%d:%010d', $Tachikoma::Now, $COUNTER;
}

sub msg_sent {
    my $self = shift;
    if (@_) {
        $self->{msg_sent} = shift;
    }
    return $self->{msg_sent};
}

sub buffer_fills {
    my $self = shift;
    if (@_) {
        $self->{buffer_fills} = shift;
    }
    return $self->{buffer_fills};
}

sub rsp_sent {
    my $self = shift;
    if (@_) {
        $self->{rsp_sent} = shift;
    }
    return $self->{rsp_sent};
}

sub pmsg_sent {
    my $self = shift;
    if (@_) {
        $self->{pmsg_sent} = shift;
    }
    return $self->{pmsg_sent};
}

sub rsp_received {
    my $self = shift;
    if (@_) {
        $self->{rsp_received} = shift;
    }
    return $self->{rsp_received};
}

sub errors_passed {
    my $self = shift;
    if (@_) {
        $self->{errors_passed} = shift;
    }
    return $self->{errors_passed};
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

sub max_attempts {
    my $self = shift;
    if (@_) {
        $self->{max_attempts} = shift;
    }
    return $self->{max_attempts};
}

sub on_max_attempts {
    my $self = shift;
    if (@_) {
        $self->{on_max_attempts} = shift;
    }
    return $self->{on_max_attempts};
}

sub cache {
    my $self = shift;
    if (@_) {
        $self->{cache} = shift;
    }
    return $self->{cache};
}

sub responders {
    my $self = shift;
    if (@_) {
        $self->{responders} = shift;
    }
    return $self->{responders};
}

sub buffer_mode {
    my $self = shift;
    if (@_) {
        $self->{buffer_mode} = shift;
    }
    return $self->{buffer_mode};
}

sub last_buffer {
    my $self = shift;
    if (@_) {
        $self->{last_buffer} = shift;
    }
    return $self->{last_buffer};
}

sub last_clear_time {
    my $self = shift;
    if (@_) {
        $self->{last_clear_time} = shift;
    }
    return $self->{last_clear_time};
}

sub last_expire_time {
    my $self = shift;
    if (@_) {
        $self->{last_expire_time} = shift;
    }
    return $self->{last_expire_time};
}

sub delay {
    my $self = shift;
    if (@_) {
        $self->{delay}     = shift;
        $self->{is_active} = 1;
    }
    return $self->{delay};
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

sub timer {
    my $self = shift;
    if (@_) {
        $self->{timer} = shift;
    }
    return $self->{timer};
}

sub set_timer {
    my ( $self, $span, @args ) = @_;
    $self->{timer} = $Tachikoma::Right_Now + ( $span // 0 ) / 1000;
    return $self->SUPER::set_timer( $span, @args );
}

sub is_active {
    my $self = shift;
    if (@_) {
        $self->{is_active} = shift;
    }
    return $self->{is_active};
}

sub owner {
    my $self = shift;
    if (@_) {
        $self->{owner} = shift;
        $self->set_timer(0);
    }
    return $self->{owner};
}

sub default_max_attempts {
    my $self = shift;
    if (@_) {
        $DEFAULT_MAX_ATTEMPTS = shift;
    }
    return $DEFAULT_MAX_ATTEMPTS;
}

sub db_dir {
    my $self = shift;
    if (@_) {
        $DB_DIR = shift;
    }
    return $DB_DIR;
}

1;
