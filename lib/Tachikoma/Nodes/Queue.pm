#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Queue
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Queue;
use strict;
use warnings;
use Tachikoma::Nodes::Buffer;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD IS_UNTHAWED
    TM_BYTESTREAM TM_STORABLE TM_INFO TM_PERSIST
    TM_COMMAND TM_RESPONSE TM_ERROR TM_EOF
);
use DBI;
use Data::Dumper;
use POSIX qw( strftime );
use Storable qw( nfreeze );
use parent qw( Tachikoma::Nodes::Buffer );

use version; our $VERSION = qv('v2.0.280');

my $CLEAR_INTERVAL  = 900;
my $DEFAULT_TIMEOUT = 900;
my $HOME            = Tachikoma->configuration->home || ( getpwuid $< )[7];
my $DB_DIR          = "$HOME/.tachikoma/queues";
my $HEARTBEAT_INTERVAL = 15;    # seconds
my %C                  = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    my $c     = { %{ $self->{interpreter}->commands } };
    $c->{$_} = $C{$_} for ( keys %C );
    $self->{interpreter}->commands($c);
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Queue <node name> <filename> [ <max_unanswered> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $filename, $max_unanswered ) =
            split q( ), $self->{arguments}, 2;
        die "ERROR: no filename specified\n" if ( not $filename );
        $self->is_active(undef);
        $self->msg_unanswered( {} );
        $self->close_db if ( $self->{dbh} );
        $self->filename($filename);
        $self->check_payload('true') if ( $filename =~ m{[.]q$} );
        $self->key_regex(undef);
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
    my $dbh            = $self->{dbh} // $self->dbh;
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
    my $sth = $dbh->prepare('SELECT count(1) FROM queue WHERE message_id=?');
    do {
        $message_id = $self->msg_counter;
        $sth->execute($message_id);
    } while ( $sth->fetchrow_arrayref->[0] );
    $copy->[TYPE] = $type | TM_PERSIST;
    $copy->[FROM] = $self->{name};
    $copy->[TO]   = $self->{owner};
    $copy->[ID]   = $message_id;
    my $has_payload = undef;
    my $message_key = $self->get_message_key($copy);
    if ( length $message_key ) {
        $sth =
            $dbh->prepare(
            'DELETE FROM queue WHERE message_key=? AND attempts=0');
        $sth->execute($message_key);
        $self->{cache}       = undef;
        $self->{buffer_size} = undef;
    }
    else {
        $self->get_buffer_size if ( not defined $self->{buffer_size} );
        $self->{buffer_size}++;
    }
    $sth = $dbh->prepare('INSERT INTO queue VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
    $sth->execute( $Tachikoma::Now + $self->{delay},
        0, $copy->[TYPE], $copy->[ID], $copy->[STREAM],
        $copy->[TIMESTAMP], $copy->[PAYLOAD], $message_key );
    $self->{buffer_fills}++;
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
    my $dbh            = $self->dbh;
    my $sth =
        $dbh->prepare('SELECT message_type FROM queue WHERE message_id=?');
    $sth->execute($message_id);
    my $row  = $sth->fetchrow_arrayref;
    my $type = $row ? $row->[0] : 0;
    $payload = 'cancel' if ( $payload eq 'answer' and $type & TM_ERROR );

    if ( $payload eq 'cancel' ) {
        my $buffer_size = $self->{buffer_size} // $self->get_buffer_size;
        $buffer_size-- if ( $buffer_size > 0 and $type );
        $self->{buffer_size} = $buffer_size;
        $sth = $dbh->prepare('DELETE FROM queue WHERE message_id=?');
        $sth->execute($message_id);
        delete $msg_unanswered->{$message_id};
        $self->send_event(
            {   'type'  => 'MSG_CANCELED',
                'key'   => $response->[STREAM],
                'value' => q(),
            }
        );
    }
    else {
        delete $msg_unanswered->{$message_id};
    }
    $self->{rsp_received}++;
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
        my $span = ( $Tachikoma::Right_Now - $msg_unanswered->{$key} ) * 1000;
        if ( $span > $timeout ) {
            delete $msg_unanswered->{$key};
            $self->{cache} = undef;
        }
        else {
            $self->set_timer( $timeout - $span )
                if ( $timeout - $span
                < ( $self->{timer_interval} // $HEARTBEAT_INTERVAL * 1000 ) );
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
                    if ( $self->{dbh} ) {
                        $self->{dbh}->disconnect;
                        unlink $self->filename or warn;
                        $self->dbh(undef);
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
        my %streams = ();
        my %keys    = ();
        my $sth     = $self->dbh->prepare(<<'EOF');
              SELECT message_id, message_stream, message_key
                FROM queue
            ORDER BY message_id
               LIMIT 1000
EOF
        $sth->execute;
        $cache = [];
        for my $row ( @{ $sth->fetchall_arrayref } ) {
            next
                if ( ( length $row->[1] and $streams{ $row->[1] } )
                or ( length $row->[2] and $keys{ $row->[2] } ) );
            push @{$cache}, $row->[0];
            $streams{ $row->[1] } = 1;
            $keys{ $row->[2] }    = 1;
        }
        $self->{cache} = $cache;
    }
    $self->{cache} = undef if ( not @{$cache} );
    return shift @{$cache};
}

sub refill {
    my $self           = shift;
    my $key            = shift;
    my $dbh            = $self->{dbh};
    my $msg_unanswered = $self->{msg_unanswered};
    my $max_attempts   = $self->{max_attempts} || $self->default_max_attempts;
    my $sth = $dbh->prepare('SELECT * FROM queue WHERE message_id=?');
    $sth->execute($key);
    my $value = $sth->fetchrow_arrayref;
    return if ( $msg_unanswered->{$key} or not defined $value );
    my ( $next_attempt, $attempts, $type, $id, $stream, $timestamp, $payload )
        = @{$value};
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
            $sth = $dbh->prepare('DELETE FROM queue WHERE message_id=?');
            $sth->execute($key);
            return;
        }
        else {
            $self->print_less_often(
                "$key has failed $attempts attempts - skipping");
            return;
        }
    }
    my $message = Tachikoma::Message->new;
    if ($type) {
        if ( $self->{is_active} ) {
            $sth = $dbh->prepare(<<'EOF');
                UPDATE queue SET next_attempt=?,
                                     attempts=?
                             WHERE message_id=?
EOF
            $sth->execute( $Tachikoma::Right_Now + $timeout,
                $attempts + 1, $key );
        }
        $message->[TYPE]      = $type;
        $message->[FROM]      = $self->{name};
        $message->[TO]        = $to;
        $message->[ID]        = $id;
        $message->[STREAM]    = $stream;
        $message->[TIMESTAMP] = $timestamp;
        $message->[PAYLOAD]   = $payload;
        $self->{pmsg_sent}++;
        $msg_unanswered->{$key} = $Tachikoma::Right_Now;
        $self->{sink}->fill($message);
        $self->send_event(
            {   'type'  => 'MSG_SENT',
                'key'   => $stream,
                'value' => $type & TM_BYTESTREAM ? $payload : q(),
            }
        );
    }
    else {
        $self->{buffer_size}-- if ( $self->{buffer_size} > 0 );
        delete $msg_unanswered->{$key};
        $sth = $dbh->prepare('DELETE FROM queue WHERE message_id=?');
        $sth->execute($key);
    }
    return;
}

sub get_message_key {
    my ( $self, $message ) = @_;
    my $message_key = undef;
    if ( $message->[TYPE] & TM_STORABLE ) {
        $Storable::canonical    = 1;
        $message->[PAYLOAD]     = nfreeze( $message->payload );
        $message->[IS_UNTHAWED] = 0;
    }
    if ( $self->{check_payload} ) {
        my $key_regex = $self->{key_regex};
        if ($key_regex) {
            $message_key = join q(:), $message->[PAYLOAD] =~ m{$key_regex}i;
        }
        else {
            $message_key = $message->[PAYLOAD];
        }
    }
    elsif ( $self->{check_stream} ) {
        my $key_regex = $self->{key_regex};
        if ($key_regex) {
            $message_key = join q(:), $message->[STREAM] =~ m{$key_regex}i;
        }
        else {
            $message_key = $message->[STREAM];
        }
    }
    return $message_key;
}

sub lookup {
    my ( $self, $key ) = @_;
    my $dbh   = $self->dbh;
    my $value = undef;
    if ( length $key ) {
        my $sth = $dbh->prepare(<<'EOF');
            SELECT next_attempt, attempts,
                   message_timestamp, message_stream, message_payload
              FROM queue WHERE message_id=?
EOF
        $sth->execute($key);
        $value = $sth->fetchrow_hashref;
    }
    else {
        $value = [];
        my $sth = $dbh->prepare(<<'EOF');
            SELECT message_id, next_attempt, attempts,
                   message_timestamp, message_stream, message_payload
              FROM queue
EOF
        $sth->execute;
        while ( my $row = $sth->fetchrow_hashref ) {
            push @{$value}, $row;
        }
    }
    return $value;
}

$C{list_messages} = sub {
    my $self           = shift;
    my $command        = shift;
    my $envelope       = shift;
    my $arguments      = $command->arguments;
    my $dbh            = $self->patron->dbh;
    my $msg_unanswered = $self->patron->msg_unanswered;
    my $list_all       = $arguments =~ m{a};
    my $verbose        = $arguments =~ m{v};
    my $buffer_size    = $self->patron->buffer_size || 0;
    my $response       = q();
    return if ( $list_all and $buffer_size > 10000 );
    my $hash;
    my $sth;

    if ($list_all) {
        $hash = {};
        $sth  = $dbh->prepare('SELECT message_id FROM queue');
        $sth->execute;
        $hash->{ $_->[0] } = undef for ( @{ $sth->fetchall_arrayref } );
    }
    else {
        $hash = $msg_unanswered;
    }
    if ($verbose) {
        my $output = [
            [ 'left', 'left',   'right',    'right', 'left' ],
            [ 'ID',   'STREAM', 'ATTEMPTS', 'AGE',   'NEXT ATTEMPT' ]
        ];
        $sth = $dbh->prepare(<<'EOF');
            SELECT next_attempt, attempts, message_timestamp, message_stream
              FROM queue WHERE message_id=?
EOF
        for my $key ( sort keys %{$hash} ) {
            $sth->execute($key);
            my ( $next_attempt, $attempts, $timestamp, $stream ) =
                @{ $sth->fetchrow_arrayref };
            push @{$output},
                [
                $key,
                $stream,
                $attempts,
                $Tachikoma::Now - $timestamp,
                $next_attempt
                ? strftime( '%F %T %Z', localtime $next_attempt )
                : 'N/A'
                ];
        }
        $response = $self->tabulate($output);
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
    my $dbh      = $self->patron->dbh;
    my $key      = $command->arguments;
    if ( $key eq q(*) ) {
        $dbh->disconnect;
        unlink $self->patron->filename or warn;
        $self->patron->msg_unanswered( {} );
        $self->patron->buffer_size(undef);
        $self->patron->dbh(undef);
        $self->{last_clear_time} = $Tachikoma::Now;
        return $self->okay($envelope);
    }
    else {
        delete $self->patron->msg_unanswered->{$key};
        my $sth =
            $dbh->prepare('SELECT count(1) FROM queue WHERE message_id=?');
        $sth->execute($key);
        if ( $sth->fetchrow_arrayref->[0] ) {
            $sth = $dbh->prepare('DELETE FROM queue WHERE message_id=?');
            $sth->execute($key);
            $self->patron->{buffer_size}--;
            return $self->okay($envelope);
        }
        else {
            return $self->error( $envelope,
                qq(couldn't find message: "$key"\n) );
        }
    }
};

$C{rm} = $C{remove_message};

$C{dump_message} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $key      = $command->arguments;
    my $dbh      = $self->patron->dbh;
    if ( $key eq q(*) ) {
        $key = each %{ $self->patron->msg_unanswered };
        if ( not $key ) {
            return $self->response( $envelope, "no messages in flight\n" );
        }
    }
    my $sth = $dbh->prepare('SELECT * FROM queue WHERE message_id=?');
    $sth->execute($key);
    my $value = $sth->fetchrow_arrayref;
    if ($value) {
        my ( $next_attempt, $attempts, $type, $id, $stream,
            $timestamp, $payload, $message_key )
            = @{$value};
        return $self->response(
            $envelope,
            Dumper(
                {   'next_attempt' =>
                        strftime( '%F %T %Z', localtime $next_attempt ),
                    'attempts' => $attempts,
                    'message'  => {
                        'type'      => $type,
                        'from'      => $self->patron->name,
                        'to'        => $self->patron->owner,
                        'id'        => $id,
                        'stream'    => $stream,
                        'timestamp' => $timestamp,
                        'payload'   => $payload,
                        'key'       => $message_key,
                    }
                }
            )
        );
    }
    else {
        return $self->error( $envelope, qq(can't find message "$key"\n) );
    }
};

$C{dump} = $C{dump_message};

$C{check_payload} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->check_payload(1);
    $self->patron->check_stream(undef);
    return $self->okay($envelope);
};

$C{check_stream} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->check_payload(undef);
    $self->patron->check_stream(1);
    return $self->okay($envelope);
};

$C{no_check} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->check_payload(undef);
    $self->patron->check_stream(undef);
    return $self->okay($envelope);
};

$C{set_key_regex} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->key_regex( $command->arguments );
    return $self->okay($envelope);
};

$C{kick} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $patron   = $self->patron;
    $patron->is_active(undef);
    $patron->msg_unanswered( {} );
    $patron->cache(undef);
    $patron->buffer_size(undef);
    $patron->fire;
    return $self->okay($envelope);
};

sub dump_config {
    my $self            = shift;
    my $response        = undef;
    my $settings        = undef;
    my $name            = $self->{name};
    my $filename        = $self->{filename};
    my $max_unanswered  = $self->{max_unanswered};
    my $max_attempts    = $self->{max_attempts};
    my $on_max_attempts = $self->{on_max_attempts};
    my $buffer_mode     = $self->{buffer_mode};
    my $last_buffer     = $self->{last_buffer};
    my $delay           = $self->{delay};
    my $timeout         = $self->{timeout};
    my $key_regex       = $self->{key_regex};
    my $check_payload   = $self->{check_payload};
    my $check_stream    = $self->{check_stream};
    $response = "make_node Queue $self->{name}";
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
    $settings .= "  set_key_regex $key_regex\n" if ($key_regex);
    $settings .= "  check_payload\n"            if ($check_payload);
    $settings .= "  check_stream\n"             if ($check_stream);
    $response .= "cd $self->{name}\n" . $settings . "cd ..\n" if ($settings);
    return $response;
}

sub get_buffer_size {
    my $self = shift;
    my $dbh  = $self->dbh;
    my $sth  = $dbh->prepare('SELECT count(1) FROM queue');
    $sth->execute;
    $self->{buffer_size} = $sth->fetchrow_arrayref->[0];
    return $self->{buffer_size};
}

sub dbh {
    my $self = shift;
    if (@_) {
        $self->{dbh} = shift;
    }
    if ( not defined $self->{dbh} ) {
        $self->make_dirs( $self->db_dir );
        my $path = $self->filename;
        if ( -e "${path}.clean" ) {
            unlink "${path}.clean" or warn;
        }
        else {
            ## no critic (RequireCheckedSyscalls)
            local %ENV = ();
            system "/bin/rm -f ${path}*";
        }
        my $dbh = DBI->connect("dbi:SQLite:dbname=$path");
        my $sth = $dbh->table_info( undef, undef, 'queue' );
        $sth->execute;
        if ( not @{ $sth->fetchall_arrayref } ) {
            $dbh->do(<<'EOF');
                CREATE TABLE queue (next_attempt INTEGER,
                                        attempts INTEGER,
                                    message_type INTEGER,
                                      message_id,
                                  message_stream,
                               message_timestamp,
                                 message_payload,
                                     message_key)
EOF
            $dbh->do('CREATE UNIQUE INDEX id_index ON queue (message_id)');
            $dbh->do('CREATE INDEX       key_index ON queue (message_key)')
                if ( $self->{check_payload} or $self->{check_stream} );
        }
        $dbh->do('PRAGMA synchronous = OFF');
        $dbh->do('PRAGMA journal_mode = TRUNCATE');
        $self->{dbh} = $dbh;
    }
    return $self->{dbh};
}

sub close_db {
    my $self = shift;
    $self->{dbh}->disconnect if ( $self->{dbh} );
    $self->{dbh} = undef;
    my $path = $self->filename;
    open my $fh, '>>', "${path}.clean" or warn;
    close $fh or warn;
    $self->buffer_size(undef);
    return;
}

sub check_payload {
    my $self = shift;
    if (@_) {
        $self->{check_payload} = shift;
    }
    return $self->{check_payload};
}

sub check_stream {
    my $self = shift;
    if (@_) {
        $self->{check_stream} = shift;
    }
    return $self->{check_stream};
}

sub key_regex {
    my $self = shift;
    if (@_) {
        $self->{key_regex} = shift;
    }
    return $self->{key_regex};
}

sub db_dir {
    my $self = shift;
    if (@_) {
        $DB_DIR = shift;
    }
    return $DB_DIR;
}

sub remove_node {
    my $self = shift;
    $self->close_db if ( $self->{filename} );
    $self->{filename} = undef;
    $self->SUPER::remove_node;
    return;
}

1;
