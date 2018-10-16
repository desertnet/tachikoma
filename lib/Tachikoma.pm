#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma
# ----------------------------------------------------------------------
#
# $Id: Tachikoma.pm 35268 2018-10-16 06:52:24Z chris $
#

package Tachikoma;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_PERSIST TM_RESPONSE TM_EOF
    VECTOR_SIZE
);
use Tachikoma::Config qw(
    %Tachikoma $ID %SSL_Config %Var $Wire_Version load_module include_conf
);
use Tachikoma::Crypto;
use Tachikoma::Nodes::Callback;
use Digest::MD5 qw( md5 );
use IO::Socket::SSL qw( SSL_VERIFY_PEER SSL_VERIFY_FAIL_IF_NO_PEER_CERT );
use POSIX qw( setsid dup2 F_SETFL O_NONBLOCK EAGAIN );
use Socket qw(
    PF_UNIX PF_INET SOCK_STREAM inet_aton pack_sockaddr_in pack_sockaddr_un
    SOL_SOCKET SO_SNDBUF SO_RCVBUF SO_SNDLOWAT SO_KEEPALIVE
);
use Time::HiRes qw( usleep );
use parent qw( Tachikoma::Node Tachikoma::Crypto );

use version; our $VERSION = qv('v2.0.101');

use constant {
    DEFAULT_PORT    => 4230,
    DEFAULT_TIMEOUT => 900,
    MAX_INT         => 2**32,
    BUFSIZ          => 262144,
};

my $COUNTER          = 0;
my $MY_PID           = 0;
my $LOG_FILE_HANDLE  = undef;
my $INIT_TIME        = time;
my @CONFIG_VARIABLES = qw( Log_File Log_Dir Pid_File Pid_Dir );

$Tachikoma::Max_Int           = MAX_INT;
$Tachikoma::Now               = undef;
$Tachikoma::Right_Now         = undef;
$Tachikoma::Event_Framework   = undef;
%Tachikoma::Nodes             = ();
$Tachikoma::Nodes_By_ID       = {};
$Tachikoma::Nodes_By_FD       = {};
$Tachikoma::Nodes_By_PID      = {};
@Tachikoma::Closing           = ();
@Tachikoma::Reconnect         = ();
$Tachikoma::Profiles          = undef;
@Tachikoma::Stack             = ();
@Tachikoma::Recent_Log        = ();
%Tachikoma::Recent_Log_Timers = ();
$Tachikoma::Shutting_Down     = undef;
$Tachikoma::Scheme            = 'rsa';
$Tachikoma::SSL_Ciphers       = q();
$Tachikoma::SSL_Version       = 'TLSv1';

sub unix_client {
    my $class    = shift;
    my $filename = shift;
    my $socket;
    srand;
    socket $socket, PF_UNIX, SOCK_STREAM, 0 or die "socket: $!";
    setsockopts($socket);
    connect $socket, pack_sockaddr_un($filename) or die "connect: $!\n";
    my $node = $class->new($socket);
    $node->reply_to_server_challenge;
    $node->auth_server_response;
    return $node;
}

sub inet_client {
    my $class    = shift;
    my $hostname = shift || 'localhost';
    my $port     = shift || DEFAULT_PORT;
    my $use_ssl  = shift;
    my $iaddr    = inet_aton($hostname) or die "ERROR: no host: $hostname\n";
    my $proto    = getprotobyname 'tcp';
    my $socket;
    srand;
    socket $socket, PF_INET, SOCK_STREAM, $proto or die "socket: $!";
    setsockopts($socket);
    connect $socket, pack_sockaddr_in( $port, $iaddr )
        or die "connect: $!\n";

    if ($use_ssl) {
        my $ssl_config = \%SSL_Config;
        die "ERROR: SSL not configured\n"
            if ( not $ssl_config->{SSL_client_ca_file} );
        my $ssl_socket = IO::Socket::SSL->start_SSL(
            $socket,
            SSL_key_file       => $ssl_config->{SSL_client_key_file},
            SSL_cert_file      => $ssl_config->{SSL_client_cert_file},
            SSL_ca_file        => $ssl_config->{SSL_client_ca_file},
            SSL_startHandshake => 1,
            SSL_use_cert       => 1,

            # SSL_cipher_list     => $SSL_Ciphers,
            SSL_version         => $Tachikoma::SSL_Version,
            SSL_verify_callback => sub {
                my $okay  = $_[0];
                my $error = $_[3];
                return 1 if ($okay);
                if ( $error eq 'error:0000000A:lib(0):func(0):DSA lib' ) {
                    print {*STDERR}
                        "WARNING: SSL certificate verification error: $error";
                    return 1;
                }
                print {*STDERR}
                    "ERROR: SSL certificate verification failed: $error";
                return 0;
            },
            SSL_verify_mode => $use_ssl eq 'noverify'
            ? 0
            : SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
        );
        if ( not $ssl_socket or not ref $ssl_socket ) {
            my $ssl_error = $IO::Socket::SSL::SSL_ERROR;
            $ssl_error =~ s{(error)(error)}{$1: $2};
            die join q(: ),
                q(ERROR: couldn't start_SSL),
                grep {$_} $!, $ssl_error;
        }
        $socket = $ssl_socket;
    }
    my $node = $class->new($socket);
    $node->hostname($hostname);
    $node->port($port);
    $node->use_SSL($use_ssl);
    $node->reply_to_server_challenge;
    $node->auth_server_response;
    return $node;
}

sub new {
    my $class        = shift;
    my $fh           = shift;
    my $input_buffer = q();
    my $self         = $class->SUPER::new;
    $self->{name}           = $$;
    $self->{fh}             = $fh;
    $self->{hostname}       = undef;
    $self->{port}           = undef;
    $self->{auth_challenge} = undef;
    $self->{auth_timestamp} = undef;
    $self->{input_buffer}   = \$input_buffer;
    $self->{persist}        = 'cancel';
    $self->{timeout}        = DEFAULT_TIMEOUT;
    bless $self, $class;
    return $self;
}

sub setsockopts {
    my $socket = shift;
    if ( $Tachikoma{Buffer_Size} ) {
        setsockopt $socket, SOL_SOCKET, SO_SNDBUF, $Tachikoma{Buffer_Size}
            or die "FAILED: setsockopt: $!";
    }
    if ( $Tachikoma{Buffer_Size} ) {
        setsockopt $socket, SOL_SOCKET, SO_RCVBUF, $Tachikoma{Buffer_Size}
            or die "FAILED: setsockopt: $!";
    }
    if ( $Tachikoma{Low_Water_Mark} ) {
        setsockopt $socket, SOL_SOCKET, SO_SNDLOWAT,
            $Tachikoma{Low_Water_Mark}
            or die "FAILED: setsockopt: $!";
    }
    if ( $Tachikoma{Keep_Alive} ) {
        setsockopt $socket, SOL_SOCKET, SO_KEEPALIVE, 1
            or die "FAILED: setsockopt: $!";
    }
    return;
}

sub reply_to_server_challenge {
    my $self = shift;
    my ( $got, $message ) = $self->read_block;
    return if ( not $message );
    my $version = $message->[ID];
    if ( not $version or $version ne $Wire_Version ) {
        die "ERROR: reply_to_server_challenge failed: version mismatch\n";
    }
    my $command = Tachikoma::Command->new( $message->[PAYLOAD] );
    if ( $command->{arguments} ne 'client' ) {
        die "ERROR: reply_to_server_challenge failed: wrong challenge type\n";
    }
    elsif ( length $ID ) {
        exit 1
            if (
            not $self->verify_signature( 'server', $message, $command ) );
    }
    $command->sign( $self->scheme, $message->timestamp );
    $message->payload( $command->packed );
    $self->{counter}++;
    $self->{auth_challenge} = rand;
    my $response =
        $self->command( 'challenge', 'server',
        md5( $self->{auth_challenge} ) );
    $response->[ID] = $Wire_Version;
    $self->{auth_timestamp} = $response->[TIMESTAMP];
    my $wrote = syswrite $self->{fh},
        ${ $message->packed } . ${ $response->packed };
    die "ERROR: reply_to_server_challenge couldn't write: $!\n"
        if ( not $wrote or $! );

    if ( $got > 0 ) {
        print {*STDERR}
            "WARNING: discarding $got excess bytes from server challenge.\n";
        my $new_buffer = q();
        $self->{input_buffer} = \$new_buffer;
    }
    return;
}

sub auth_server_response {
    my $self = shift;
    my ( $got, $message ) = $self->read_block;
    return if ( not $message or not $ID );
    my $command = Tachikoma::Command->new( $message->[PAYLOAD] );
    if ( $command->{arguments} ne 'server' ) {
        die "ERROR: auth_server_response failed: wrong challenge type\n";
    }
    elsif ( length $ID ) {
        exit 1
            if (
            not $self->verify_signature( 'server', $message, $command ) );
    }
    elsif ( $message->[TIMESTAMP] ne $self->{auth_timestamp} ) {
        die "ERROR: auth_server_response failed: incorrect timestamp\n";
    }
    elsif ( $command->{payload} ne md5( $self->{auth_challenge} ) ) {
        die "ERROR: auth_server_response failed: incorrect response\n";
    }
    $self->{counter}++;
    $self->{auth_challenge} = undef;
    return;
}

sub read_block {
    my $self = shift;
    my ( $buffer, $got, $read, $size ) = $self->read_buffer;
    if ( $size > BUFSIZ ) {
        my $caller = ( split m{::}, ( caller 1 )[3] )[-1];
        die "ERROR: $caller failed: size $size > BUFSIZ\n";
    }
    if ( $got >= $size and $size > 0 ) {
        my $message =
            Tachikoma::Message->new( \substr ${$buffer}, 0, $size, q() );
        $got -= $size;
        return ( $got, $message );
    }
    if ( not defined $read ) {
        my $caller = ( split m{::}, ( caller 1 )[3] )[-1];
        die "ERROR: $caller couldn't read: $!\n";
    }
    return;
}

sub drain {
    my $self    = shift;
    my $buffer  = $self->{input_buffer};
    my $fh      = $self->{fh} or return;
    my $timeout = $self->{timeout};
    my $got     = length ${$buffer};
    while ($fh) {
        my $read = undef;
        my $rv   = eval {
            local $SIG{ALRM} = sub { die "alarm\n" };    # NB: \n required
            alarm $timeout if ($timeout);
            $read = sysread $fh, ${$buffer}, BUFSIZ, $got;
            alarm 0 if ($timeout);
            return 1;
        };
        if ( not $rv ) {
            die $@ if ( $@ ne "alarm\n" );    # propagate unexpected errors
            $got = 0;                         # don't try to read messages
        }
        $got += $read if ( defined $read );

        # XXX:M
        # my $size =
        #     $got > VECTOR_SIZE
        #     ? VECTOR_SIZE + unpack 'N', ${$buffer}
        #     : 0;
        my $size = $got > VECTOR_SIZE ? unpack 'N', ${$buffer} : 0;
        while ( $got >= $size and $size > 0 ) {
            my $message =
                Tachikoma::Message->new( \substr ${$buffer}, 0, $size, q() );
            $got -= $size;

            # XXX:M
            # $size =
            #     $got > VECTOR_SIZE
            #     ? VECTOR_SIZE + unpack 'N', ${$buffer}
            #     : 0;
            $size = $got > VECTOR_SIZE ? unpack 'N', ${$buffer} : 0;
            $fh = undef if ( not defined $self->{sink}->fill($message) );
        }
        if ( not defined $read or $read < 1 ) {
            $self->close_filehandle;
            $fh = undef;
            my $message = Tachikoma::Message->new;
            $message->[TYPE] = TM_EOF;
            $self->{sink}->fill($message);
        }
    }
    return;
}

sub drain_cycle {
    my $self = shift;
    fcntl $self->{fh}, F_SETFL, O_NONBLOCK or die "fcntl: $!";
    my ( $buffer, $got, $read, $size ) = $self->read_buffer;
    fcntl $self->{fh}, F_SETFL, 0 or die "fcntl: $!";
    my $rv = 1;
    while ( $got >= $size and $size > 0 ) {
        my $message =
            Tachikoma::Message->new( \substr ${$buffer}, 0, $size, q() );
        $got -= $size;

        # XXX:M
        # $size =
        #     $got > VECTOR_SIZE
        #     ? VECTOR_SIZE + unpack 'N', ${$buffer}
        #     : 0;
        $size = $got > VECTOR_SIZE ? unpack 'N', ${$buffer} : 0;
        $self->{sink}->fill($message);
    }
    if ( ( not defined $read or $read < 1 ) and $! != EAGAIN ) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE] = TM_EOF;
        $self->{sink}->fill($message);
        $self->close_filehandle;
        $rv = undef;
    }
    return $rv;
}

sub read_buffer {
    my $self   = shift;
    my $buffer = $self->{input_buffer};
    my $fh     = $self->{fh} or return ( $buffer, 0, undef, 0 );
    my $got    = length ${$buffer};
    my $read   = undef;
    my $rv     = eval {
        local $SIG{ALRM} = sub { die "alarm\n" };    # NB: \n required
        alarm $self->{timeout} if ( $self->{timeout} );
        $read = sysread $fh, ${$buffer}, BUFSIZ, $got;
        alarm 0 if ( $self->{timeout} );
        return 1;
    };
    if ( not $rv ) {
        die $@ if ( $@ ne "alarm\n" );    # propagate unexpected errors
        $got = 0;                         # don't try to read messages
    }
    $got += $read if ( defined $read );

    # XXX:M
    # my $size =
    #     $got > VECTOR_SIZE
    #     ? VECTOR_SIZE + unpack 'N', ${$buffer}
    #     : 0;
    my $size = $got > VECTOR_SIZE ? unpack 'N', ${$buffer} : 0;
    return ( $buffer, $got, $read, $size );
}

sub fill {
    my $self        = shift;
    my $message     = shift;
    my $fh          = $self->{fh} or return;
    my $wrote       = 0;
    my $packed      = $message->packed;
    my $packed_size = length ${$packed};
    while ( $wrote < $packed_size ) {
        my $rv = syswrite $fh, ${$packed}, $packed_size - $wrote, $wrote;
        last if ( not $rv or $rv < 1 );
        $wrote += $rv;
    }
    die "ERROR: wrote $wrote < $packed_size\n"
        if ( $wrote and $wrote != $packed_size );
    die "ERROR: couldn't write: $!\n" if ($!);
    return $wrote;
}

sub reconnect {
    my $self      = shift;
    my $hostname  = shift;
    my $port      = shift;
    my $use_ssl   = shift;
    my $tachikoma = undef;
    $hostname = $self->hostname if ( not $hostname and ref $self );
    $port     = $self->port     if ( not $port     and ref $self );
    $use_ssl  = $self->use_SSL  if ( not $use_ssl  and ref $self );
    do {
        $tachikoma =
            eval { Tachikoma->inet_client( $hostname, $port, $use_ssl ) };
        if ( not $tachikoma ) {
            print {*STDERR} $@;
            sleep 1;
        }
    } while ( not $tachikoma );
    if ( ref $self ) {
        $tachikoma->persist( $self->{persist} );
        $tachikoma->timeout( $self->{timeout} );
    }
    return $tachikoma;
}

sub answer {
    my ( $self, $message ) = @_;
    return if ( not $message->[TYPE] & TM_PERSIST );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_PERSIST | TM_RESPONSE;
    $response->[TO]      = $message->[FROM] or return;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = 'answer';
    return $self->fill($response);
}

sub cancel {
    my ( $self, $message, $to ) = @_;
    return if ( not $message->[TYPE] & TM_PERSIST );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_PERSIST | TM_RESPONSE;
    $response->[TO]      = ( $to // $message->[FROM] ) or return;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = 'cancel';
    return $self->fill($response);
}

sub close_filehandle {
    my $self = shift;
    if ( $self->{fh} ) {
        close $self->{fh} or die $!;
    }
    $self->{fh} = undef;
    return;
}

sub initialize {
    my $self      = shift;
    my $name      = shift;
    my $daemonize = shift;
    $0 = $name if ($name);    ## no critic (RequireLocalizedPunctuationVars)
    srand;
    $self->copy_variables;
    $self->check_pid;
    $self->daemonize if ($daemonize);
    $self->reset_signal_handlers;
    $self->open_log_file;
    $self->write_pid;
    $self->load_event_framework;
    return;
}

sub copy_variables {
    my $self = shift;
    for my $name (@CONFIG_VARIABLES) {
        my $value = undef;
        if ( ref $Var{$name} ) {
            $value = join q(), @{ $Var{$name} };
        }
        else {
            $value = $Var{$name};
        }
        $Tachikoma{$name} = $value if ( length $value );
    }
    return;
}

sub check_pid {
    my $self    = shift;
    my $old_pid = $self->get_pid;
    if ( $old_pid and kill 0, $old_pid ) {
        print {*STDERR} "ERROR: $0 already running as pid $old_pid\n";
        exit 3;
    }
    return;
}

sub daemonize {    # from perlipc manpage
    my $self = shift;
    open STDIN, '<', '/dev/null' or die "ERROR: couldn't read /dev/null: $!";
    open STDOUT, '>', '/dev/null'
        or die "ERROR: couldn't write /dev/null: $!";
    defined( my $pid = fork ) or die "ERROR: couldn't fork: $!";
    exit 0 if ($pid);
    setsid() or die "ERROR: couldn't start session: $!";
    open STDERR, '>&', STDOUT or die "ERROR: couldn't dup STDOUT: $!";
    return;
}

sub reset_signal_handlers {
    my $self = shift;
    ## no critic (RequireLocalizedPunctuationVars)
    $SIG{TERM} = 'IGNORE';
    $SIG{INT}  = 'IGNORE';
    $SIG{PIPE} = 'IGNORE';
    $SIG{HUP}  = 'IGNORE';
    $SIG{USR1} = 'IGNORE';
    return;
}

sub open_log_file {
    my $self = shift;
    my $log = $self->log_file or die "ERROR: no log file specified\n";
    chdir q(/) or die "ERROR: couldn't chdir /: $!";
    open $LOG_FILE_HANDLE, '>>', $log
        or die "ERROR: couldn't open log file $log: $!\n";
    $LOG_FILE_HANDLE->autoflush(1);
    ## no critic (ProhibitTies)
    tie *STDOUT, 'Tachikoma', $self or die "ERROR: couldn't tie STDOUT: $!";
    tie *STDERR, 'Tachikoma', $self or die "ERROR: couldn't tie STDERR: $!";
    ## use critic
    return 'success';
}

sub write_pid {
    my $self = shift;
    my $file = $self->pid_file or die "ERROR: no pid file specified\n";
    $MY_PID = $$;
    open my $fh, '>', $file or die "ERROR: couldn't open pid file $file: $!";
    print {$fh} "$MY_PID\n";
    close $fh or die $!;
    return;
}

sub load_event_framework {
    my $self      = shift;
    my $framework = eval {
        my $module = 'Tachikoma::EventFrameworks::KQueue';
        load_module($module);
        return $module->new;
    };
    if ( not $framework ) {
        $framework = eval {
            my $module = 'Tachikoma::EventFrameworks::Epoll';
            load_module($module);
            return $module->new;
        };
    }
    if ( not $framework ) {
        $framework = eval {
            my $module = 'Tachikoma::EventFrameworks::Select';
            load_module($module);
            return $module->new;
        };
        die "ERROR: $@" if ( not $framework );
    }
    $self->event_framework($framework);
    return;
}

sub touch_log_file {
    my $self = shift;
    my $log = $self->log_file or die "ERROR: no log file specified\n";
    $self->close_log_file;
    $self->open_log_file;
    utime $Tachikoma::Now, $Tachikoma::Now, $log
        or die "ERROR: couldn't utime $log: $!";
    return;
}

sub close_log_file {
    my $self = shift;
    untie *STDOUT;
    untie *STDERR;
    close $LOG_FILE_HANDLE or die $!;
    return;
}

sub reload_config {
    my $self   = shift;
    my $config = $Tachikoma{Config};
    include_conf($config);
    $Tachikoma{Config} = $config;
    return;
}

sub get_pid {
    my $self = shift;
    my $name = shift;
    my $file = $self->pid_file($name);
    my $pid;
    return if ( not $file or not -f $file );
    open my $fh, '<', $file or die "ERROR: couldn't open pid file $file: $!";
    $pid = <$fh>;
    close $fh or die $!;
    chomp $pid if ($pid);
    return $pid;
}

sub remove_pid {
    my $self = shift;
    my $file = $self->pid_file or die "ERROR: no pid file specified\n";
    if ( $file and $$ == $MY_PID ) {
        unlink $file or die $!;
    }
    return;
}

sub pid_file {
    my $self     = shift;
    my $name     = shift // $0;
    my $pid_file = undef;
    if ( $Tachikoma{Pid_File} ) {
        $pid_file = $Tachikoma{Pid_File};
    }
    elsif ( $Tachikoma{Pid_Dir} ) {
        $pid_file = join q(), $Tachikoma{Pid_Dir}, q(/), $name, '.pid';
    }
    else {
        die "ERROR: couldn't determine pid_file\n";
    }
    return $pid_file;
}

sub log_file {
    my $self     = shift;
    my $name     = $0;
    my $log_file = undef;
    if ( $Tachikoma{Log_File} ) {
        $log_file = $Tachikoma{Log_File};
    }
    elsif ( $Tachikoma{Log_Dir} ) {
        $log_file = join q(), $Tachikoma{Log_Dir}, q(/), $name, '.log';
    }
    else {
        die "ERROR: couldn't determine log_file\n";
    }
    return $log_file;
}

sub callback {
    my $self = shift;
    if (@_) {
        my $node = $self->{callback};
        if ( not $node ) {
            $node             = Tachikoma::Nodes::Callback->new;
            $self->{callback} = $node;
            $self->{sink}     = $node;
        }
        $node->{callback} = shift;
    }
    return $self->{callback};
}

sub fh {
    my $self = shift;
    if (@_) {
        $self->{fh} = shift;
    }
    return $self->{fh};
}

sub hostname {
    my $self = shift;
    if (@_) {
        $self->{hostname} = shift;
    }
    return $self->{hostname};
}

sub port {
    my $self = shift;
    if (@_) {
        $self->{port} = shift;
    }
    return $self->{port};
}

sub use_SSL {
    my $self = shift;
    if (@_) {
        $self->{use_SSL} = shift;
    }
    return $self->{use_SSL};
}

sub auth_challenge {
    my $self = shift;
    if (@_) {
        $self->{auth_challenge} = shift;
    }
    return $self->{auth_challenge};
}

sub auth_timestamp {
    my $self = shift;
    if (@_) {
        $self->{auth_timestamp} = shift;
    }
    return $self->{auth_timestamp};
}

sub input_buffer {
    my $self = shift;
    if (@_) {
        $self->{input_buffer} = shift;
    }
    return $self->{input_buffer};
}

sub persist {
    my $self = shift;
    if (@_) {
        $self->{persist} = shift;
    }
    return $self->{persist};
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

sub counter {
    my $self = shift;
    if (@_) {
        $COUNTER = shift;
    }
    $COUNTER = ( $COUNTER + 1 ) % $Tachikoma::Max_Int;
    return $COUNTER;
}

sub my_pid {
    return $MY_PID;
}

sub init_time {
    return $INIT_TIME;
}

sub now {
    return $Tachikoma::Now;
}

sub right_now {
    return $Tachikoma::Right_Now;
}

sub event_framework {
    my $self = shift;
    if (@_) {
        $Tachikoma::Event_Framework = shift;
    }
    return $Tachikoma::Event_Framework;
}

sub nodes {
    return \%Tachikoma::Nodes;
}

sub nodes_by_id {
    return $Tachikoma::Nodes_By_ID;
}

sub nodes_by_fd {
    return $Tachikoma::Nodes_By_FD;
}

sub nodes_by_pid {
    return $Tachikoma::Nodes_By_PID;
}

sub closing {
    my $self = shift;
    if (@_) {
        my $closing = shift;
        @Tachikoma::Closing = @{$closing};
    }
    return \@Tachikoma::Closing;
}

sub nodes_to_reconnect {
    return \@Tachikoma::Reconnect;
}

sub profiles {
    my $self = shift;
    if (@_) {
        $Tachikoma::Profiles = shift;
    }
    return $Tachikoma::Profiles;
}

sub stack {
    my $self = shift;
    if (@_) {
        my $stack = shift;
        @Tachikoma::Stack = @{$stack};
    }
    return \@Tachikoma::Stack;
}

sub recent_log {
    return \@Tachikoma::Recent_Log;
}

sub recent_log_timers {
    return \%Tachikoma::Recent_Log_Timers;
}

sub shutting_down {
    my $self = shift;
    if (@_) {
        $Tachikoma::Shutting_Down = shift;
    }
    return $Tachikoma::Shutting_Down;
}

sub TIEHANDLE {
    my $class  = shift;
    my $self   = shift;
    my $scalar = \$self;
    return bless $scalar, $class;
}

sub OPEN {
    my $self = shift;
    my $path = shift;
    return $self;
}

sub FILENO {
    my $self = shift;
    return fileno $LOG_FILE_HANDLE;
}

sub WRITE {
    my ( $self, $buf, $length, $offset ) = @_;
    $length //= 0;
    $offset //= 0;
    return syswrite $LOG_FILE_HANDLE, $buf, $length, $offset;
}

sub PRINT {
    my ( $self, @args ) = @_;
    my @msg = grep { defined and $_ ne q() } @args;
    return if ( not @msg );
    push @Tachikoma::Recent_Log, @msg;
    shift @Tachikoma::Recent_Log while ( @Tachikoma::Recent_Log > 100 );
    return print {$LOG_FILE_HANDLE} @msg;
}

sub PRINTF {
    my ( $self, $fmt, @args ) = @_;
    my @msg = grep { defined and $_ ne q() } @args;
    return if ( not @msg );
    push @Tachikoma::Recent_Log, sprintf $fmt, @msg;
    shift @Tachikoma::Recent_Log while ( @Tachikoma::Recent_Log > 100 );
    return print {$LOG_FILE_HANDLE} sprintf $fmt, @msg;
}

1;

__END__

=head1 NAME

Tachikoma.pm - Synchronous interface for Tachikoma

=head1 SYNOPSIS

 #!/usr/bin/perl
 use strict;
 use warnings;
 use Tachikoma;
 use Tachikoma::Message qw( TM_BYTESTREAM );
 require '/usr/local/etc/tachikoma.conf';
 my $tachikoma = Tachikoma->inet_client;
 my $message = Tachikoma::Message->new;
 $message->type(TM_BYTESTREAM);
 $message->to("echo");
 $message->payload("hello, world\n");
 $tachikoma->fill($message);
 $tachikoma->callback(sub {
     my $message = shift;
     print $message->payload;
     return;
 });
 $tachikoma->drain;

=head1 DESCRIPTION

This interface is useful in writing simple clients, as well as well as designing jobs to run as "islands of serialization in a sea of concurrency".

=head1 CLASS CONSTRUCTOR METHODS

=head2 unix_client( $filename )

Opens a connection to a tachikoma server located on the same physical machine, using the specified unix domain socket.  Returns a node object that you can L</fill()> and L</drain()>.

=head2 inet_client( $hostname, $port )

Opens a TCP/IP connection to a tachikoma server using the specified hostname and port.  Returns a node object that you can L</fill()> and L</drain()>.

=head1 INSTANCE METHODS

=head2 fill( $message )

Sends a message to the server.  Returns the number of bytes written or undef.

=head2 callback( $new_callback )

Sets the callback to receive messages during L</drain()>.  Your callback should return undef when you want L</drain()> to stop.

=head2 timeout( $new_timeout )

Sets the timeout used by L</drain()> to set an alarm() before blocking on sysread().  L</drain()> will stop if the alarm fires.  The default timeout is 15 minutes.

=head2 drain()

Drains the socket of messages using sysread() until sysread() or your callback returns undef or until the alarm fires (see L</timeout()>).

=head1 DETAILS

The L</callback()> method actually creates a L<Tachikoma::Nodes::Callback> node and sets it as the L</sink()>.  As an alternative to using a callback, you can set this sink yourself to any node you like.  However, this node must still return undef when you want to stop L</drain()>.

Be aware that return values are not consistent across all nodes.  Most nodes were designed for the asynchronous interface, which pays no heed to return values.

Some return whatever their own sink's L</fill()> method returned--in which case you can probably get away with putting your own node at the end.  A L<Tachikoma::Nodes::Callback> node might be a good choice for the end point.

Unfortunately, other nodes may simply return undef regardless of the situation, or worse, always return a true value.  It's important to check the code for any nodes you plan to employ and make sure you understand how they will interact.

=head1 CLASS CONVENIENCE METHODS

counter()

closing()

profiles()

stack()

my_pid()

init_time()

event_framework()

nodes()

nodes_by_id()

nodes_by_fd()

nodes_by_pid()

nodes_to_reconnect()

shutting_down()

scheme()

=head1 DEPENDENCIES

L<Tachikoma::Node>

L<Tachikoma::Message>

L<Tachikoma::Config>

L<Tachikoma::Crypto>

L<Tachikoma::Nodes::Callback>

L<Digest::MD5>

L<IO::Socket::SSL>

L<Socket>

L<POSIX>

=head1 SEE ALSO

unix(4)

socket(2)

=head1 AUTHOR

Christopher Reaume C<< <chris@desert.net> >>

=head1 COPYRIGHT

Copyright (c) 2018 DesertNet
