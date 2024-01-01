#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma
# ----------------------------------------------------------------------
#

package Tachikoma;
use strict;
use warnings;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_PERSIST TM_RESPONSE
);
use Tachikoma::Config qw( load_module include_conf );
use POSIX qw( setsid );

use version; our $VERSION = qv('v2.0.101');

use constant {
    DEFAULT_PORT    => 4230,
    DEFAULT_TIMEOUT => 900,
    MAX_INT         => 2**32,
    BUFSIZ          => 262144,
    TK_R            => 000001,    #    1
    TK_W            => 000002,    #    2
    TK_SYNC         => 000004,    #    4
};

$Tachikoma::Max_Int         = MAX_INT;
$Tachikoma::Now             = undef;
$Tachikoma::Right_Now       = undef;
$Tachikoma::Event_Framework = undef;
%Tachikoma::Nodes           = ();
$Tachikoma::Nodes_By_ID     = {};
$Tachikoma::Nodes_By_FD     = {};
@Tachikoma::Closing         = ();

my $COUNTER            = 0;
my $MY_PID             = 0;
my $LOG_FILE_HANDLE    = undef;
my $INIT_TIME          = time;
my @NODES_TO_RECONNECT = ();
my @RECENT_LOG         = ();
my %RECENT_LOG_TIMERS  = ();
my $SHUTTING_DOWN      = undef;

sub inet_client {
    my ( $class, $host, $port, $use_SSL ) = @_;
    $host ||= 'localhost';
    $port ||= DEFAULT_PORT;
    require Tachikoma::EventFrameworks::Select;
    require Tachikoma::Nodes::Router;
    require Tachikoma::Nodes::Callback;
    require Tachikoma::Nodes::Socket;
    my $self = $class->new;
    $self->{this_framework} = Tachikoma::EventFrameworks::Select->new;
    $self->set_framework;
    $self->{sink} = Tachikoma::Nodes::Router->new;
    $self->{connector} =
        Tachikoma::Nodes::Socket->inet_client( $host, $port, TK_SYNC,
        $use_SSL );
    $self->{connector}->{sink} = $self->{sink};
    $self->restore_framework;
    return $self;
}

sub new {
    my $class = shift;
    my $self  = {
        parent_framework => undef,
        this_framework   => undef,
        connector        => undef,
        sink             => undef,
        fh               => undef,
        timeout          => DEFAULT_TIMEOUT,
    };
    bless $self, $class;
    return $self;
}

sub callback {
    my ( $self, $callback ) = @_;
    my $node = Tachikoma::Nodes::Callback->new(
        sub {
            my $message = shift;
            $message->[FROM] =~ s{^\d+/}{};
            $self->{sink}->stop if ( not &{$callback}($message) );
            return;
        }
    );
    $self->{connector}->sink($node);
    return;
}

sub drain {
    my ($self) = @_;
    alarm $self->{timeout} if ( $self->{timeout} );
    local $SIG{ALRM} = sub { die "TIMEOUT\n" };
    $self->set_framework;
    $self->{sink}->drain;
    $self->restore_framework;
    alarm 0 if ( $self->{timeout} );
    return;
}

sub fill {
    my ( $self, $message ) = @_;
    $self->set_framework;
    $self->{connector}->fill($message);
    $self->restore_framework;
    return;
}

sub command {
    my ( $self, $name, $arguments ) = @_;
    return $self->{connector}->command( $name, $arguments );
}

sub close_filehandle {
    my ($self) = @_;
    $self->{sink}->remove_node;
    $self->{sink} = undef;
    $self->{connector}->remove_node;
    $self->{connector} = undef;
    $self->{fh}        = undef;
    return;
}

sub set_framework {
    my ($self) = @_;
    $self->{parent_framework}   = $Tachikoma::Event_Framework;
    $Tachikoma::Event_Framework = $self->{this_framework};
    $self->{fh}                 = $self->{connector}->{fh};
    return;
}

sub restore_framework {
    my ($self) = @_;
    $Tachikoma::Event_Framework = $self->{parent_framework};
    $self->{fh} = $self->{connector}->{fh};
    return;
}

sub parent_framework {
    my $self = shift;
    if (@_) {
        $self->{parent_framework} = shift;
    }
    return $self->{parent_framework};
}

sub this_framework {
    my $self = shift;
    if (@_) {
        $self->{this_framework} = shift;
    }
    return $self->{this_framework};
}

sub connector {
    my $self = shift;
    if (@_) {
        $self->{connector} = shift;
    }
    return $self->{connector};
}

sub sink {
    my $self = shift;
    if (@_) {
        $self->{sink} = shift;
    }
    return $self->{sink};
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
    $self->{connector}->fill($response);
    return;
}

sub cancel {
    my ( $self, $message ) = @_;
    return if ( not $message->[TYPE] & TM_PERSIST );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_PERSIST | TM_RESPONSE;
    $response->[TO]      = $message->[FROM] or return;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = 'cancel';
    $self->{connector}->fill($response);
    return;
}

sub fh {
    my ( $self, @args ) = shift;
    return $self->{connector}->fh(@args);
}

sub debug_state {
    my $self = shift;
    if (@_) {
        $self->{debug_state} = shift;
        $self->{connector}->debug_state( $self->{debug_state} );
        $self->{sink}->debug_state( $self->{debug_state} );
    }
    return $self->{debug_state};
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

################
# class methods
################

sub initialize {
    my $self      = shift;
    my $name      = shift;
    my $daemonize = shift;
    $0 = $name if ($name);    ## no critic (RequireLocalizedPunctuationVars)
    srand;
    $self->check_pid;
    if ($daemonize) {
        $self->daemonize;
        $self->open_log_file;
    }
    $self->reset_signal_handlers;
    $self->write_pid;
    $self->load_event_framework;
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
    open STDIN,  '<', '/dev/null' or die "ERROR: couldn't read /dev/null: $!";
    open STDOUT, '>', '/dev/null'
        or die "ERROR: couldn't write /dev/null: $!";
    defined( my $pid = fork ) or die "ERROR: couldn't fork: $!";
    exit 0 if ($pid);
    setsid() or die "ERROR: couldn't start session: $!";
    open STDERR, '>&', STDOUT or die "ERROR: couldn't dup STDOUT: $!";
    return;
}

sub open_log_file {
    my $self = shift;
    my $log  = $self->log_file or die "ERROR: no log file specified\n";
    if ( not defined $LOG_FILE_HANDLE ) {
        chdir q(/) or die "ERROR: couldn't chdir /: $!";
        open $LOG_FILE_HANDLE, '>>', $log
            or die "ERROR: couldn't open log file $log: $!\n";
        $LOG_FILE_HANDLE->autoflush(1);
    }
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
    my $log  = $self->log_file or die "ERROR: no log file specified\n";
    if ( defined $LOG_FILE_HANDLE ) {
        $self->close_log_file;
        $self->open_log_file;
        utime $Tachikoma::Now, $Tachikoma::Now, $log
            or die "ERROR: couldn't utime $log: $!";
    }
    return;
}

sub close_log_file {
    my $self = shift;
    close $LOG_FILE_HANDLE or die $!;
    undef $LOG_FILE_HANDLE;
    return;
}

sub reload_config {
    my $self   = shift;
    my $config = Tachikoma->configuration;
    $config->load_config_file( $config->config_file );
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
    my $config   = Tachikoma->configuration;
    if ( $config->pid_file ) {
        $pid_file = $config->pid_file;
    }
    elsif ( $config->pid_dir ) {
        $pid_file = join q(), $config->pid_dir, q(/), $name, '.pid';
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
    my $config   = Tachikoma->configuration;
    if ( $config->log_file ) {
        $log_file = $config->log_file;
    }
    elsif ( $config->log_dir ) {
        $log_file = join q(), $config->log_dir, q(/), $name, '.log';
    }
    else {
        die "ERROR: couldn't determine log_file\n";
    }
    return $log_file;
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

sub closing {
    my $self = shift;
    if (@_) {
        my $closing = shift;
        @Tachikoma::Closing = @{$closing};
    }
    return \@Tachikoma::Closing;
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

sub nodes_to_reconnect {
    return \@NODES_TO_RECONNECT;
}

sub recent_log {
    return \@RECENT_LOG;
}

sub recent_log_timers {
    return \%RECENT_LOG_TIMERS;
}

sub shutting_down {
    my $self = shift;
    if (@_) {
        $SHUTTING_DOWN = shift;
    }
    return $SHUTTING_DOWN;
}

sub shutdown_all_nodes {
    my ( $self, @args ) = @_;
    return Tachikoma::Node->shutdown_all_nodes(@args);
}

sub print_least_often {
    my ( $self, @args ) = @_;
    return Tachikoma::Node->print_less_often(@args);
}

sub print_less_often {
    my ( $self, @args ) = @_;
    return Tachikoma::Node->print_less_often(@args);
}

sub stderr {
    my ( $self, @args ) = @_;
    my $msg = join q(), grep { defined and $_ ne q() } @args;
    return if ( not length $msg );
    my $rv = undef;
    push @RECENT_LOG, $msg;
    shift @RECENT_LOG while ( @RECENT_LOG > 100 );
    if ( defined *STDERR ) {
        $rv = print {*STDERR} $msg;
    }
    if ( defined $LOG_FILE_HANDLE ) {
        $rv = print {$LOG_FILE_HANDLE} $msg;
    }
    return $rv;
}

sub log_prefix {
    my ( $self, @args ) = @_;
    return Tachikoma::Node->log_prefix(@args);
}

sub configuration {
    my $self = shift;
    return Tachikoma::Config->global;
}

sub scheme {
    my ( $self, @args ) = @_;
    return $self->configuration->scheme(@args);
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

event_framework()

nodes()

configuration()

counter()

my_pid()

init_time()

shutting_down()

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

Copyright (c) 2024 DesertNet
