#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::BufferMonitor
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::BufferMonitor;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw(
    TYPE FROM TO TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_COMMAND TM_INFO TM_EOF
);
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.368');

my $TIMER_INTERVAL = 60;
my $ALERT_DELAY    = 600;
my $ALERT_INTERVAL = 3600;
my $EMAIL_AFTER    = 300;
my $TRAP_AFTER     = 1200;
my %C              = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{buffers}          = {};
    $self->{email_thresholds} = {};
    $self->{trap_thresholds}  = {};
    $self->{hosts}            = {};
    $self->{email_alerts}     = {};
    $self->{trap_alerts}      = {};
    $self->{email}            = q();
    $self->{trap}             = q();
    $self->{last_email}       = 0;
    $self->{last_trap}        = 0;
    $self->{email_address}    = q();
    $self->{mon_path}         = q();
    $self->{interpreter}      = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments}    = shift;
        $self->{buffers}      = {};
        $self->{email_alerts} = {};
        $self->{trap_alerts}  = {};
        $self->set_timer( $TIMER_INTERVAL * 1000 );
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_COMMAND or $message->[TYPE] & TM_EOF ) {
        return $self->interpreter->fill($message);
    }

    # if ( $message->[TYPE] & TM_INFO ) {
    #     $self->arguments( $self->arguments );
    #     return;
    # }
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my $thresholds  = $self->{email_thresholds};
    my $buffers     = $self->{buffers};
    my $new_buffers = {};
LINE: for my $line ( split m{^}, $message->[PAYLOAD] ) {
        next if ( $line !~ m{^hostname:} );
        my $buffer     = { map { split m{:}, $_, 2 } split q( ), $line };
        my $buffer_id  = join q(:), $buffer->{hostname}, $buffer->{buff_name};
        my $old_buffer = $buffers->{$buffer_id};
        $new_buffers->{$buffer_id} = $buffer;
        $buffer->{id}              = $buffer_id;
        $buffer->{last_update}     = $Tachikoma::Right_Now;
        $buffer->{last_timestamp}  = $message->[TIMESTAMP];
        $buffer->{last_email}      = $old_buffer->{last_email} || 0;
        $buffer->{last_trap}       = $old_buffer->{last_trap}  || 0;
        $buffer->{lag}             = sprintf '%.1f',
            $buffer->{last_update} - $buffer->{last_timestamp};
        $buffer->{age} ||= 0;

        if ( $buffer->{lag} > $EMAIL_AFTER ) {
            $self->alert(
                'email', $buffer,
                "lag exceeded $EMAIL_AFTER seconds",
                sub { $_[0]->{lag} > $EMAIL_AFTER }
            );
            next LINE;
        }
        for my $regex ( keys %{$thresholds} ) {
            next if ( $buffer_id !~ m{$regex} );
            my $threshold = $thresholds->{$regex};
            if ( $buffer->{msg_in_buf} > $threshold ) {
                $self->alert(
                    'email', $buffer,
                    "msg_in_buf exceeded $threshold",
                    sub { $_[0]->{msg_in_buf} > $threshold }
                );
            }
            next LINE;
        }
        if ( $buffer->{msg_in_buf} > 1000 ) {
            $self->alert(
                'email', $buffer,
                'msg_in_buf exceeded 1000',
                sub { $_[0]->{msg_in_buf} > 1000 }
            );
            next LINE;
        }
    }
    $thresholds = $self->{trap_thresholds};
AGAIN: for my $buffer_id ( keys %{$new_buffers} ) {
        my $buffer = $new_buffers->{$buffer_id};
        $buffers->{$buffer_id} = $buffer;
        if ( $buffer->{lag} > $TRAP_AFTER ) {
            $self->alert(
                'trap', $buffer,
                "lag exceeded $TRAP_AFTER seconds",
                sub { $_[0]->{lag} > $TRAP_AFTER }
            );
            next AGAIN;
        }
        for my $regex ( keys %{$thresholds} ) {
            next if ( $buffer_id !~ m{$regex} );
            my $threshold = $thresholds->{$regex};
            if ( $buffer->{msg_in_buf} > $threshold ) {
                $self->alert(
                    'trap', $buffer,
                    "msg_in_buf exceeded $threshold",
                    sub { $_[0]->{msg_in_buf} > $threshold }
                );
            }
            next AGAIN;
        }
        if ( $buffer->{msg_in_buf} > 100000 ) {
            $self->alert(
                'trap', $buffer,
                'msg_in_buf exceeded 100000',
                sub { $_[0]->{msg_in_buf} > 100000 }
            );
            next AGAIN;
        }
    }
    return $self->cancel($message);
}

sub fire {
    my $self    = shift;
    my $buffers = $self->{buffers};
    for my $buffer_id ( keys %{$buffers} ) {
        my $buffer = $buffers->{$buffer_id};
        $buffer->{age} = $Tachikoma::Right_Now - $buffer->{last_update};
        if ( $buffer->{age} > $EMAIL_AFTER ) {
            $self->alert(
                'email', $buffer,
                "age exceeded $EMAIL_AFTER seconds",
                sub { $_[0]->{age} > $EMAIL_AFTER }
            );
        }
        if ( $buffer->{age} > $TRAP_AFTER ) {
            $self->alert(
                'trap', $buffer,
                "age exceeded $TRAP_AFTER seconds",
                sub { $_[0]->{age} > $TRAP_AFTER }
            );
        }
    }
    $self->get_alerts('email');
    $self->send_alerts('email');
    $self->get_alerts('trap');
    $self->send_alerts('trap');
    return;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: set_email_address <address>\n"
            . "          list_email_thresholds\n"
            . "          add_email_threshold <regex> <threshold>\n"
            . "          remove_email_threshold <regex>\n" . "\n"
            . "          set_mon_path  <address>\n"
            . "          list_trap_thresholds\n"
            . "          add_trap_threshold  <regex> <threshold>\n"
            . "          remove_trap_threshold  <regex>\n" );
};

$C{set_email_address} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->email_address( $command->arguments );
    return $self->okay;
};

$C{list_email_thresholds} = sub {
    my $self       = shift;
    my $command    = shift;
    my $envelope   = shift;
    my $glob       = $command->arguments;
    my $thresholds = $self->patron->email_thresholds;
    my $response   = q();
    for my $path ( sort keys %{$thresholds} ) {
        $response .= sprintf "%30s %10d\n", $path, $thresholds->{$path}
            if ( not $glob or $path =~ m{$glob} );
    }
    return $self->response( $envelope, $response );
};

$C{ls} = $C{list_email_thresholds};

$C{add_email_threshold} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $regex, $threshold ) = split q( ), $command->arguments, 2;
    $self->patron->email_thresholds->{$regex} = $threshold;
    $self->patron->email_alerts( {} );
    return $self->okay;
};

$C{add} = $C{add_email_threshold};

$C{remove_email_threshold} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $patron   = $self->patron;
    delete $patron->email_thresholds->{ $command->arguments };
    $self->patron->email_alerts( {} );
    return $self->okay;
};

$C{rm} = $C{remove_email_threshold};

$C{set_mon_path} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->mon_path( $command->arguments );
    return $self->okay;
};

$C{list_trap_thresholds} = sub {
    my $self       = shift;
    my $command    = shift;
    my $envelope   = shift;
    my $glob       = $command->arguments;
    my $thresholds = $self->patron->trap_thresholds;
    my $response   = q();
    for my $path ( sort keys %{$thresholds} ) {
        $response .= sprintf "%30s %10d\n", $path, $thresholds->{$path}
            if ( not $glob or $path =~ m{$glob} );
    }
    return $self->response( $envelope, $response );
};

$C{ls_traps} = $C{list_trap_thresholds};

$C{add_trap_threshold} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my ( $regex, $threshold ) = split q( ), $command->arguments, 2;
    $self->patron->trap_thresholds->{$regex} = $threshold;
    $self->patron->trap_alerts( {} );
    return $self->okay;
};

$C{add_trap} = $C{add_trap_threshold};

$C{remove_trap_threshold} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $patron   = $self->patron;
    delete $patron->trap_thresholds->{ $command->arguments };
    $self->patron->trap_alerts( {} );
    return $self->okay;
};

$C{rm_trap} = $C{remove_trap_threshold};

sub alert {
    my $self       = shift;
    my $type       = shift;
    my $buffer     = shift;
    my $warning    = shift;
    my $callback   = shift;
    my $alerts     = $self->{"${type}_alerts"};
    my $id         = $buffer->{id};
    my $short_host = ( $buffer->{hostname} =~ m{^([^.]+)} )[0];
    my $subject    = join q( ), $warning, 'on', $short_host,
        $buffer->{buff_name};
    $alerts->{$id} ||= {};
    $alerts->{$id}->{$subject} ||= [ $Tachikoma::Now => $callback ];
    return;
}

sub get_alerts {
    my $self    = shift;
    my $type    = shift;
    my $buffers = $self->{buffers};
    my $alerts  = $self->{"${type}_alerts"};
    my %hosts   = ();
    my $body    = q();
    for my $id ( sort keys %{$alerts} ) {
        my $subjects = $alerts->{$id};
        my $buffer   = $buffers->{$id};
        my $chunk    = q();
        for my $subject ( sort keys %{$subjects} ) {
            my ( $timestamp, $callback ) = @{ $subjects->{$subject} };
            next if ( $Tachikoma::Now - $timestamp < $ALERT_DELAY );
            if ( not &{$callback}($buffer) ) {
                delete $subjects->{$subject};
                next;
            }
            my $short_host = ( $buffer->{hostname} =~ m{^([^.]+)} )[0];
            $hosts{$short_host} = 1;
            $chunk .= $subject . "\n";
        }
        delete $alerts->{$id} if ( not keys %{$subjects} );
        next                  if ( not $chunk );
        $chunk .= $self->get_details($buffer) . "\n\n"
            if ( $type eq 'email' );
        $body .= $chunk;
    }
    $self->{hosts} = \%hosts;
    $self->{$type} = $body;
    return;
}

sub get_details {
    my $self    = shift;
    my $buffer  = shift;
    my $details = q();
    $details .= sprintf "%20s: %s\n", 'hostname',  $buffer->{hostname};
    $details .= sprintf "%20s: %s\n", 'buff_name', $buffer->{buff_name};
    for my $field ( sort keys %{$buffer} ) {
        next
            if ( $field eq 'id'
            or $field eq 'hostname'
            or $field eq 'buff_name'
            or $field =~ m{^last_} );
        $details .= sprintf "%20s: %12d\n", $field, $buffer->{$field};
    }
    return $details;
}

sub send_alerts {
    my $self = shift;
    my $type = shift;
    if ( $type eq 'email' ) {
        return
            if ( not $self->{email}
            or $Tachikoma::Now - $self->{last_email} < $ALERT_INTERVAL );
        $self->{'last_email'} = $Tachikoma::Now;
        my $email = $self->{email_address};
        delete @ENV{qw(IFS CDPATH ENV BASH_ENV)};
        local $ENV{PATH} = q();
        my $subject = 'WARNING: BufferMonitor threshold(s) exceeded';
        open my $mail, q(|-), qq(/usr/bin/mail -s "$subject" $email)
            or die "couldn't open mail: $!";
        print {$mail} scalar( localtime time ),
            qq(\n\n),
            q(Affected hosts: ),
            join( q(, ), sort keys %{ $self->{hosts} } ),
            qq(\n\n),
            $self->{email};
        $self->send_log( $mail, 'server' );
        $self->send_log( $mail, 'system' );
        close $mail or $self->stderr("ERROR: couldn't close mail: $!");
    }
    elsif ( $self->{mon_path} ) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE] = TM_BYTESTREAM;
        $message->[TO]   = $self->{mon_path};
        if ( $self->{trap} ) {
            $message->[PAYLOAD] = join q(),
                "BufferMonitor threshold(s) exceeded\n",
                $self->{trap};
        }
        else {
            $message->[PAYLOAD] = $Tachikoma::Now . "\n";
        }
        $self->SUPER::fill($message);
    }
    $self->{hosts} = {};
    $self->{$type} = q();
    return;
}

sub send_log {
    my $self = shift;
    my $mail = shift;
    my $type = shift;
    my $file = "/logs/tachikoma/${type}s/${type}s.log";
    if ( -f $file ) {
        my $grep = '/usr/bin/grep';
        for my $host ( keys %{ $self->{hosts} } ) {
            $grep .= " -e $host";
        }
        $grep .= " $file | /usr/bin/tail -n100";
        ## no critic (ProhibitBacktickOperators)
        my $logs = `$grep`;
        print {$mail} qq(\n\n),
            qq(Last 100 $type log entries from affected hosts:\n\n),
            $logs;
    }
    return;
}

sub buffers {
    my $self = shift;
    if (@_) {
        $self->{buffers} = shift;
    }
    return $self->{buffers};
}

sub email_thresholds {
    my $self = shift;
    if (@_) {
        $self->{email_thresholds} = shift;
    }
    return $self->{email_thresholds};
}

sub trap_thresholds {
    my $self = shift;
    if (@_) {
        $self->{trap_thresholds} = shift;
    }
    return $self->{trap_thresholds};
}

sub hosts {
    my $self = shift;
    if (@_) {
        $self->{hosts} = shift;
    }
    return $self->{hosts};
}

sub email_alerts {
    my $self = shift;
    if (@_) {
        $self->{email_alerts} = shift;
    }
    return $self->{email_alerts};
}

sub trap_alerts {
    my $self = shift;
    if (@_) {
        $self->{trap_alerts} = shift;
    }
    return $self->{trap_alerts};
}

sub email {
    my $self = shift;
    if (@_) {
        $self->{email} = shift;
    }
    return $self->{email};
}

sub trap {
    my $self = shift;
    if (@_) {
        $self->{trap} = shift;
    }
    return $self->{trap};
}

sub last_email {
    my $self = shift;
    if (@_) {
        $self->{last_email} = shift;
    }
    return $self->{last_email};
}

sub last_trap {
    my $self = shift;
    if (@_) {
        $self->{last_trap} = shift;
    }
    return $self->{last_trap};
}

sub email_address {
    my $self = shift;
    if (@_) {
        $self->{email_address} = shift;
    }
    return $self->{email_address};
}

sub mon_path {
    my $self = shift;
    if (@_) {
        $self->{mon_path} = shift;
    }
    return $self->{mon_path};
}

1;
