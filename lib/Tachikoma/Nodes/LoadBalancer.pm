#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::LoadBalancer
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::LoadBalancer;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM
    TM_COMMAND TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use Digest::MD5 qw( md5 );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.280');

my $Default_Timeout = 3600;
my %C               = ();

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{owner}          = [];
    $self->{pointer}        = 0;
    $self->{streams}        = {};
    $self->{msg_unanswered} = {};
    $self->{max_unanswered} = 0;
    $self->{timeout}        = $Default_Timeout;
    $self->{method}         = 'round-robin';
    $self->{mode}           = 'persistent';
    $self->{hash}           = q();
    $self->{interpreter}    = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node LoadBalancer <node name> [ <max_unanswered> [ <timeout> ] ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $max_unanswered, $timeout ) =
            split q( ), $self->{arguments}, 2;
        $self->{max_unanswered} = $max_unanswered || 0;
        $self->{timeout}        = $timeout        || $Default_Timeout;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    if ( $type & TM_COMMAND or ( $type & TM_EOF and not $message->[STREAM] ) )
    {
        $self->interpreter->fill($message);
    }
    elsif ( length $message->[TO] ) {
        $self->handle_response($message);
        $self->{sink}->fill($message);
    }
    elsif ( $type & TM_RESPONSE ) {
        return $self->print_less_often( 'WARNING: unexpected response from ',
            $message->[FROM] );
    }
    elsif ( $type != TM_ERROR ) {
        my $owner = $self->get_stream_owner($message);
        return $self->print_less_often('WARNING: no recipients')
            if ( not length $owner );
        my $mode = $self->{mode};
        if ( $type & TM_PERSIST and $mode eq 'persistent' ) {
            $self->stamp_message( $message, $self->{name} ) or return;
            $self->{msg_unanswered}->{$owner}++;
        }
        elsif ( not $type & TM_EOF and $mode eq 'all' ) {
            $self->{msg_unanswered}->{$owner}++;
        }
        $self->{counter}++;
        $message->[TO] = $owner;
        if ( $mode ne 'none' and not $self->{timer_is_active} ) {
            $self->set_timer;
        }
        $self->{sink}->fill($message);
    }
    return;
}

sub handle_response {
    my $self    = shift;
    my $message = shift;
    return if ( $self->{mode} eq 'none' );
    my $streams        = $self->{streams};
    my $msg_unanswered = $self->{msg_unanswered};
    my $id = length $message->[STREAM] ? $message->[STREAM] : $message->[ID];
    $id = ( split m{/}, $message->[FROM], 2 )[0] if ( not length $id );
    my $stream = length $id     ? $streams->{$id}  : undef;
    my $owner  = length $stream ? $stream->{owner} : undef;
    return if ( not $stream );

    if ( $owner and defined $msg_unanswered->{$owner} ) {
        if ( $msg_unanswered->{$owner} > 0 ) {
            $msg_unanswered->{$owner}--;
        }
        else {
            delete $msg_unanswered->{$owner};
        }
    }
    shift @{ $stream->{timestamps} };
    delete $streams->{$id} if ( $stream->{count}-- <= 1 );
    return;
}

sub fire {
    my $self           = shift;
    my $streams        = $self->{streams};
    my $msg_unanswered = $self->{msg_unanswered};
    my $timeout        = $self->{timeout};
    my %exists         = map { $_ => 1 } @{ $self->{owner} };
    for my $owner ( keys %{$msg_unanswered} ) {
        delete $msg_unanswered->{$owner} if ( not $exists{$owner} );
    }
    for my $id ( keys %{$streams} ) {
        my $stream = $streams->{$id};
        if ($Tachikoma::Now - ( $stream->{timestamps}->[0] || 0 ) > $timeout )
        {
            my $owner = $stream->{owner};
            if ( $owner and defined $msg_unanswered->{$owner} ) {
                if ( $msg_unanswered->{$owner} > 0 ) {
                    $msg_unanswered->{$owner}--;
                }
                else {
                    delete $msg_unanswered->{$owner};
                }
            }
            shift @{ $stream->{timestamps} };
            delete $streams->{$id} if ( $stream->{count}-- <= 1 );
        }
    }
    if ( not keys %{$msg_unanswered} and not keys %{$streams} ) {
        $self->stop_timer;
    }
    return;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list_streams\n"
            . "          set_count <max unanswered count>\n"
            . "          set_timeout <seconds>\n"
            . "          set_method <round-robin | preferred | hash>\n"
            . "          set_mode <none | persistent | all>\n"
            . "          set_hash <token>\n"
            . "          kick\n" );
};

$C{list_streams} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $streams  = $self->patron->streams;
    my $response = [
        [   [ 'STREAM' => 'left' ],
            [ 'OWNER'  => 'right' ],
            [ 'COUNT'  => 'right' ],
            [ 'TIME'   => 'right' ]
        ]
    ];
    for my $id ( sort keys %{$streams} ) {
        my $stream = $streams->{$id};
        push @{$response},
            [
            $id,              $stream->{owner},
            $stream->{count}, $Tachikoma::Now - $stream->{timestamps}->[-1]
            ];
    }
    return $self->response( $envelope, $self->tabulate($response) );
};

$C{ls} = $C{list_streams};

$C{set_count} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $command->arguments =~ m{\D} ) {
        return $self->error( $envelope, "count must be an integer\n" );
    }
    $self->patron->max_unanswered( $command->arguments );
    return $self->okay($envelope);
};

$C{set_timeout} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    if ( $command->arguments =~ m{\D} ) {
        return $self->error( $envelope, "seconds must be an integer\n" );
    }
    $self->patron->timeout( $command->arguments );
    return $self->okay($envelope);
};

$C{set_method} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->method( $command->arguments );
    return $self->okay($envelope);
};

$C{set_mode} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->mode( $command->arguments );
    return $self->okay($envelope);
};

$C{set_hash} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->hash( $command->arguments );
    return $self->okay($envelope);
};

$C{kick} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->patron->streams( {} );
    $self->patron->msg_unanswered( {} );
    return $self->okay($envelope);
};

# left nut
sub get_stream_owner {
    my $self    = shift;
    my $message = shift;
    my $id = length $message->[STREAM] ? $message->[STREAM] : $message->[ID];
    my $owner = undef;
    my $mode  = $self->{mode};
    if ( $mode eq 'none' or not length $id ) {
        $owner = $self->get_next_owner( $message, $id );
        if ( defined $owner and $mode eq 'all' ) {
            $id = $owner;
        }
        else {
            return $owner;
        }
    }
    my $streams = $self->{streams};
    if ( $streams->{$id} ) {
        my $last_owner = $streams->{$id}->{owner};
        my $name       = ( split m{/}, $last_owner, 2 )[0];
        my %exists     = map { $_ => 1 } @{ $self->{owner} };
        if ( not $exists{$last_owner} or not $Tachikoma::Nodes{$name} ) {
            delete $streams->{$id};
            delete $self->{msg_unanswered}->{$last_owner};
            $self->disconnect_node( $self->{name}, $last_owner )
                if ( not $Tachikoma::Nodes{$name} );
        }
    }
    if ( not $streams->{$id} ) {
        $owner //= $self->get_next_owner( $message, $id );
        return if ( not defined $owner );
        $streams->{$id} = {
            owner      => $owner,
            count      => 0,
            timestamps => []
        };
    }
    my $stream = $streams->{$id};
    if ( not $message->[TYPE] & TM_EOF ) {
        push @{ $stream->{timestamps} }, $Tachikoma::Now;
        $stream->{count}++;
    }
    return $stream->{owner};
}

# right nut
sub get_next_owner {
    my $self    = shift;
    my $message = shift;
    my $id      = shift;
    my $owners  = $self->{owner};
    return if ( not @{$owners} );
    my $msg_unanswered = $self->{msg_unanswered};
    my $method         = $self->{method};
    my $mode           = $self->{mode};
    my $owner          = undef;

    if ( length $id and $method eq 'hash' ) {
        my $i = 0;
        $i += $_ for ( unpack 'C*', md5( join q(:), $self->{hash}, $id ) );
        $owner = $owners->[ $i % @{$owners} ];
    }
    elsif ($method eq 'round-robin'
        or $mode eq 'none'
        or ( $method eq 'hash' and not length $id ) )
    {
        my %by_count = ();
        my $total    = 0;
        for my $owner ( @{$owners} ) {
            my $count = $msg_unanswered->{$owner} || 0;
            $by_count{$count} ||= [];
            push @{ $by_count{$count} }, $owner;
            $total += $count;
        }
        $self->{max_unanswered} = $total
            if ( $total > $self->{max_unanswered} );
        my $pointer = $self->{pointer};
        $pointer = ( $pointer + 1 ) % $Tachikoma::Max_Int;
        $self->{pointer} = $pointer;
        my $count      = ( sort { $a <=> $b } keys %by_count )[0];
        my $num_owners = scalar @{ $by_count{$count} };
        $owner = $by_count{$count}->[ $pointer % $num_owners ];
    }
    else {
        my %by_count = ();
        my $total    = 0;
        for my $owner ( @{$owners} ) {
            my $count = $msg_unanswered->{$owner} || 0;
            $by_count{$count} ||= [];
            push @{ $by_count{$count} }, $owner;
            $total += $count;
        }
        $self->{max_unanswered} = $total
            if ( $total > $self->{max_unanswered} );
        my $count = ( sort { $a <=> $b } keys %by_count )[0];
        $owner = $by_count{$count}->[0];
    }
    return $owner;
}

sub dump_config {
    my $self           = shift;
    my $response       = undef;
    my $settings       = undef;
    my $name           = $self->{name};
    my $max_unanswered = $self->{max_unanswered};
    my $timeout        = $self->{timeout};
    my $method         = $self->{method};
    my $mode           = $self->{mode};
    $response = "make_node LoadBalancer $self->{name}";
    $response .= " $max_unanswered"       if ($max_unanswered);
    $response .= " $timeout"              if ( $timeout ne $Default_Timeout );
    $response .= "\n";
    $settings .= "  set_method $method\n" if ( $method ne 'round-robin' );
    $settings .= "  set_mode $mode\n"     if ( $mode ne 'persistent' );
    $response .= "cd $self->{name}\n" . $settings . "cd ..\n" if ($settings);
    return $response;
}

sub streams {
    my $self = shift;
    if (@_) {
        $self->{streams} = shift;
    }
    return $self->{streams};
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

sub method {
    my $self = shift;
    if (@_) {
        $self->{method} = shift;
    }
    return $self->{method};
}

sub mode {
    my $self = shift;
    if (@_) {
        $self->{mode} = shift;
    }
    return $self->{mode};
}

sub hash {
    my $self = shift;
    if (@_) {
        $self->{hash} = shift;
    }
    return $self->{hash};
}

1;
