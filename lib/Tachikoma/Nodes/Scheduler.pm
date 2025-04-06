#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Scheduler
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::Scheduler;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw(
    TYPE FROM TIMESTAMP PAYLOAD
    TM_COMMAND TM_EOF TM_NOREPLY
);
use Tachikoma::Command;
use Time::Local;
use POSIX qw( strftime );
use BerkeleyDB;
use Data::Dumper;
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.368');

my $HOME     = Tachikoma->configuration->home || ( getpwuid $< )[7];
my $DB_DIR   = "$HOME/.tachikoma/schedules";
my %C        = ();
my %DISABLED = ( 3 => { map { $_ => 1 } qw( schedule ) } );

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{filename}    = undef;
    $self->{tiedhash}    = undef;
    $self->{buffer_size} = undef;
    $self->{interpreter} = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    $self->{interpreter}->disabled( \%DISABLED );
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Scheduler <node name> [ <filename> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->untie_hash if ( $self->{tiedhash} );
        $self->filename( $self->{arguments} );
        $self->set_timer(1000);
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    if ( $type & TM_COMMAND or $type & TM_EOF ) {
        return $self->interpreter->fill($message);
    }
    return $self->SUPER::fill($message);
}

sub fire {
    my $self      = shift;
    my $next_when = 30;
    for my $message_id ( sort { $a <=> $b } keys %{ $self->tiedhash } ) {
        my ( $time, $repeat, $enabled, $text, $packed ) =
            unpack 'N N N Z* a*',
            $self->tiedhash->{$message_id};
        my $when = $time - $Tachikoma::Now;
        $next_when = $when if ( $when < $next_when );
        next if ( $when > 0 or not $enabled );
        my $message = undef;
        my $okay    = eval {
            $message = Tachikoma::Message->unpacked( \$packed );
            return 1;
        };
        if ( not $okay ) {
            $self->stderr($@);
            delete $self->tiedhash->{$message_id};
            next;
        }
        if ($message) {
            my $name    = ( split m{/}, $message->[FROM], 2 )[0] || q();
            my $command = undef;
            $okay = eval {
                $command = Tachikoma::Command->new( $message->[PAYLOAD] );
                return 1;
            };
            if ( not $okay ) {
                $self->stderr($@);
                delete $self->tiedhash->{$message_id};
                next;
            }
            $message->[FROM] = $self->{owner}
                if ( not $Tachikoma::Nodes{$name} );
            $message->[TYPE] |= TM_NOREPLY if ( not length $message->[FROM] );
            $message->[TIMESTAMP] = $Tachikoma::Now;
            $command->sign( $self->scheme, $Tachikoma::Now );
            $message->payload( $command->packed );
            if ( $C{ $command->{name} } ) {
                $self->{interpreter}->fill($message);
            }
            else {
                $self->{sink}->fill($message);
            }
        }
        if ( $message and $repeat ) {
            do { $time += $repeat } while ( $time < $Tachikoma::Now );
            $self->tiedhash->{$message_id} = pack 'N N N Z* a*',
                $time, $repeat, $enabled, $text, $packed;
            $next_when = 0;
        }
        else {
            delete $self->tiedhash->{$message_id};
        }
    }
    $next_when = 0 if ( $next_when < 0 );
    $self->set_timer( $next_when * 1000 );
    return;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list_events\n"
            . "          at <YYYY-MM-DD HH:MM:SS> <command>\n"
            . "          in <#hms> <command>\n"
            . "          every <#hms> <command>\n"
            . "          remove_event <key>\n"
            . "          dump_event <key>\n" );
};

$C{list_events} = sub {
    my $self      = shift;
    my $command   = shift;
    my $envelope  = shift;
    my $glob      = $command->arguments;
    my $tiedhash  = $self->patron->tiedhash;
    my @responses = ();
    if ( $glob eq '-a' ) {
        for my $key ( keys %{$tiedhash} ) {
            push @responses, "$key\n";
        }
    }
    else {
        my %by_time = ();
        for my $key ( keys %{$tiedhash} ) {
            my ( $time, $repeat, $enabled, $text, $packed ) =
                unpack 'N N N Z* a*', $tiedhash->{$key};
            $by_time{$time} ||= [];
            push @{ $by_time{$time} },
                {
                key     => $key,
                enabled => $enabled,
                text    => $text
                };
        }
        for my $time ( sort { $a <=> $b } keys %by_time ) {
            for my $event ( @{ $by_time{$time} } ) {
                next if ( length $glob and $event->{text} !~ m{$glob} );
                if ( $event->{enabled} ) {
                    push @responses,
                        sprintf "%s (%d) %s\n",
                        strftime( '%F %T %Z', localtime $time ),
                        $event->{key}, $event->{text};
                }
                else {
                    push @responses,
                        sprintf "%23s (%d) %s\n",
                        'disabled', $event->{key}, $event->{text};
                }
            }
        }
    }
    return $self->response( $envelope, join q(), @responses );
};

$C{events} = $C{list_events};

$C{ls} = $C{list_events};

$C{in} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'schedule' )
        or return $self->error("verification failed\n");
    my $text = join q( ), $command->name, $command->arguments;
    if ( $command->arguments =~ m{^(\d+[dhms]+)(?:/(\d+))? (.+)}s ) {
        my ( $spec, $divisor, $imperative ) = ( $1, $2, $3 );
        my ( $name, $arguments ) = split q( ), $imperative, 2;
        my $message = $self->command( $name, $arguments );
        my $time    = 0;
        if ( $spec =~ m{(\d+)d} ) {
            $time += $1 * 60 * 60 * 24;
            $spec =~ s{\d+d}{};
        }
        if ( $spec =~ m{(\d+)h} ) {
            $time += $1 * 60 * 60;
            $spec =~ s{\d+h}{};
        }
        if ( $spec =~ m{(\d+)m} ) {
            $time += $1 * 60;
            $spec =~ s{\d+m}{};
        }
        if ( $spec =~ m{(\d+)s} ) {
            $time += $1;
            $spec =~ s{\d+s}{};
        }
        $time /= $divisor if ($divisor);
        my $own_name = $self->patron->name;
        $message->type( $envelope->type );
        $message->from( $envelope->from );
        $self->patron->schedule( $Tachikoma::Now + $time,
            0, 1, $text, $message );
    }
    else {
        return $self->error( $envelope, "invalid command: $text\n" );
    }
    return $self->okay($envelope);
};

$C{at} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'schedule' )
        or return $self->error("verification failed\n");
    my $text = join q( ), $command->name, $command->arguments;
    my $pattern =
        qr{^(?:(\d+)-(\d+)-(\d+)\s+)?(?:(\d+):(\d+))?(?::(\d+))? (.+)}s;
    if ( $command->arguments =~ m{$pattern} ) {
        my ( $year, $month, $day, $hour, $min, $sec, $imperative ) =
            ( $1, $2, $3, $4, $5, $6, $7 );
        $year -= 1900 if ($year);
        $month--      if ($month);
        my ( $name, $arguments ) = split q( ), $imperative, 2;
        my $message = $self->command( $name, $arguments );
        my $now     = $Tachikoma::Now;
        $now += 60 if ( length $sec and not length $min );
        my ( $nsec, $nmin, $nhour, $nday, $nmonth, $nyear ) = localtime $now;
        $year  ||= $nyear;
        $month ||= $nmonth;
        $day   ||= $nday;
        $hour = $nhour if ( length $sec and not length $hour );
        $min  = $nmin  if ( length $sec and not length $min );
        $sec ||= 0;
        my $time = timelocal( $sec, $min, $hour, $day, $month, $year );
        $time += 86400 if ( $time < $Tachikoma::Now );
        my $own_name = $self->patron->name;
        $message->type( $envelope->type );
        $message->from( $envelope->from );
        $self->patron->schedule( $time, 0, 1, $text, $message );
    }
    else {
        return $self->error( $envelope, "invalid command: $text\n" );
    }
    return $self->okay($envelope);
};

$C{every} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'schedule' )
        or return $self->error("verification failed\n");
    my $text = join q( ), $command->name, $command->arguments;
    if ( $command->arguments =~ m{^(\d+[dhms]+)(?:/(\d+))? (.+)}s ) {
        my ( $spec, $divisor, $imperative ) = ( $1, $2, $3 );
        my ( $name, $arguments ) = split q( ), $imperative, 2;
        my $message = $self->command( $name, $arguments );
        my $time    = 0;
        if ( $spec =~ m{(\d+)d} ) {
            $time += $1 * 60 * 60 * 24;
            $spec =~ s{\d+d}{};
        }
        if ( $spec =~ m{(\d+)h} ) {
            $time += $1 * 60 * 60;
            $spec =~ s{\d+h}{};
        }
        if ( $spec =~ m{(\d+)m} ) {
            $time += $1 * 60;
            $spec =~ s{\d+m}{};
        }
        if ( $spec =~ m{(\d+)s} ) {
            $time += $1;
            $spec =~ s{\d+s}{};
        }
        $time /= $divisor if ($divisor);
        my $own_name = $self->patron->name;
        $message->type( $envelope->type );
        $message->from( $envelope->from );
        $self->patron->schedule( $envelope->[TIMESTAMP],
            $time, 1, $text, $message );
    }
    else {
        return $self->error( $envelope, "invalid command: $text\n" );
    }
    return $self->okay($envelope);
};

$C{enable_event} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'schedule' )
        or return $self->error("verification failed\n");
    my $key      = $command->arguments;
    my $tiedhash = $self->patron->tiedhash;
    my $event    = $tiedhash->{$key};
    if ( not $event ) {
        return $self->error( $envelope, qq(can't find event "$key"\n) );
    }
    my ( $time, $repeat, $enabled, $text, $packed ) = unpack 'N N N Z* a*',
        $event;
    $enabled          = 1;
    $tiedhash->{$key} = pack 'N N N Z* a*', $time, $repeat, $enabled, $text,
        $packed;
    return $self->okay($envelope);
};

$C{enable} = $C{enable_event};

$C{disable_event} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'schedule' )
        or return $self->error("verification failed\n");
    my $key      = $command->arguments;
    my $tiedhash = $self->patron->tiedhash;
    my $event    = $tiedhash->{$key};
    if ( not $event ) {
        return $self->error( $envelope, qq(can't find event "$key"\n) );
    }
    my ( $time, $repeat, $enabled, $text, $packed ) = unpack 'N N N Z* a*',
        $event;
    $enabled          = 0;
    $tiedhash->{$key} = pack 'N N N Z* a*', $time, $repeat, $enabled, $text,
        $packed;
    return $self->okay($envelope);
};

$C{disable} = $C{disable_event};

$C{remove_event} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'schedule' )
        or return $self->error("verification failed\n");
    my $tiedhash = $self->patron->tiedhash;
    my $key      = $command->arguments;
    if ( exists $tiedhash->{$key} ) {
        delete $tiedhash->{$key};
        return $self->okay($envelope);
    }
    else {
        return $self->error(qq(couldn't find event: "$key"\n));
    }
};

$C{rm} = $C{remove_event};

$C{dump_event} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $key      = $command->arguments;
    my $tiedhash = $self->patron->tiedhash;
    my $event    = $tiedhash->{$key};
    if ( not $event ) {
        return $self->error( $envelope, qq(can't find event "$key"\n) );
    }
    my $packed  = ( unpack 'N N N Z* a*', $event )[4];
    my $message = Tachikoma::Message->unpacked( \$packed );
    my $payload = Tachikoma::Command->new( $message->payload );
    $payload->{signature} = q();
    $message->payload($payload);
    return $self->response( $envelope, Dumper $message);
};

$C{dump} = $C{dump_event};

sub schedule {    ## no critic (ProhibitManyArgs)
    my $self       = shift;
    my $time       = shift;
    my $repeat     = shift;
    my $enabled    = shift;
    my $text       = shift;
    my $message    = shift;
    my $tiedhash   = $self->tiedhash;
    my $message_id = undef;
    do {
        $message_id = $self->{counter}++;
    } while ( exists $tiedhash->{$message_id} );
    $tiedhash->{$message_id} = pack 'N N N Z* a*',
        $time, $repeat, $enabled, $text, ${ $message->packed };
    $self->set_timer(0);
    return;
}

sub remove_node {
    my $self = shift;
    $self->untie_hash if ( $self->{filename} );
    $self->{filename} = undef;
    $self->SUPER::remove_node;
    return;
}

sub tiedhash {
    my $self = shift;
    if (@_) {
        $self->{tiedhash} = shift;
    }
    if ( not defined $self->{tiedhash} ) {
        if ( $self->{filename} ) {
            ## no critic (ProhibitTies)
            my %h    = ();
            my $path = $self->filename;
            my $ext  = ( $path =~ m{[.](db|hash)$} )[0] || 'db';
            $self->make_parent_dirs($path);
            if ( -e "${path}.clean" ) {
                open my $fh, '<', "${path}.clean"
                    or die "ERROR: couldn't open ${path}.clean: $!\n";
                my $count = <$fh>;
                close $fh
                    or die "ERROR: couldn't close ${path}.clean: $!\n";
                if ( length $count ) {
                    chomp $count;
                    $self->{buffer_size} = $count;
                }
                unlink "${path}.clean"
                    or die "ERROR: couldn't unlink ${path}.clean: $!\n";
            }
            elsif ( -e $path ) {
                unlink $path or die "ERROR: couldn't unlink $path: $!\n";
            }
            if ( $ext eq 'db' ) {
                tie %h, 'BerkeleyDB::Btree',
                    -Filename => $path,
                    -Flags    => DB_CREATE,
                    -Mode     => oct 600
                    or die "couldn't tie $path: $!\n";
            }
            elsif ( $ext eq 'hash' ) {
                tie %h, 'BerkeleyDB::Hash',
                    -Filename => $path,
                    -Flags    => DB_CREATE,
                    -Mode     => oct 600
                    or die "couldn't tie $path: $!\n";
            }
            $self->{tiedhash} = \%h;
        }
        else {
            $self->{tiedhash} = {};
        }
    }
    return $self->{tiedhash};
}

sub untie_hash {
    my $self = shift;
    untie %{ $self->{tiedhash} } if ( $self->{tiedhash} );
    if ( $self->{filename} ) {
        my $path = $self->filename;
        open my $fh, '>>', "${path}.clean"
            or $self->stderr("ERROR: couldn't open ${path}.clean: $!");
        print {$fh} $self->{buffer_size}, "\n"
            if ( $fh and defined $self->{buffer_size} );
        close $fh
            or $self->stderr("ERROR: couldn't close ${path}.clean: $!");
    }
    $self->{tiedhash} = undef;
    return;
}

sub filename {
    my $self = shift;
    my $path = undef;
    if (@_) {
        my $filename = shift;
        $path = ( $filename =~ m{^(.*)$} )[0] if ( defined $filename );
        $self->{filename} = $path;
    }
    if ( $self->{filename} ) {
        if ( $self->{filename} =~ m{^/} ) {
            $path = $self->{filename};
        }
        else {
            $path = join q(/), $self->db_dir, $self->{filename};
        }
    }
    return $path;
}

sub db_dir {
    my $self = shift;
    if (@_) {
        $DB_DIR = shift;
    }
    return $DB_DIR;
}

sub buffer_size {
    my $self = shift;
    if (@_) {
        $self->{buffer_size} = shift;
    }
    return $self->{buffer_size};
}

1;
