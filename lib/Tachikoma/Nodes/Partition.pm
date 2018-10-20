#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Partition
# ----------------------------------------------------------------------
#
# $Id: Partition.pm 29406 2017-04-29 11:18:09Z chris $
#

package Tachikoma::Nodes::Partition;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD LAST_MSG_FIELD
    TM_BYTESTREAM TM_BATCH TM_INFO TM_PERSIST TM_ERROR TM_EOF
);
use Fcntl qw( :flock SEEK_SET SEEK_END );
use Getopt::Long qw( GetOptionsFromString );
use Time::HiRes qw( usleep );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = 'v2.0.165';

use constant {
    LOG_OFFSET => 0,
    LOG_SIZE   => 1,
    LOG_FH     => 2,
    BUFSIZ     => 131072,
};

my $Default_Num_Segments     = 8;
my $Default_Segment_Size     = 128 * 1024 * 1024;
my $Default_Segment_Lifespan = 7 * 86400;
my $Touch_Interval           = 3600;
my $Num_Offsets              = 10;
my $Get_Timeout              = 300;
my $Offset                   = LAST_MSG_FIELD + 1;
my %Leader_Commands = map { $_ => 1 } qw( GET_VALID_OFFSETS GET ACK EMPTY );
my %Follower_Commands =
    map { $_ => 1 } qw( VALID_OFFSETS UPDATE COMMIT DELETE EMPTY );

sub help {
    my $self = shift;
    return <<'EOF';
make_node Partition <node name> --filename=<path>            \
                                --num_segments=<int>         \
                                --segment_size=<int>         \
                                --segment_lifespan=<seconds> \
                                --leader=<node path>
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        my $arguments        = shift;
        my $filename         = undef;
        my $path             = undef;
        my $num_segments     = $Default_Num_Segments;
        my $segment_size     = $Default_Segment_Size;
        my $segment_lifespan = $Default_Segment_Lifespan;
        my $leader           = undef;
        my ( $r, $argv ) = GetOptionsFromString(
            $arguments,
            'filename=s'         => \$filename,
            'num_segments=i'     => \$num_segments,
            'segment_size=i'     => \$segment_size,
            'segment_lifespan=i' => \$segment_lifespan,
            'leader=s'           => \$leader,
        );
        die "ERROR: bad arguments for Partition\n" if ( not $r );
        $filename //= shift @{$argv};

        if ($filename) {
            $path = ( $filename =~ m{^(/.*)$} )[0];
            die "ERROR: invalid path: $filename\n" if ( not defined $path );
        }
        $self->{arguments}        = $arguments;
        $self->{filename}         = $path;
        $self->{num_segments}     = $num_segments;
        $self->{segment_size}     = $segment_size;
        $self->{segment_lifespan} = $segment_lifespan;
        $self->{status}           = 'ACTIVE';
        $self->{leader}           = undef;
        $self->{leader_path}      = undef;
        $self->{followers}        = {};
        $self->{in_sync_replicas} = {};
        $self->{replica_offsets}  = {};
        $self->{segments} //= [];
        $self->{last_commit_offset}   = undef;
        $self->{last_truncate_offset} = undef;
        $self->{valid_offsets}        = [];
        $self->{offset}               = undef;
        $self->{responses}            = [];
        $self->{waiting}              = {};
        $self->{batch}                = [];
        $self->{expecting}            = undef;
        $self->{broker_id}            = $Tachikoma::Nodes{broker}->broker_id
            if ( $Tachikoma::Nodes{broker} );

        if ($leader) {
            $self->leader($leader);
            $self->restart_follower;
        }
        else {
            $self->restart_leader;
        }
    }
    return $self->{arguments};
}

# write to leader
#   leader sends UPDATE to follower
# follower sends GET to leader
#   leader sends data
# follower sends GET to leader
#   leader sends EOF and COMMIT
# follower sends ACK
#   leader acks write

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self    = shift;
    my $message = shift;
    return if ( not $self->{filename} );
    if ( $message->[TYPE] & TM_INFO ) {
        my $payload = $message->[PAYLOAD];
        chomp $payload;
        my ( $command, $offset, $args ) = split q( ), $payload, 3;
        if ( $self->{leader} ) {
            if ( not $Follower_Commands{$command} ) {
                if ( $Leader_Commands{$command} ) {
                    return $self->send_error( $message, "NOT_LEADER\n" );
                }
                else {
                    return $self->send_error( $message, "BAD_COMMAND\n" );
                }
            }
        }
        elsif ( not $Leader_Commands{$command} ) {
            if ( $Follower_Commands{$command} ) {
                return $self->send_error( $message, "NOT_FOLLOWER\n" );
            }
            else {
                return $self->send_error( $message, "BAD_COMMAND\n" );
            }
        }
        if ( $command eq 'GET' ) {
            $self->process_get( $message, $offset, $args );
        }
        elsif ( $command eq 'ACK' ) {
            $self->process_ack( $offset, $args );
        }
        elsif ( $command eq 'UPDATE' ) {
            $self->get_batch if ( not $self->{expecting} );
        }
        elsif ( $command eq 'COMMIT' ) {
            $self->write_offset($offset);
            $self->send_ack($offset);
        }
        elsif ( $command eq 'GET_VALID_OFFSETS' ) {
            $self->process_get_valid_offsets( $message, $args );
        }
        elsif ( $command eq 'VALID_OFFSETS' ) {
            $self->process_valid_offsets($offset);
        }
        elsif ( $command eq 'DELETE' ) {
            $self->process_delete( $offset, $args );
        }
        elsif ( $command eq 'EMPTY' ) {
            $self->empty_partition;
        }
        else {
            die;
        }
        return;
    }
    elsif ( $message->[TYPE] & TM_ERROR ) {
        $self->stderr( 'ERROR: got ', $message->[PAYLOAD] )
            if ( $message->[PAYLOAD] ne "NOT_AVAILABLE\n" );
        return;
    }
    elsif ( $message->[TYPE] & TM_EOF ) {

        # only create new follower segments on message boundaries!
        if ( $self->{leader} ) {
            return $self->send_error( $message, "NOT_LEADER\n" )
                if ( $message->[FROM] ne $self->{leader_path} );
            my $offset = ( split m{:}, $message->[ID], 2 )[0];
            return $self->reset_follower($offset)
                if ( $offset != $self->{offset} );
            my $segment = $self->{segments}->[-1];
            $self->create_segment
                if ( $segment->[LOG_SIZE] >= $self->{segment_size} );
            $self->{expecting} = undef;
        }
        return;
    }
    elsif ( $message->[TO] ) {
        $self->stderr( 'ERROR: message addressed to ', $message->[TO] );
        return;
    }
    if ( $self->{leader} ) {
        return $self->send_error( $message, "NOT_LEADER\n" )
            if ( $message->[FROM] ne $self->{leader_path} );
        $self->{expecting} = undef;
        my $offset = ( split m{:}, $message->[ID], 2 )[0];
        return $self->reset_follower($offset)
            if ( $offset != $self->{offset} );
    }
    elsif ( $self->{status} eq 'HALT' ) {
        $self->send_error( $message, "NOT_AVAILABLE\n" );
        return;
    }
    push @{ $self->{batch} }, $message;
    $self->set_timer( 0, 'oneshot' );
    return;
}

sub activate {    ## no critic (RequireArgUnpacking, RequireFinalReturn)
    push @{ $_[0]->{batch} },
        (
        bless [ TM_BYTESTREAM, q(), q(), q(), q(), $Tachikoma::Now,
            ${ $_[1] } ],
        'Tachikoma::Message'
        );
    $_[0]->set_timer( 0, 'oneshot' );
}

sub fire {
    my $self      = shift;
    my $batch     = $self->{batch};
    my $responses = $self->{responses};
    my $segment   = $self->{segments}->[-1];
    return if ( not $self->{filename} );
    if ( @{$batch} and $segment ) {
        my $fh    = $segment->[LOG_FH];
        my $wrote = 0;
        sysseek $fh, 0, SEEK_END or die "ERROR: couldn't seek: $!";
        for my $message ( @{$batch} ) {
            $wrote +=
                syswrite $fh,
                $message->[TYPE] & TM_BATCH
                ? $message->[PAYLOAD]
                : ${ $message->packed };
        }
        $segment->[LOG_SIZE] += $wrote;
        $self->{offset}      += $wrote;
        $self->{counter}     += @{$batch};
        for my $message ( @{$batch} ) {
            next if ( not $message->[TYPE] & TM_PERSIST );
            $message->[$Offset] = $self->{offset};
            $message->[PAYLOAD] = q();
            push @{$responses}, $message;
        }
        @{$batch} = ();

        # only create new leader segments at this point.
        # followers can only safely create segments when they catch up.
        $self->create_segment
            if ( not $self->{leader}
            and $segment->[LOG_SIZE] >= $self->{segment_size} );
        for my $follower ( keys %{ $self->{waiting} } ) {
            my $name = $self->{waiting}->{$follower};
            if ( $Tachikoma::Nodes{$name} ) {
                my $message = Tachikoma::Message->new;
                $message->[TYPE]    = TM_INFO;
                $message->[TO]      = $follower;
                $message->[PAYLOAD] = "UPDATE\n";
                $self->{sink}->fill($message);
            }
            delete $self->{waiting}->{$follower};
        }
    }
    if ( $self->{leader} ) {
        if ( $self->{expecting} ) {
            if ( $Tachikoma::Now - $self->{expecting} > $Get_Timeout ) {
                $self->stderr('WARNING: GET timeout - retrying');
                $self->{expecting} = undef;
            }
        }
        else {
            if ( not $segment ) {
                $self->get_valid_offsets;
            }
            else {
                $self->get_batch;
            }
        }
    }
    elsif ( not keys %{ $self->{followers} } ) {
        $self->commit_messages;
    }
    return;
}

sub commit_messages {
    my $self = shift;
    return if ( not defined $self->{offset} );
    if ( keys %{ $self->{followers} } ) {

        # find new LCO (the lowest ISR offset)
        my $offset             = undef;
        my $last_commit_offset = $self->{last_commit_offset};
        my $isr                = $self->{in_sync_replicas};
        for my $broker_id ( keys %{$isr} ) {
            $offset = $isr->{$broker_id}
                if ( not defined $offset or $offset > $isr->{$broker_id} );
        }
        if ( defined $offset and $offset != $last_commit_offset ) {
            $self->send_offset($offset);
            $self->write_offset($offset);
        }
    }
    else {

        # replication unavailable or disabled in config
        $self->write_offset( $self->{offset} );
        my $responses = $self->{responses};
        $self->cancel($_) for ( @{$responses} );
        @{$responses} = ();
    }
    return;
}

sub send_offset {
    my $self   = shift;
    my $offset = shift;
    for my $broker_id ( keys %{ $self->{followers} } ) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_INFO;
        $message->[TO]      = $self->{followers}->{$broker_id};
        $message->[PAYLOAD] = join q(), 'COMMIT ', $offset, "\n";
        $self->{sink}->fill($message);
    }
    return;
}

sub write_offset {
    my $self   = shift;
    my $offset = shift;
    my $lco    = $self->{last_commit_offset};
    return
        if ( not @{ $self->{segments} }
        or $offset > $self->{offset}
        or $offset == $lco );
    if ( $self->{leader} and $offset < $lco ) {
        $self->stderr("WARNING: commit_offset $offset < my $lco");
        $self->purge_offsets(-1);
        $self->open_segments(-1);
        $self->restart_follower;
    }
    else {
        # $self->{segments}->[-1]->[LOG_FH]->sync;
        if ( defined $lco and $lco != $self->{last_truncate_offset} ) {
            my $offset_file = join q(/), $self->{filename}, 'offsets', $lco;
            if ( -e $offset_file ) {
                unlink $offset_file
                    or die "ERROR: couldn't unlink $offset_file: $!";
            }
        }
        my $new_file = join q(/), $self->{filename}, 'offsets', $offset;
        my $fh = undef;
        open $fh, '>', $new_file
            or die "ERROR: couldn't open $new_file: $!";
        close $fh or die "ERROR: couldn't close $new_file: $!";
        $self->{last_commit_offset} = $offset;
    }
    return;
}

sub process_get_valid_offsets {
    my ( $self, $message, $broker_id ) = @_;
    my $offsets = join q(,), @{ $self->{valid_offsets} },
        $self->{last_commit_offset};
    my $to = $message->[FROM];
    my ( $name, $path ) = split m{/}, $to, 2;
    my $node = $Tachikoma::Nodes{$name} or return;
    return if ( not $node or not $broker_id );
    $self->{followers}->{$broker_id}        = $to;
    $self->{in_sync_replicas}->{$broker_id} = -1;
    $self->{last_commit_offset}             = -1;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_INFO;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $path;
    $response->[PAYLOAD] = join q(), 'VALID_OFFSETS ', $offsets, "\n";
    $node->fill($response);
    return;
}

sub process_valid_offsets {
    my ( $self, $valid_offsets ) = @_;
    $self->{expecting} = 0;

    # Corruption happens when a follower doesn't truncate enough data.
    # If for some reason the leader truncates more than the follower,
    # then the follower will probably have a bad offset and cleave a
    # message from the leader when it tries to resume.
    my $last_commit_offset = $self->get_last_commit_offset;
    my $old_offsets        = $self->{valid_offsets};
    $self->{valid_offsets} = [ split m{,}, $valid_offsets ];
    my %valid = map { $_ => 1 } @{ $self->{valid_offsets} };
    my $should_purge = undef;
    while ( @{$old_offsets} ) {
        $last_commit_offset = $old_offsets->[-1];
        last if ( $valid{$last_commit_offset} );
        pop @{$old_offsets};
        $should_purge = 1;
    }
    if ($should_purge) {
        if ( @{$old_offsets} ) {
            $self->purge_offsets($last_commit_offset);
            $last_commit_offset = $self->get_last_commit_offset;
        }
        elsif ($last_commit_offset) {
            $self->stderr("WARNING: invalid offset $last_commit_offset");
            $self->stderr("DEBUG: valid offsets: [$valid_offsets]");
            $self->purge_offsets(-1);
            $self->open_segments(-1);
            $self->restart_follower;
            return;
        }
    }
    $self->open_segments($last_commit_offset);
    $self->create_segment if ( not @{ $self->{segments} } );
    $self->stop_timer;    # have faith
    $self->set_timer( 0, 'oneshot' );
    return;
}

sub process_get {
    my ( $self, $message, $offset, $broker_id ) = @_;
    my $segment = $self->get_segment($offset);
    if ( $offset < 0 ) {
        if ( $offset == -1 ) {
            $offset = $segment->[LOG_OFFSET] + $segment->[LOG_SIZE];
        }
        elsif ( not $segment->[LOG_SIZE] and @{ $self->{segments} } > 1 ) {
            $segment = $self->{segments}->[-2];
        }
    }
    if ( not defined $segment ) {
        $self->stderr("ERROR: couldn't find offset: $offset")
            if ( $self->{filename} );
        return;
    }
    $offset = $segment->[LOG_OFFSET] if ( $offset < $segment->[LOG_OFFSET] );
    $offset = $self->{offset}        if ( $offset > $self->{offset} );
    my $fh = $segment->[LOG_FH];
    sysseek $fh, $offset - $segment->[LOG_OFFSET], SEEK_SET
        or die "ERROR: couldn't seek: $!";
    my $buffer = undef;
    my $to     = $message->[FROM];
    my ( $name, $path ) = split m{/}, $to, 2;
    my $node = $Tachikoma::Nodes{$name} or return;
    my $response = Tachikoma::Message->new;
    $response->[TYPE] = TM_BATCH;
    $response->[FROM] = $self->{name};
    $response->[TO]   = $path;
    my $read = sysread $fh, $buffer, BUFSIZ;
    die if ( not defined $read );
    my $next_offset = $offset + $read;

    if ( $read > 0
        and ( $next_offset <= $self->{last_commit_offset} or $broker_id ) )
    {
        delete $self->{waiting}->{$to};
        $response->[ID] = join q(:), $offset, $next_offset;
        $response->[PAYLOAD] = $buffer;
        $node->fill($response);
        $offset = $next_offset;
    }
    else {
        $response->[TYPE] = TM_EOF;
        $response->[ID]   = $offset;
        $node->fill($response);
        if ($broker_id) {
            $self->{in_sync_replicas}->{$broker_id} = $offset;
            $self->{waiting}->{$to} //= $name;
            $self->commit_messages;
        }
    }
    return;
}

sub process_ack {
    my ( $self, $offset, $broker_id ) = @_;
    return if ( not $broker_id );
    my $isr = $self->{replica_offsets};
    my $lco = $offset;
    $isr->{$broker_id} = $offset;

    # cancel messages up to the LCO
    for my $broker_id ( keys %{$isr} ) {
        $lco = $isr->{$broker_id}
            if ( not defined $lco or $lco > $isr->{$broker_id} );
    }
    my $responses = $self->{responses};
    while ( defined $offset and @{$responses} ) {
        last if ( $responses->[0]->[$Offset] > $lco );
        $self->cancel( shift @{$responses} );
    }
    return;
}

sub process_delete {
    my ( $self, $delete, $lco ) = @_;
    return if ( not $self->{filename} );
    my $segments = $self->{segments};
    my $i        = $#{$segments} - $self->{num_segments} + 1;
    my $keep     = $segments->[ $i > 0 ? $i : 0 ];
    return if ( not defined $keep );
    if ( not defined $delete ) {
        return if ( $self->{leader} );

        # make sure the follower doesn't delete more than the leader
        $delete = $keep->[LOG_OFFSET];
        for my $broker_id ( keys %{ $self->{in_sync_replicas} } ) {
            my $message = Tachikoma::Message->new;
            $message->[TYPE]    = TM_INFO;
            $message->[TO]      = $self->{followers}->{$broker_id};
            $message->[PAYLOAD] = join q(), 'DELETE ', $delete, q( ),
                $self->{last_commit_offset}, "\n";
            $self->{sink}->fill($message);
        }
    }
    elsif ( $delete > $keep->[LOG_OFFSET] ) {
        $delete = $keep->[LOG_OFFSET];
    }
    while ( $self->should_delete( $delete, $segments ) ) {
        $self->unlink_segment( shift @{$segments} );
    }
    if ( @{$segments} ) {
        my $last_modified = ( stat $segments->[-1]->[LOG_FH] )[9];
        $self->touch_files
            if ( $Tachikoma::Now - $last_modified > $Touch_Interval );
    }
    else {
        $self->stderr('WARNING: process_delete removed all segments');
        $self->create_segment;
    }
    $self->update_offsets($lco);
    return;
}

sub empty_partition {
    my $self = shift;
    return if ( not $self->{filename} );
    while ( my $segment = shift @{ $self->{segments} } ) {
        $self->unlink_segment($segment);
    }
    $self->create_segment;
    if ( $self->{leader} ) {
        $self->update_offsets( $self->{last_truncate_offset} );
    }
    else {
        $self->update_offsets( $self->{last_commit_offset} );
    }
    return;
}

sub should_delete {
    my ( $self, $delete, $segments ) = @_;
    my $rv = undef;

    # the most recent segment might be empty, so keeping at least
    # two guarantees cache partitions will always have data
    if ( @{$segments} > 2 ) {
        my $segment = $segments->[0];
        if ( $segment->[LOG_OFFSET] + $segment->[LOG_SIZE] <= $delete ) {
            $rv = 1;
        }
        else {
            my $last_modified = ( stat $segment->[LOG_FH] )[9];
            $rv = 1
                if ($self->{segment_lifespan}
                and $Tachikoma::Now - $last_modified
                > $self->{segment_lifespan} );
        }
    }
    return $rv;
}

sub unlink_segment {
    my $self    = shift;
    my $segment = shift;
    my $path    = $self->{filename} or return;
    flock $segment->[LOG_FH], LOCK_UN or die "ERROR: couldn't unlock: $!";
    if ( $segment->[LOG_FH] ) {
        close $segment->[LOG_FH] or die "ERROR: couldn't close: $!";
    }
    my $log_file = join q(), $path, q(/), $segment->[LOG_OFFSET], '.log';
    if ( not unlink $log_file ) {
        $self->stderr("WARNING: couldn't unlink $log_file: $!");
    }
    return;
}

sub update_offsets {
    my ( $self, $lco ) = @_;
    my $offsets_dir = join q(/), $self->{filename}, 'offsets';
    return if ( not -d $offsets_dir );
    my $lowest_offset = $self->{segments}->[0]->[LOG_OFFSET];
    my $valid_offsets = $self->{valid_offsets};
    my $dh            = undef;
    opendir $dh, $offsets_dir
        or die "ERROR: couldn't opendir $offsets_dir: $!";
    my @offsets = sort { $a <=> $b } grep m{^[^.]}, readdir $dh;
    closedir $dh or die "ERROR: couldn't closedir $offsets_dir: $!";

    while ( @offsets > $Num_Offsets
        or ( @offsets and $offsets[0] < $lowest_offset ) )
    {
        my $old_offset  = shift @offsets;
        my $offset_file = "$offsets_dir/$old_offset";
        unlink $offset_file
            or die "ERROR: couldn't unlink $offset_file: $!";
    }
    shift @{$valid_offsets}
        while ( @{$valid_offsets} and $valid_offsets->[0] < $lowest_offset );
    if ( $self->{leader} ) {

        # don't write offsets received from the leader until log catches up
        while ( @{$valid_offsets} and $valid_offsets->[0] < $self->{offset} )
        {
            my $offset = shift @{$valid_offsets};
            my $new_file = join q(/), $self->{filename}, 'offsets', $offset;
            next if ( -e $new_file );
            my $fh = undef;
            open $fh, '>', $new_file
                or die "ERROR: couldn't open $new_file: $!";
            close $fh
                or die "ERROR: couldn't close $new_file: $!";
        }

        # synchronize and preserve valid offset
        $self->{last_truncate_offset} = $lco;
    }
    else {
        $self->{last_truncate_offset} = $self->{last_commit_offset};
    }
    return;
}

sub reset_follower {
    my $self   = shift;
    my $offset = shift;
    if ( $self->{offset} > 0 ) {
        $self->stderr( "WARNING: batch $offset != my ", $self->{offset} );
        $self->purge_offsets(-1);
        $self->open_segments(-1);
        $self->restart_follower;
        return;
    }
    else {
        $self->stderr("starting new log from $offset");
        $self->purge_offsets(-1);
        $self->open_segments(-1);
        $self->{offset} = $offset;
        $self->create_segment;
        $self->set_timer( 0, 'oneshot' );
    }
    return;
}

sub restart_leader {
    my $self = shift;
    return if ( not $self->{filename} );
    $self->open_segments( $self->get_last_commit_offset );
    $self->create_segment if ( not @{ $self->{segments} } );
    return;
}

sub restart_follower {
    my $self = shift;
    return if ( not $self->{filename} );
    $self->{last_commit_offset} = -1;
    $self->{offset}             = 0;
    $self->set_timer;
    return;
}

sub purge_offsets {
    my $self            = shift;
    my $truncate_offset = shift;
    my $offsets_dir     = join q(/), $self->{filename}, 'offsets';
    my @offsets         = ();
    if ( -d $offsets_dir ) {
        my $dh = undef;
        opendir $dh, $offsets_dir
            or die "ERROR: couldn't opendir $offsets_dir: $!";
        @offsets = sort { $a <=> $b } grep m{^[^.]}, readdir $dh;
        closedir $dh or die "ERROR: couldn't closedir $offsets_dir: $!";
        while ( @offsets and $offsets[-1] > $truncate_offset ) {
            my $old_offset  = pop @offsets;
            my $offset_file = "$offsets_dir/$old_offset";
            unlink $offset_file
                or die "ERROR: couldn't unlink $offset_file: $!";
            $self->stderr("DEBUG: unlinked $offset_file");
        }
    }
    return \@offsets;
}

sub open_segments {
    my $self               = shift;
    my $last_commit_offset = shift;
    my $dh                 = undef;
    my $path               = $self->{filename};
    $self->close_segments;
    $self->make_dirs( join q(/), $path, 'offsets' );
    opendir $dh, $path or die "ERROR: couldn't opendir $path: $!";
    my @unsorted = ();

    for my $readdir_file ( readdir $dh ) {
        my $file = ( $readdir_file =~ m{^(.*)$} )[0];
        $file =~ m{^(\d+)[.]log$} or next;
        my $offset = $1;
        if ( $offset > $last_commit_offset ) {
            $self->stderr("WARNING: unlinking $path/$file");
            unlink "$path/$file"
                or die "ERROR: couldn't unlink $path/$file: $!";
            next;
        }
        my $size = ( stat "$path/$file" )[7];
        my $fh   = undef;
        open $fh, '+<', "$path/$file"
            or die "ERROR: couldn't open $path/$file: $!";
        $self->get_lock($fh);
        my $new_size = $last_commit_offset - $offset;
        if ( $new_size < $size ) {
            $self->stderr(
                'WARNING: truncating ' . ( $size - $new_size ) . ' bytes' );
            $size = $new_size;
            truncate $fh, $size or die "ERROR: couldn't truncate: $!";
            sysseek $fh, 0, SEEK_END or die "ERROR: couldn't seek: $!";
        }
        push @unsorted, [ $offset, $size, $fh ];
    }
    closedir $dh or die "ERROR: couldn't closedir $path: $!";
    $self->{segments} = [ sort { $a->[0] <=> $b->[0] } @unsorted ];
    my $segment = $self->{segments}->[-1];
    if ($segment) {
        $self->{offset} = $segment->[LOG_OFFSET] + $segment->[LOG_SIZE];
    }
    else {
        $self->{offset} = 0;
    }
    $self->{last_truncate_offset} = $self->{offset};
    if ( not $self->{leader}
        and $self->{offset} < $last_commit_offset )
    {
        $self->stderr('WARNING: offset < last_commit_offset');
    }
    return;
}

sub close_segments {
    my $self = shift;
    if ( @{ $self->{segments} } ) {
        for my $segment ( @{ $self->{segments} } ) {
            flock $segment->[LOG_FH], LOCK_UN
                or die "ERROR: couldn't unlock: $!";
            close $segment->[LOG_FH] or die "ERROR: couldn't close: $!";
        }
    }
    $self->{segments} = [];
    return;
}

sub create_segment {
    my $self   = shift;
    my $path   = $self->{filename};
    my $offset = $self->{offset};
    my $fh     = undef;
    open $fh, '>', "$path/$offset.log"
        or die "ERROR: couldn't open $path/$offset.log: $!";
    close $fh
        or die "ERROR: couldn't close $path/$offset.log: $!";
    open $fh, '+<', "$path/$offset.log"
        or die "ERROR: couldn't open $path/$offset.log: $!";
    $self->get_lock($fh);
    push @{ $self->{segments} }, [ $offset, 0, $fh ];
    $self->process_delete if ( $self->{arguments} );
    return;
}

sub touch_files {
    my $self     = shift;
    my $path     = $self->{filename} or return;
    my $log_file = join q(), $path, q(/),
        $self->{segments}->[-1]->[LOG_OFFSET], '.log';
    my $offset_file = join q(/), $path, 'offsets',
        $self->{last_commit_offset};
    utime $Tachikoma::Now, $Tachikoma::Now, $log_file
        or $self->stderr("ERROR: couldn't utime $log_file: $!");
    utime $Tachikoma::Now, $Tachikoma::Now, $offset_file
        or $self->stderr("ERROR: couldn't utime $offset_file: $!");
    return;
}

sub purge_tree {
    my $self = shift;
    my $path = shift;
    $path //= $self->{filename} if ( ref $self );
    return if ( not $path or not -d $path );
    my @filenames = ();
    my $dh        = undef;
    if ( opendir $dh, $path ) {
        @filenames = grep m{^[^.]}, readdir $dh;
        closedir $dh or $self->stderr("ERROR: couldn't closedir $path: $!");
    }
    ## no critic (RequireCheckedSyscalls)
    for my $filename (@filenames) {
        if ( -d "$path/$filename" ) {
            Tachikoma::Nodes::Partition->purge_tree("$path/$filename");
        }
        else {
            unlink "$path/$filename";
        }
    }
    rmdir $path;
    return;
}

sub get_last_commit_offset {
    my $self = shift;
    return if ( not $self->{filename} );
    my $offsets_dir        = join q(/), $self->{filename}, 'offsets';
    my $last_commit_offset = undef;
    my $valid_offsets      = [];
    if ( -d $offsets_dir ) {
        my $dh   = undef;
        my $path = $self->{filename};
        opendir $dh, $path or die "ERROR: couldn't opendir $path: $!";
        my $log_offset = 0;
        for my $file ( readdir $dh ) {
            $file =~ m{^(\d+)[.]log$} or next;
            my $offset = $1;
            my $size   = ( stat "$path/$file" )[7];
            $log_offset = $offset + $size
                if ( $offset + $size > $log_offset );
        }
        closedir $dh or die "ERROR: couldn't closedir $path: $!";
        $valid_offsets      = $self->purge_offsets($log_offset);
        $last_commit_offset = $valid_offsets->[-1];
    }
    else {
        $self->make_dirs($offsets_dir);
    }
    if ( not defined $last_commit_offset ) {
        $last_commit_offset = 0;
        $valid_offsets      = [0];
        my $new_file = join q(/), $offsets_dir, $last_commit_offset;
        my $fh = undef;
        open $fh, '>', $new_file
            or die "ERROR: couldn't open $new_file: $!\n";
        close $fh or die "ERROR: couldn't close $new_file: $!";
    }
    $self->{last_commit_offset} = $last_commit_offset;
    $self->{valid_offsets}      = $valid_offsets;
    return $last_commit_offset;
}

sub get_lock {
    my $self = shift;
    my $fh   = shift;
    if ( not flock $fh, LOCK_EX | LOCK_NB ) {
        $self->stderr("WARNING: flock: $!\n");
        $self->remove_node;
        my $broker = $Tachikoma::Nodes{broker};
        $broker->rebalance_partitions('inform_brokers') if ($broker);
    }
    return;
}

sub get_segment {
    my $self    = shift;
    my $offset  = shift;
    my $segment = undef;
    if ( $offset < 0 ) {
        $segment = $self->{segments}->[-1];
    }
    else {
        for my $this ( @{ $self->{segments} } ) {
            if ( $this->[LOG_OFFSET] > $offset ) {
                $segment //= $this;
                last;
            }
            $segment = $this;
        }
    }
    return $segment;
}

sub send_error {
    my $self     = shift;
    my $message  = shift;
    my $error    = shift;
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_ERROR;
    $response->[FROM]    = $self->{name};
    $response->[TO]      = $message->[FROM];
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = $error;
    chomp $error;
    $self->stderr( "DEBUG: $error for ", $message->[FROM] )
        if ( $error ne 'NOT_AVAILABLE' );
    return $self->{sink}->fill($response);
}

sub halt {
    my $self = shift;
    $self->{status} = 'HALT';
    return;
}

sub remove_node {
    my $self = shift;
    $self->close_segments if ( $self->{segments} );
    $self->{filename} = undef;
    return $self->SUPER::remove_node;
}

# follower support
sub get_valid_offsets {
    my $self    = shift;
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_INFO;
    $message->[FROM]    = $self->{name};
    $message->[TO]      = $self->{leader_path};
    $message->[PAYLOAD] = join q(), 'GET_VALID_OFFSETS 0 ',
        $self->{broker_id} // $self->{name}, "\n";
    $self->{expecting} = $Tachikoma::Now;
    $self->{sink}->fill($message);
    return;
}

sub get_batch {
    my $self    = shift;
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_INFO;
    $message->[FROM]    = $self->{name};
    $message->[TO]      = $self->{leader_path};
    $message->[PAYLOAD] = join q(), 'GET ', $self->{offset} // 0, q( ),
        $self->{broker_id} // $self->{name},
        "\n";
    $self->{expecting} = $Tachikoma::Now;
    $self->{sink}->fill($message);
    return;
}

sub send_ack {
    my $self    = shift;
    my $offset  = shift // 0;
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_INFO;
    $message->[FROM]    = $self->{name};
    $message->[TO]      = $self->{leader_path};
    $message->[PAYLOAD] = join q(), 'ACK ', $offset, q( ),
        $self->{broker_id} // $self->{name},
        "\n";
    $self->{sink}->fill($message);
    return;
}

# shared support
sub filename {
    my $self = shift;
    if (@_) {
        $self->{filename} = shift;
    }
    return $self->{filename};
}

sub num_segments {
    my $self = shift;
    if (@_) {
        $self->{num_segments} = shift;
        if ( $self->{num_segments} < 2 ) {
            $self->stderr('WARNING: num_segments must be >= 2');
            $self->{num_segments} = 2;
        }
    }
    return $self->{num_segments};
}

sub segment_size {
    my $self = shift;
    if (@_) {
        $self->{segment_size} = shift;
    }
    return $self->{segment_size};
}

sub segment_lifespan {
    my $self = shift;
    if (@_) {
        $self->{segment_lifespan} = shift;
    }
    return $self->{segment_lifespan};
}

sub status {
    my $self = shift;
    if (@_) {
        $self->{status} = shift;
    }
    return $self->{status};
}

sub leader {
    my $self = shift;
    if (@_) {
        my $leader = shift;
        $self->{leader}           = $leader;
        $self->{followers}        = {};
        $self->{in_sync_replicas} = {};
        $self->{replica_offsets}  = {};
        if ($leader) {
            my $name = $self->{name};
            $self->{leader_path} = "$leader/$name";
        }
        else {
            $self->{leader_path} = undef;
        }
    }
    return $self->{leader};
}

sub leader_path {
    my $self = shift;
    if (@_) {
        $self->{leader_path} = shift;
    }
    return $self->{leader_path};
}

sub followers {
    my $self = shift;
    if (@_) {
        $self->{followers} = shift;
    }
    return $self->{followers};
}

sub in_sync_replicas {
    my $self = shift;
    if (@_) {
        $self->{in_sync_replicas} = shift;
        $self->{leader}           = undef;
        $self->{leader_path}      = undef;
        $self->{replica_offsets}  = {};
    }
    return $self->{in_sync_replicas};
}

sub replica_offsets {
    my $self = shift;
    if (@_) {
        $self->{replica_offsets} = shift;
    }
    return $self->{replica_offsets};
}

sub segments {
    my $self = shift;
    if (@_) {
        $self->{segments} = shift;
    }
    return $self->{segments};
}

sub last_commit_offset {
    my $self = shift;
    if (@_) {
        $self->{last_commit_offset} = shift;
    }
    return $self->{last_commit_offset};
}

sub last_truncate_offset {
    my $self = shift;
    if (@_) {
        $self->{last_truncate_offset} = shift;
    }
    return $self->{last_truncate_offset};
}

sub valid_offsets {
    my $self = shift;
    if (@_) {
        $self->{valid_offsets} = shift;
    }
    return $self->{valid_offsets};
}

sub offset {
    my $self = shift;
    if (@_) {
        $self->{offset} = shift;
    }
    return $self->{offset};
}

sub responses {
    my $self = shift;
    if (@_) {
        $self->{responses} = shift;
    }
    return $self->{responses};
}

sub waiting {
    my $self = shift;
    if (@_) {
        $self->{waiting} = shift;
    }
    return $self->{waiting};
}

sub batch {
    my $self = shift;
    if (@_) {
        $self->{batch} = shift;
    }
    return $self->{batch};
}

# follower support
sub expecting {
    my $self = shift;
    if (@_) {
        $self->{expecting} = shift;
    }
    return $self->{expecting};
}

sub broker_id {
    my $self = shift;
    if (@_) {
        $self->{broker_id} = shift;
    }
    return $self->{broker_id};
}

1;
