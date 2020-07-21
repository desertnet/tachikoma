#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Jobs::DirCheck
# ----------------------------------------------------------------------
#
# $Id: DirCheck.pm 415 2008-12-24 21:08:33Z chris $
#

package Tachikoma::Jobs::DirCheck;
use strict;
use warnings;
use Tachikoma::Job;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_PING TM_PERSIST TM_RESPONSE
);
use Digest::MD5;
use File::Path qw( remove_tree );
use parent qw( Tachikoma::Job );

use version; our $VERSION = qv('v2.0.368');

# my $Separator   = chr 0;
my $Separator   = join q(), chr 30, q( -> ), chr 30;
my %Dot_Include = map { $_ => 1 } qw(
    .htaccess
    .svn
    .git
);

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self    = shift;
    my $message = shift;
    if ( $message->[TYPE] & TM_PING ) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE] = TM_RESPONSE;
        return $self->SUPER::fill($response);
    }
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    my ( $relative, $stats ) = split m{\n}, $message->payload, 2;
    chomp $relative;
    return $self->stderr("ERROR: bad path: $relative")
        if ( $relative =~ m{^[.][.]$|^[.][.]/|/[.][.](?=/)|/[.][.]$} );
    my ( $prefix, $delete_threshold, $mode ) =
        split q( ), $self->{arguments}, 3;
    $mode ||= 'update';
    my $should_delete = ( $mode eq 'update' ) ? $delete_threshold : undef;
    $delete_threshold ||= 43200;
    my $my_path = join q(/), $prefix, $relative;
    $my_path =~ s{/+$}{};
    my $message_to = $message->[FROM];
    $message_to =~ s{/DirStats:tee}{};

    if ( $relative eq '.intent' ) {
        my $fh;
        my $payload = q();
        if ( open $fh, q(<), $my_path ) {
            $payload .= $_ while (<$fh>);
            close $fh or $self->stderr("ERROR: couldn't close $my_path: $!");
        }
        else {
            $payload = "couldn't open $my_path: $!";
        }
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_PERSIST | TM_RESPONSE;
        $response->[TO]      = $message_to;
        $response->[ID]      = $message->[ID];
        $response->[STREAM]  = $message->[STREAM];
        $response->[PAYLOAD] = $payload;
        return $self->SUPER::fill($response);
    }
    my %other = ();
    for my $line ( split m{\n}, $stats ) {
        my ( $stat, $size, $perms, $last_modified, $digest, $entry ) =
            ( split m{\s}, $line, 6 );
        my $link;
        ( $entry, $link ) = split $Separator, $entry, 2
            if ( $stat eq 'L' );
        $other{$entry} =
            [ $stat, $size, $perms, $last_modified, $digest, $link ];
    }
    my $dh;
    if ( not opendir $dh, $my_path ) {
        if ( $! =~ m{No such file or directory|Not a directory} ) {
            if ( $mode eq 'update' and $! =~ m{Not a directory} ) {
                $self->print_less_often( 'removing ', $my_path );
                unlink $my_path
                    or $self->stderr("ERROR: couldn't remove $my_path: $!");
            }
            for my $entry ( keys %other ) {
                my $their_path_entry = join q(/), $relative, $entry;
                my $response         = Tachikoma::Message->new;
                $response->[TYPE]    = TM_BYTESTREAM;
                $response->[TO]      = $message_to;
                $response->[PAYLOAD] = join q(), 'update:',
                    $their_path_entry, "\n";
                $self->SUPER::fill($response);
            }
        }
        else {
            $self->stderr("ERROR: couldn't opendir $my_path: $!");
        }
        return $self->cancel($message);
    }
    my $recent  = $message->[TIMESTAMP] - $delete_threshold;
    my @entries = readdir $dh;
    closedir $dh or $self->stderr("ERROR: couldn't closedir $my_path: $!");
    my %checked = ();
    for my $entry (@entries) {
        my $my_path_entry = join q(/), $my_path, $entry;
        my @lstat         = lstat $my_path_entry;
        next if ( not @lstat );
        my $last_modified = $lstat[9];
        if ( $entry =~ m{^[.]} and not $Dot_Include{$entry} ) {
            if (    $entry =~ m{^[.]temp-\w{16}$}
                and $mode eq 'update'
                and $Tachikoma::Now - $last_modified > 3600 )
            {
                $self->stderr("unlinking stale temp file: $my_path_entry");
                unlink $my_path_entry
                    or $self->stderr(
                    "ERROR: couldn't remove $my_path_entry: $!");
            }
            next;
        }
        my $stat = ( -l _ )         ? 'L'       : ( -d _ ) ? 'D' : 'F';
        my $size = ( $stat eq 'F' ) ? $lstat[7] : q(-);
        my $perms         = sprintf '%04o', $lstat[2] & 07777;
        my $other_entry   = $other{$entry};
        my $their_stat    = $other_entry ? $other_entry->[0] : q();
        my $their_size    = $other_entry ? $other_entry->[1] : q(-);
        my $their_perms   = $other_entry ? $other_entry->[2] : q();
        my $my_is_dir     = ( $stat eq 'D' ) ? 1 : 0;
        my $theirs_is_dir = ( $their_stat eq 'D' ) ? 1 : 0;
        my $digest        = q(-);
        my $theirs_exists = exists $other{$entry};

        if ( not $theirs_exists or $my_is_dir != $theirs_is_dir ) {
            if ( $last_modified > $recent ) {
                $checked{$entry} = 1;
                next;
            }
            if ( not $should_delete ) {
                if ( not $theirs_exists ) {
                    $self->print_less_often( 'WARNING: possible orphan: ',
                        $my_path_entry )
                        if ($my_path_entry !~ m{.svn/[^/]+$}
                        and $my_path_entry !~ m{/deleted/} );
                }
                else {
                    $self->stderr("WARNING: type mismatch: $my_path_entry");
                    $checked{$entry} = 1;
                }
                next;
            }
            $self->print_less_often( 'removing ', $my_path_entry );
            if ($my_is_dir) {
                my $errors = [];
                remove_tree( $my_path_entry, { error => \$errors } );
                if ( @{$errors} ) {
                    $self->stderr( "ERROR: couldn't remove $my_path_entry: ",
                        values %{ $errors->[0] } );
                }
            }
            else {
                unlink $my_path_entry
                    or $self->stderr(
                    "ERROR: couldn't remove $my_path_entry: $!");
            }
        }
        elsif ( $last_modified > $other_entry->[3] ) {
            $checked{$entry} = 1;
        }
        elsif ( $their_stat eq $stat
            and ( $theirs_is_dir or $their_size eq $size ) )
        {
            if ( $stat eq 'L' ) {
                my $my_link    = readlink $my_path_entry;
                my $their_link = $other_entry->[5];
                next if ( $my_link ne $their_link );
            }
            next if ( $their_perms ne $perms );
            next
                if ($mode eq 'update'
                and $last_modified < $other_entry->[3] );
            my $their_digest = $other_entry->[4];
            if ( $stat eq 'F' and $their_digest ne q(-) ) {
                my $md5 = Digest::MD5->new;
                if ( open my $fh, q(<), $my_path_entry ) {
                    $md5->addfile($fh);
                    $digest = $md5->hexdigest;
                    close $fh
                        or $self->stderr(
                        "ERROR: couldn't close $my_path_entry: $!");
                }
                else {
                    $self->stderr("ERROR: couldn't open $my_path_entry: $!");
                    $digest = $their_digest;
                }
            }
            next if ( $their_digest ne $digest );
            $checked{$entry} = 1;
        }
    }
    for my $entry ( keys %other ) {
        next if ( $checked{$entry} );
        my $their_path_entry =
            $relative
            ? join q(/), $relative, $entry
            : $entry;
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_BYTESTREAM;
        $response->[TO]      = $message_to;
        $response->[PAYLOAD] = join q(), 'update:', $their_path_entry, "\n";
        $self->SUPER::fill($response);
    }
    return $self->cancel($message);
}

1;
