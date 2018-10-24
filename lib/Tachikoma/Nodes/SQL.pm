#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::SQL
# ----------------------------------------------------------------------
#
# $Id: SQL.pm 23016 2015-07-24 21:29:45Z chris $
#

package Tachikoma::Nodes::SQL;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_INFO TM_ERROR
);
use Time::HiRes;
use DBI;
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.368');

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->{dbh}       = DBI->connect( $self->{arguments} );
        $self->{dbh}->{mysql_auto_reconnect} = 1;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    return if ( not $type & TM_BYTESTREAM );
    my $dbh       = $self->{dbh};
    my $statement = $message->[PAYLOAD];
    chomp $statement;
    $self->{counter}++;

    if ( $statement =~ m{^SELECT|^SHOW|^DESCRIBE}i ) {
        my $count = 0;
        my $sth   = $dbh->prepare($statement);
        if ( not $sth or not $sth->execute ) {
            $self->respond( $message, TM_ERROR, join q(), 'ERROR: ',
                $dbh->errstr, "\n" );
            return;
        }
        while ( my $row = $sth->fetchrow_hashref ) {
            $self->respond( $message, TM_STORABLE, $row );
            $count++;
        }
        $self->respond( $message, TM_INFO, join q(),
            $count, ' rows in ', Time::HiRes::time - $Tachikoma::Right_Now,
            " seconds\n" );
    }
    else {
        if ( not $dbh->do($statement) ) {
            $self->respond( $message, TM_ERROR, join q(), 'ERROR: ',
                $dbh->errstr, "\n" );
            return;
        }
        if ( length $message->[TO] ) {
            $self->respond(
                $message,
                TM_INFO,
                join q(),
                ( split q( ), $statement, 2 )[0],
                ' took ',
                Time::HiRes::time - $Tachikoma::Right_Now,
                " seconds\n"
            );
        }
    }
    return;
}

sub respond {
    my $self     = shift;
    my $message  = shift;
    my $type     = shift;
    my $payload  = shift;
    my $response = Tachikoma::Message->new;
    $response->[TYPE] = $type;
    $response->[TO]   = $message->[FROM]
        if ( $message->[TO] eq '_return_to_sender' );
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = $payload;
    return $self->SUPER::fill($response);
}

sub remove_node {
    my $self = shift;
    $self->dbh->disconnect if ( $self->dbh );
    return $self->SUPER::remove_node(@_);
}

sub dbh {
    my $self = shift;
    if (@_) {
        $self->{dbh} = shift;
    }
    return $self->{dbh};
}

1;
