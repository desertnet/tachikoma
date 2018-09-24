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

my @Commands = qw(
    get mget count set remove expire
);

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my %arguments = split( ' ', $self->{arguments} );
        my $table = $arguments{table};
        delete( $arguments{table} );
        $self->{table}                       = $table;
        $self->{dbh}                         = DBI->connect(%arguments);
        $self->{dbh}->{mysql_auto_reconnect} = 1;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $type    = $message->[TYPE];
    if ( $type & TM_BYTESTREAM ) {
        return if ( $message->[PAYLOAD] !~ m(\S) );
        my ( $command, $arguments ) = split( ' ', $message->[PAYLOAD], 2 );
        chomp($arguments);
        if ( grep { $command eq $_ } @Commands ) {
            eval { $self->$command( $arguments, $message ) };
            return $self->stderr("unexpected error: $@") if ($@);
        }
        else {
            return $self->run_generic_SQL($message);
        }
        return 1;
    }
    elsif ( $type & TM_STORABLE ) {
        my $dbh    = $self->{dbh};
        my $table  = $self->table or die "ERROR: no table specified";
        my $object = $message->payload;
        if ( not $object->{_fetched} ) {
            if ( $object->{_objid} ) {
                my $row = $self->fetch_row( $object->{_objid} );
                $self->send_object( $row, $message );
            }
            else {
                $self->stderr("no objid specified");
                $self->cancel($message);
            }
            return 1;
        }
        else {
            my $write = undef;
            if ( $object->{_default} ) {
                $write ||= $self->fetch_row( $object->{_objid} ) || {};
                for my $field ( @{ $object->{_default} } ) {
                    my $new_value = $object->{$field};
                    next
                        if ( not defined $new_value
                        or defined $write->{$field} );
                    $write->{$field} = $new_value;
                }
            }
            if ( $object->{_fields} ) {
                $write ||= $self->fetch_row( $object->{_objid} ) || {};
                $write->{$_} = $object->{$_} for ( @{ $object->{_fields} } );
            }
            if ( $object->{_update} ) {
                $write ||= $self->fetch_row( $object->{_objid} ) || {};
                if ($write) {
                    $write->{$_} = $object->{$_}
                        for ( @{ $object->{_update} } );
                }
            }
            else {
                $write ||= $object;
            }
            if ($write) {
                my $field_settings =
                    join( ' ', map qq($_=?), sort keys %$write );
                my $statement = qq(
                    UPDATE $table
                       SET $field_settings
                     WHERE _objid=?
                );
                my $update = $dbh->prepare($statement);
                if ( not $update ) {
                    die "ERROR: " . $dbh->errstr . "\n";
                }
                $update->execute( map $write->{$_}, sort keys %$write )
                    or die "ERROR: " . $dbh->errstr . "\n";
            }
            $object->{_stored} = 'true';
            $message->[TO] = $message->[FROM]
                if ( $message->[TO] eq '_return_to_sender' );
            $self->SUPER::fill($message);
            return 1;
        }
    }
    else {
        return $self->SUPER::fill($message);
    }
    return $self->cancel($message);
}

sub run_generic_SQL {
    my $self      = shift;
    my $message   = shift;
    my $dbh       = $self->{dbh};
    my $statement = $message->[PAYLOAD];
    chomp($statement);
    $self->{counter}++;
    if ( $statement =~ m(^SELECT|^SHOW|^DESCRIBE)i ) {
        my $count = 0;
        my $sth   = $dbh->prepare($statement);
        if ( not $sth or not $sth->execute ) {
            my $response = Tachikoma::Message->new;
            $response->[TYPE] = TM_ERROR;
            $response->[TO]   = $message->[FROM]
                if ( $message->[TO] eq '_return_to_sender' );
            $response->[PAYLOAD] = join( '', 'ERROR: ', $dbh->errstr, "\n" );
            return $self->SUPER::fill($response);
        }
        while ( my $row = $sth->fetchrow_hashref ) {
            my $response = Tachikoma::Message->new;
            $response->[TYPE] = TM_STORABLE;
            $response->[TO]   = $message->[FROM]
                if ( $message->[TO] eq '_return_to_sender' );
            $response->[PAYLOAD] = $row;
            $self->SUPER::fill($response);
            $count++;
        }
        my $response = Tachikoma::Message->new;
        $response->[TYPE] = TM_INFO;
        $response->[TO]   = $message->[FROM]
            if ( $message->[TO] eq '_return_to_sender' );
        $response->[PAYLOAD] = join( '',
            $count, ' rows in ', Time::HiRes::time - $Tachikoma::Right_Now,
            " seconds\n" );
        $self->SUPER::fill($response);
    }
    else {
        if ( not $dbh->do($statement) ) {
            my $response = Tachikoma::Message->new;
            $response->[TYPE] = TM_ERROR;
            $response->[TO]   = $message->[FROM]
                if ( $message->[TO] eq '_return_to_sender' );
            $response->[PAYLOAD] = join( '', 'ERROR: ', $dbh->errstr, "\n" );
            return $self->SUPER::fill($response);
        }
        if ( $message->[TO] ) {
            my $response = Tachikoma::Message->new;
            $response->[TYPE] = TM_INFO;
            $response->[TO]   = $message->[FROM]
                if ( $message->[TO] eq '_return_to_sender' );
            $response->[PAYLOAD] = join( '',
                lc( ( split( ' ', $statement, 2 ) )[0] ),
                ' took ', Time::HiRes::time - $Tachikoma::Right_Now,
                " seconds\n" );
            $self->SUPER::fill($response);
        }
    }
    return;
}

sub fetch_row {
    my $self  = shift;
    my $objid = shift;
    my $dbh   = $self->dbh;
    my $table = $self->table or die "ERROR: no table specified";
    my $sth   = $dbh->prepare(
        qq(
        SELECT *
          FROM $table
         WHERE _objid=?
    )
    );
    if ( not $sth or not $sth->execute($objid) ) {
        die "ERROR: " . $dbh->errstr . "\n";
    }
    return $sth->fetchrow_hashref;
}

sub get {
    my $self      = shift;
    my $arguments = shift;
    my $message   = shift;
    my $dbh       = $self->dbh;
    my $table     = $self->table or die "ERROR: no table specified";
    my ( $distinct, $fields, $query_string, $limit ) = (
        $arguments =~ m(
            ^ (?:(distinct) \s+)? (.*?)
              (?:\s+ where \s+ (.+?))?
              (?:\s+ limit \s+ (\d+) \s*)?
            $
        )sx
    );
    $fields       ||= '*';
    $query_string ||= '_objid not eq ""';
    $limit        ||= 1000;
    my @fields = split( m(,\s*), $fields );
    my $sth = $dbh->prepare(
        qq(
        SELECT $fields
          FROM $table
         WHERE $query_string
         LIMIT $limit
    )
    );

    if ( not $sth or not $sth->execute ) {
        die "ERROR: " . $dbh->errstr . "\n";
    }
    my $results = [];
    my $count   = 0;
    my $to      = $message->[TO];
    $to = $message->[FROM] if ( $to eq '_return_to_sender' );
    if ( $fields eq '_objid' ) {
        while ( my $row = $sth->fetchrow_hashref ) {
            push( @$results, $row->{_objid} );
        }
        $count = scalar(@$results);
        while ( my @segment = splice( @$results, 0, 256 ) ) {
            my $response = Tachikoma::Message->new;
            $response->[TYPE]    = TM_BYTESTREAM;
            $response->[TO]      = $to;
            $response->[PAYLOAD] = join( '', join( ' ', @segment ), "\n" );
            $self->SUPER::fill($response) or return;
        }
    }
    else {
        while ( my $row = $sth->fetchrow_hashref ) {
            push( @$results, $row );
        }
        while ( my @segment = splice( @$results, 0, 256 ) ) {
            for my $object (@segment) {
                if ($fields) {
                    $self->send_bytestream( $object, \@fields, $message )
                        or return;
                    $count++;
                }
                else {
                    $self->send_object( $object, $message )
                        or return;
                    $count++;
                }
            }
        }
    }
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_INFO;
    $response->[TO]      = $to;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = sprintf( "%d results in %.4f seconds\n",
        $count, Time::HiRes::time - $Tachikoma::Right_Now );
    $self->SUPER::fill($response) or return;
    return $count;
}

sub mget {
    my $self      = shift;
    my $arguments = shift;
    my $message   = shift;
    my $objids    = join( '", "', split( ' ', $arguments ) );
    my $dbh       = $self->dbh;
    my $table     = $self->table or die "ERROR: no table specified";
    my $sth       = $dbh->prepare(
        qq(
        SELECT *
          FROM $table
         WHERE _objid IN ("$objids")
    )
    );

    if ( not $sth or not $sth->execute ) {
        die "ERROR: " . $dbh->errstr . "\n";
    }
    my $results = [];
    my $count   = 0;
    my $to      = $message->[TO];
    $to = $message->[FROM] if ( $to eq '_return_to_sender' );
    while ( my $object = $sth->fetchrow_hashref ) {
        next if ( not keys %$object );
        $self->send_object( $object, $message )
            or return;
        $count++;
    }
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_INFO;
    $response->[TO]      = $to;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = sprintf( "%d results in %.4f seconds\n",
        $count, Time::HiRes::time - $Tachikoma::Right_Now );
    $self->SUPER::fill($response) or return;
    return $count;
}

sub count {
    my $self         = shift;
    my $arguments    = shift;
    my $message      = shift;
    my $dbh          = $self->dbh;
    my $table        = $self->table or die "ERROR: no table specified";
    my $query_string = ( $arguments =~ m(^where\s+(.+)$)s )[0];
    $query_string ||= '_objid not eq ""';
    my $sth = $dbh->prepare(
        qq(
        SELECT count(*)
          FROM $table
         WHERE $query_string
    )
    );

    if ( not $sth or not $sth->execute ) {
        die "ERROR: " . $dbh->errstr . "\n";
    }
    my $results = $sth->fetchrow_arrayref;
    my $count   = $results ? $results->[0] : 0;
    my $to      = $message->[TO];
    $to = $message->[FROM] if ( $to eq '_return_to_sender' );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM | TM_INFO;
    $response->[TO]      = $to;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = sprintf( "%d matches in %.4f seconds\n",
        $count, Time::HiRes::time - $Tachikoma::Right_Now );
    return $self->SUPER::fill($response);
}

sub set {
    my $self      = shift;
    my $arguments = shift;
    my $message   = shift;
    my $dbh       = $self->dbh;
    my $table     = $self->table or die "ERROR: no table specified";
    my ( $field, $value, $query_string ) = (
        $arguments =~ m(
            ^ (\S+)="([^"]*)"
              (?:\s+ where \s+ (.+?))?
            $
        )sx
    );
    die "syntax error" if ( not $field );
    $query_string ||= '_objid not eq ""';
    my $update = $dbh->prepare(
        qq(
        UPDATE $table
           SET $field=?
         WHERE $query_string
    )
    );

    if ( not $update or not $update->execute($value) ) {
        die "ERROR: " . $dbh->errstr . "\n";
    }
    my $results = $update->fetchrow_arrayref;
    my $count   = $results ? ( $results->[0] =~ m((\d+)) )[0] || 0 : 0;
    my $to      = $message->[TO];
    $to = $message->[FROM] if ( $to eq '_return_to_sender' );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM | TM_INFO;
    $response->[TO]      = $to;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join( '',
        $count,
        ' matches updated in ',
        Time::HiRes::time - $Tachikoma::Right_Now,
        " seconds\n" );
    return $self->SUPER::fill($response);
}

sub remove {
    my $self      = shift;
    my $arguments = shift;
    my $message   = shift;
    my $dbh       = $self->dbh;
    my $table     = $self->table or die "ERROR: no table specified";
    my $delete    = undef;
    if ( $arguments =~ m(^where\s+(.+)$)s ) {
        my $query_string = $1;
        $delete = $dbh->prepare(
            qq(
            DELETE FROM $table
                  WHERE $query_string
        )
        );
    }
    else {
        my $objids = join( '", "', split( ' ', $arguments ) );
        $delete = $dbh->prepare(
            qq(
            DELETE FROM $table
                  WHERE _objid IN "$objids"
        )
        );
    }
    if ( not $delete or not $delete->execute ) {
        die "ERROR: " . $dbh->errstr . "\n";
    }
    my $to = $message->[TO];
    $to = $message->[FROM] if ( $to eq '_return_to_sender' );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $to;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join( '',
        'remove took ', Time::HiRes::time - $Tachikoma::Right_Now,
        " seconds\n" );
    return $self->SUPER::fill($response);
}

sub expire {
    my $self      = shift;
    my $arguments = shift;
    my $message   = shift;
    my $dbh       = $self->dbh;
    my $table     = $self->table or die "ERROR: no table specified";
    my ( $field, $time ) = split( ' ', $arguments, 2 );
    $time ||= 0;
    die "no field specified" if ( not $field );
    die "invalid time" if ( $time !~ m(^\d+$) );
    my $query_string = join( '', $field, ' < ', $Tachikoma::Now - $time );
    my $delete = $dbh->prepare(
        qq(
        DELETE FROM $table
              WHERE $query_string
    )
    );

    if ( not $delete or not $delete->execute ) {
        die "ERROR: " . $dbh->errstr . "\n";
    }
    my $to = $message->[TO];
    $to = $message->[FROM] if ( $to eq '_return_to_sender' );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $to;
    $response->[ID]      = $message->[ID];
    $response->[STREAM]  = $message->[STREAM];
    $response->[PAYLOAD] = join( '',
        'expire took ', Time::HiRes::time - $Tachikoma::Right_Now,
        " seconds\n" );
    return $self->SUPER::fill($response);
}

sub send_bytestream {
    my $self    = shift;
    my $object  = shift;
    my $fields  = shift;
    my $message = shift;
    my $to      = $message->[TO];
    $to = $message->[FROM] if ( $to eq '_return_to_sender' );
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[TO]      = $to;
    $response->[PAYLOAD] = join( '',
        map { join( '', $_, ': ', $object->{$_} || '', "\n" ) } @$fields );
    return $self->SUPER::fill($response);
}

sub send_object {
    my $self    = shift;
    my $row     = shift;
    my $message = shift;
    my $object  = {};
    my $to      = $message->[TO];
    $to = $message->[FROM] if ( $to eq '_return_to_sender' );
    $object->{$_} = $row->{$_} for ( keys %$row );
    $object->{_fetched} = 'true';
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_STORABLE;
    $response->[TO]      = $to;
    $response->[PAYLOAD] = $object;
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

sub table {
    my $self = shift;
    if (@_) {
        $self->{table} = shift;
    }
    return $self->{table};
}

1;
