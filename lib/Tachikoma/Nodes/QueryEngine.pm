#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::QueryEngine
# ----------------------------------------------------------------------
#
# $Id: QueryEngine.pm 31247 2017-11-06 05:42:49Z chris $
#

package Tachikoma::Nodes::QueryEngine;
use strict;
use warnings;
use Tachikoma::Nodes::Index;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_BYTESTREAM TM_STORABLE
);
use parent qw( Tachikoma::Nodes::Index );

use version; our $VERSION = 'v2.0.197';

my %Operators = ();

sub help {
    my $self = shift;
    return <<'EOF';
make_node QueryEngine <name> <index1> <index2> ...
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my @indexes = split q( ), $self->{arguments};
        $self->{indexes} = { map { $_ => 1 } @indexes };
    }
    return $self->{arguments};
}

sub fill {
    my ( $self, $message ) = @_;
    my $query = $message->payload;
    if ( $message->[TYPE] & TM_STORABLE ) {
        my $value = eval { $self->execute($query) };
        $value //= { error => $@ };
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = ref($value) ? TM_STORABLE : TM_BYTESTREAM;
        $response->[FROM]    = $self->{name};
        $response->[TO]      = $message->[FROM];
        $response->[ID]      = $message->[ID];
        $response->[STREAM]  = $message->[STREAM];
        $response->[PAYLOAD] = $value;
        $self->{sink}->fill($response);
        $self->{counter}++;
    }
    else {
        $self->stderr( 'ERROR: bad request from: ', $message->[FROM] );
    }
    return;
}

sub execute {
    my ( $self, $query ) = @_;
    my $results = undef;
    if ( ref $query eq 'ARRAY' ) {
        $results = &{ $Operators{'and'} }( $self, { q => $query } );
    }
    elsif ( ref $query eq 'HASH' ) {
        my $op = $query->{op};
        die qq("$op" is not a valid operator\n) if ( not $Operators{$op} );
        $results = &{ $Operators{$op} }( $self, $query );
    }
    $results //= {};
    return $results;
}

$Operators{'and'} = sub {
    my ( $self, $query ) = @_;
    my %rv = ();
    for my $subquery ( @{ $query->{q} } ) {
        my $value = $self->execute($subquery);
        $rv{$_}++ for ( keys %{$value} );
    }
    return \%rv;
};

$Operators{'or'} = sub {
    my ( $self, $query ) = @_;
    my %rv = ();
    for my $subquery ( @{ $query->{q} } ) {
        my $value = $self->execute($subquery);
        $rv{$_} = 1 for ( keys %{$value} );
    }
    return \%rv;
};

$Operators{'eq'} = sub {
    my ( $self, $query ) = @_;
    return $self->lookup( $query->{field}, $query->{key} );
};

$Operators{'ne'} = sub {
    my ( $self, $query ) = @_;
    return $self->lookup( $query->{field}, $query->{key}, 'negate' );
};

$Operators{'re'} = sub {
    my ( $self, $query ) = @_;
    return $self->search( $query->{field}, $query->{key} );
};

$Operators{'nr'} = sub {
    my ( $self, $query ) = @_;
    return $self->search( $query->{field}, $query->{key}, 'negate' );
};

$Operators{'ge'} = sub {
    my ( $self, $query ) = @_;
    return $self->range( $query->{field}, $query->{key}, undef );
};

$Operators{'le'} = sub {
    my ( $self, $query ) = @_;
    return $self->range( $query->{field}, undef, $query->{key} );
};

$Operators{'range'} = sub {
    my ( $self, $query ) = @_;
    return $self->range( $query->{field}, $query->{from}, $query->{to} );
};

$Operators{'keys'} = sub {
    my ( $self, $query ) = @_;
    return $self->get_keys( $query->{field} );
};

sub lookup {
    my ( $self, $field, $key, $negate ) = @_;
    return $self->get_index($field)->lookup( $key, $negate );
}

sub search {
    my ( $self, $field, $key, $negate ) = @_;
    return $self->get_index($field)->search( $key, $negate );
}

sub range {
    my ( $self, $field, $from, $to ) = @_;
    return $self->get_index($field)->range( $from, $to );
}

sub get_keys {
    my ( $self, $field ) = @_;
    return $self->get_index($field)->get_keys;
}

sub get_index {
    my ( $self, $field ) = @_;
    die "no such field: $field\n" if ( not $self->{indexes}->{$field} );
    my $node = $Tachikoma::Nodes{$field};
    die "invalid field: $field\n"
        if ( not $node->isa('Tachikoma::Nodes::Index') );
    return $node;
}

########################
# synchronous interface
########################

sub query {
    my ( $self, $query ) = @_;
    die 'ERROR: no query' if ( not ref $query );
    my $responses   = {};
    my $num_queries = ref $query eq 'ARRAY' ? scalar @{$query} : 1;
    my @results     = ();
    my %join        = ();
    my %offsets     = ();
    my @messages    = ();
    my %counts      = ();
    my $request     = Tachikoma::Message->new;
    $request->type(TM_STORABLE);
    $request->to('QueryEngine');
    $request->payload($query);
    $self->send_request( $request, $responses );

    for my $host_port ( keys %{ $self->{connector} } ) {
        my $tachikoma = $self->{connector}->{$host_port};
        $tachikoma->drain;
        my $payload = $responses->{$host_port};
        if ( $payload->{error} ) {
            push @messages, $payload;
            last;
        }
        elsif ( ref $query eq 'HASH' and $query->{op} eq 'keys' ) {
            $counts{$_} += $payload->{$_} for ( keys %{$payload} );
        }
        else {
            for my $entry ( keys %{$payload} ) {
                $join{$entry} += $payload->{$entry};
            }
        }
    }
    if ( ref $query eq 'HASH' and $query->{op} eq 'keys' ) {
        @messages = \%counts;
    }
    else {
        for my $entry ( keys %join ) {
            next if ( $join{$entry} < $num_queries );
            push @results, $entry;
            $offsets{$entry} = 1;
        }
    }
    $self->{results}  = [ sort @results ];
    $self->{offsets}  = \%offsets;
    $self->{messages} = \@messages;
    return;
}

sub send_request {
    my ( $self, $request, $responses ) = @_;
    $self->{connector} ||= {};
    for my $host ( @{ $self->{hosts} } ) {
        for my $port ( @{ $self->{ports} } ) {
            my $host_port = join q(:), $host, $port;
            my $tachikoma = $self->{connector}->{$host_port};
            if ( not $tachikoma ) {
                $tachikoma =
                    eval { return Tachikoma->inet_client( $host, $port ) };
                next if ( not $tachikoma );
                $self->{connector}->{$host_port} = $tachikoma;
            }
            $tachikoma->callback(
                sub {
                    my $message = shift;
                    if ( $message->type & TM_STORABLE ) {
                        $responses->{$host_port} = $message->payload;
                    }
                    else {
                        die 'ERROR: query failed';
                    }
                    return;
                }
            );
            $tachikoma->fill($request);
        }
    }
    return;
}

sub fetchrow {
    my $self = shift;
    if ( @{ $self->{results} } and not @{ $self->{messages} } ) {
        my $offsets = $self->{offsets};
        while ( my $entry = shift @{ $self->{results} } ) {
            my ( $partition, $offset ) = split m{:}, $entry, 2;
            next if ( not defined $offset or not $offsets->{$entry} );
            $self->{messages} =
                $self->fetch_offset( $partition, $offset, $offsets );
            next if ( not @{ $self->{messages} } );
            last;
        }
    }
    return shift @{ $self->{messages} };
}

# async support
sub indexes {
    my $self = shift;
    if (@_) {
        $self->{indexes} = shift;
    }
    return $self->{indexes};
}

# sync support
sub hosts {
    my $self = shift;
    if (@_) {
        $self->{hosts} = shift;
    }
    return $self->{hosts};
}

sub host {
    my $self = shift;
    if (@_) {
        $self->{host}  = shift;
        $self->{hosts} = [ $self->{host} ];
    }
    return $self->{host};
}

sub ports {
    my $self = shift;
    if (@_) {
        $self->{ports} = shift;
    }
    return $self->{ports};
}

sub port {
    my $self = shift;
    if (@_) {
        $self->{port}  = shift;
        $self->{ports} = [ $self->{port} ];
    }
    return $self->{port};
}

sub results {
    my $self = shift;
    if (@_) {
        $self->{results} = shift;
    }
    return $self->{results};
}

sub offsets {
    my $self = shift;
    if (@_) {
        $self->{offsets} = shift;
    }
    return $self->{offsets};
}

sub messages {
    my $self = shift;
    if (@_) {
        $self->{messages} = shift;
    }
    return $self->{messages};
}

1;
