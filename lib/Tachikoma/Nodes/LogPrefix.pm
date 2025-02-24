#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::LogPrefix
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::LogPrefix;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use Sys::Hostname      qw( hostname );
use POSIX              qw( strftime );
use parent             qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.367');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{my_hostname} = hostname();
    $self->{fields}      = [];
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node LogPrefix <node name> [ <date|hostname|process> ... ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        $self->{fields}    = [];
        for my $field ( split q( ), $self->{arguments} ) {
            die "ERROR: unknown field '$field'\n"
                if ( $field !~ m{^(?:date|hostname|process)$} );
            push @{ $self->{fields} }, $field;
        }
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    my $prefix  = q();
    if ( @{ $self->{fields} } ) {
        for my $field ( @{ $self->{fields} } ) {
            if ( $field eq 'date' ) {
                $prefix .= strftime( '%F %T %Z ', localtime );
            }
            elsif ( $field eq 'hostname' ) {
                $prefix .= $self->{my_hostname} . q( );
            }
            elsif ( $field eq 'process' ) {
                $prefix .= $0 . '[' . $$ . ']: ';
            }
        }
    }
    else {
        $prefix = Tachikoma->log_prefix;
    }
    $message->[PAYLOAD] =~ s{^}{$prefix}mg
        if ( $message->[TYPE] & TM_BYTESTREAM );
    return $self->SUPER::fill($message);
}

1;
