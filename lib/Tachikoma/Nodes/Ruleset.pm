#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Ruleset
# ----------------------------------------------------------------------
#
# $Id: Ruleset.pm 4142 2010-01-21 23:17:02Z chris $
#

package Tachikoma::Nodes::Ruleset;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::CommandInterpreter;
use Tachikoma::Message qw(
    TYPE FROM TO ID STREAM PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_COMMAND TM_EOF
);
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.368');

my %C = ();
my %Exclude_To = map { $_ => 1 } qw( copy redirect rewrite );

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{rules}       = {};
    $self->{interpreter} = Tachikoma::Nodes::CommandInterpreter->new;
    $self->{interpreter}->patron($self);
    $self->{interpreter}->commands( \%C );
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Ruleset <node name>
EOF
}

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self         = shift;
    my $message      = shift;
    my $message_type = $message->[TYPE];
    my $response     = undef;
    my $rules        = $self->{rules};
    my $message_from = $message->[FROM];
    my $message_to   = $message->[TO];
    my $packed       = $message->packed;

    for my $id ( sort { $a <=> $b } keys %{$rules} ) {
        my ( $type, $from, $to, $field, $re ) = @{ $rules->{$id} };
        next
            if (
            ( $field and $field == PAYLOAD and $message_type & TM_STORABLE )
            or ( defined $from and $message_from !~ m{$from} )
            or (    not $Exclude_To{$type}
                and defined $to
                and $message_to !~ m{$to} )
            );
        my $copy = Tachikoma::Message->new($packed);
        my @matches = $field ? $copy->[$field] =~ m{$re} : ();
        next if ( $field and not @matches );
        if ( $type eq 'deny' ) {
            last;
        }
        elsif ( $type eq 'cancel' ) {
            $self->cancel($message);
            last;
        }
        elsif ( $type eq 'allow' or $type eq 'redirect' or $type eq 'copy' ) {
            $copy->[TO] =
                   ( $type eq 'allow' ? $message_to : $to )
                || $self->{owner}
                || q();
            $response = $self->{sink}->fill($copy);
            last if ( $type ne 'copy' );
        }
        elsif ( $type eq 'rewrite' ) {
            $to =~ s{\$$_(?!\d)}{$matches[$_ - 1]}g for ( 1 .. @matches );
            $copy->[$field] =~ s{$re}{$to}s;
            $packed = $copy->packed;
        }
        elsif ( $type eq 'log' ) {
            my $out = "logging under rule $id: " . $copy->type_as_string;
            $out .= ' from ' . $copy->[FROM]
                if ( $from and $message_from =~ m{$from} );
            $out .= ' to ' . $copy->[TO]
                if ( $to and $message_to =~ m{$to} );
            if ( $field and $re and $copy->[$field] =~ m{$re} ) {
                my $field_name =
                    qw( TYPE FROM TO ID STREAM TIMESTAMP PAYLOAD ) [$field];
                if ( $message->[TYPE] & TM_COMMAND and $field == PAYLOAD ) {
                    my $command = Tachikoma::Command->new( $copy->[PAYLOAD] );
                    $out .= " $field_name=" . ( $@ || $command->{payload} );
                }
                else {
                    $out .= " $field_name=" . $copy->[$field];
                }
            }
            $self->stderr($out);
        }
    }
    $self->{counter}++;
    return $response;
}

$C{help} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    return $self->response( $envelope,
              "commands: list_rules\n"
            . "          add_rule <id> <type>  [ from <path> ]\n"
            . "                                [ to <path> ]\n"
            . "                                [ where <field>=<regex> ]\n"
            . "          add_rule <id> rewrite [ <field>=<regex> ]\n"
            . "                                [ to <replacement> ]\n"
            . "          remove_rule <id>\n"
            . "   types: allow deny cancel copy redirect log\n" );
};

$C{list_rules} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    my $rules    = $self->patron->rules;
    my $response = undef;
    if ( $command->arguments eq '-a' ) {
        $response = join( "\n", sort keys %{$rules} ) . "\n";
    }
    else {
        $response = q();
        my @enum = qw( type from to id stream timestamp payload );
        for my $id ( sort { $a <=> $b } keys %{$rules} ) {
            my ( $type, $from, $to, $field_id, $regex ) = @{ $rules->{$id} };
            my $field = $field_id ? $enum[$field_id] : undef;
            if ($regex) {
                $regex =~ s{^[(][?][\^]:}{};
                $regex =~ s{[)]$}{};
                $regex =~ s{'}{\\'}g;
            }
            if ( $type eq 'rewrite' ) {
                $response
                    .= "$id $type" . " $field='$regex'" . " to $to" . "\n";
            }
            else {
                $response .= "$id $type";
                $response .= " from $from" if ($from);
                $response .= " to $to" if ($to);
                $response .= " where $field='$regex'" if ($field);
                $response .= "\n";
            }
        }
    }
    return $self->response( $envelope, $response );
};

$C{ls} = $C{list_rules};

$C{add_rule} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'update_rules' )
        or return $self->error("verification failed\n");
    my %enum = (
        type    => TYPE,
        from    => FROM,
        to      => TO,
        id      => ID,
        stream  => STREAM,
        payload => PAYLOAD,
    );
    ## no critic (ProhibitComplexRegexes)
    my ( $id, $type, $from, $to, $field_id, $regex ) = (
        $command->arguments =~ m{
            ^
               \s* (\S+) \s+ (\S+)
            (?:\s+ from  \s+ (\S+))?
            (?:\s+ to    \s+ (\S+))?
            (?:\s+ where \s+ ([^\s=]+) \s* = \s* (.*?))?
               \s*
            $
        }x
    );
    if ( not $type ) {
        ( $id, $type, $field_id, $regex, $to ) = (
            $command->arguments =~ m{
                ^
                   \s* (\S+) \s+ (\S+)
                   \s+ ([^\s=]+) \s* = \s* (\S+)
                   \s+ to    \s+ (.*?)
                   \s*
                $
            }x
        );
    }
    my %valid_types =
        map { $_ => 1 } qw( allow deny cancel copy redirect rewrite log );
    if ( not $type or not $valid_types{$type} ) {
        return $self->error( $envelope, "syntax error\n" );
    }
    if ( not exists $self->patron->rules->{$id} ) {
        $self->patron->rules->{$id} = [
            $type,
            defined $from ? qr{$from} : undef,
            ( not $Exclude_To{$type} and defined $to ) ? qr{$to} : $to,
            $field_id      ? $enum{$field_id} : undef,
            defined $regex ? qr{$regex}       : undef
        ];
        return $self->okay($envelope);
    }
    else {
        return $self->error( $envelope, "can't create, rule exists: $id\n" );
    }
};

$C{add} = $C{add_rule};

$C{remove_rule} = sub {
    my $self     = shift;
    my $command  = shift;
    my $envelope = shift;
    $self->verify_key( $envelope, ['meta'], 'update_rules' )
        or return $self->error("verification failed\n");
    my $id = $command->arguments;
    if ( exists $self->patron->rules->{$id} ) {
        delete $self->patron->rules->{$id};
        return $self->okay($envelope);
    }
    else {
        return $self->error( $envelope, "can't remove, no such rule: $id\n" );
    }
};

$C{rm} = $C{remove_rule};

sub name {
    my $self = shift;
    if (@_) {
        my $name = shift;
        $self->{interpreter}->name( $name . ':config' );
        $self->SUPER::name($name);
    }
    return $self->{name};
}

sub dump_config {
    my $self     = shift;
    my $response = $self->SUPER::dump_config;
    my $rules    = $self->{rules};
    my @enum     = qw( type from to id stream timestamp payload );
    for my $id ( sort { $a <=> $b } keys %{$rules} ) {
        my ( $type, $from, $to, $field_id, $regex ) = @{ $rules->{$id} };
        my $field = $field_id ? $enum[$field_id] : undef;
        if ($regex) {
            $regex =~ s{^[(][?][\^]:}{};
            $regex =~ s{[)]$}{};
            $regex =~ s{'}{\\'}g;
        }
        if ( $type eq 'rewrite' ) {
            $response
                .= "command $self->{name} add_rule $id $type"
                . " $field='$regex'"
                . " to $to" . "\n";
        }
        else {
            $response .= "command $self->{name} add_rule $id $type";
            $response .= " from $from" if ($from);
            $response .= " to $to" if ($to);
            $response .= " where $field='$regex'" if ($field);
            $response .= "\n";
        }
    }
    return $response;
}

sub rules {
    my $self = shift;
    if (@_) {
        $self->{rules} = shift;
    }
    return $self->{rules};
}

1;
