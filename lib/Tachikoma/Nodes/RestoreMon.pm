#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::RestoreMon
# ----------------------------------------------------------------------
#
# $Id: RestoreMon.pm 24458 2016-01-13 23:25:50Z chris $
#

package Tachikoma::Nodes::RestoreMon;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Job;
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM TM_INFO );
use parent qw( Tachikoma::Node );

sub help {
    my $self = shift;
    return <<'EOF';
make_node RestoreMon <node name>
EOF
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return
        if (not $message->[TYPE] & TM_BYTESTREAM
        and not $message->[TYPE] & TM_INFO );
    my $arguments = ( split( ' ', $message->[PAYLOAD], 3 ) )[2];
    chomp($arguments);
    return $self->stderr( "ERROR: bad payload: ", $message->[PAYLOAD] )
        if ( not $arguments );
    my $output = $self->slurp( '/var/db/tachikoma/bin/silcbot_moncmd',
        '-a', 'enable', $arguments );
    chomp($output);
    my $response = Tachikoma::Message->new;
    $response->[TYPE]    = TM_BYTESTREAM;
    $response->[PAYLOAD] = "$output ($arguments)\n";
    return $self->SUPER::fill($response);
}

sub slurp {
    my $self    = shift;
    my @command = @_;
    my @output  = ();
    my $pid     = open( my $child, "-|" );
    die "ERROR: can't fork: $!" if ( not defined($pid) );
    if ($pid) {
        push( @output, $_ ) while (<$child>);
        close($child) or $self->stderr("WARNING: child exited $?");
    }
    else {
        exec(@command) or $self->stderr("ERROR: can't exec $command[0]: $!");
    }
    return join( '', @output );
}

1;
