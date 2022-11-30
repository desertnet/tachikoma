#!/usr/bin/perl
# ----------------------------------------------------------------------
# Accessories::Nodes::Spool
# ----------------------------------------------------------------------
#

package Accessories::Nodes::Spool;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw(
    TYPE FROM STREAM PAYLOAD
    TM_BYTESTREAM TM_PERSIST TM_RESPONSE TM_ERROR TM_EOF
);
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.400');

my $Counter         = 0;
my $Default_Timeout = 900;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{spool}          = {};
    $self->{spool_dir}      = undef;
    $self->{timeout}        = $Default_Timeout;
    $self->{max_unanswered} = 1;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node Spool <node name> <directory> [ <timeout> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $filename, $timeout, $max_unanswered ) = split q( ),
            $self->{arguments}, 3;
        die "ERROR: bad arguments for Spool\n" if ( not $filename );
        my $dir = ( $filename =~ m{^(/.*)$} )[0];
        $self->{spool}          = {};
        $self->{spool_dir}      = $dir;
        $self->{timeout}        = $timeout || $Default_Timeout;
        $self->{max_unanswered} = $max_unanswered || 1;
        $self->make_dirs($dir);
        $self->set_timer;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( $message->[TYPE] & TM_EOF );
    if ( $message->[TYPE] == ( TM_PERSIST | TM_RESPONSE ) ) {
        return $self->stderr('ERROR: unexpected response')
            if ( not $self->{spool}->{ $message->[STREAM] } );
        if ( $message->[PAYLOAD] eq 'cancel' ) {
            my $path = join q(/), $self->{spool_dir}, $message->[STREAM];
            unlink $path
                or return $self->stderr("ERROR: couldn't unlink $path: $!");
            $self->stop_timer;
            $self->set_timer(0);
            delete $self->{spool}->{ $message->[STREAM] };
        }
        else {
            $self->stderr( 'WARNING: unexpected answer from ',
                $message->[FROM] );
        }
    }
    elsif ( $message->[TYPE] & TM_ERROR ) {
        delete $self->{spool}->{ $message->[STREAM] };
    }
    else {
        $self->stderr('ERROR: unexpected payload');
    }
    return;
}

sub fire {
    my $self = shift;
    return if ( not $self->{owner} );
    my $spool   = $self->{spool};
    my $timeout = $self->{timeout};
    for my $file ( keys %{$spool} ) {
        delete $spool->{$file}
            if ( $Tachikoma::Now - $spool->{$file} > $timeout );
    }
    my $dir = $self->{spool_dir};
    local $/ = undef;
    opendir my $dh, $dir or die "ERROR: couldn't opendir $dir: $!";
    for my $file ( grep m{^[^.]}, readdir $dh ) {
        last if ( keys %{$spool} >= $self->{max_unanswered} );
        next if ( $spool->{$file} );
        $self->process_file($file);
    }
    closedir $dh or die "ERROR: clouldn't closedir $dir: $!";
    $self->stop_timer;
    $self->set_timer;
    return;
}

sub process_file {
    my $self = shift;
    my $file = shift;
    my $path = join q(/), $self->{spool_dir}, $file;
    $self->{spool}->{$file} = $Tachikoma::Now;
    my $message = Tachikoma::Message->new;
    $message->[TYPE]   = TM_BYTESTREAM | TM_PERSIST;
    $message->[FROM]   = $self->{name};
    $message->[STREAM] = $file;
    open my $fh, '<', $path or die "ERROR: couldn't open $path: $!";
    $message->[PAYLOAD] = <$fh>;
    close $fh or die "ERROR: couldn't close $path: $!";
    $self->SUPER::fill($message);
    return;
}

sub msg_counter {
    my $self = shift;
    $Counter = ( $Counter + 1 ) % $Tachikoma::Max_Int;
    return sprintf '%d:%010d', $Tachikoma::Now, $Counter;
}

sub spool {
    my $self = shift;
    if (@_) {
        $self->{spool} = shift;
    }
    return $self->{spool};
}

sub spool_dir {
    my $self = shift;
    if (@_) {
        $self->{spool_dir} = shift;
    }
    return $self->{spool_dir};
}

sub timeout {
    my $self = shift;
    if (@_) {
        $self->{timeout} = shift;
    }
    return $self->{timeout};
}

sub max_unanswered {
    my $self = shift;
    if (@_) {
        $self->{max_unanswered} = shift;
    }
    return $self->{max_unanswered};
}

1;
