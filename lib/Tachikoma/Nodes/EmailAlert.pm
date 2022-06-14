#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::EmailAlert
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::EmailAlert;
use strict;
use warnings;
use Tachikoma::Message qw(
    TYPE PAYLOAD
    TM_BYTESTREAM
);
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.368');

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{alert_interval} = 0;
    $self->{email_address}  = q();
    $self->{last_email}     = 0;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $interval, $address ) = split q( ), $self->{arguments}, 2;
        $self->{alert_interval} = $interval;
        $self->{email_address}  = $address;
        $self->{last_email}     = 0;
    }
    return $self->{arguments};
}

sub fill {
    my $self    = shift;
    my $message = shift;
    return if ( not $message->[TYPE] & TM_BYTESTREAM );
    $self->send_alert( $message->[PAYLOAD] );
    return $self->cancel($message);
}

sub send_alert {
    my $self  = shift;
    my $alert = shift;
    return
        if ( not $self->{email_address}
        or $Tachikoma::Now - $self->{last_email} < $self->{alert_interval} );
    $self->{'last_email'} = $Tachikoma::Now;
    my $email = $self->{email_address};
    delete @ENV{qw(IFS CDPATH ENV BASH_ENV)};
    local $ENV{PATH} = q();
    my $subject = 'WARNING: ' . $self->name . ' alert';
    open my $mail, q(|-), qq(/usr/bin/mail -s "$subject" $email)
        or die "couldn't open mail: $!";
    print {$mail} scalar( localtime time ), qq(\n\n), $alert;
    close $mail or $self->stderr("ERROR: couldn't close mail: $!");
    return;
}

sub alert_interval {
    my $self = shift;
    if (@_) {
        $self->{alert_interval} = shift;
    }
    return $self->{alert_interval};
}

sub last_email {
    my $self = shift;
    if (@_) {
        $self->{last_email} = shift;
    }
    return $self->{last_email};
}

sub email_address {
    my $self = shift;
    if (@_) {
        $self->{email_address} = shift;
    }
    return $self->{email_address};
}

1;
