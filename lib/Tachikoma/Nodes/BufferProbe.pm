#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::BufferProbe
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::BufferProbe;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE FROM TO PAYLOAD TM_BYTESTREAM );
use Time::HiRes;
use Sys::Hostname qw( hostname );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.368');

my $Default_Interval = 5;    # seconds

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{my_hostname} = hostname();
    $self->{prefix}      = q();
    $self->{last_time}   = $Tachikoma::Right_Now;
    bless $self, $class;
    return $self;
}

sub help {
    my $self = shift;
    return <<'EOF';
make_node BufferProbe <node name> <seconds> [ <prefix> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $seconds, $prefix ) = split q( ), $self->{arguments}, 2;
        die "ERROR: bad arguments for BufferProbe\n"
            if ( $seconds and $seconds =~ m{\D} );
        $seconds ||= $Default_Interval;
        $self->set_timer( $seconds * 1000 );
        $self->prefix( $prefix || $0 );
    }
    return $self->{arguments};
}

sub fire {
    my $self     = shift;
    my $out      = q();
    my $interval = $self->{timer_interval} / 1000;
    my $elapsed  = Time::HiRes::time - $self->{last_time};
    $self->stderr(
        sprintf 'WARNING: degraded performance detected'
            . ' - timer fired %.2f seconds late',
        $elapsed - $interval
    ) if ( $elapsed > $interval * 2 );
    $self->{last_time} = Time::HiRes::time;
    for my $name ( keys %Tachikoma::Nodes ) {
        my $node = $Tachikoma::Nodes{$name};
        if ( $node->isa('Tachikoma::Nodes::Buffer') ) {
            my $buff_name = $node->{filename};
            $buff_name ||= join q(/), $self->{prefix}, $node->{name};
            $buff_name =~ s{:}{_}g;
            $out .= join q(),
                'hostname:'        => $self->{my_hostname},
                ' buff_name:'      => $buff_name,
                ' buff_fills:'     => $node->{buffer_fills},
                ' err_sent:'       => $node->{errors_passed},
                ' max_unanswered:' => $node->{max_unanswered},
                ' msg_in_buf:'     => $node->{buffer_size}
                // $node->get_buffer_size,
                ' msg_rcvd:' => $node->{counter},
                ' msg_sent:' => $node->{msg_sent},
                ' msg_unanswered:' =>
                scalar( keys %{ $node->{msg_unanswered} } ),
                ' p_msg_sent:' => $node->{pmsg_sent},
                ' resp_rcvd:'  => $node->{rsp_received},
                ' resp_sent:'  => $node->{rsp_sent},
                "\n";
        }
        elsif ( $node->isa('Tachikoma::Nodes::Consumer') ) {
            my $buff_name = join q(/), $self->{prefix}, $node->{name};
            $buff_name =~ s{:}{_}g;
            my $partition = $Tachikoma::Nodes{ $node->{partition} } or next;
            $out .= join q(),
                'hostname:'        => $self->{my_hostname},
                ' buff_name:'      => $buff_name,
                ' buff_fills:'     => $partition->{counter},
                ' err_sent:'       => 0,
                ' max_unanswered:' => $node->{max_unanswered},
                ' msg_in_buf:' => $partition->{counter} - $node->{counter},
                ' msg_rcvd:'   => $partition->{counter},
                ' msg_sent:'   => 0,
                ' msg_unanswered:' => $node->{msg_unanswered},
                ' p_msg_sent:'     => $node->{counter},
                ' resp_rcvd:' => $node->{counter} - $node->{msg_unanswered},
                ' resp_sent:' => 0,
                "\n";
        }
    }
    if ($out) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE]    = TM_BYTESTREAM;
        $message->[FROM]    = $self->{name};
        $message->[TO]      = $self->{owner};
        $message->[PAYLOAD] = $out;
        $self->{sink}->fill($message);
        $self->{counter}++;
    }
    return;
}

sub my_hostname {
    my $self = shift;
    if (@_) {
        $self->{my_hostname} = shift;
    }
    return $self->{my_hostname};
}

sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
}

1;
