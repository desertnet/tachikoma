#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::TopicProbe
# ----------------------------------------------------------------------
#

package Tachikoma::Nodes::TopicProbe;
use strict;
use warnings;
use Tachikoma::Nodes::Timer;
use Tachikoma::Message qw( TYPE FROM TO PAYLOAD TM_BYTESTREAM );
use Time::HiRes;
use Sys::Hostname qw( hostname );
use parent qw( Tachikoma::Nodes::Timer );

use version; our $VERSION = qv('v2.0.367');

my $DEFAULT_INTERVAL = 5;    # seconds

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
make_node TopicProbe <node name> <seconds> [ <prefix> ]
EOF
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $seconds, $prefix ) = split q( ), $self->{arguments}, 2;
        die "ERROR: bad arguments for TopicProbe\n"
            if ( $seconds and $seconds =~ m{\D} );
        $seconds ||= $DEFAULT_INTERVAL;
        $self->set_timer( $seconds * 1000 );
        $self->prefix( $prefix || q() );
    }
    return $self->{arguments};
}

sub fill {
    return;
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
        if ( $node->isa('Tachikoma::Nodes::Partition') ) {
            next if ( $node->{leader} );
            my $partition_name = $node->{name};
            $partition_name = join q(/), $self->{prefix}, $partition_name
                if ( $self->{prefix} );
            $partition_name =~ s{:}{_}g;
            $out .= join q(),
                'partition:' => $partition_name,
                ' p_offset:' => $node->{last_commit_offset} // 0,
                "\n";
        }
        elsif ( $node->isa('Tachikoma::Nodes::Consumer') ) {
            next if ( not $node->{set_state}->{ACTIVE} );
            my $partition_name = $node->{partition};
            $partition_name =~ s{.*/}{};
            $partition_name = join q(/), $self->{prefix}, $partition_name
                if ( $self->{prefix} );
            $partition_name =~ s{:}{_}g;
            my $consumer_name = $node->{name};
            $consumer_name =~ s{^_}{};
            $consumer_name = join q(/), $self->{prefix}, $consumer_name
                if ( $self->{prefix} );
            $consumer_name =~ s{:}{_}g;
            $out .= join q(),
                'hostname:'        => $self->{my_hostname},
                ' partition:'      => $partition_name,
                ' consumer:'       => $consumer_name,
                ' c_offset:'       => $node->{offset} // 0,
                ' cache_size:'     => $node->{last_cache_size} // 0,
                ' msg_sent:'       => $node->{counter} // 0,
                ' msg_unanswered:' => $node->{msg_unanswered} // 0,
                ' max_unanswered:' => $node->{max_unanswered} // 0,
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
