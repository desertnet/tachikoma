#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::TTY
# ----------------------------------------------------------------------
#
# Tachikoma::Nodes::STDIO plus Readline support
#
# $Id: TTY.pm 35263 2018-10-16 06:32:59Z chris $
#

package Tachikoma::Nodes::TTY;
use strict;
use warnings;
use Tachikoma::Nodes::STDIO qw( TK_R TK_W TK_SYNC );
use Tachikoma::Message qw( TYPE PAYLOAD TM_BYTESTREAM );
use Term::ReadLine qw();
use vars qw( @EXPORT_OK );
use parent qw( Tachikoma::Nodes::STDIO );
@EXPORT_OK = qw( TK_R TK_W TK_SYNC );

use version; our $VERSION = qv('v2.0.280');

my $Winch = undef;
my $Home  = ( getpwuid $< )[7];

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = $class->SUPER::new(@_);
    $self->{drain_fh}     = \&drain_fh;
    $self->{use_readline} = undef;
    $self->{term}         = undef;
    $self->{prompt}       = undef;
    $self->{width}        = undef;
    $self->{completions}  = {};
    $self->{queue}        = [];
    $self->{readline_EOF} = undef;
    bless $self, $class;
    return $self;
}

sub drain_fh {
    my $self = shift;
    if ( $self->{use_readline} ) {
        $self->width if ($Winch);
        $self->{term}->callback_read_char;
        for my $message ( @{ $self->{queue} } ) {
            $self->{sink}->fill($message);
        }
        $self->{queue} = [];
        if ( $self->{readline_EOF} ) {
            $self->handle_EOF;
            $self->{readline_EOF} = undef;
        }
        return;
    }
    return $self->SUPER::drain_fh(@_);
}

sub set_completions {
    my $self = shift;
    my $type = shift;
    my $list = shift;
    if ( $type eq 'help' ) {
        my $help = [];
        for my $line ( split m{\n}, $list ) {
            $line =~ s{^\S+.*?:\s+}{}g;
            push @{$help}, ( split q( ), $line )[0];
        }
        $self->completions->{'help'} = $help;
    }
    elsif ( $type eq 'ls' ) {
        my @completions = ();
        for my $line ( split m{\n}, $list ) {
            next if ( not defined $line );
            $line =~ s{^\S+.*?:\s+}{}g;
            push @completions, ( split q( ), $line, 2 )[0];
        }
        $self->completions->{'ls'} = \@completions;
    }
    return;
}

sub pause {
    my $self = shift;
    if ( $self->use_readline ) {
        $self->term->callback_handler_remove;
    }
    return;
}

sub resume {
    my $self = shift;
    if ( $self->use_readline ) {
        my $term = $self->term;
        $term->callback_handler_install(
            q{},
            sub {
                my $buf = shift;
                $self->prompt(q{});
                if ( defined $buf ) {
                    $term->addhistory($buf) if ( $buf =~ /\S/ );
                    my $message = Tachikoma::Message->new;
                    $message->[TYPE]    = TM_BYTESTREAM;
                    $message->[PAYLOAD] = $buf . "\n";
                    push @{ $self->{queue} }, $message;
                }
                else {
                    $self->readline_EOF('true');
                }
                return;
            }
        );
    }
    return;
}

sub close_filehandle {
    my $self = shift;
    if ( $self->use_readline ) {
        $self->term->callback_handler_remove;
        $self->term->WriteHistory("$Home/.tachikoma/history");
    }
    $self->SUPER::close_filehandle;
    return;
}

sub use_readline {
    my $self = shift;
    if (@_) {
        $self->{use_readline} = shift;
        return $self->{use_readline} if ( not $self->{use_readline} );
        my $term = Term::ReadLine->new('tachikoma');
        if ( $term->ReadLine ne 'Term::ReadLine::Gnu' ) {
            $self->{use_readline} = undef;
            return;
        }
        ## no critic (RequireLocalizedPunctuationVars)
        $SIG{WINCH} = sub { $Winch = 1 };
        $self->term($term);
        $term->ReadHistory("$Home/.tachikoma/history");
        $self->resume;
        my $attribs = $term->Attribs;
        $attribs->{completion_query_items}        = 0;
        $attribs->{attempted_completion_function} = sub {
            my $text    = shift;
            my $line    = shift;
            my $start   = shift;
            my $context = substr $line, 0, $start;
            if ( $context =~ m{^\s*$} ) {
                $attribs->{completion_word} = $self->{completions}->{'help'};
            }
            elsif ( $context =~ m{[\[\](){};]} ) {
                $attribs->{completion_word} = [
                    @{ $self->{completions}->{'help'} },
                    @{ $self->{completions}->{'ls'} }
                ];
            }
            else {
                $attribs->{completion_word} =
                    [ @{ $self->{completions}->{'ls'} } ];
            }
            return $term->completion_matches( $text,
                $attribs->{list_completion_function} );
        };
    }
    return $self->{use_readline};
}

sub term {
    my $self = shift;
    if (@_) {
        $self->{term} = shift;
    }
    return $self->{term};
}

sub prompt {
    my $self = shift;
    if (@_) {
        $self->{prompt} = shift;
        if ( $self->{term} ) {
            $self->{term}->set_prompt( $self->{prompt} );
            $self->{term}->redisplay if ( $self->{prompt} );
        }
    }
    elsif ( $self->{term} ) {
        $self->{term}->forced_update_display;
    }
    return;
}

sub line_length {
    my $self = shift;
    return length( $self->{prompt} ) + $self->{term}->Attribs->{point};
}

sub width {
    my $self = shift;
    if ( $Winch or not defined $self->{width} ) {
        if ($Winch) {
            $Winch = undef;
            $self->{term}->resize_terminal;
        }
        $self->{width} = ( $self->{term}->get_screen_size )[1];
    }
    return $self->{width};
}

sub completions {
    my $self = shift;
    if (@_) {
        $self->{completions} = shift;
    }
    return $self->{completions};
}

sub queue {
    my $self = shift;
    if (@_) {
        $self->{queue} = shift;
    }
    return $self->{queue};
}

sub readline_EOF {
    my $self = shift;
    if (@_) {
        $self->{readline_EOF} = shift;
    }
    return $self->{readline_EOF};
}

1;
