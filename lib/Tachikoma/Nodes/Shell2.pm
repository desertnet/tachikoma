#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::Shell2
# ----------------------------------------------------------------------
#
# $Id$
#

package Tachikoma::Nodes::Shell2;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::Shell;
use Tachikoma::Message qw(
    TYPE FROM TO TIMESTAMP PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_COMMAND TM_PING TM_EOF
    TM_INFO TM_COMPLETION TM_NOREPLY
);
use Tachikoma::Command;
use Tachikoma::Config qw( %Tachikoma %Functions %Help %Var );
use Data::Dumper qw( Dumper );
use Storable qw( nfreeze );
my $USE_JSON;

BEGIN {
    $USE_JSON = eval {
        my $module_name = 'JSON';
        my $module_path = 'JSON.pm';
        require $module_path;
        import $module_name;
        return 1;
    };
}
use parent qw( Tachikoma::Node );

use version; our $VERSION = qv('v2.0.280');

my %H = ();

# special characters that need to be escaped: [(){}\[\]<>&|;"'`]
my $ident_re  = qr{[^ . + \- * / # () {} \[\] ! = & | ; " ' ` \s \\ ]+}x;
my $not_op_re = qr{(?: [.](?![.=]) | [+](?![+=]) | -(?![-=])
                                   | [*](?![=])  | /(?![=]) )*}x;
my $math_re    = qr{ [+](?![+=]) | -(?![-=]) | [*](?![=]) | /(?![=]) }x;
my $logical_re = qr{ !~ | =~ | !=? | <=? | >=? | == }x;

my %Tokens = (
    whitespace => qr{\s+},
    number     => qr{-?(?:\d+(?:[.]\d+)?|[.]\d+)},
    ident      => qr{(?: [.+\-*/]* $ident_re $not_op_re | \\. )+}x,
    logical    => qr{(?: [.](?:[.]|(?!\=)) | $math_re | $logical_re )}x,
    op         => qr{(?: [.]= | [+][+] | -- | [|][|]=
                        | //= | [+]=   | -= | [*]= | /= | = )}x,
    and           => qr{&&},
    or            => qr{[|][|]},
    command       => qr{[;&]},
    pipe          => qr{[|]},
    open_paren    => qr{[(]},
    close_paren   => qr{[)]},
    open_brace    => qr/[{]/,
    close_brace   => qr/[}]/,
    open_bracket  => qr{\[},
    close_bracket => qr{\]},
    newline       => qr{\\$},
    string1       => qr{"},
    string2       => qr{'},
    string3       => qr{`},
    string4       => qr{\\.}s,
    comment       => qr{#.*?\n},
    eos           => qr{\s*$},
);

my @Token_Types = qw(
    whitespace
    number
    ident
    logical
    op
    and
    or
    command
    pipe
    open_paren
    close_paren
    open_brace
    close_brace
    open_bracket
    close_bracket
    newline
    string1
    string2
    string3
    string4
    comment
    eos
);

# Build Trie
use re 'eval';
my $re = undef;
$re = $re ? "$re|$Tokens{$_}(?{'$_'})" : "$Tokens{$_}(?{'$_'})"
    for (@Token_Types);
my $Trie = qr{\G(?:$re)};
no re 'eval';

my %Ident   = map { $_ => 1 } qw( ident whitespace number leaf );
my %Strings = map { $_ => 1 } qw( string1 string2 string3 string4 );
my %Partial = map { $_ => 1 } qw( string1 string2 string3 );
my %Logical = map { $_ => 1 } qw( command and or pipe );
my %Syntax  = map { $_ => 1 } qw(
    open_paren   close_paren
    open_brace   close_brace
    open_bracket close_bracket leaf );
my %Open    = map { $_ => 1 } qw( open_paren open_brace open_bracket );
my %Close   = map { $_ => 1 } qw( close_paren close_brace close_bracket );
my %Command = map { $_ => 1 } qw( open_brace open_bracket command pipe );
my %Matching = (
    open_paren   => 'close_paren',
    open_brace   => 'close_brace',
    open_bracket => 'close_bracket',
);
my %Prompts = (
    string1 => q("> ),
    string2 => q('> ),
    string3 => q(`> ),
    newline => q(> ),
);
my %Evaluators = ();
my %Builtins   = ();
my %Operators  = ();
my %Local      = ();
my %Shared     = ();
## no critic (ProhibitTies)
tie %Shared, 'Tachikoma::Nodes::Shell2';

for my $type (@Token_Types) {
    $Evaluators{$type} = sub {
        die "internal parser error - unexpected $type\n";
    };
}
my $Counter = 0;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{cwd}           = undef;
    $self->{mode}          = 'command';
    $self->{isa_tty}       = undef;
    $self->{should_reply}  = undef;
    $self->{stdin}         = undef;
    $self->{dumper}        = undef;
    $self->{responder}     = undef;
    $self->{validate}      = undef;
    $self->{errors}        = 0;
    $self->{parse_buffer}  = q{};
    $self->{callbacks}     = {};
    $self->{message_id}    = undef;
    $self->{dirty}         = undef;
    $self->{prompt}        = undef;
    $self->{last_prompt}   = 0;
    $self->{show_parse}    = undef;
    $self->{show_commands} = undef;
    bless $self, $class;
    $Help{$_} //= $H{$_} for ( keys %H );
    return $self;
}

sub new_func {
    my $self     = shift;
    my $name     = shift;
    my $body     = shift;
    my $function = join q{}, "func $name {",
        $body =~ m{^\s} ? "\n$body};\n" : " $body };\n";
    $Help{$name} = [$function];
    $self->evaluate( $self->parse($function) );
    return;
}

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self      = shift;
    my $message   = shift;
    my $responder = $self->{responder}->{name};
    $message->[FROM] = $responder if ($responder);
    if ( $self->mode eq 'command' ) {
        if ( $message->type & TM_EOF ) {
            $self->stderr('ERROR: got EOF while waiting for tokens')
                if ( $self->parse_buffer );
            return $self->sink->fill($message);
        }
        $self->{counter}++;
        my $parse_buffer = join q{}, $self->parse_buffer, $message->payload;
        my $parse_tree = undef;
        $self->parse_buffer(q{});
        local $SIG{INT} = sub { die "^C\n" }
            if ( $self->{isa_tty} );
        my $okay = eval {
            $parse_tree = $self->parse($parse_buffer);
            return 1;
        };

        if ( not $okay ) {
            $self->report_error( $@ // 'parse: unknown error' );
        }
        elsif ($parse_tree) {
            $self->stderr( Dumper($parse_tree) ) if ( $self->show_parse );
            if ( not $self->{validate} ) {
                $okay = eval {
                    $self->send_command($parse_tree);
                    return 1;
                };
                if ( not $okay ) {
                    $self->report_error( $@
                            // 'send_command: unknown error' );
                }
            }
        }
        elsif ( $parse_buffer =~ m{\S} and $parse_buffer !~ m{^\s*#} ) {
            $self->parse_buffer($parse_buffer);
            return;
        }
        if ( $self->{isa_tty} ) {
            $self->get_completions if ( $self->dirty );
            $self->prompt if ( $self->mode eq 'command' );
        }
    }
    else {
        if ( $message->type & TM_EOF ) {
            $message->from(q{});
            $self->sink->fill($message);
            $self->path( $self->cwd );
            $self->cwd(undef);
            $self->mode('command');
            $self->prompt if ( $self->isa_tty );
        }
        elsif ( $message->payload eq ".\n" ) {
            $self->path( $self->cwd );
            $self->cwd(undef);
            $self->mode('command');
            $self->prompt if ( $self->isa_tty );
        }
        else {
            $message->to( $self->path );
            $message->stream( $Local{'message.stream'} );
            $self->sink->fill($message);
        }
    }
    return;
}

sub parse {    ## no critic (ProhibitExcessComplexity)
    my $self       = shift;
    my $proto      = shift;
    my $expecting  = shift;
    my $parse_tree = undef;
    my $input_ref  = ref $proto ? $proto : \$proto;
    $expecting ||= 'eos';
    while ( my $tok = $self->get_next_token($input_ref) ) {
        next if ( not $parse_tree and $tok->{type} eq 'whitespace' );
        my $this_branch = undef;
        if ( $Partial{$expecting} ) {
            $parse_tree //= q{};
            if ( $expecting eq $tok->{type} ) {
                return $parse_tree;
            }
            elsif ( $tok->{type} eq 'eos' ) {
                $self->stdin->prompt( $Prompts{$expecting} )
                    if ( $self->{isa_tty} );
                return;
            }
            else {
                $parse_tree .= $tok->{value}->[0];
            }
        }
        elsif ( $Partial{ $tok->{type} } ) {
            my $inner = $self->parse( $input_ref, $tok->{type} );
            return if ( not defined $inner );
            $tok->{value} = [$inner];
            $self->dequote($tok);
            $this_branch = $tok;
        }
        elsif ( $Open{ $tok->{type} } ) {
            my $inner = $self->parse( $input_ref, $Matching{ $tok->{type} } );
            return if ( not defined $inner );
            my $final = ref $inner ? $inner->{value}->[-1] : undef;
            if ( ref $final ) {
                my $trimmed = $self->trim($final);
                pop @{ $inner->{value} }
                    if ( $final->{type} eq 'leaf'
                    and not @{ $trimmed->{value} } );
            }
            $tok->{value} = [$inner];
            $this_branch = $tok;
        }
        elsif ( $Logical{ $tok->{type} } ) {
            $self->fatal_parse_error('unexpected logical operator')
                if ( not $parse_tree );
            $parse_tree = { type => $tok->{type}, value => [$parse_tree] }
                if ( $parse_tree->{type} ne $tok->{type} );
            $tok->{type}  = 'leaf';
            $tok->{value} = [];
            $this_branch  = $tok;
        }
        elsif ( $Close{ $tok->{type} } ) {
            $parse_tree //= q{};
            return $parse_tree if ( $expecting eq $tok->{type} );
            $self->unexpected_tok($tok);
        }
        elsif ( $tok->{type} eq 'newline' ) {
            $self->stdin->prompt( $Prompts{ $tok->{type} } )
                if ( $self->{isa_tty} );
            return;
        }
        elsif ( $tok->{type} eq 'eos' ) {
            if ( $expecting eq 'eos' ) {
                return $parse_tree;
            }
            else {
                $self->stdin->prompt( $expecting . '> ' )
                    if ( $self->{isa_tty} );
                return;
            }
        }
        elsif ( $tok->{type} ne 'comment' ) {
            $self->dequote($tok)
                if ( $Ident{ $tok->{type} } or $Strings{ $tok->{type} } );
            $this_branch = $tok;
        }
        if ($this_branch) {
            my $cursor = undef;
            $cursor = $parse_tree->{value}->[-1]
                if ($parse_tree
                and $Logical{ $parse_tree->{type} }
                and $this_branch->{type} ne 'leaf' );
            my $branch = $cursor || $parse_tree;
            if ($branch) {
                push @{ $branch->{value} }, $this_branch;
            }
            else {
                $parse_tree = $this_branch;
            }
        }
    }
    $self->fatal_parse_error('Unexpected end of tokens.');
    return;
}

sub get_next_token {
    my $self      = shift;
    my $input_ref = shift;
    if ( ${$input_ref} !~ m{$Trie}go ) {
        my $input = ${$input_ref};
        chomp $input;
        $self->fatal_parse_error(qq(Undefined token at "$input"));
    }
    return { type => $^R, value => [$&] };    ## no critic (ProhibitMatchVars)
}

sub unexpected_tok {
    my $self = shift;
    my $tok  = shift;
    $self->fatal_parse_error(
        qq(syntax error near unexpected token '$tok->{type}')
            . (
            $tok->{type} ne 'whitespace' ? qq(: "$tok->{value}->[0]") : q{}
            )
    );
    return;
}

sub fatal_parse_error {
    my $self  = shift;
    my $error = shift;
    die "ERROR: $error, line $self->{counter}\n";
}

sub dequote {
    my $self  = shift;
    my $tok   = shift;
    my $type  = $tok->{type};
    my $value = \$tok->{value}->[0];
    if (   $type eq 'ident'
        or $type eq 'string1'
        or $type eq 'string3' )
    {
        ${$value} =~ s{\\e}{\e}g;
        ${$value} =~ s{\\n}{\n}g;
        ${$value} =~ s{\\r}{\r}g;
        ${$value} =~ s{\\t}{\t}g;
        ${$value} =~ s{\\([^<\w>])}{$1}g;
    }
    elsif ( $type eq 'string2' ) {
        ${$value} =~ s{\\(')}{$1}g;
    }
    elsif ( $type eq 'string4' ) {
        ${$value} =~ s{^\\}{};
    }
    return;
}

# trim() copies a layer of parse tree,
# removing parentheses, braces, and whitespace.
sub trim {
    my $self     = shift;
    my $raw_tree = shift;
    my @values   = ();
    $self->fatal_parse_error("unexpected token: '$raw_tree'")
        if ( not ref $raw_tree );
    for my $tok ( @{ $raw_tree->{value} } ) {
        next if ( ref $tok and $tok->{type} eq 'whitespace' );
        push @values, $tok;
    }
    return {
        type  => $raw_tree->{type},
        value => \@values
    };
}

sub evaluate {
    my $self     = shift;
    my $raw_tree = shift;
    return
          defined $raw_tree
        ? ref $raw_tree
            ? &{ $Evaluators{ $raw_tree->{type} } }( $self, $raw_tree )
            : [$raw_tree]
        : [];
}

#############
# Evaluators
#############

# evaluate statements
$Evaluators{'open_bracket'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $values   = $raw_tree->{value};
    my $rv       = [];

    # send contents of brackets as a command:
    my $command = $values->[0];
    my $result  = $self->send_command($command);
    push @{$rv}, @{$result} if ( defined $result );

    # evaluate statements after the close bracket:
    ## no critic (ProhibitCStyleForLoops)
    for ( my $i = 1; $i < @{$values}; $i++ ) {
        if ( ref $values->[$i] ) {
            $result = $self->evaluate( $values->[$i] );
            push @{$rv}, @{$result} if ( defined $result );
        }
        elsif ( defined $values->[$i] ) {
            push @{$rv}, $values->[$i];
        }
    }
    return $rv;
};

# localize variables and evaluate statements
$Evaluators{'open_brace'} = sub {
    my $self      = shift;
    my $raw_tree  = shift;
    my $values    = $raw_tree->{value};
    my $rv        = [];
    my %old_local = ();
    %old_local = %Local;

    # send contents of braces as a command:
    my $command = $values->[0];
    my $result  = undef;
    $result = $self->send_command($command) if ( ref $command );
    push @{$rv}, @{$result} if ( defined $result );
    %Local = %old_local;

    # evaluate statements after the close bracket:
    ## no critic (ProhibitCStyleForLoops)
    for ( my $i = 1; $i < @{$values}; $i++ ) {
        if ( ref $values->[$i] ) {
            $result = $self->evaluate( $values->[$i] );
            push @{$rv}, @{$result} if ( defined $result );
        }
        elsif ( defined $values->[$i] ) {
            push @{$rv}, $values->[$i];
        }
    }
    return $rv;
};

# evaluate expressions
$Evaluators{'open_paren'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $values   = $raw_tree->{value};
    my $first    = $values->[0];
    my $trimmed  = ref $first ? $self->trim($first) : undef;
    my $inner    = $trimmed ? $trimmed->{value} : [];
    my $op       = @{$inner} == 3 ? $inner->[1]->{value}->[0] : undef;
    my $rv       = undef;

    # handle expressions
    if ( $op and $Operators{$op} ) {
        my @result = &{ $Operators{$op} }(
            join(
                q{},
                @{  $self->evaluate(
                        {   type  => $values->[0]->{type},
                            value => [ $inner->[0] ]
                        }
                    )
                }
            ),
            join q{},
            @{ $self->evaluate( $inner->[2] ) }
        );
        my @padded = ();
        for my $i ( 0 .. $#result ) {
            push @padded, $result[$i];
            push @padded, q{ } if ( $i < $#result );
        }
        $rv = \@padded;
    }
    else {
        $rv = $self->evaluate( $values->[0] );
        my $line     = join q{}, @{$rv};
        my $number   = qr{\s*-?(?:\d+(?:[.]\d*)?|[.]\d+)};
        my $operator = qr{\s*(?:[+]|-(?!-)|[*]|/|!=?|<=?|>=?|==)};
        if ( $line =~ m{^$number(?:$operator$number)+\s*$}o ) {
            $rv = [ eval($line) // $line ]; ## no critic (ProhibitStringyEval)
        }
    }

    # evaluate statements after the close parenthesis:
    ## no critic (ProhibitCStyleForLoops)
    for ( my $i = 1; $i < @{$values}; $i++ ) {
        if ( ref $values->[$i] ) {
            my $result = $self->evaluate( $values->[$i] );
            push @{$rv}, @{$result} if ( defined $result );
        }
        elsif ( defined $values->[$i] ) {
            push @{$rv}, $values->[$i];
        }
    }
    return $rv;
};

$Evaluators{'op'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    return [ $raw_tree->{value}->[0] ];
};

$Evaluators{'logical'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    return [ $raw_tree->{value}->[0] ];
};

$Evaluators{'and'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $rv       = [];
    for my $branch ( @{ $raw_tree->{value} } ) {
        next if ( ref $branch and $branch->{type} eq 'whitespace' );
        $rv = $self->evaluate($branch);
        my $test = join q{}, @{$rv};
        $test =~ s{\s+}{}g;
        last if ( not $test );
    }
    return $rv;
};

$Evaluators{'or'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $rv       = [];
    for my $branch ( @{ $raw_tree->{value} } ) {
        next if ( ref $branch and $branch->{type} eq 'whitespace' );
        $rv = $self->evaluate($branch);
        my $test = join q{}, @{$rv};
        $test =~ s{\s+}{}g;
        last if ($test);
    }
    return $rv;
};

# send a series of commands
$Evaluators{'command'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $rv       = [];
    return $rv if ( $self->{validate} );
    for my $branch ( @{ $raw_tree->{value} } ) {
        next if ( ref $branch and $branch->{type} eq 'whitespace' );
        $rv = $self->send_command($branch);
    }
    return $rv;
};

$Evaluators{'pipe'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    $self->fatal_parse_error('bad arguments for pipe')
        if ( not $parse_tree or @{ $parse_tree->{value} } < 2 );
    my @values    = @{ $parse_tree->{value} };
    my $cmd_tree  = shift @values;
    my @functions = ();
    while (@values) {
        my $func_tree = shift @values;

        # unwrap nested statements
        my $trimmed = $self->trim($func_tree);
        my $first   = $trimmed->{value}->[0];
        my $type    = ref $first ? $first->{type} : q{};
        $func_tree = $first
            if ( $type eq 'open_brace' or $type eq 'open_bracket' );
        push @functions, $func_tree;
    }
    return [] if ( $self->{validate} );
    my $id = $self->msg_counter;
    $self->message_id($id);
    if ( @functions > 1 ) {
        $self->callbacks->{$id} = {
            type  => 'pipe',
            value => \@functions
        };
    }
    else {
        $self->callbacks->{$id} = $functions[0];
    }
    my $old_local = $self->set_local( { 'message.from' => '_responder' } );
    $self->send_command($cmd_tree);
    $self->restore_local($old_local);
    $self->fatal_parse_error('no command for pipe') if ( $self->message_id );
    return [];
};

for my $type ( keys %Strings, keys %Ident ) {
    $Evaluators{$type} = sub {
        my $self     = shift;
        my $raw_tree = shift;
        my $rv       = [];
        for my $tok ( @{ $raw_tree->{value} } ) {
            if ( not ref $tok ) {
                push @{$rv}, @{ $self->get_values( $raw_tree, $tok ) };
            }
            else {
                push @{$rv}, @{ $self->evaluate($tok) };
            }
        }
        return $rv;
    };
}

for my $type (qw( close_paren close_brace close_bracket )) {
    $Evaluators{$type} = sub {
        my $self = shift;
        $self->fatal_parse_error("unexpected $type");
    };
}

###########
# Builtins
###########

$H{'read'} = [ "read [ <var name> ]\n", "local foo = {read}\n", ];

$Builtins{'read'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $name_tree  = $parse_tree->{value}->[1];
    my $name       = q{};
    $name = join q{}, @{ $self->evaluate($name_tree) } if ($name_tree);
    $self->fatal_parse_error('bad arguments for read')
        if ( @{ $parse_tree->{value} } > 2 );
    $self->stdin->pause if ( $self->{isa_tty} );
    my $line = eval {<STDIN>};
    $self->stdin->resume if ( $self->{isa_tty} );
    die $@ if ( not defined $line );

    if ($name) {
        chomp $line;
        $Local{$name} = $line;
        return [];
    }
    else {
        return [$line];
    }
};

$H{'print'} = [qq(print "<message>\\n"\n)];

$Builtins{'print'} = sub {
    my $self          = shift;
    my $raw_tree      = shift;
    my $parse_tree    = $self->trim($raw_tree);
    my $argument_tree = $parse_tree->{value}->[1];
    $self->fatal_parse_error('bad arguments for print')
        if ( @{ $parse_tree->{value} } > 2 );
    my $output = join q{}, @{ $self->evaluate($argument_tree) };
    syswrite STDOUT, $output or die if ( length $output );
    return [];
};

for my $type (qw( local var )) {
    $H{$type} = [
        "$type <name> [ <op> [ <value> ] ]\n",
        "    operators: = .= += -= *= /= //= ||=\n"
    ];

    $Builtins{$type} = sub {
        my $self       = shift;
        my $raw_tree   = shift;
        my $key_tree   = undef;
        my $op_tree    = undef;
        my $value_tree = undef;
        my $values     = $raw_tree->{value};
        my $i          = 0;
        $i++
            while ( $raw_tree->{type} eq 'leaf'
            and $values->[$i]->{type} eq 'whitespace' );
        $i++;
        $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
        $key_tree = $values->[ $i++ ];
        $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
        $op_tree = $values->[ $i++ ];
        $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
        $value_tree = $self->fake_tree( 'open_paren', $values, $i );
        my $hash = ( $type eq 'local' ) ? \%Local : \%Var;
        my $key = join q{}, @{ $self->evaluate($key_tree) };
        my $op  = join q{}, @{ $self->evaluate($op_tree) };
        my $rv  = [];

        if ( length $op ) {
            $rv = $self->operate( $key, $op, $value_tree, $hash );
        }
        elsif ( length $key ) {
            $hash->{$key} //= q{};
            if ( ref $hash->{$key} ) {
                $rv = $hash->{$key};
            }
            else {
                $rv = [ $hash->{$key} ];
            }
        }
        else {
            for my $key ( sort keys %{$hash} ) {
                my $output = "$key=";
                if ( ref $hash->{$key} ) {
                    $output
                        .= '["'
                        . join( q{", "}, grep m{\S}, @{ $hash->{$key} } )
                        . '"]';
                }
                else {
                    $output .= $hash->{$key} // q{};
                }
                chomp $output;
                push @{$rv}, "$output\n";
            }
            my $output = join q{}, @{$rv};
            syswrite STDOUT, $output or die if ($output);
            $rv = [];
        }
        return $rv;
    };
}

$H{'shift'} = [ "local foo = { shift [ <var name> ] }\n", ];

$Builtins{'shift'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $key_tree   = $parse_tree->{value}->[1];
    my $key        = q{@};
    $key = join q{}, @{ $self->evaluate($key_tree) } if ($key_tree);
    $self->fatal_parse_error('bad arguments for shift')
        if ( @{ $parse_tree->{value} } > 2 );
    my $hash = undef;

    if ( defined $Local{$key} ) {
        $hash = \%Local;
    }
    elsif ( defined $Var{$key} ) {
        $hash = \%Var;
    }
    else {
        $self->stderr( "WARNING: use of uninitialized value <$key>,",
            " line $self->{counter}\n" );
        return [];
    }
    my $value = $hash->{$key};
    my $rv    = undef;
    if ( ref $value ) {
        $rv = [ shift @{$value} // q{} ];
        shift @{$value} if ( @{$value} and $value->[0] =~ m{^\s*$} );
        $hash->{$key} = q{} if ( not @{$value} );
    }
    else {
        $rv = [$value];
        $hash->{$key} = q{};
    }
    return $rv;
};

$H{'while'} = ["while (<expression>) { <commands> }\n"];

$Builtins{'while'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $test_tree  = $parse_tree->{value}->[1];
    my $then_tree  = $parse_tree->{value}->[2];
    $self->fatal_parse_error('missing argument') if ( not $then_tree );
    $self->fatal_parse_error('bad arguments for while')
        if ( @{ $parse_tree->{value} } > 3 );
    my $test = join q{}, @{ $self->evaluate($test_tree) };
    $test =~ s{^\s*|\s*$}{}g;
    my $rv = [];

    while ($test) {
        $rv = $self->evaluate($then_tree);
    }
    return $rv;
};

$H{'if'} = [
    "if (<expression>) { <commands> }\n"
        . "[ elsif (<expression>) { <commands> } ]\n",
    "[ else { <commands> } ];\n"
];

$Builtins{'if'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $test_tree  = $parse_tree->{value}->[1];
    my $then_tree  = $parse_tree->{value}->[2];
    $self->fatal_parse_error('missing argument') if ( not $then_tree );
    my @elsif_tests = ();
    my @elsif_trees = ();
    my $else_tree   = undef;
    if ( @{ $parse_tree->{value} } > 3 ) {
        my $i = 3;
        while ( $i <= $#{ $parse_tree->{value} } - 1 ) {
            my $cane = $parse_tree->{value}->[ $i++ ];
            my $sugar = join q{}, @{ $self->evaluate($cane) };
            if ( $sugar eq 'elsif' ) {
                my $elsif = $parse_tree->{value}->[ $i++ ];
                my $then  = $parse_tree->{value}->[ $i++ ];
                $self->fatal_parse_error('missing argument') if ( not $then );
                push @elsif_tests, $elsif;
                push @elsif_trees, $then;
            }
            elsif ( $sugar eq 'else' ) {
                my $then = $parse_tree->{value}->[ $i++ ];
                $self->fatal_parse_error('missing argument') if ( not $then );
                push @elsif_tests, { type => 'ident', value => [1] };
                push @elsif_trees, $then;
            }
            else {
                $self->fatal_parse_error(
                    qq(bad arguments: "$sugar" expected: "else" or "elsif"));
            }
        }
        $self->fatal_parse_error(
            "wrong number of arguments for 'if' $i != "
                . scalar @{ $parse_tree->{value} } )
            if ( $i != @{ $parse_tree->{value} } );
    }
    my $test = join q{}, @{ $self->evaluate($test_tree) };
    $test =~ s{^\s*|\s*$}{}g;
    my $rv = [];
    if ($test) {
        $rv = $self->evaluate($then_tree);
    }
    elsif (@elsif_tests) {
        ## no critic (ProhibitCStyleForLoops)
        for ( my $i = 0; $i < @elsif_tests; $i++ ) {
            $test = join q{}, @{ $self->evaluate( $elsif_tests[$i] ) };
            $test =~ s{^\s*|\s*$}{}g;
            if ($test) {
                $rv = $self->evaluate( $elsif_trees[$i] );
                last;
            }
        }
    }
    return $rv;
};

$Builtins{'elsif'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    $self->fatal_parse_error('unexpected elsif');
    return [];
};

$Builtins{'else'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    $self->fatal_parse_error('unexpected else');
    return [];
};

$H{'not'} = [
    qq(not <expression>;\n),
    qq(    ex: if (not 0 > 1;) { print "nope\\n" }\n)
];

# XXX: requires a semicolon in expressions to execute...
# e.g. if (not 0 > 1;) { ... }
$Builtins{'not'} = sub {
    my $self      = shift;
    my $raw_tree  = shift;
    my $test_tree = $self->fake_tree( 'open_paren', $raw_tree->{value}, 1 );
    my $test      = join q{}, @{ $self->evaluate($test_tree) };
    $test =~ s{^\s*|\s*$}{}g;
    return [ not $test ];
};

$H{'for'} = [
    "for <var> (<commands>) { <commands> }\n",
    qq(    ex: for i (one two three) { print "<i>\\n" }\n)
];

$Builtins{'for'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $values   = $raw_tree->{value};
    my $i        = 0;
    $i++
        while ( $raw_tree->{type} eq 'leaf'
        and $values->[$i]->{type} eq 'whitespace' );
    $i++;
    $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
    my $var_tree = $values->[ $i++ ];
    $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
    my $each_tree = $values->[ $i++ ];
    $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
    my $do_tree = $values->[ $i++ ];
    $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
    my $var = join q{}, @{ $self->evaluate($var_tree) };
    $self->fatal_parse_error('bad arguments in for loop')
        if ( not ref $each_tree
        or not @{ $each_tree->{value} }
        or not ref $do_tree
        or not @{ $do_tree->{value} }
        or @{ $raw_tree->{value} } > $i );
    my %old_local = ();
    $old_local{$_} = $Local{$_}
        for ( grep exists $Local{$_}, qw( index total ), $var );
    my $result = $self->evaluate($each_tree);
    $Local{index} = [1];
    $Local{total} = [ scalar @{$result} ];
    my $rv = [];

    for my $i ( @{$result} ) {
        next if ( $i !~ m{\S} );
        $Local{$var} = [$i];
        $rv = $self->evaluate($do_tree);
        $Local{index}->[0]++;
    }
    for my $key ( qw( index total ), $var ) {
        if ( exists $old_local{$key} ) {
            $Local{$key} = $old_local{$key};
        }
        else {
            delete $Local{$key};
        }
    }
    return $rv;
};

$H{'eval'} = [qq(eval "<commands>"\n)];

$Builtins{'eval'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $text_tree  = $parse_tree->{value}->[1];
    my $rv         = [];
    $self->fatal_parse_error('bad arguments for eval')
        if ( @{ $parse_tree->{value} } > 2 );
    if ($text_tree) {
        my $text = join q{}, @{ $self->evaluate($text_tree) };
        $rv = $self->evaluate( $self->parse($text) );
    }
    return $rv;
};

$H{'include'} = ["include <filename>\n"];

$Builtins{'include'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    $self->fatal_parse_error('bad arguments for include')
        if ( not $parse_tree );
    my $path_tree = $parse_tree->{value}->[1];
    my $relative  = join q{}, @{ $self->evaluate($path_tree) };
    my @arguments = ();
    my %new_local = ();
    my %old       = ();
    my @lines     = ();
    my $home      = $Tachikoma{Home} || ( getpwuid $< )[7];
    my $prefix    = join q{/}, $home, '.tachikoma';
    my $path =
          $relative =~ m{^/}
        ? $relative
        : join q{/}, $prefix, $relative;

    if ( $parse_tree and @{ $parse_tree->{value} } > 2 ) {
        push @arguments, $parse_tree->{value}->[$_]
            for ( 2 .. $#{ $parse_tree->{value} } );
    }
    while (@arguments) {
        my $key_tree = shift @arguments;
        my $op_tree  = shift @arguments
            or $self->fatal_parse_error('expected op, got eos');
        my $value_tree = shift @arguments
            or $self->fatal_parse_error('expected string, got eos');
        $self->fatal_parse_error("expected ident, got $key_tree->{type}\n")
            if ( not $Ident{ $key_tree->{type} } );
        $self->fatal_parse_error("expected op, got $op_tree->{type}")
            if ( $op_tree->{type} ne 'op' );
        $self->fatal_parse_error("expected string, got $value_tree->{type}\n")
            if (not $Ident{ $value_tree->{type} }
            and not $Strings{ $value_tree->{type} } );
        my $key   = join q{}, @{ $self->evaluate($key_tree) };
        my $op    = join q{}, @{ $self->evaluate($op_tree) };
        my $value = join q{}, @{ $self->evaluate($value_tree) };
        $self->fatal_parse_error("expected =, got $op")
            if ( $op ne q{=} );
        $new_local{$key} = $value;
    }
    open my $fh, '<', $path
        or $self->fatal_parse_error("couldn't open $path: $!");
    push @lines, $_ while (<$fh>);
    close $fh or die $!;
    my $shell = undef;
    if ( $lines[0] eq "v1\n" ) {
        shift @lines;
        $shell = Tachikoma::Nodes::Shell->new;
        $shell->sink( $self->sink );
    }
    else {
        shift @lines if ( $lines[0] eq "v2\n" );
        $shell = $self;
    }
    my $old_local = $self->set_local( \%new_local );
    for my $line (@lines) {
        my $message = Tachikoma::Message->new;
        $message->[TYPE] = TM_BYTESTREAM;
        $message->[TIMESTAMP] += 3600;    # give the server plenty of time
        $message->[PAYLOAD] = $line;
        $shell->fill($message);
    }
    $self->restore_local($old_local);
    return [];
};

$H{'func'} = ["func <name> { <commands> }\n"];

$Builtins{'func'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $name_tree  = $parse_tree ? $parse_tree->{value}->[1] : undef;
    my $func_tree  = $parse_tree ? $parse_tree->{value}->[2] : undef;
    my $name       = undef;
    my $rv         = [];
    $self->fatal_parse_error('bad arguments for func')
        if ( $parse_tree and @{ $parse_tree->{value} } > 3 );
    $name = join q{}, @{ $self->evaluate($name_tree) } if ($name_tree);

    if ($func_tree) {
        $Functions{$name} = $func_tree;
        $rv = [$func_tree];
    }
    elsif ($name) {
        delete $Functions{$name};
    }
    else {
        for my $key ( sort keys %Functions ) {
            push @{$rv}, "$key\n";
        }
        my $output = join q{}, @{$rv};
        syswrite STDOUT, $output or die if ($output);
        $rv = [];
    }
    return $rv;
};

$H{'remote_func'} = [ "remote_func [ <name> [ { <commands> } ] ]\n", ];

$Builtins{'remote_func'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $name_tree  = $parse_tree ? $parse_tree->{value}->[1] : undef;
    my $func_tree  = $parse_tree ? $parse_tree->{value}->[2] : undef;
    my $name       = undef;
    my $rv         = [];
    $self->fatal_parse_error('bad arguments for func')
        if ( $parse_tree and @{ $parse_tree->{value} } > 3 );
    $name = join q{}, @{ $self->evaluate($name_tree) } if ($name_tree);
    $self->_send_command( 'remote_func', $name,
        $func_tree ? nfreeze($func_tree) : undef )
        if ( not $self->{validate} );
    return $rv;
};

$H{'return'} = ["return <value>\n"];

$Builtins{'return'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $value_tree = $parse_tree->{value}->[1];
    $self->fatal_parse_error('bad arguments for return')
        if ( @{ $parse_tree->{value} } > 2 );
    my $value = join q{}, @{ $self->evaluate($value_tree) };
    die "RV:$value\n";
};

$H{'die'} = ["die <value>\n"];

$Builtins{'die'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $value_tree = $parse_tree->{value}->[1];
    $self->fatal_parse_error('bad arguments for die')
        if ( @{ $parse_tree->{value} } > 2 );
    delete $self->callbacks->{ $self->message_id }
        if ( $self->{message_id} );
    my $value = join q{}, @{ $self->evaluate($value_tree) };
    die "DIE:$value\n";
};

$H{'on'} = ["on <node> <event> <commands>\n"];

$Builtins{'on'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    $self->fatal_parse_error('bad arguments for pipe')
        if ( @{ $parse_tree->{value} } < 4 );
    my @values = @{ $raw_tree->{value} };
    shift @values
        while ( $raw_tree->{type} eq 'leaf'
        and $values[0]->{type} eq 'whitespace' );
    shift @values;
    shift @values if ( $values[0]->{type} eq 'whitespace' );
    my $name_tree   = shift @values;
    my $name_values = $name_tree->{value};
    push @{$name_values}, shift @values
        while ( @values and $values[0]->{type} ne 'whitespace' );
    shift @values if ( $values[0]->{type} eq 'whitespace' );
    my $event_tree = shift @values;
    shift @values if ( $values[0]->{type} eq 'whitespace' );
    my $func_tree = $self->fake_tree( 'open_brace', \@values );
    my $name  = join q{}, @{ $self->evaluate($name_tree) };
    my $event = join q{}, @{ $self->evaluate($event_tree) };
    $self->_send_command( 'on', "$name $event", nfreeze($func_tree) )
        if ( not $self->{validate} );
    return [];
};

$H{'command_node'} = [
    "command_node <path> <command> [ <arguments> ]\n",
    "    alias: command, cmd\n"
];

$Builtins{'command_node'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $values   = $raw_tree->{value};
    my $i        = 0;
    $i++
        while ( $raw_tree->{type} eq 'leaf'
        and $values->[$i]->{type} eq 'whitespace' );
    $i++;
    $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
    my $path_tree   = $values->[ $i++ ];
    my $path_values = $path_tree->{value};
    push @{$path_values}, $values->[ $i++ ]
        while ( $i < @{$values} and $values->[$i]->{type} ne 'whitespace' );
    $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
    $self->fatal_parse_error('bad arguments for command_node')
        if ( $i > $#{$values} );
    my $cmd_tree = $self->fake_tree( 'open_brace', $values, $i );
    my $sub_path = join q{}, @{ $self->evaluate($path_tree) };
    my $old_path = $self->path;
    my $path     = $old_path ? join q{/}, $old_path, $sub_path : $sub_path;
    $self->path($path);    # set $message->[TO]
    $self->cwd($path);     # don't run functions with send_command()
    $self->send_command($cmd_tree);
    $self->path($old_path);
    $self->cwd(undef);
    return [];
};

$Builtins{'command'} = $Builtins{'command_node'};
$Builtins{'cmd'}     = $Builtins{'command_node'};

$H{'tell_node'} = [ "tell_node <path> <info>\n", "    alias: tell\n" ];

$Builtins{'tell_node'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $line     = join q{}, @{ $self->evaluate($raw_tree) };
    my ( $proto, $path, $payload ) = split q{ }, $line, 3;
    my $message = Tachikoma::Message->new;
    $message->type(TM_INFO);
    $message->from( $Local{'message.from'} // $self->{responder}->{name} );
    $message->stream( $Local{'message.stream'} // q{} );
    $message->to( $self->prefix($path) );
    $message->payload( $payload // q{} );
    return [ $self->sink->fill($message) ];
};

$Builtins{'tell'} = $Builtins{'tell_node'};

$H{'send_node'} = [ "send_node <path> <bytes>\n", "    alias: send\n" ];

$Builtins{'send_node'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $line     = join q{}, @{ $self->evaluate($raw_tree) };
    my ( $proto, $path, $payload ) = split q{ }, $line, 3;
    my $message = Tachikoma::Message->new;
    $message->type(TM_BYTESTREAM);
    $message->from( $Local{'message.from'} // $self->{responder}->{name} );
    $message->stream( $Local{'message.stream'} );
    $message->to( $self->prefix($path) );
    $message->payload( $payload // q{} );
    return [ $self->sink->fill($message) ];
};

$Builtins{'send'} = $Builtins{'send_node'};

$H{'send_hash'} = [ "send_hash <path> [ <name> <value> ... ]\n", ];

$Builtins{'send_hash'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    die "ERROR: no JSON support\n" if ( not $USE_JSON );
    my $json = JSON->new;
    my $line = join q{}, @{ $self->evaluate($raw_tree) };
    my ( $proto, $path, $payload ) = split q{ }, $line, 3;
    my $message = Tachikoma::Message->new;
    $message->type(TM_STORABLE);
    $message->from( $Local{'message.from'} // $self->{responder}->{name} );
    $message->stream( $Local{'message.stream'} );
    $message->to( $self->prefix($path) );
    $message->payload( $json->decode($payload) );
    return [ $self->sink->fill($message) ];
};

$H{'bytestream'} = [ "bytestream <path>\n", ];

$Builtins{'bytestream'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $name_tree  = $parse_tree->{value}->[1];
    my $name       = q{};
    $name = join q{}, @{ $self->evaluate($name_tree) } if ($name_tree);
    $self->fatal_parse_error('bad arguments for bytestream')
        if ( @{ $parse_tree->{value} } > 2 );
    my $cwd = $self->path;
    $self->cwd($cwd);    # store cwd
    $self->path( $self->cd( $cwd, $name ) );
    $self->mode('bytestream');
    return [];
};

$H{'ping'} = ["ping [ <path> ]\n"];

$Builtins{'ping'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $line     = join q{}, @{ $self->evaluate($raw_tree) };
    $line =~ s{\s*$}{};
    my ( $proto, $path ) = split q{ }, $line, 2;
    my $message = Tachikoma::Message->new;
    $message->type(TM_PING);
    $message->from( $Local{'message.from'} // $self->{responder}->{name} );
    $message->to( $self->prefix($path) );
    $message->payload($Tachikoma::Right_Now);
    return [ $self->sink->fill($message) ];
};

$H{'debug'} = [ "debug [ <level> ]\n", "    levels: 0, 1, 2\n" ];

$Builtins{'debug'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $level_tree = $parse_tree->{value}->[1];
    my $level      = not $self->dumper->debug;
    $level = join q{}, @{ $self->evaluate($level_tree) } if ($level_tree);
    $self->fatal_parse_error('bad arguments for debug')
        if ( @{ $parse_tree->{value} } > 2 );
    $self->dumper->debug($level) if ( $self->dumper );
    return [];
};

$H{'show_parse'} = [ "show_parse [ <value> ]\n", "    values: 0, 1\n" ];

$Builtins{'show_parse'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $value_tree = $parse_tree->{value}->[1];
    my $value      = not $self->show_parse;
    $value = join q{}, @{ $self->evaluate($value_tree) } if ($value_tree);
    $self->fatal_parse_error('bad arguments for show_parse')
        if ( @{ $parse_tree->{value} } > 2 );
    $self->show_parse($value);
    return [];
};

$H{'show_commands'} = [ "show_commands [ <value> ]\n", "    values: 0, 1\n" ];

$Builtins{'show_commands'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    my $value_tree = $parse_tree->{value}->[1];
    my $value      = not $self->show_commands;
    $value = join q{}, @{ $self->evaluate($value_tree) } if ($value_tree);
    $self->fatal_parse_error('bad arguments for show_commands')
        if ( @{ $parse_tree->{value} } > 2 );
    $self->show_commands($value);
    return [];
};

$H{'respond'} = ["respond\n"];

$Builtins{'respond'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    $self->fatal_parse_error('bad arguments for respond')
        if ( @{ $parse_tree->{value} } > 1 );
    $self->responder->ignore(undef);
    return [];
};

$H{'ignore'} = ["ignore\n"];

$Builtins{'ignore'} = sub {
    my $self       = shift;
    my $raw_tree   = shift;
    my $parse_tree = $self->trim($raw_tree);
    $self->fatal_parse_error('bad arguments for ignore')
        if ( @{ $parse_tree->{value} } > 1 );
    $self->responder->ignore('true');
    return [];
};

$H{'chdir'} = [ "chdir [ <path> ]\n", "    alias: cd\n" ];

$Builtins{'chdir'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $line     = join q{}, @{ $self->evaluate($raw_tree) };
    $line =~ s{\s*$}{};
    my ( $proto, $path ) = split q{ }, $line, 2;
    my $cwd = $self->path;
    $self->path( $self->cd( $cwd, $path ) );
    $self->get_completions;
    return [];
};

$Builtins{'cd'} = $Builtins{'chdir'};

$H{'pwd'} = ["pwd\n"];

$Builtins{'pwd'} = sub {
    my $self = shift;
    $self->_send_command( 'pwd', $self->path );
    return [];
};

$H{'sleep'} = ["sleep <seconds>\n"];

$Builtins{'sleep'} = sub {
    my $self     = shift;
    my $raw_tree = shift;
    my $line     = join q{}, @{ $self->evaluate($raw_tree) };
    $line =~ s{\s*$}{};
    my ( $proto, $seconds ) = split q{ }, $line, 2;
    $self->fatal_parse_error("bad arguments for sleep: $seconds")
        if ( $seconds =~ m{\D} );
    sleep $seconds;
    return [];
};

$Operators{'lt'} = sub { $_[0] lt $_[1] };
$Operators{'gt'} = sub { $_[0] gt $_[1] };
$Operators{'le'} = sub { $_[0] le $_[1] };
$Operators{'ge'} = sub { $_[0] ge $_[1] };
$Operators{'ne'} = sub { $_[0] ne $_[1] };
$Operators{'eq'} = sub { $_[0] eq $_[1] };
$Operators{q{=~}} = sub {
    my @rv = ( ( $_[0] // q{} ) =~ ( $_[1] // q{} ) );
    for my $i ( 0 .. $#rv ) {
        $Local{ '_' . ( $i + 1 ) } = [ $rv[$i] ];
    }
    return @rv;
};
$Operators{q{!~}} = sub { $_[0] !~ $_[1] };
$Operators{q{..}} = sub { $_[0] .. $_[1] };

sub fake_tree {
    my $self   = shift;
    my $type   = shift;
    my $values = shift;
    my $i      = shift // 0;
    return if ( $i > $#{$values} );
    my $first = $values->[$i] // return;
    return {
        type  => $type,
        value => [
            {   type => ref $first ? $first->{type} : 'ident',
                value => [ @{$values}[ $i .. $#{$values} ] ]
            }
        ]
    };
}

# get string values, expanding variables in the process
sub get_values {
    my $self   = shift;
    my $tok    = shift;
    my $value  = shift;
    my $values = undef;
    if ( $tok->{type} eq 'ident' ) {
        if ( $value =~ m{^<([^<>]+)>(\s*)$} ) {
            my $key        = $1;
            my $whitespace = $2;
            if ( defined $Local{$key} ) {
                $values = $Local{$key};
            }
            elsif ( defined $Var{$key} ) {
                $values = $Var{$key};
            }
            else {
                $self->stderr( "WARNING: use of uninitialized value <$key>,",
                    " line $self->{counter}\n" );
                $values = [];
            }
            $values = (
                ref $values
                ? [ @{$values}, $whitespace ]
                : [ $values, $whitespace ]
            );
        }
        else {
            $value =~ s{(?<!\\)<([^<>]+)>}{$Shared{$1}}g;
            $value =~ s{\\([<>])}{$1}g;
            $values = [$value];
        }
    }
    elsif ( $tok->{type} eq 'string1' ) {
        $value =~ s{(?<!\\)<([^<>]+)>}{$Shared{$1}}g;
        $value =~ s{\\([<>])}{$1}g;
        $values = [$value];
    }
    elsif ( $tok->{type} eq 'string3' ) {
        $value =~ s{(?<!\\)<([^<>]+)>}{$Shared{$1}}g;
        $value =~ s{\\([<>])}{$1}g;
        ## no critic (ProhibitBacktickOperators)
        $values = [ split m{(\s+)}, `$value` // q{} ];
    }
    else {
        $values = [$value];
    }
    return $values;
}

sub send_command {
    my $self     = shift;
    my $raw_tree = shift or return [];
    my $cwd      = $self->{cwd};         # set by command_node
    return $self->evaluate($raw_tree) if ( $Command{ $raw_tree->{type} } );
    my $values = $raw_tree->{value};
    my $rv     = [];
    my $i      = 0;
    $i++
        while ( $i < @{$values}
        and ref $values->[$i]
        and $values->[$i]->{type} eq 'whitespace' );
    my $name_tree = $raw_tree->{value}->[ $i++ ] // q{};
    my $name      = join q{},
        @{
        ref $name_tree
        ? $self->evaluate($name_tree)
        : $self->get_values( $raw_tree, $name_tree )
        };
    $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
    my $second_tree = $raw_tree->{value}->[ $i++ ];

    if ( $second_tree and $second_tree->{type} eq 'op' ) {
        $rv = $self->assignment( $name, $raw_tree );
    }
    elsif ( $Builtins{$name} ) {
        $rv = &{ $Builtins{$name} }( $self, $raw_tree );
    }
    elsif ( $Functions{$name} and not $cwd ) {
        $rv = $self->_call_function( $name, $raw_tree );
    }
    elsif ( $raw_tree->{type} eq 'string3' ) {
        $rv = [$name];
    }
    else {
        my $line = $name;
        if ($second_tree) {
            my @arguments = @{ $self->evaluate($second_tree) };
            while ( my $branch = $raw_tree->{value}->[ $i++ ] ) {
                push @arguments, @{ $self->evaluate($branch) };
            }
            $line .= join q{}, q{ }, @arguments;
        }
        $rv = $self->_send_command( split q{ }, $line, 2 )
            if ( $line =~ m{\S} );
    }
    return $rv;
}

sub assignment {
    my $self     = shift;
    my $key      = shift;
    my $raw_tree = shift;
    my $values   = $raw_tree->{value};
    my $i        = 0;
    $i++
        while ( $raw_tree->{type} eq 'leaf'
        and $values->[$i]->{type} eq 'whitespace' );
    $i++;
    $i++ if ( $values->[$i]->{type} eq 'whitespace' );
    my $op_tree = $values->[$i];
    $i++ if ( $values->[$i]->{type} eq 'op' );
    $self->fatal_parse_error('bad arguments for assigment')
        if ( $i > $#{$values} );
    $i++ if ( $i < @{$values} and $values->[$i]->{type} eq 'whitespace' );
    my $value_tree = $self->fake_tree( 'open_paren', $values, $i );
    my $op = join q{}, @{ $self->evaluate($op_tree) };

    if ( exists $Local{$key} ) {
        $self->operate( $key, $op, $value_tree, \%Local );
    }
    elsif ( exists $Var{$key} ) {
        $self->operate( $key, $op, $value_tree, \%Var );
    }
    else {
        $self->fatal_parse_error("no such variable: $key");
    }
    return [];
}

sub operate {    ## no critic (ProhibitExcessComplexity)
    my ( $self, $key, $op, $value_tree, $hash ) = @_;
    my $rv = [];
    my $v  = $hash->{$key};
    $v = [] if ( not defined $v or not length $v );
    $v = [$v] if ( not ref $v );

    if ($value_tree) {
        my $result = $self->evaluate($value_tree);
        shift @{$result} if ( @{$result} and $result->[0] =~ m{^\s+$} );
        pop @{$result}   if ( @{$result} and $result->[-1] =~ m{^\s+$} );
        my $joined = join q{}, @{$result};
        if ( $op eq q{.=} and @{$v} ) { unshift @{$result}, q{ }; }
        if ( $op eq q{=} ) { $v = $result; }
        elsif ( $op eq q{.=} ) { push @{$v}, @{$result}; }
        elsif ( $op eq q{+=} ) { $v->[0] //= 0; $v->[0] += $joined; }
        elsif ( $op eq q{-=} ) { $v->[0] //= 0; $v->[0] -= $joined; }
        elsif ( $op eq q{*=} ) { $v->[0] //= 0; $v->[0] *= $joined; }
        elsif ( $op eq q{/=} ) { $v->[0] //= 0; $v->[0] /= $joined; }
        elsif ( $op eq q{//=} and not @{$v} ) { $v = $result; }
        elsif ( $op eq q{||=} and not join q{}, @{$v} ) { $v = $result; }
        else { $self->fatal_parse_error("invalid operator: $op"); }

        if ( @{$v} > 1 ) {
            $hash->{$key} = $v;
        }
        else {
            $hash->{$key} = $v->[0] // q{};
        }
        $rv = $v;
    }
    elsif ( length $op ) {
        if    ( $op eq q{=} )  { delete $hash->{$key}; }
        elsif ( $op eq q{++} ) { $v->[0]++; $hash->{$key} = $v->[0]; }
        elsif ( $op eq q{--} ) { $v->[0]--; $hash->{$key} = $v->[0]; }
        else { $self->fatal_parse_error("bad arguments: $op"); }
    }
    else {
        $self->fatal_parse_error('internal parser error');
    }
    return $rv;
}

sub _call_function {
    my $self      = shift;
    my $name      = shift;
    my $raw_tree  = shift;
    my @values    = ();
    my @trimmed   = ();
    my $arguments = {};

    # like trim() but keep a copy with whitespace
    for my $tok ( @{ $raw_tree->{value} } ) {
        if ( ref $tok and $tok->{type} eq 'whitespace' ) {
            push @values, $tok->{value}->[0] if (@values);
            next;
        }
        my $dequoted = $self->evaluate($tok);
        push @values, join q{}, @{$dequoted};
        push @trimmed, $dequoted;
    }
    pop @values if ( $values[-1] =~ m{^\s+$} );
    $arguments->{$_} = $trimmed[$_] for ( 1 .. $#trimmed );
    $arguments->{'0'} = shift @values;
    shift @values if ( @values and $values[0] =~ m{^\s+$} );
    $arguments->{'@'}  = \@values;    ## no critic (ProhibitNoisyQuotes)
    $arguments->{'_C'} = $#trimmed;
    return $self->call_function( $name, $arguments );
}

sub call_function {
    my $self      = shift;
    my $name      = shift;
    my $new_local = shift // {};
    my %old_local = %Local;
    my $rv        = [];
    $Local{$_} = $new_local->{$_} for ( keys %{$new_local} );
    my $okay = eval {
        $rv = $self->evaluate( $Functions{$name} );
        return 1;
    };
    %Local = %old_local;

    if ( not $okay ) {
        my $trap = $@ // 'call_function: unknown error';
        chomp $trap;
        my ( $type, $value ) = split m{:}, $trap, 2;
        if ( $type eq 'RV' ) {
            $rv = [$value] if ( defined $value );
        }
        elsif ( $type eq 'DIE' ) {
            $value .= " in function $name\n" if ( $trap !~ m{\n} );
            die $value;
        }
        else {
            die "$trap in function $name\n";
        }
    }
    return $rv;
}

sub _send_command {
    my $self      = shift;
    my $name      = shift;
    my $arguments = shift // q{};
    my $payload   = shift;
    my $path      = shift;
    my $message   = $self->command( $name, $arguments, $payload );
    $message->type( TM_COMMAND | TM_NOREPLY ) if ( not $self->should_reply );
    $message->from( $Local{'message.from'} // $self->{responder}->{name}
            // q{} );
    $message->to( $self->prefix($path) );
    $message->id( $self->message_id );
    $self->dirty($name);
    $self->stderr("+ $name $arguments") if ( $self->{show_commands} );
    return [ $self->sink->fill($message) // 0 ];
}

sub callback {
    my $self      = shift;
    my $id        = shift;
    my $payload   = shift;
    my $error     = shift;
    my $callbacks = $self->callbacks;
    my $rv        = undef;
    if ( $callbacks->{$id} ) {
        my %arguments = ();
        $arguments{'0'} = $id;
        if ( not $error ) {
            $arguments{'1'}  = $payload;
            $arguments{'@'}  = $payload;    ## no critic (ProhibitNoisyQuotes)
            $arguments{'_C'} = 1;
            $arguments{'_ERROR'} = q{};
        }
        else {
            $arguments{'1'}      = q{};
            $arguments{'@'}      = q{};     ## no critic (ProhibitNoisyQuotes)
            $arguments{'_C'}     = 0;
            $arguments{'_ERROR'} = $payload;
        }
        my %old_local = %Local;
        $Local{$_} = $arguments{$_} for ( keys %arguments );
        my $okay = eval {
            $self->send_command( $callbacks->{$id} );
            return 1;
        };
        $self->stderr( $@ // 'callback: unknown error' ) if ( not $okay );
        %Local = %old_local;
        $rv    = 1;
    }
    else {
        $self->stderr( "WARNING: couldn't find callback for id $id\n",
            Dumper $self->callbacks );
    }
    return $rv;
}

sub cd {
    my $self = shift;
    my $cwd  = shift || q{};
    my $path = shift || q{};
    if ( $path =~ m{^/} ) {
        $cwd = $path;
    }
    elsif ( $path =~ m{^[.][.]/?} ) {
        $cwd =~ s{/?[^/]+$}{};
        $path =~ s{^[.][.]/?}{};
        $cwd = $self->cd( $cwd, $path );
    }
    elsif ($path) {
        $cwd .= "/$path";
    }
    $cwd =~ s{(^/|/$)}{}g if ($cwd);
    return $cwd;
}

sub get_completions {
    my $self = shift;
    return
        if ( not $self->stdin
        or not $self->stdin->isa('Tachikoma::Nodes::TTY')
        or not $self->stdin->use_readline );
    my $message = $self->command('help');
    $message->[TYPE] |= TM_COMPLETION;
    $message->[FROM] = $self->responder->name;
    $message->[TO]   = $self->path;
    $self->sink->fill($message);
    $message = $self->command( 'ls', '-a' );
    $message->[TYPE] |= TM_COMPLETION;
    $message->[FROM] = $self->responder->name;
    $message->[TO]   = $self->path;
    $self->sink->fill($message);
    $self->dirty(undef);
    return;
}

sub set_local {
    my $self      = shift;
    my $new_local = shift;
    my %old_local = %Local;
    $Local{$_} = $new_local->{$_} for ( keys %{$new_local} );
    return \%old_local;
}

sub restore_local {
    my $self      = shift;
    my $old_local = shift;
    %Local = %{$old_local};
    return;
}

sub callbacks {
    my $self = shift;
    if (@_) {
        $self->{callbacks} = shift;
    }
    return $self->{callbacks};
}

sub message_id {
    my $self = shift;
    my $rv   = undef;
    if (@_) {
        $self->{message_id} = shift;
    }
    else {
        $rv = $self->{message_id};
        $self->{message_id} = undef;
    }
    return $rv;
}

sub name {
    my $self = shift;
    if (@_) {
        die "ERROR: named Shell nodes are not allowed\n";
    }
    return $self->{name};
}

sub remove_node {
    my $self = shift;
    $self->stdin(undef);
    $self->dumper(undef);
    $self->responder(undef);
    $self->SUPER::remove_node;
    return;
}

sub cwd {
    my $self = shift;
    if (@_) {
        $self->{cwd} = shift;
    }
    return $self->{cwd};
}

sub path {
    my $self = shift;
    if (@_) {
        $self->{path} = shift;
    }
    return $self->{path};
}

sub prefix {
    my $self  = shift;
    my $path  = shift;
    my @paths = ();
    push @paths, $self->{path} if ( length $self->{path} );
    push @paths, $path         if ( length $path );
    return join q{/}, @paths;
}

sub mode {
    my $self = shift;
    if (@_) {
        $self->{mode} = shift;
        if ( $self->stdin ) {
            if ( $self->{mode} ne 'bytestream' ) {

                # print STDERR "un-ignoring EOF\n";
                $self->stdin->on_EOF('close');
            }
            else {
                # print STDERR "ignoring EOF\n";
                $self->stdin->on_EOF('send');
            }
        }
    }
    return $self->{mode};
}

sub msg_counter {
    my $self = shift;
    $Counter = ( $Counter + 1 ) % $Tachikoma::Max_Int;
    return sprintf '%d:%010d', $Tachikoma::Now, $Counter;
}

sub report_error {
    my $self = shift;
    if (@_) {
        my $error = shift;
        $self->{errors}++;
        $self->{validate} = 1 if ( not $self->{isa_tty} );
        $self->stderr($error);
    }
    return;
}

sub isa_tty {
    my $self = shift;
    if (@_) {
        $self->{isa_tty} = shift;
        Tachikoma::EventFrameworks::Select::register_router_node()
            if ( $self->{isa_tty} );
    }
    return $self->{isa_tty};
}

sub should_reply {
    my $self = shift;
    if (@_) {
        $self->{should_reply} = shift;
    }
    return $self->{should_reply} // $Local{'message.from'};
}

sub stdin {
    my $self = shift;
    if (@_) {
        $self->{stdin} = shift;
    }
    return $self->{stdin};
}

sub dumper {
    my $self = shift;
    if (@_) {
        $self->{dumper} = shift;
    }
    return $self->{dumper};
}

sub responder {
    my $self = shift;
    if (@_) {
        $self->{responder} = shift;
    }
    return $self->{responder};
}

sub validate {
    my $self = shift;
    if (@_) {
        $self->{validate} = shift;
    }
    return $self->{validate};
}

sub errors {
    my $self = shift;
    if (@_) {
        $self->{errors} = shift;
    }
    return $self->{errors};
}

sub parse_buffer {
    my $self = shift;
    if (@_) {
        $self->{parse_buffer} = shift;
    }
    return $self->{parse_buffer};
}

sub dirty {
    my $self = shift;
    if (@_) {
        my $name = shift;
        my %dirty = map { $_ => 1 } qw(
            listen_inet listen_unix
            connect_inet disconnect_inet
            connect_unix disconnect_unix
            make_node make
            make_connected_node make_x
            move_node mv
            remove_node rm
            start_job start stop_job stop
            maintain_job maintain sudo_job sudo
            maintain_sudo_job maintain_sudo
            in at every
            add_branch add
            command cmd
        );
        $self->{dirty} = $name ? $dirty{$name} : undef;
    }
    return $self->{dirty};
}

sub prompt {
    my $self = shift;
    if ( $Tachikoma::Now - $self->{last_prompt} > 60 ) {
        $self->{prompt}      = $self->command('prompt');
        $self->{last_prompt} = $Tachikoma::Now;
    }
    my $message = $self->{prompt};
    $message->from( $self->{responder}->{name} );
    $message->to( $self->path );
    return $self->sink->fill($message);
}

sub last_prompt {
    my $self = shift;
    if (@_) {
        $self->{last_prompt} = shift;
    }
    return $self->{last_prompt};
}

sub show_parse {
    my $self = shift;
    if (@_) {
        $self->{show_parse} = shift;
    }
    return $self->{show_parse};
}

sub show_commands {
    my $self = shift;
    if (@_) {
        $self->{show_commands} = shift;
    }
    return $self->{show_commands};
}

sub builtins {
    return \%Builtins;
}

sub TIEHASH {
    my $self = shift;
    my $node = {};
    bless $node, $self;
    return $node;
}

sub FETCH {
    my $self = shift;
    my $key  = shift;
    my $rv   = q{};
    if ( defined $Local{$key} ) {
        $rv = $Local{$key};
    }
    elsif ( defined $Var{$key} ) {
        $rv = $Var{$key};
    }
    else {
        print {*STDERR} "WARNING: use of uninitialized value <$key>\n";
    }
    return ref $rv ? join q{}, @{$rv} : $rv;
}

1;
