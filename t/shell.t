#!/usr/bin/env perl
# ----------------------------------------------------------------------
# tachikoma shell tests
# ----------------------------------------------------------------------
#
# $Id$
#
use strict;
use warnings;
use Test::More tests => 80;

use Tachikoma;
use Tachikoma::Nodes::Shell2;
use Tachikoma::Nodes::Callback;
use Tachikoma::Message qw( TM_COMMAND );
use Tachikoma::Command;
use Tachikoma::Config qw( %Var );

use Data::Dumper;
$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Useperl  = 1;

#####################################################################
# shell->new
my $shell = Tachikoma::Nodes::Shell2->new;
is( ref($shell), 'Tachikoma::Nodes::Shell2', 'Shell2::new()' );

# callback->new
my $answer;
my $destination = Tachikoma::Nodes::Callback->new;
is( ref($destination), 'Tachikoma::Nodes::Callback', 'Callback::new()' );
$shell->sink($destination);
$destination->callback(
    sub {
        my $message = shift;
        my $payload = $message->payload;
        if ( $message->type & TM_COMMAND ) {
            my $command = Tachikoma::Command->new($payload);
            $answer .= join q{}, q{[}, $command->name, q{][},
                $command->arguments, q{]}, "\n";
        }
        else {
            $answer .= "{$payload}\n";
        }
        return $message->size;
    }
);

#####################################################################
# test parse
my $parse_tree = $shell->parse('hello');
is_deeply(
    $shell->trim($parse_tree),
    {   'type'  => 'ident',
        'value' => ['hello']
    },
    'basic parse'
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "[hello][]\n", 'basic evaluate' );

#####################################################################
# test variables
$parse_tree = $shell->parse('var foo=5');
is_deeply(
    $shell->trim($parse_tree),
    {   'type'  => 'ident',
        'value' => [
            'var',
            {   'type'  => 'ident',
                'value' => ['foo']
            },
            {   'type'  => 'op',
                'value' => ['=']
            },
            {   'type'  => 'number',
                'value' => [5]
            },
        ]
    },
    'parse set variable'
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer,   "", 'evaluate set variable' );
is( $Var{foo}, 5,  'variable is set correctly' );

#####################################################################
# test variable with extended arguments 1
$parse_tree = $shell->parse('var bar=(<foo> + 5)');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer,   "", 'evaluate variable with extended arguments 1' );
is( $Var{bar}, 10, 'variable is set correctly' );

#####################################################################
# test variable with extended arguments 2
$parse_tree = $shell->parse('bar=<foo> + 3');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer,   "", 'evaluate variable with extended arguments 2' );
is( $Var{bar}, 8,  'variable is set correctly' );

#####################################################################
# test variable with extended arguments 3
$parse_tree = $shell->parse('date; bar = <foo> + 7');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer,   "[date][]\n", 'evaluate variable with extended arguments 3' );
is( $Var{bar}, 12,           'variable is set correctly' );

#####################################################################
# test variable with extended arguments 4
$parse_tree = $shell->parse(<<'EOF');
    func true { return 1 };
    func false { return 0 };
    var baz={false;};
    var zab={true;};
EOF
$answer = '';
$shell->send_command($parse_tree);
is( $answer,   "", 'evaluate variable with extended arguments 4' );
is( $Var{baz}, 0,  'variable is set correctly' );
is( $Var{zab}, 1,  'variable is set correctly' );

#####################################################################
# test variable iteration
$parse_tree = $shell->parse('var bar++');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer,   "", 'evaluate variable iteration' );
is( $Var{bar}, 13, 'variable is set correctly' );

#####################################################################
# test math 1
$parse_tree = $shell->parse('send echo (2 - -1)');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{3}\n", 'evaluate math 1' );

#####################################################################
# test math 2
$parse_tree = $shell->parse('send echo (-2 + 1)');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{-1}\n", 'evaluate math 2' );

#####################################################################
# test multiple commands
$parse_tree = $shell->parse('send echo foo ; date');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{foo }\n[date][]\n", 'evaluate multiple commands' );

#####################################################################
# test command inside a block
$parse_tree = $shell->parse('{ date }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "[date][]\n", 'evaluate command inside a block' );

#####################################################################
# test multiple commands inside a block
$parse_tree = $shell->parse('{ send echo foo ; date }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer,
    "{foo }\n[date][]\n",
    'evaluate multiple commands inside a block'
);

#####################################################################
# test unescape
$parse_tree = $shell->parse('send echo hi there\!');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{hi there!}\n", 'evaluate unescape' );

#####################################################################
# test unescape 2
$parse_tree = $shell->parse('send echo "hi there\\\\"');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{hi there\\}\n", 'evaluate unescape 2' );

#####################################################################
# test unescape 3
$parse_tree = $shell->parse("send echo 'foo'\\''bar'");
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{foo'bar}\n", 'evaluate unescape 3' );

#####################################################################
# test unescape 4
$parse_tree = $shell->parse("send echo '\\\\'");
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{\\\\}\n", 'evaluate unescape 4' );

#####################################################################
# test unescape 5
$parse_tree = $shell->parse("send echo \\\\\\\n\n");
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{\\\n\n}\n", 'evaluate unescape 5' );

#####################################################################
# test whitespace
$parse_tree = $shell->parse('send echo foo --set arg=\"one two\"');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, qq({foo --set arg="one two"}\n), 'evaluate whitespace' );

#####################################################################
# test multiple commands in a loop
$parse_tree = $shell->parse( '
    for name ("foo" "bar") {
        send echo hi <name>\n;
        send echo bye <name>\n;
    }
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, '{hi foo
}
{bye foo
}
{hi bar
}
{bye bar
}
', 'evaluate multiple commands in a loop'
);

#####################################################################
# test loop arguments 1
$parse_tree = $shell->parse('for x (((1)+1)) { send echo <x> }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{2 }\n", 'evaluate loop arguments 1' );

#####################################################################
# test loop arguments 2
$parse_tree = $shell->parse('for x ((1 + (0 || 1))) { send echo <x>; }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{2}\n", 'evaluate loop arguments 2' );

#####################################################################
# test simple logic in a block
$parse_tree = $shell->parse( '
{
    if ( 1 ) { date };
    if ( 0 ) { uptime; };
}
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "[date][]\n", 'evaluate simple logic in a block' );

#####################################################################
# test logical operators and math
$parse_tree = $shell->parse( '
{
    if ( 2 + 2 == 4 ) { date };
    if ( 2 + 2 == 5 ) { uptime };
}
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "[date][]\n", 'evaluate logical operators and math' );

#####################################################################
# test blocks and math
$parse_tree = $shell->parse( '
{
    local foo = 2;
    local bar = 3;
    local test = [local foo] * [local bar];
    send echo <test>;
}
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{6}\n", 'evaluate logical operators and math' );

#####################################################################
# test negation
$parse_tree = $shell->parse( '
{
    if ( not 1 + 1 == 3; ) { date; };
    if ( not 2 + 2 == 4; ) { uptime; };
}
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "[date][]\n", 'evaluate negation' );

#####################################################################
# test negation on operators
$parse_tree = $shell->parse( '
{
    if ( not 1 > 10; ) { date };
    if ( not 0 <= 5; ) { uptime };
}
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "[date][]\n", 'evaluate negation on operators' );

#####################################################################
# test negation on functions
$parse_tree = $shell->parse( '
{
    if ( not { true }; ) { date };
    if ( not { false }; ) { uptime };
}
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "[uptime][]\n", 'evaluate negation on functions' );

#####################################################################
# test more logical operators
$parse_tree = $shell->parse( '
{
    if ( 0 || 1 && 2 ) { send echo yes; } else { send echo no; };
    if ( 0 || 1 && 0 ) { send echo yes; } else { send echo no; };
}
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{yes}\n{no}\n", 'evaluate more logical operators' );

#####################################################################
# test nested loops
$parse_tree = $shell->parse( '
    for x (1 .. 2) {
        for y (3 .. 4) {
           send echo <index> - <x> - <y>\n;
        };
    }
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer,
    '{1 - 1 - 3
}
{2 - 1 - 4
}
{1 - 2 - 3
}
{2 - 2 - 4
}
', 'evaluate nested loop'
);

#####################################################################
# test nested loops, logic, and variables
$parse_tree = $shell->parse( '
{
    var max_x=2;
    var max_y=(6 / 3 * 2 - 2);
    for x (1 .. <max_x>) {
        for y (1 .. <max_y>) {
            if (<x> == <y>) {
                send echo "<x> == <y>\n";
            }
            else {
                send echo "<x> != <y>\n";
            };
        };
    }
}
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, '{1 == 1
}
{1 != 2
}
{2 != 1
}
{2 == 2
}
', 'evaluate nested loops, logic, and variables'
);

#####################################################################
# test nested logical operators
$parse_tree = $shell->parse('if ( 0 == (1 && 0) ) { send echo ok; }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{ok}\n", 'evaluate nested logical operators' );

#####################################################################
# test nested math operators
$parse_tree = $shell->parse('send echo ( ( 1 * 3 ) + 5 )');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{8}\n", 'evaluate nested math operators' );

#####################################################################
# test operators out of context 1
$parse_tree = $shell->parse('cd ..');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "", 'evaluate operators out of context 1' );

#####################################################################
# test operators out of context 2
$parse_tree = $shell->parse('echo .*');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "[echo][.*]\n", 'evaluate operators out of context 2' );

#####################################################################
# test reserved words out of context
$parse_tree = $shell->parse('send echo if foo var eval func return okay');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer,
    "{if foo var eval func return okay}\n",
    'evaluate reserved words out of context'
);

#####################################################################
# test quoted operators 1
$parse_tree = $shell->parse('if ("foo eq bar" eq "foo eq bar") { version; }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "[version][]\n", 'evaluate quoted operators 1' );

#####################################################################
# test quoted operators 2
$parse_tree = $shell->parse('if ("foo eq bar" ne "foo eq bar") { version; }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, '', 'evaluate quoted operators 2' );

#####################################################################
# test operators on empty strings 1
$parse_tree = $shell->parse('if ("" eq "foo") { version; }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, '', 'evaluate operators on empty strings 2' );

#####################################################################
# test operators on empty strings 2
$parse_tree = $shell->parse('if ("" ne "") { version; }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, '', 'evaluate operators on empty strings 2' );

#####################################################################
# test regex 1
$parse_tree = $shell->parse(
    '{
    if ("date" =~ "^\\w+$") { send echo yes; }
}'
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{yes}\n", 'evaluate regex 1' );

#####################################################################
# test regex 2
$parse_tree = $shell->parse(
    '{
    if ("foo123bar" =~ "(\d+)") { send echo <_1>; }
}'
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{123}\n", 'evaluate regex 2' );

#####################################################################
# test functions
$parse_tree = $shell->parse( '
    func test {
        var x=(<1> * 5);
        var y=(<2> * 5);
        send echo "1: <1> 2: <2> x: <x> y: <y>\n";
    };
    test 2 3;
    test 10 10;
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, '{1: 2 2: 3 x: 10 y: 15
}
{1: 10 2: 10 x: 50 y: 50
}
', 'evaluate functions'
);

#####################################################################
# test functions 2
$parse_tree = $shell->parse( '
    func test2 {
        var x2=(<1> * 5);
        var y2=(<2> * 5);
        send echo "1: <1> 2: <2> x: <x2> y: <y2>\n";
    };
    test2 2 3;
    test2 10 10;
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, '{1: 2 2: 3 x: 10 y: 15
}
{1: 10 2: 10 x: 50 y: 50
}
', 'evaluate functions 2'
);

#####################################################################
# test function return 1
$parse_tree = $shell->parse( '
    func test { return "hello\n"; };
    send echo { test; };
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{hello\n}\n", 'evaluate function return 1' );

#####################################################################
# test function return 2
$parse_tree = $shell->parse( '
    func test { version; return "hello\n"; };
    send echo { test; };
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "[version][]\n{hello\n}\n", 'evaluate function return 2' );

#####################################################################
# test returned values 1
$parse_tree = $shell->parse('if (1 < {date;}) { send echo yes; }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "[date][]\n{yes}\n", 'evaluate returned values 1' );

#####################################################################
# test returned values 2
$parse_tree = $shell->parse('if ({date} > 1) { send echo yes; }');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "[date][]\n{yes}\n", 'evaluate returned values 2' );

#####################################################################
# test returned values 3
$parse_tree = $shell->parse( '
    func echo { send echo "<@>\n"; };
    func test { return "okay"; };
    if ({test;} eq "okay") { echo "success"; };
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{success\n}\n", 'evaluate returned values 3' );

#####################################################################
# test floating point
$parse_tree = $shell->parse('echo (.5 + -.2)');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{0.3\n}\n", 'evaluate floating point' );

#####################################################################
# test variable functions
$parse_tree = $shell->parse( '
    var f=echo;
    <f>   foo   bar
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{foo   bar\n}\n", 'evaluate variable functions' );

#####################################################################
# test function arguments 1
$parse_tree = $shell->parse( '
    func test { send echo <0> <@>\n; };
    test "foo  bar"
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{test foo  bar\n}\n", 'evaluate function arguments 1' );

#####################################################################
# test function arguments 2
$parse_tree = $shell->parse( '
    func test { echo <0> <@>; };
    test "foo  bar"
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{test foo  bar\n}\n", 'evaluate function arguments 2' );

#####################################################################
# test nested parentheses 1
$parse_tree = $shell->parse( '
    var i=;
    func test { var i++; echo <i>; return <i> };
    echo (((test();) + 100))
' );
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{1\n}\n{101\n}\n", 'evaluate nested parentheses 1' );

#####################################################################
# test nested parentheses 2
$parse_tree = $shell->parse('send echo ((foo eq foo)\n);');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{1\n}\n", 'evaluate nested parentheses 2' );

#####################################################################
# test commands in nested blocks 1
$parse_tree = $shell->parse(q({ local; { echo foo } }));
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{foo\n}\n[28][]\n", 'evaluate commands in nested blocks 1' );

#####################################################################
# test commands in nested blocks 2
$parse_tree = $shell->parse(q({ local; { echo foo } date }));
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{foo\n}\n[28][date]\n",
    'evaluate commands in nested blocks 2' );

#####################################################################
# test commands in nested blocks 3
$parse_tree = $shell->parse(q({ local; { echo foo } ; date }));
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{foo\n}\n[28][]\n[date][]\n",
    'evaluate commands in nested blocks 3' );

#####################################################################
# test variable expansion 1
$parse_tree = $shell->parse(
    q(
    func replace {
        if ("<1>" =~ "<2>") {
            local result=( eval "\"<3>\""; );
            return <result>;
        }
        else {
            return <1>;
        };
    };
    echo {replace "echo1234date" '^(\D+)\d+(\D+)$' '<_1> <_2>'};
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{echo date\n}\n", 'evaluate variable expansion 1' );

#####################################################################
# test variable expansion 2
$parse_tree = $shell->parse(
    q(
    var list=("one two" three);
    for x ("foo bar" <list> kk) { echo <x> };
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, '{foo bar
}
{one two
}
{three
}
{kk
}
', 'evaluate variable expansion 2'
);

#####################################################################
# test variable expansion 3
$parse_tree = $shell->parse(
    q(
    list=(1 2);
    for x (<list>) { echo <x>; };
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{1\n}\n{2\n}\n", 'evaluate variable expansion 3' );

#####################################################################
# test variable expansion 4
$parse_tree = $shell->parse(
    q(
    var list=(one  two);
    echo <list>;
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{one  two\n}\n", 'evaluate variable expansion 4' );

#####################################################################
# test variable expansion 5
$parse_tree = $shell->parse(
    q(
    for x (1 .. 4) { cmd host<x> foo<x>bar }
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer,
    "[foo1bar][]\n[foo2bar][]\n[foo3bar][]\n[foo4bar][]\n",
    'evaluate variable expansion 5'
);

#####################################################################
# test variable escape
$parse_tree = $shell->parse('echo "\<foo\>"');
$answer     = '';
$shell->send_command($parse_tree);
is( $answer, "{<foo>\n}\n", 'evaluate variable escape' );

#####################################################################
# test list arguments
$parse_tree = $shell->parse(
    q(
    func test { for this (<1>) { echo <this> } };
    test ("foo bar" baz)
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{foo bar\n}\n{baz\n}\n", 'evaluate list arguments' );

#####################################################################
# test comments
$parse_tree = $shell->parse(
    q(
    foo=bar; # comment!
    # { var foo=bad };
    echo <foo>;
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "{bar\n}\n", 'evaluate comments' );

#####################################################################
# test list
$parse_tree = $shell->parse(
    q(
    list=("one two" "three");
    <list>;
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "[one][two three]\n", 'evaluate list' );

#####################################################################
# test var list
$parse_tree = $shell->parse(
    q(
    var list;
    var foo=(var list;);
    <foo>;
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer, "[one][two three]\n", 'evaluate var list' );

#####################################################################
# test variable localization 1
$parse_tree = $shell->parse(
    q(
    {
        local sum=42;
        {
            local sum=0;
            for i (1 .. 10) {
                local sum += <i>
            };
            echo sum == <sum>
        };
        echo sum == <sum>
    }
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer,
    "{sum == 0\n}\n[33][]\n{sum == 42\n}\n",
    'evaluate variable localization 1'
);

#####################################################################
# test variable localization 2
$parse_tree = $shell->parse(
    q(
    {
        local sum=23;
        {
            local sum=0;
            for i (1 .. 10) [
                local sum += <i>
            ];
            echo sum == <sum>
        };
        echo sum == <sum>
    }
)
);
$answer = '';
$shell->send_command($parse_tree);
is( $answer,
    "{sum == 55\n}\n[34][]\n{sum == 23\n}\n",
    'evaluate variable localization 2'
);

#####################################################################
1;
