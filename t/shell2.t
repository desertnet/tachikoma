#!/usr/bin/env perl -T
# ----------------------------------------------------------------------
# tachikoma shell tests
# ----------------------------------------------------------------------
#

use strict;
use warnings;
use Test::More tests => 97;

use Tachikoma::Nodes::Shell2;
use Tachikoma::Nodes::Callback;
use Tachikoma::Message qw( TM_COMMAND );
use Tachikoma::Command;

my $var = Tachikoma->configuration->{var};

use Data::Dumper;
$Data::Dumper::Indent   = 1;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Useperl  = 1;

#####################################################################
my $shell = Tachikoma::Nodes::Shell2->new;
is( ref $shell, 'Tachikoma::Nodes::Shell2', 'Shell2->new is ok' );

# callback->new
my $answer;
my $destination = Tachikoma::Nodes::Callback->new;
is( ref $destination, 'Tachikoma::Nodes::Callback', 'Callback->new is ok' );
$shell->sink($destination);
$destination->callback(
    sub {
        my $message = shift;
        my $payload = $message->payload;
        if ( $message->type & TM_COMMAND ) {
            my $command = Tachikoma::Command->new($payload);
            $answer .= join q(), q([), $command->name, q(][),
                $command->arguments, q(]), "\n";
        }
        else {
            $answer .= "{$payload}\n";
        }
        return $message->size;
    }
);

#####################################################################

my $parse_tree = $shell->parse('hello');
is_deeply(
    $shell->trim($parse_tree),
    {   'type'  => 'ident',
        'value' => ['hello']
    },
    'parse returns basic parse tree'
);
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[hello][]\n", 'send_command sends commands' );

#####################################################################

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
    'parse returns correct parse tree'
);
$answer = q();
$shell->send_command($parse_tree);
is( $answer,     "", 'var builtin sends nothing' );
is( $var->{foo}, 5,  'var builtin sets variables correctly' );

#####################################################################

$parse_tree = $shell->parse('var bar=(<foo> + 5)');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer,     "", 'nothing is sent by var builtin' );
is( $var->{bar}, 10, 'arithmetic in parenthesis is evaluated' );

#####################################################################

$parse_tree = $shell->parse('bar=<foo> + 3');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer,     "", 'nothing is sent by assignment operator' );
is( $var->{bar}, 8,  'arithmetic in assignment is evaluated' );

#####################################################################

$parse_tree = $shell->parse('var baz ||= <foo> + 4; baz//=<foo> + 5');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer,     "", 'nothing is sent by logical assignment operators' );
is( $var->{baz}, 9,  'logical assignment is evaluated correctly' );

#####################################################################

$parse_tree = $shell->parse('date; bar = <foo> + 7');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer,     "[date][]\n", 'semicolon terminates commands' );
is( $var->{bar}, 12,           'expressions after semicolon are evaluated' );

#####################################################################

$parse_tree = $shell->parse(<<'EOF');
    func true { return 1 };
    func false { return 0 };
    var baz={false;};
    var zab={true;};
EOF
$answer = q();
$shell->send_command($parse_tree);
is( $answer,     "", 'nothing is sent by func builtin' );
is( $var->{baz}, 0,  'expressions inside braces are evaluated' );
is( $var->{zab}, 1,  'functions return correct values' );

#####################################################################

$parse_tree = $shell->parse('var bar++');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer,     "", 'variable iteration sends nothing' );
is( $var->{bar}, 13, 'variable iteration sets variables correctly' );

#####################################################################

$parse_tree = $shell->parse('send echo (2 - -1)');
$answer     = q();
$shell->send_command($parse_tree);
$parse_tree = $shell->parse('send echo (-2 + 1)');
$shell->send_command($parse_tree);
is( $answer, "{3}\n{-1}\n", 'arithmetic operators are evaluated correctly' );

#####################################################################

$parse_tree = $shell->parse('version ; date');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "[version][]\n[date][]\n", 'semicolon separates commands' );

#####################################################################

$parse_tree = $shell->parse('{ date }');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n", 'braces send commands' );

#####################################################################

$parse_tree = $shell->parse('{ send echo foo ; date }');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer,
    "{foo }\n[date][]\n",
    'commands and builtins can be mixed inside a block'
);

#####################################################################

$parse_tree = $shell->parse('send echo hi // there\!');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{hi // there!}\n", 'backslash escapes characters' );

#####################################################################

$parse_tree = $shell->parse('send echo "hi there\\\\"');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{hi there\\}\n", 'backslash escapes backslash' );

#####################################################################

$parse_tree = $shell->parse("send echo 'foo'\\''bar'");
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{foo'bar}\n", 'backslash escapes quotes' );

#####################################################################

$parse_tree = $shell->parse("send echo '\\t\\n'");
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{\\t\\n}\n", 'single quotes escape backslash' );

#####################################################################

$parse_tree = $shell->parse('send echo "\\\\"');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{\\}\n", 'double quotes do not escape backslash' );

#####################################################################

$parse_tree = $shell->parse('send echo "\<test\>"');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{<test>}\n",
    'variables in double quotes are escaped with backslash' );

#####################################################################

$parse_tree = $shell->parse('send echo \<test\>');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{<test>}\n", 'unquoted variables are escaped with backslash' );

#####################################################################
$parse_tree = $shell->parse("send echo \\\\\\\n\n");
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{\\\n\n}\n", 'multiple backslashes are escaped correctly' );

#####################################################################

$parse_tree = $shell->parse('send echo   foo --set arg=\"one two\"  ');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer,
    qq({foo --set arg="one two"  }\n),
    'send builtin drops leading whitespace and preserves trailing whitespace'
);

#####################################################################

$parse_tree = $shell->parse('send echo "  foo --set arg=\"one two\"  "');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer,
    qq({  foo --set arg="one two"  }\n),
    'send builtin preserves leading and trailing quoted whitespace'
);

#####################################################################

$parse_tree = $shell->parse( '
    for name ("foo" "bar") {
        send echo hi "<name>\n";
        send echo bye <name>\n;
    }
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, '{hi foo
}
{bye foon}
{hi bar
}
{bye barn}
', 'for loops set variables and iterate correctly'
);

#####################################################################

$parse_tree = $shell->parse('for x (((1)+1)) { send echo <x> }');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{2 }\n",
    'for loop arguments are evaluated as parenthesized expressions' );

#####################################################################

$parse_tree = $shell->parse('for x ((1 + (0 || 1))) { send echo <x>; }');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{2}\n",
    'parenthesized expressions evaluate arithmetic and logical operators' );

#####################################################################

$parse_tree = $shell->parse( '
{
    if ( 1 ) { date };
    if ( 0 ) { uptime; };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n", 'if statements branch correctly' );

#####################################################################

$parse_tree = $shell->parse( '
{
    if ( 2 + 2 == 4 ) { date };
    if ( 2 + 2 == 5 ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n",
    'if statements evaluate math and logical operators correctly' );

#####################################################################

$parse_tree = $shell->parse( '
{
    if ( "foo" eq "bar" || 1 ) { one };
    if ( "foo" eq "bar" || 0 ) { two };
    if ( "foo" eq "bar" && 1 ) { three };
    if ( "foo" eq "bar" && 0 ) { four };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[one][]\n", 'logical operators are evaluated correctly' );

#####################################################################

$parse_tree = $shell->parse( '
{
    if ( 1 || ) { one };
    if ( 0 || ) { two };
    if ( 1 && ) { three };
    if ( 0 && ) { four };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[one][]\n",
    'logical operators do what you expect when missing values' );

#####################################################################

$parse_tree = $shell->parse( '
{
    if (("foo" eq "bar") || 1) { date };
    if (("foo" eq "bar") || 0) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n",
    'logical operators with parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    if (1 || ("foo" eq "bar")) { date };
    if (0 || ("foo" eq "bar")) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n",
    'logical operators with parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    var mode="";
    if ( (! <mode>) || <mode> eq "update" ) { date };
    var mode="update";
    if ( (! <mode>) || <mode> eq "update" ) { uptime };
    var mode="check";
    if ( (! <mode>) || <mode> eq "update" ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n[uptime][]\n",
    'logical operators with parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    var mode="";
    if ( ! <mode> || (<mode> eq "update") ) { date };
    var mode="update";
    if ( ! <mode> || (<mode> eq "update") ) { uptime };
    var mode="check";
    if ( ! <mode> || (<mode> eq "update") ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n[uptime][]\n",
    'logical operators with parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    var mode="";
    if ( (<mode> eq "update") || (! <mode>) ) { date };
    var mode="update";
    if ( (<mode> eq "update") || (! <mode>) ) { uptime };
    var mode="check";
    if ( (<mode> eq "update") || (! <mode>) ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n[uptime][]\n",
    'logical operators with parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    var mode="";
    if ( (<mode> eq "update") || ! <mode> ) { date };
    var mode="update";
    if ( (<mode> eq "update") || ! <mode> ) { uptime };
    var mode="check";
    if ( (<mode> eq "update") || ! <mode> ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n[uptime][]\n",
    'logical operators with parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    var mode="";
    if ( <mode> eq "update" || (! <mode>) ) { date };
    var mode="update";
    if ( <mode> eq "update" || (! <mode>) ) { uptime };
    var mode="check";
    if ( <mode> eq "update" || (! <mode>) ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n[uptime][]\n",
    'logical operators with parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    var mode="";
    if ( ! <mode> || <mode> eq "update" ) { date };
    var mode="update";
    if ( ! <mode> || <mode> eq "update" ) { uptime };
    var mode="check";
    if ( ! <mode> || <mode> eq "update" ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n[uptime][]\n",
    'logical operators without parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    var mode="";
    if ( <mode> eq "update" || (! <mode>) ) { date };
    var mode="update";
    if ( <mode> eq "update" || (! <mode>) ) { uptime };
    var mode="check";
    if ( <mode> eq "update" || (! <mode>) ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n[uptime][]\n",
    'logical operators with parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    var enable="";
    var tachikoma.foo=1;
    var user=foo;
    if ( <enable> && (! [var "tachikoma.<user>"]) ) { date };
    var enable=1;
    if ( <enable> && (! [var "tachikoma.<user>"]) ) { uptime };
    var tachikoma.bar=0;
    var user=bar;
    if ( <enable> && (! [var "tachikoma.<user>"]) ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[uptime][]\n",
    'logical operators with parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    var mode="";
    if ( <mode> eq "update" || ! <mode> ) { date };
    var mode="update";
    if ( <mode> eq "update" || ! <mode> ) { uptime };
    var mode="check";
    if ( <mode> eq "update" || ! <mode> ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n[uptime][]\n",
    'logical operators without parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    var enable="";
    var tachikoma.foo=1;
    var user=foo;
    if ( <enable> && ! [var "tachikoma.<user>"] ) { date };
    var enable=1;
    if ( <enable> && ! [var "tachikoma.<user>"] ) { uptime };
    var tachikoma.bar=0;
    var user=bar;
    if ( <enable> && ! [var "tachikoma.<user>"] ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[uptime][]\n",
    'logical operators without parenthesized expressions are evaluated correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
{
    local foo = 2;
    local bar = 3;
    local test = [local foo] * [local bar];
    send echo <test>;
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{6}\n", 'expressions are evaluated before operators' );

#####################################################################
$parse_tree = $shell->parse( '
{
    if ( ! 1 > 10 ) { date };
    if ( ! 0 <= 5 ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n", 'not negates operators' );

#####################################################################

$parse_tree = $shell->parse( '
{
    if ( ! 1 + 1 == 3 ) { date; };
    if ( ! 2 + 2 == 4 ) { uptime; };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n", 'not negates arithmetic' );

#####################################################################

$parse_tree = $shell->parse( '
{
    if ( ! { true } ) { date };
    if ( ! { false } ) { uptime };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[uptime][]\n", 'not negates functions' );

#####################################################################

$parse_tree = $shell->parse( '
{
    if ( 0 || 1 && 2 ) { send echo yes; } else { send echo no; };
    if ( 0 || 1 && 0 ) { send echo yes; } else { send echo no; };
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{yes}\n{no}\n", 'else statements branch correctly' );

#####################################################################

$parse_tree = $shell->parse( '
    for x (1 .. 2) {
        for y (3 .. 4) {
           send echo "<index> - <x> - <y>\n";
        };
    }
' );
$answer = q();
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
', 'loops can be nested'
);

#####################################################################

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
$answer = q();
$shell->send_command($parse_tree);
is( $answer, '{1 == 1
}
{1 != 2
}
{2 != 1
}
{2 == 2
}
', 'loops with complex logic can be nested'
);

#####################################################################

$parse_tree = $shell->parse('if ( 0 == (1 && 0) ) { send echo ok; }');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{ok}\n", 'logical operators can be nested' );

#####################################################################

$parse_tree = $shell->parse('send echo ( ( 1 * 3 ) + 5 )');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{8}\n", 'math operators can be nested' );

#####################################################################

$parse_tree = $shell->parse('cd ..');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "", 'out of context operators are not evaluated' );

#####################################################################

$parse_tree = $shell->parse('echo .*');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "[echo][.*]\n", 'out of context operators are passed through' );

#####################################################################

$parse_tree = $shell->parse('send echo if foo var eval func return okay');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer,
    "{if foo var eval func return okay}\n",
    'out of context reserved words are passed through'
);

#####################################################################

$parse_tree = $shell->parse('if ("foo eq bar" eq "foo eq bar") { version; }');
$answer     = q();
$shell->send_command($parse_tree);
$parse_tree = $shell->parse('if ("foo eq bar" ne "foo eq bar") { version; }');
$shell->send_command($parse_tree);
is( $answer, "[version][]\n", 'quoted operators are not evaluated' );

#####################################################################

$parse_tree = $shell->parse('if ("" eq "foo") { version; }');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, q(), 'empty strings evaluate as values' );

#####################################################################

$parse_tree = $shell->parse( '
    var empty = "";
    if ("" ne <empty>) { version; }
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, q(), 'empty variables evaluate as values' );

#####################################################################

$parse_tree = $shell->parse( '
{
    if ("date" =~ "^\\w+$") { send echo yes; }
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{yes}\n", 'regexes can be used as expressions' );

#####################################################################

$parse_tree = $shell->parse( '
{
    if ("foo123bar" =~ "(\d+)") { send echo <_1>; }
}
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{123}\n", 'regexes can capture matches' );

#####################################################################

$parse_tree = $shell->parse( '
    func test {
        var x=(<1> * 5);
        var y=(<2> * 5);
        send echo "1: <1> 2: <2> x: <x> y: <y>\n";
    };
    test 2 3;
    test 10 10;
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, '{1: 2 2: 3 x: 10 y: 15
}
{1: 10 2: 10 x: 50 y: 50
}
', 'functions can be called with arguments'
);

#####################################################################

$parse_tree = $shell->parse( '
    func test { return "hello\n"; };
    send echo { test; };
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{hello\n}\n", 'functions can return values' );

#####################################################################

$parse_tree = $shell->parse( '
    func test { version; return "hello\n"; };
    send echo { test; };
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[version][]\n{hello\n}\n", 'functions return correct values' );

#####################################################################

$parse_tree = $shell->parse('if (1 < {date;}) { send echo yes; }');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n{yes}\n",
    'braces can pass return values from commands with semicolons' );

#####################################################################

$parse_tree = $shell->parse('if ({date} > 1) { send echo yes; }');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "[date][]\n{yes}\n",
    'braces can pass return values from commands without semicolons' );

#####################################################################

$parse_tree = $shell->parse( '
    func echo { send echo "<@>\n"; };
    func test { return "okay"; };
    if ({test;} eq "okay") { echo "success"; };
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{success\n}\n",
    'braces pass correct return values from functions' );

#####################################################################

$parse_tree = $shell->parse('echo (.5 + -.2)');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{0.3\n}\n", 'floating point arithmetic can be used' );

#####################################################################

$parse_tree = $shell->parse( '
    var f=echo;
    <f>   foo   bar
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{foo   bar\n}\n", 'variables can be used as function names' );

#####################################################################

$parse_tree = $shell->parse( '
    func test { send echo <0> <@>"\n"; };
    test "foo  bar"
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer,
    "{test foo  bar\n}\n",
    'whitespace is preserved in function arguments'
);

#####################################################################

$parse_tree = $shell->parse( '
    func test { echo <0> <@>; };
    test "foo  bar"
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer,
    "{test foo  bar\n}\n",
    'whitespace is preserved through a stack of functions'
);

#####################################################################

$parse_tree = $shell->parse('send echo ((foo eq foo)"\n");');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{1\n}\n", 'nested parenthesis are evaluated for operators' );

#####################################################################

$parse_tree = $shell->parse( '
    var i=;
    func test { var i++; echo <i>; return <i> };
    echo (((test();) + 100))
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{1\n}\n{101\n}\n", 'nested parentheses are otherwise ignored' );

#####################################################################

$parse_tree = $shell->parse(q({ uptime; { echo foo } }));
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "[uptime][]\n{foo\n}\n[38][]\n", 'commands can be nested inside blocks' );

#####################################################################

$parse_tree = $shell->parse(q({ uptime; { echo foo } date }));
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "[uptime][]\n{foo\n}\n[38][date]\n",
    'expressions after blocks are evaluated' );

#####################################################################

$parse_tree = $shell->parse(q({ uptime; { echo foo } ; date }));
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "[uptime][]\n{foo\n}\n[38][]\n[date][]\n",
    'semicolons separate blocks and expressions' );

#####################################################################

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
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{echo date\n}\n", 'variables are expanded correctly in eval' );

#####################################################################

$parse_tree = $shell->parse( '
    var list=(1 2);
    for x (<list>) { echo <x>; };
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{1\n}\n{2\n}\n", 'variables can be expanded as lists' );

#####################################################################

$parse_tree = $shell->parse( '
    list=("one two" three);
    for x ("foo bar" <list> kk) { echo <x> };
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, '{foo bar
}
{one two
}
{three
}
{kk
}
', 'variables with list values are expanded correctly'
);

#####################################################################

$parse_tree = $shell->parse( '
    var list=(one  two);
    echo <list>;
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer,
    "{one  two\n}\n",
    'variables with list values still preserve whitespace'
);

#####################################################################

$parse_tree = $shell->parse( '
    for x (1 .. 4) { cmd host<x> foo<x>bar }
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer,
    "[foo1bar][]\n[foo2bar][]\n[foo3bar][]\n[foo4bar][]\n",
    'variables expand without additional whitespace'
);

#####################################################################

$parse_tree = $shell->parse( '
    echo ( 0 || foo "bar\n" )
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer,
    "{foo bar\n\n}\n",
    'logical expressions expand without additional whitespace'
);

#####################################################################

$parse_tree = $shell->parse('echo "\<foo\>"');
$answer     = q();
$shell->send_command($parse_tree);
is( $answer, "{<foo>\n}\n", 'variables can be escaped with backslash' );

#####################################################################

$parse_tree = $shell->parse( '
    func test { for this (<1>) { echo <this> } };
    test ("foo bar" baz)
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer,
    "{foo bar\n}\n{baz\n}\n",
    'a parenthesized list is a single function argument'
);

#####################################################################

$parse_tree = $shell->parse( '
    foo=bar; # comment!
    # { var foo=bad };
    echo <foo>;
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "{bar\n}\n", 'comments can be used' );

#####################################################################

$parse_tree = $shell->parse( '
    list=("one two" "three");
    <list>;
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer,
    "[one][two three]\n",
    'lists are expanded into commands on whitespace boundaries'
);

#####################################################################

$parse_tree = $shell->parse( '
    var list;
    var foo=(var list;);
    <foo>;
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer, "[one][two three]\n", 'lists can be copied between variables' );

#####################################################################

$parse_tree = $shell->parse( '
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
' );
$answer = q();
$shell->send_command($parse_tree);
is( $answer,
    "{sum == 0\n}\n[43][]\n{sum == 42\n}\n",
    'local controls variable scope'
);

#####################################################################
1;
