#!/usr/bin/perl
use strict;
use warnings;
require 'config.pl';

sub workstation_header {
    print <<EOF;
v2
include services/config.tsl

# listen_inet 127.0.0.1:4230
make_node CommandInterpreter hosts
make_node JobController      jobs
command jobs start_job Tail  local_server_log /var/log/tachikoma/tachikoma-server.log
make_node Ruleset            server_log:ruleset
make_node Tee                server_log:tee
make_node Tee                server_log
make_node Tee                error_log:tee
make_node Tee                error_log
make_node Ruleset            local_system_log:ruleset
make_node Ruleset            system_log:ruleset
make_node Tee                system_log:tee
make_node Tee                system_log
make_node Tee                silc_dn:tee
make_node Null               null
make_node Echo               echo
make_node Scheduler          scheduler

cd server_log:ruleset:config
  add  100 deny where payload=.* FROM: .* ID: "tachikoma\@<hostname>(?:\\.?.*)" COMMAND: .*
  add  200 deny where payload=silo .* pub .* user .* addr .* is rfc1918
  add  999 copy to error_log:tee where payload="WARNING:|ERROR:|FAILED:|TRAP:|COMMAND:|CircuitTester:|reconnect:"
  add 1000 redirect to server_log:tee
cd ..

cd local_system_log:ruleset:config
  add 100 allow where payload=sudo
  add 1000 deny
cd ..

cd system_log:ruleset:config
  add 200  deny where payload=ipmi0: KCS
  add 1000 redirect to system_log:tee
cd ..

command jobs start_job Transform server_log:color '/usr/local/etc/tachikoma/LogColor.conf' 'Log::Color::filter(\@_)'
command jobs start_job Transform error_log:color  '/usr/local/etc/tachikoma/LogColor.conf' 'Log::Color::filter(\@_)'
command jobs start_job Transform system_log:color '/usr/local/etc/tachikoma/LogColor.conf' 'Log::Color::filter(\@_)'
command jobs start_job Tail      http_log         /var/log/tachikoma/http-access.log

connect_node system_log:color         system_log
connect_node system_log:tee           system_log:color
connect_node local_system_log:ruleset system_log:ruleset
connect_node error_log:color          error_log
connect_node error_log:tee            error_log:color
connect_node server_log:color         server_log
connect_node server_log:tee           server_log:color
connect_node local_server_log         server_log:ruleset
connect_node http_log                 null

EOF
}

sub workstation_benchmarks {
    print <<EOF;


# benchmarks
listen_inet       127.0.0.1:5000
listen_inet       127.0.0.1:5001
listen_inet --io  127.0.0.1:6000
connect_edge 127.0.0.1:5000 null
connect_edge 127.0.0.1:6000 null
on 127.0.0.1:5001 authenticated {
    make_node Null benchmark:timer 0 512 100;
    connect_sink benchmark:timer <1>;
}
on 127.0.0.1:5001 EOF rm benchmark:timer
on 127.0.0.1:6000 connected {
    make_node Null benchmark:timer 0 16 65000;
    connect_sink benchmark:timer <1>;
}
on 127.0.0.1:6000 EOF rm benchmark:timer

EOF
}

sub workstation_partitions {
    print <<EOF;


# partitions
make_node Partition scratch:log --filename=/logs/scratch.log --segment_size=(32 * 1024 * 1024)
make_node Partition offset:log  --filename=/logs/offset.log  --segment_size=(256 * 1024)
make_node Consumer scratch:consumer --partition=scratch:log --offsetlog=offset:log

EOF
}

sub workstation_services {
    print <<EOF;


# services
var services = "<home>/.tachikoma/services";

command jobs  run_job Shell <services>/hubs.tsl
command hosts connect_inet localhost:<tachikoma.hubs.port>      hubs:service

command jobs  run_job Shell <services>/indexers.tsl
command hosts connect_inet localhost:<tachikoma.indexers.port>  indexers:service

command jobs  run_job Shell <services>/tables.tsl
command hosts connect_inet localhost:<tachikoma.tables.port>    tables:service

command jobs  run_job Shell <services>/engines.tsl
command hosts connect_inet localhost:5499                       engines:service

command jobs  run_job Shell <services>/hunter.tsl
command hosts connect_inet localhost:<tachikoma.hunter.port>    hunter:service

command jobs  run_job Shell <services>/topic_top.tsl
command hosts connect_inet localhost:<tachikoma.topic_top.port> topic_top:service

command jobs  run_job Shell <services>/http.tsl
command hosts connect_inet localhost:<tachikoma.http.port>      http:service



# ingest server logs
connect_node server_log:tee indexers:service/server_log

EOF
}

sub workstation_sound_effects {
    print <<EOF;


# sound effects
func get_sound   { return "/System/Library/Sounds/<1>.aiff" }
func afplay      { send AfPlay:sieve <1> }
func cozmo_alert { send CozmoAlert:sieve <1> }

make_node MemorySieve AfPlay:sieve     1
make_node JobFarmer   AfPlay           4 AfPlay
make_node MemorySieve CozmoAlert:sieve 1
make_node JobFarmer   CozmoAlert       1 CozmoAlert
make_node Function server_log:sounds '{
    local sound = "";
    # if (<1> =~ "\\sWARNING:\\s")    [ sound = Tink;                    ]
    if (<1> =~ "\\sERROR:\\s(.*)")  [ sound = Tink;   cozmo_alert <_1> ]
    elsif (<1> =~ "\\sFAILURE:\\s") [ sound = Sosumi;                  ]
    elsif (<1> =~ "\\sCOMMAND:\\s") [ sound = Hero;                    ];
    if (<sound>) { afplay { get_sound <sound> } };
}'
make_node Function silc:sounds '{
    local sound = Pop;
    if (<1> =~ "\\bchris\\b(?!>)") [ sound = Glass ];
    afplay { get_sound <sound> };
}'
command AfPlay     lazy on
command CozmoAlert lazy on
connect_node CozmoAlert       null
connect_node CozmoAlert:sieve CozmoAlert:load_balancer
connect_node AfPlay           null
connect_node AfPlay:sieve     AfPlay:load_balancer
connect_node server_log:tee   server_log:sounds
connect_node silc_dn:tee      silc:sounds

EOF
}

sub workstation_hosts {
    print <<EOF;
cd hosts
  connect_inet --scheme=rsa-sha256 --use-ssl tachikoma:4231
  connect_inet --scheme=rsa-sha256 --use-ssl tachikoma:4232 server_logs
  connect_inet --scheme=rsa-sha256 --use-ssl tachikoma:4233 system_logs
  connect_inet --scheme=rsa-sha256 --use-ssl tachikoma:4234 silc_dn
cd ..

connect_node silc_dn                  silc_dn:tee
connect_node system_logs              system_log:ruleset
connect_node server_logs              server_log:ruleset

EOF
}

1;
