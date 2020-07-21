#!/usr/bin/perl
use strict;
use warnings;
require './config.pl';

sub workstation_header {
    print <<'EOF';
v2
include services/config.tsl

make_node CommandInterpreter hosts
make_node JobController      jobs
make_node Echo               server_log:link
make_node Ruleset            server_log:ruleset
make_node LogColor           server_log:color
make_node Tee                server_log
make_node Ruleset            local_system_log:ruleset
make_node Ruleset            system_log:ruleset
make_node LogColor           system_log:color
make_node Tee                system_log
make_node Tee                http_log
make_node Null               null
make_node Echo               echo
make_node Scheduler          scheduler

cd server_log:ruleset:config
    add  100 cancel where payload=".* FROM: .* ID: tachikoma@<hostname>(?:[.].*)? COMMAND: .*"
    add 1000 allow
cd ..

cd local_system_log:ruleset:config
    add 1000 allow
cd ..

cd system_log:ruleset:config
    add 1000 allow
cd ..

command jobs start_job TailForks tails localhost:4230
cd tails
    add_tail <home>/.tachikoma/log/tachikoma-server.log server_log:link
    add_tail <home>/.tachikoma/log/http-access.log      http_log
    add_tail <home>/.tachikoma/log/tasks-access.log     http_log
    add_tail <home>/.tachikoma/log/tables-access.log    http_log
    if ( `uname` eq "Darwin\n" ) {
        add_tail /var/log/system.log       local_system_log:ruleset;
        add_tail /var/log/local/system.log local_system_log:ruleset;
    } else {
        add_tail /var/log/syslog       local_system_log:ruleset;
        add_tail /var/log/local/syslog local_system_log:ruleset;
    }
cd ..

connect_node http_log                 _responder
connect_node system_log               _responder
connect_node server_log               _responder
connect_node system_log:color         system_log
connect_node server_log:color         server_log
connect_node system_log:ruleset       system_log:color
connect_node server_log:ruleset       server_log:color
connect_node local_system_log:ruleset system_log:ruleset
connect_node server_log:link          server_log:ruleset

EOF
}

sub workstation_services {
    print <<'EOF';
# services
var services = "<home>/.tachikoma/services";
func start_service {
    local service = <1>;
    command jobs  start_job Shell <service>:job <services>/<service>.tsl;
    command hosts connect_inet localhost:[var "tachikoma.<service>.port"] <service>:service;
    command tails add_tail <home>/.tachikoma/log/<service>.log server_log:link;
}
func stop_service {
    local service = <1>;
    command jobs stop_job <service>:job;
    # rm <service>:service;
}
for service (<tachikoma.services>) {
    start_service <service>;
}

# ingest server logs
connect_hub
make_node ConsumerBroker server_log:consumer --broker=<broker>    \
                                             --topic=server_log   \
                                             --group=console      \
                                             --default_offset=end \
                                             --poll_interval=0.1  \
                                             --max_unanswered=64
connect_node server_log          sounds:service/server_log:sounds
connect_node server_log:consumer server_log:ruleset
connect_node server_log:link     server_logs:service/server_log

EOF
}

sub workstation_footer {
    print <<'EOF';
cd tails
    tail_probe
    start_tail
    insecure
cd ..
EOF
}

1;
