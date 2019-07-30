#!/usr/bin/perl
use strict;
use warnings;


our %fsync_ports = (
    'fsync'  => 5600,
    'fsync2' => 5700,
);

sub header {
    my %options = @_;
    my $version = $options{v} || 1;
    print <<"EOF";
v$version
#######################################################################
# Make nodes and jobs
#######################################################################

make_node Tail server_log /var/log/.tachikoma/log/tachikoma-server.log
make_node Tail system_log /var/log/messages
make_node CommandInterpreter hosts
make_node JobController      jobs
connect server_log tachikoma/server_logs:tee
connect system_log tachikoma/system_logs:tee

EOF
}

sub buffer_probe {
    my $prefix = shift || '';
    my $host   = shift || 'localhost';
    $prefix   .= '_' if ($prefix);
    print <<"EOF";

  # buffer probe
  command hosts connect_inet --scheme=rsa --host=$host --port=4395 --name=${prefix}buffer_top
  command hosts make_node MemorySieve ${prefix}buffer_top:sieve 4 should_warn
  make_node BufferProbe ${prefix}buffer_probe 4
  connect_node ${prefix}buffer_top:sieve ${prefix}buffer_top
  connect_node ${prefix}buffer_probe     ${prefix}buffer_top:sieve
EOF
}

sub fsync_source {
    my %options    = @_;
    my $name       = $options{name}       || 'fsync';
    my $path       = $options{path}       || '/usr/local/sync';
    my $user       = $options{user};
    my $count      = $options{count}      // ($user ? 1 : 4);
    my $probe      = $options{probe}      // 1;
    my $broadcasts = $options{broadcasts} || [ $path ];
    my $interval   = $options{interval}   || 120;
    my $max_files  = $options{max_files}  || 256;
    my $secure     = $options{secure};
    my $pedantic   = $options{pedantic}   // '';
    my $port = $fsync_ports{$name} or die "invalid fsync channel: $name\n";
    $port    =~ s(00$)() or die "invalid fsync port: $port\n";
    die "$name: port is deprecated!\n" if ($options{port});
    my $farmer = undef;
    if ($user) {
        $farmer = sub { "make_node SudoFarmer $_[0] $user $_[1]\n" };
    }
    else {
        $farmer = sub { "make_node JobFarmer  $_[0] $_[1]\n" };
    }
    print <<"EOF";


#######################################################################
# $name source
#######################################################################

command jobs start_job CommandInterpreter $name:source
cd $name:source
  make_node JobController      jobs
  make_node CommandInterpreter hosts
EOF
    if ($count) {
        print <<"EOF";
  make_node Buffer             file:buffer         $name.db $max_files
  make_node Watchdog           file:watchdog       file:gate
  make_node Gate               file:gate
  make_node AgeSieve           file:sieve          120
  make_node FileController     FileController
  make_node LoadBalancer       FileSender:load_balancer
  make_node Tee                output:tee
  make_node Scheduler          scheduler
  make_node Watchdog           DirStats:watchdog   DirStats:gate
  make_node Gate               DirStats:gate
  make_node SetStream          DirStats:set_stream
  make_node Buffer             DirStats:buffer
  make_node AgeSieve           DirStats:sieve      30
EOF
        my $args = "$path localhost:${port}01 $max_files";
        $args   .= " $pedantic" if ($pedantic);
        print '  ', &$farmer('        DirStats', "           $count DirStats  $args");
    }
    print <<"EOF";
  make_node Tee                DirStats:tee
  make_node ClientConnector    DirStats:client_connector DirStats:tee
  make_node Responder          FileSender:cap
EOF
    if ($count) {
        print "  command DirStats:buffer set_count 4\n";
        for my $i (1 .. $count) {
            print "  command jobs start_job CommandInterpreter $name:bridge$i\n"
                . "  cd $name:bridge$i\n";
            if ($user) {
                print "    make_node SudoFarmer      FileSender $user 1 FileSender $path _parent/FileSender:tee\n";
            }
            else {
                print "    make_node FileSender      FileSender            $path FileSender:tee\n";
            }
            print <<"EOF";
    make_node Tee             FileSender:tee
    make_node ClientConnector FileSender:client_connector FileSender:tee
    connect_sink FileSender:tee FileSender # force responses through
    connect_node FileSender     _parent/FileSender:cap
    listen_inet --scheme=rsa --use-ssl 0.0.0.0:${port}${i}1
    register 0.0.0.0:${port}${i}1 FileSender:client_connector AUTHENTICATED
    secure 3
  cd ..
EOF
        }
        print "\n\n";
        for my $i (1 .. $count) {
            print "  connect_node FileSender:load_balancer      $name:bridge$i/FileSender\n";
        }
        print <<"EOF";
  connect_node FileSender:cap                output:tee
  connect_node DirStats:sieve                DirStats
  connect_node DirStats:buffer               DirStats:sieve
  connect_node DirStats:set_stream           DirStats:buffer
  connect_node DirStats:gate                 DirStats:set_stream
  connect_node FileController                FileSender:load_balancer
  connect_node file:sieve                    FileController
  connect_node file:gate                     file:sieve
  connect_node file:buffer                   file:gate

EOF
        # sync targets
        my $i = 1;
        for my $target (@{ $options{targets} }) {
            my $j = $i + 1;
            print <<"EOF";

  # sync $target
  connect_inet --scheme=rsa --use-ssl --host=$target --port=${port}00 --name=target$i
  make_node MemorySieve target$i:sieve 1024 should_warn
  connect_node target$i:sieve target$i/file:buffer
EOF
            if ($i == 1) {
                print <<"EOF";
  connect_node output:tee    target$i:sieve

EOF
            }
            else {
                print <<"EOF";
  make_node Gate target$i:gate
  connect_node target$i:gate target$i:sieve
  connect_node output:tee   target$i:gate

EOF
            }
            if ($j <= @{ $options{targets} }) {
                print <<"EOF";

  # fall back on target$j
  connect_inet --scheme=rsa --use-ssl --host=$target --port ${port}99 --name=target$i:heartbeat
  make_node Watchdog target$i:watchdog target$j:gate
  connect_node target$i:heartbeat target$i:watchdog
EOF
                print "  connect_node target$i:watchdog  target$j:watchdog\n"
                    if ($j < @{ $options{targets} });
                print "\n";
            }
            print "\n";
            $i++;
        }

        # peer suppression
        $i = 1;
        for my $peer (@{ $options{peers} }) {
            my $j = $i + 1;
            print <<"EOF";

  # suppress dirstats if target$i is up
  connect_inet --scheme=rsa --use-ssl --host=$peer --port=${port}99 --name=peer$i:heartbeat
  connect_node DirStats:watchdog file:watchdog
  connect_node peer$i:heartbeat  DirStats:watchdog

EOF
            $i++;
        }

        print "\n  # send dirstats\n";
        for my $update_path (@$broadcasts) {
            print "  command scheduler every ${interval}s send DirStats:gate '$update_path'\n";
        }
        print <<"EOF";


  # heartbeat
  make_node Timer           heartbeat 5000
  make_node Tee             heartbeat:tee
  make_node ClientConnector heartbeat:client_connector heartbeat:tee
  connect_node heartbeat heartbeat:tee
  listen_inet --scheme=rsa --use-ssl 0.0.0.0:${port}99
  register 0.0.0.0:${port}99 heartbeat:client_connector AUTHENTICATED
EOF
    }
    print <<"EOF";


  # listen ports for incoming connections
  listen_inet --scheme=rsa --use-ssl 0.0.0.0:${port}00
  listen_inet --scheme=rsa         127.0.0.1:${port}01
  listen_inet --scheme=rsa --use-ssl 0.0.0.0:${port}02
  register 0.0.0.0:${port}02 DirStats:client_connector AUTHENTICATED

EOF
    buffer_probe() if ($probe and $count);
    if (defined $secure) {
        print "  secure $secure\n" if ($secure > 0);
    }
    else {
        print "  secure 2\n";
    }
    print "cd ..\n\n";
}

sub fsync_destination {
    my %options          = @_;
    my $name             = $options{name}             || 'fsync';
    my $path             = $options{path}             || '/usr/local/sync';
    my $user             = $options{user};
    my $count            = $options{count}            || ($user ? 1 : 4);
    my $delete_threshold = $options{delete_threshold} // 60;
    my $mode             = $options{mode};
    my $port = $fsync_ports{$name} or die "invalid fsync channel: $name\n";
    $port    =~ s(00$)() or die "invalid fsync port: $port\n";
    die "$name: port is deprecated!\n" if ($options{port});
    return if (not $options{sources} or not @{ $options{sources} });
    my $farmer = undef;
    if ($user) {
        $farmer = sub { "make_node SudoFarmer $_[0] $user $_[1]" };
    }
    else {
        $farmer = sub { "make_node JobFarmer  $_[0] $_[1]" };
    }
    print <<"EOF";


#######################################################################
# $name destination
#######################################################################

command jobs start_job CommandInterpreter $name:destination
cd $name:destination
  make_node JobController      jobs
  make_node CommandInterpreter hosts
  make_node Null               null
EOF
    my $arguments = "$path $delete_threshold";
    $arguments   .= " $mode" if ($mode);
    print "  ", &$farmer('        DirCheck', "$count DirCheck $arguments") . "\n";
    if (not $mode or $mode eq 'update') {
        if ($user) {
            for my $i (1 .. $count) {
                print "  command jobs start_job CommandInterpreter $name:destination$i \\\n"
                    . "    make_node SudoFarmer FileReceiver $user 1 FileReceiver $path\n";
            }
        }
        else {
            for my $i (1 .. $count) {
                print "  command jobs start_job CommandInterpreter $name:destination$i \\\n"
                    . "    make_node FileReceiver FileReceiver $path\n";
            }
        }
    }
    my $j = 1;
    for my $source (@{ $options{sources} }) {
        print "\n  # $source\n";
        if (not $mode or $mode eq 'update') {
            for my $i (1 .. $count) {
                print "  command $name:destination$i connect_inet --scheme=rsa --use-ssl --owner=FileReceiver --host=$source --port=${port}${i}1\n"
            }
        }
        print "  command hosts connect_inet --scheme=rsa --use-ssl --owner=DirCheck --host=$source --port=${port}02 --name=dirstats$j\n";
        $j++;
    }
    for my $listen (@{ $options{listen} }) {
        print "  listen_inet --scheme=rsa $listen\n";
    }
    if (not $mode or $mode eq 'update') {
        for my $i (1 .. $count) {
            print "  command $name:destination$i secure 3\n";
        }
    }
    print "  secure 3\n"
        . "cd ..\n\n";
}

sub secure {
    my $level = shift;
    print "secure";
    print ' ' . $level if ($level);
    print "\n";
}

sub insecure {
    print "insecure\n";
}

1;
