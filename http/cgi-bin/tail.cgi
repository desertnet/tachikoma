#!/usr/bin/perl
# ----------------------------------------------------------------------
# topic.cgi
# ----------------------------------------------------------------------
#

use strict;
use warnings;
use Tachikoma::Nodes::ConsumerBroker;
use Tachikoma::Message qw( ID TIMESTAMP );
use CGI;
use JSON -support_by_pp;

my $home   = ( getpwuid $< )[7];
my $config = Tachikoma->configuration;
$config->load_config_file(
    "$home/.tachikoma/etc/tachikoma.conf",
    '/usr/local/etc/tachikoma.conf',
);

my $broker_ids = undef;
if ($Tachikoma::Nodes::CGI::Config) {
    $broker_ids = $Tachikoma::Nodes::CGI::Config->{broker_ids};
}
$broker_ids ||= ['localhost:5501'];
my $cgi  = CGI->new;
my $path = $cgi->path_info;
$path =~ s(^/)();
my ( $topic, $location, $count, $double_encode ) = split m{/}, $path, 4;
my $offset_string = undef;
if ($location) {
    $location      = 'recent' if ( $location eq 'last' );
    $offset_string = $location;
}
die "no topic\n" if ( not length $topic );
$location      ||= 'start';
$offset_string ||= 'start';
$count         ||= 1;
$double_encode ||= 0;
my $json = JSON->new;

# $json->escape_slash(1);
$json->canonical(1);
$json->pretty(1);
$json->allow_blessed(1);
$json->convert_blessed(0);
CORE::state %groups;
$groups{$topic} //= Tachikoma::Nodes::ConsumerBroker->new($topic);
$groups{$topic}->broker_ids($broker_ids);
my $group      = $groups{$topic};
my $partitions = $group->get_partitions;
my @offsets    = ();
my @messages   = ();
my $results    = {};

if ($partitions) {
    if ( $offset_string =~ m{^\D} ) {
        push @offsets, $offset_string for ( 0 .. keys %{$partitions} );
    }
    else {
        @offsets = split m{,}, $offset_string;
    }
    for my $partition ( keys %{$partitions} ) {
        my $consumer = $group->consumers->{$partition}
            || $group->make_sync_consumer($partition);
        my $offset = $offsets[$partition] // 'end';
        if ( $offset =~ m{^\d+$} ) {
            $consumer->next_offset($offset);
        }
        else {
            $consumer->default_offset($offset);
        }
        if ( $location eq 'recent' ) {
            do { push @messages, @{ $consumer->fetch } }
                while ( not $consumer->eos );
        }
        else {
            do { push @messages, @{ $consumer->fetch } }
                while ( @messages < $count and not $consumer->eos );
        }
    }
}

@messages = sort {
    join( q(:), $a->[TIMESTAMP], $a->[ID] ) cmp
        join( q(:), $b->[TIMESTAMP], $b->[ID] )
} @messages;
if ( $location eq 'recent' ) {
    shift @messages while ( @messages > $count );
}

if ( not $partitions or $group->sync_error ) {
    print STDERR $group->sync_error;
    my $next_url = $cgi->url( -path_info => 1, -query => 1 );
    $next_url =~ s{^http://}{https://};
    $results = {
        next_url => $next_url,
        error    => 'SERVER_ERROR'
    };
}
else {
    my @output       = ();
    my @next_offsets = ();
    for my $message (@messages) {
        if ( $double_encode and ref $message->payload ) {
            push @output, $json->utf8->encode($message->payload);
        }
        else {
            push @output, $message->payload;
        }
    }
    for my $partition ( sort keys %{$partitions} ) {
        my $consumer = $group->consumers->{$partition};
        if ($consumer) {
            push @next_offsets, $consumer->{offset};
        }
        else {
            push @next_offsets, $offsets[$partition];
        }
    }
    my $next_url = join q(/), $cgi->url, $topic, join( q(,), @next_offsets ),
        $count, $double_encode;
    $next_url =~ s{^http://}{https://};
    $results = {
        next_url => $next_url,
        payload  => \@output
    };
}

print( $cgi->header( -type => 'application/json', -charset => 'utf-8' ),
    $json->utf8->encode($results) );
