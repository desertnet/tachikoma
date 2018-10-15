#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::CGI
# ----------------------------------------------------------------------
#
# $Id: CGI.pm 3301 2009-09-24 03:33:18Z chris $
#

package Tachikoma::Nodes::CGI;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::HTTP_Responder qw( log_entry cached_strftime );
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_EOF TM_INFO TM_KILLME
);
use Digest::MD5 qw( md5_hex );
use parent qw( Tachikoma::Node );

use version; our $VERSION = 'v2.0.367';

my $Max_Requests = 500;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{config_file}     = undef;
    $self->{config}          = undef;
    $self->{tmp_path}        = undef;
    $self->{request}         = undef;
    $self->{sent_header}     = undef;
    $self->{post_data}       = undef;
    $self->{includes}        = {};
    $self->{include_times}   = {};
    $self->{include_counter} = 0;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $config_file, $tmp_path ) = split q{ }, $self->{arguments}, 2;
        my $path = ( $config_file =~ m{^([\w:./-]+)$} )[0];
        my $rv   = do $path;
        die "couldn't parse $path: $@" if ($@);
        die "couldn't do $path: $!"    if ( not defined $rv );
        die "couldn't run $path"       if ( not $rv );
        $self->{config_file} = $path;
        $self->{config}      = $Tachikoma::Nodes::CGI::Config;
        $self->{tmp_path}    = $tmp_path;
    }
    return $self->{arguments};
}

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self    = shift;
    my $message = shift;

    # looks like we're ready to die
    if ( $message->[TYPE] & TM_KILLME and $message->[FROM] ) {
        return Tachikoma::Job->shutdown_all_nodes;
    }

    # otherwise make sure it's a TM_STORABLE
    return if ( not $message->[TYPE] & TM_STORABLE );

    # server configuration
    my $request       = $message->payload;
    my $headers       = $request->{headers};
    my $server_config = $self->{config};
    my $server_name   = $headers->{host} || 'localhost';
    my $script_url    = $request->{script_url};
    $script_url =~ s{/[.][.](?=/)}{}g;
    $script_url =~ s{/+}{/}g;
    my $last_modified   = undef;
    my $found           = undef;
    my $script_name     = undef;
    my $script_path     = undef;
    my $document_root   = $server_config->{document_root};
    my $server_paths    = $server_config->{script_paths};
    my $test_path       = $script_url;
    my @path_components = ();
FIND_SCRIPT: while ($test_path) {
        $script_path = $server_paths->{$test_path} || q{};
        $script_name = $test_path;
        my $is_dir = ( $script_path =~ m{/$} ) ? 'true' : undef;
        if ( $script_path and not $is_dir ) {
            $last_modified = ( stat $script_path )[9];
            $found         = 'true';
            last FIND_SCRIPT;
        }
        elsif ( $script_path and $is_dir ) {
            chop $script_path;
            while ( my $path_component = shift @path_components ) {
                $script_path .= join q{}, q{/}, $path_component;
                $script_name .= join q{}, q{/}, $path_component;
                next if ( -d $script_path );
                if ( -f _ ) {
                    $last_modified = ( stat _ )[9];
                    $found         = 'true';
                    last FIND_SCRIPT;
                }
            }
        }
        if ( $test_path =~ s{/([^/]*)$}{} ) {
            unshift @path_components, $1 if ( length $1 );
        }
    }
    my $path_info = join q{/}, q{}, @path_components;

    $self->{counter}++;

    # couldn't find script, so let's bail
    if ( not $found ) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE]    = TM_BYTESTREAM;
        $response->[TO]      = $message->[FROM];
        $response->[STREAM]  = $message->[STREAM];
        $response->[PAYLOAD] = join q{},
            "HTTP/1.1 404 NOT FOUND\n",
            'Date: ', cached_strftime(), "\n",
            "Server: Tachikoma\n",
            "Connection: close\n",
            "Content-Type: text/plain; charset=utf8\n\n",
            "Requested URL not found.\n";
        $self->{sink}->fill($response);
        log_entry( $self, 404, $message );
        $response->[TYPE] = TM_EOF;
        $response->[TO]   = $message->[FROM];
        return $self->{sink}->fill($response);
    }

    # copy HTTP headers to environment
    ## no critic (RequireLocalizedPunctuationVars)
    local %ENV = ();
    for my $key ( keys %{$headers} ) {
        my $value = $headers->{$key};
        $key =~ s{-}{_}g;
        $key = 'HTTP_' . uc $key;
        $ENV{$key} = $value // q{};
    }
    $ENV{HTTP_AUTHORIZATION} = q{};

    my $request_uri  = $script_url;
    my $query_string = $request->{query_string};
    $request_uri .= join q{}, q{?}, $query_string if ($query_string);
    my $is_post = $request->{method} eq 'POST';
    $ENV{PATH}              = '/bin:/usr/bin';
    $ENV{DOCUMENT_ROOT}     = $document_root;
    $ENV{SERVER_SOFTWARE}   = 'Tachikoma';
    $ENV{SERVER_NAME}       = $server_name;
    $ENV{GATEWAY_INTERFACE} = 'CGI/1.1';
    $ENV{SERVER_PROTOCOL}   = 'HTTP/1.1';
    $ENV{SERVER_PORT}       = $request->{server_port};
    $ENV{REQUEST_URI}       = $request_uri;
    $ENV{REQUEST_METHOD}    = $request->{method};
    $ENV{PATH_INFO}         = $path_info;
    $ENV{PATH_TRANSLATED}   = q{};
    $ENV{SCRIPT_FILENAME}   = $script_path;
    $ENV{SCRIPT_NAME}       = $script_name;
    $ENV{SCRIPT_URI}        = join q{}, 'http://', $server_name, $script_url;
    $ENV{SCRIPT_URL}        = $script_url;
    $ENV{QUERY_STRING}      = $query_string;
    $ENV{REMOTE_ADDR}       = $request->{remote_addr};
    $ENV{REMOTE_PORT}       = $request->{remote_port};
    $ENV{AUTH_TYPE}         = $request->{auth_type} || q{};
    $ENV{REMOTE_USER}       = $request->{remote_user} || q{};
    $ENV{CONTENT_TYPE}      = $headers->{'content-type'} if ($is_post);
    $ENV{CONTENT_LENGTH}    = $headers->{'content-length'} if ($is_post);
    $ENV{UNIQUE_ID}         = md5_hex(rand);

    for my $key ( keys %ENV ) {
        $self->stderr("WARNING: \$ENV{$key} not defined\n")
            if ( not defined $ENV{$key} );
    }
    $self->{request}     = $message;
    $self->{sent_header} = undef;

    # set up STDIN and STDOUT
    ## no critic (RequireInitializationForLocalVars, ProhibitTie)
    local *STDIN;
    local *STDOUT;
    if ($is_post) {
        if ( $request->{tmp} ) {
            my $tmp_path = join q{/}, $self->{tmp_path}, 'post';
            my $tmp = ( $request->{tmp} =~ m{^($tmp_path/\w+$)} )[0];
            open *STDIN, '<', $tmp or die "ERROR: couldn't open $tmp: $!";
        }
        else {
            $self->{post_data} = $request->{body};
            tie *STDIN, 'Tachikoma::Nodes::CGI', $self;
        }
    }
    tie *STDOUT, 'Tachikoma::Nodes::CGI', $self;

    # load up the script we want to run
    my $includes      = $self->{includes};
    my $include_times = $self->{include_times};
    if ( not exists $includes->{$script_path}
        or $last_modified > $include_times->{$script_path} )
    {
        $self->include_path($script_path);
        $include_times->{$script_path} = $Tachikoma::Now;
    }

    # actually run the script
    my $okay = 1;
    $okay = eval {
        &{ $includes->{$script_path} }();
        return 1;
    } if ( $includes->{$script_path} );

    # see how it went
    my $dirty = undef;
    if ( not $okay or not $self->{sent_header} ) {
        delete $includes->{$script_path};
        delete $include_times->{$script_path};
        if ( not $self->{sent_header} ) {
            my $header = Tachikoma::Message->new;
            $header->[TYPE]    = TM_BYTESTREAM;
            $header->[TO]      = $message->[FROM];
            $header->[STREAM]  = $message->[STREAM] . "\n";    # XXX: LB hack
            $header->[PAYLOAD] = join q{},
                "HTTP/1.1 500 NOT OK\n\n",
                "Sorry, an error occurred while processing your request.\n";
            $self->{sink}->fill($header);
            log_entry( $self, 500, $message );
        }
        else {
            # inform HTTP_Cache_Writer that the script terminated
            # prematurely so it can send an unlink instead of a rename
            my $info = Tachikoma::Message->new;
            $info->[TYPE]    = TM_INFO;
            $info->[TO]      = $message->[FROM];
            $info->[STREAM]  = $message->[STREAM] . "\n";    # XXX: LB hack
            $info->[PAYLOAD] = 'renderer_crashed';
            $self->{sink}->fill($info);
        }
        $self->stderr( "ERROR: in script $script_path", $@ ? ": $@" : q{} );
        $dirty = 'true';
    }

    # shut down STDIN and STDOUT
    if ($is_post) {
        if ( $request->{tmp} ) {
            my $tmp_path = join q{/}, $self->{tmp_path}, 'post';
            my $tmp = ( $request->{tmp} =~ m{^($tmp_path/\w+$)} )[0];
            unlink $tmp or die "ERROR: couldn't unlink $tmp: $!";
            close STDIN or die "ERROR: couldn't close $tmp: $!";
        }
        else {
            untie *STDIN;
        }
    }
    untie *STDOUT;

    # send TM_EOF to signal completion of the request
    my $response = Tachikoma::Message->new;
    $response->[TYPE]   = TM_EOF;
    $response->[TO]     = $message->[FROM];
    $response->[STREAM] = $message->[STREAM];
    $self->{sink}->fill($response);

    # see if we've hit $Max_Requests
    if ( $dirty or $Max_Requests and $self->{counter} > $Max_Requests ) {
        my $shutdown = Tachikoma::Message->new;
        $shutdown->[TYPE] = TM_KILLME;
        $shutdown->[TO]   = '_parent';
        $self->{sink}->fill($shutdown);
    }

    return 1;
}

sub include_path {
    my $self        = shift;
    my $script_path = shift;
    my $counter     = $self->{include_counter};
    my $package     = $script_path;
    $package =~ s{[^\w\d]+}{_}g;
    $package =~ s{^(\d)}{_$1};
    $package .= join q{}, q{_}, $counter;
    $counter = ( $counter + 1 ) % $Tachikoma::Max_Int;
    $self->{include_counter} = $counter;
    local $/ = undef;
    open my $fh, '<', $script_path or die "couldn't open $script_path: $!";
    my $script = <$fh>;
    close $fh or die "couldn't close $script_path: $!";
    ## no critic (ProhibitStringyEval)
    $self->{includes}->{$script_path} = eval
        join q{},
        "sub {\n",
        '    CGI::initialize_globals() ',
        "        if (defined &CGI::initialize_globals);\n",
        '    package ',
        $package,
        ";\n",
        ( $script =~ m{^(.*?)(?:__END__.*)?$}s )[0],
        "\n",
        "};\n";
    return;
}

sub TIEHANDLE {
    my $class  = shift;
    my $self   = shift;
    my $scalar = \$self;
    return bless $scalar, $class;
}

sub READ {    ## no critic (RequireArgUnpacking)
    my $self   = shift;
    my $bufref = \$_[0];
    my ( undef, $length, $offset ) = @_;
    ${$bufref} .= substr ${$self}->{post_data}, 0, $length, q{};
    return $length;
}

sub WRITE {
    my ( $self, $payload, $length, $offset ) = @_;
    my $request = ${$self}->{request};
    if ( not ${$self}->{sent_header} ) {
        my $header = Tachikoma::Message->new;
        $header->[TYPE]    = TM_BYTESTREAM;
        $header->[TO]      = $request->[FROM];
        $header->[STREAM]  = $request->[STREAM] . "\n";    # XXX: LB hack;
        $header->[PAYLOAD] = "HTTP/1.1 200 OK\n";
        ${$self}->{sink}->fill($header);
        ${$self}->{sent_header} = 'true';
        log_entry( ${$self}, 200, $request );
    }
    return if ( $payload eq q{} );
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[TO]      = $request->[FROM];
    $message->[STREAM]  = $request->[STREAM] . "\n";       # XXX: LB hack;
    $message->[PAYLOAD] = $payload;
    ${$self}->{sink}->fill($message);
    return $length;
}

sub PRINT {
    my ( $self, @args ) = @_;
    my $payload = join q{}, grep defined, @args;
    my $request = ${$self}->{request};
    if ( not ${$self}->{sent_header} ) {
        my $header = Tachikoma::Message->new;
        $header->[TYPE]    = TM_BYTESTREAM;
        $header->[TO]      = $request->[FROM];
        $header->[STREAM]  = $request->[STREAM] . "\n";    # XXX: LB hack;
        $header->[PAYLOAD] = "HTTP/1.1 200 OK\n";
        ${$self}->{sink}->fill($header);
        ${$self}->{sent_header} = 'true';
        log_entry( ${$self}, 200, $request );
    }
    return if ( $payload eq q{} );
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[TO]      = $request->[FROM];
    $message->[STREAM]  = $request->[STREAM] . "\n";       # XXX: LB hack;
    $message->[PAYLOAD] = $payload;
    return ${$self}->{sink}->fill($message);
}

sub PRINTF {
    my ( $self, $fmt, @payload ) = @_;
    my $request = ${$self}->{request};
    if ( not ${$self}->{sent_header} ) {
        my $header = Tachikoma::Message->new;
        $header->[TYPE]    = TM_BYTESTREAM;
        $header->[TO]      = $request->[FROM];
        $header->[STREAM]  = $request->[STREAM] . "\n";    # XXX: LB hack;
        $header->[PAYLOAD] = "HTTP/1.1 200 OK\n";
        ${$self}->{sink}->fill($header);
        ${$self}->{sent_header} = 'true';
        log_entry( ${$self}, 200, $request );
    }
    my $payload = sprintf $fmt, @payload;
    return if ( $payload eq q{} );
    my $message = Tachikoma::Message->new;
    $message->[TYPE]    = TM_BYTESTREAM;
    $message->[TO]      = $request->[FROM];
    $message->[STREAM]  = $request->[STREAM] . "\n";       # XXX: LB hack;
    $message->[PAYLOAD] = $payload;
    return ${$self}->{sink}->fill($message);
}

sub CLOSE {
    my $self = shift;
    ${$self}->{post_data} = undef;
    return 1;
}

sub config_file {
    my $self = shift;
    if (@_) {
        $self->{config_file} = shift;
    }
    return $self->{config_file};
}

sub config {
    my $self = shift;
    if (@_) {
        $self->{config} = shift;
    }
    return $self->{config};
}

sub tmp_path {
    my $self = shift;
    if (@_) {
        $self->{tmp_path} = shift;
    }
    return $self->{tmp_path};
}

sub request {
    my $self = shift;
    if (@_) {
        $self->{request} = shift;
    }
    return $self->{request};
}

sub sent_header {
    my $self = shift;
    if (@_) {
        $self->{sent_header} = shift;
    }
    return $self->{sent_header};
}

sub post_data {
    my $self = shift;
    if (@_) {
        $self->{post_data} = shift;
    }
    return $self->{post_data};
}

sub includes {
    my $self = shift;
    if (@_) {
        $self->{includes} = shift;
    }
    return $self->{includes};
}

sub include_times {
    my $self = shift;
    if (@_) {
        $self->{include_times} = shift;
    }
    return $self->{include_times};
}

sub include_counter {
    my $self = shift;
    if (@_) {
        $self->{include_counter} = shift;
    }
    return $self->{include_counter};
}

1;
