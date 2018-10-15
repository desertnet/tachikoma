#!/usr/bin/perl
# ----------------------------------------------------------------------
# Tachikoma::Nodes::LWP
# ----------------------------------------------------------------------
#
# $Id: LWP.pm 3301 2009-09-24 03:33:18Z chris $
#

package Tachikoma::Nodes::LWP;
use strict;
use warnings;
use Tachikoma::Node;
use Tachikoma::Nodes::HTTP_Responder qw( log_entry );
use Tachikoma::Message qw(
    TYPE FROM TO STREAM PAYLOAD
    TM_BYTESTREAM TM_STORABLE TM_PERSIST TM_EOF
);
use HTTP::Request::Common qw( GET POST );
use LWP::UserAgent;
use parent qw( Tachikoma::Node );

use version; our $VERSION = 'v2.0.368';

my $Default_Timeout = 900;
my %Exclude_Headers = map { $_ => 1 } qw(
    accept-encoding
    connection
    if-modified-since
    proxy-connection
);

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new;
    $self->{user_agent} = undef;
    $self->{tmp_path}   = undef;
    bless $self, $class;
    return $self;
}

sub arguments {
    my $self = shift;
    if (@_) {
        $self->{arguments} = shift;
        my ( $timeout, $tmp_path ) = split q{ }, $self->{arguments}, 2;
        my $ua = LWP::UserAgent->new;
        $ua->agent('Tachikoma (DesertNet LWP::UserAgent/2.0)');
        $ua->timeout( $timeout || $Default_Timeout );
        $self->{user_agent} = $ua;
        $self->{tmp_path}   = $tmp_path;
    }
    return $self->{arguments};
}

sub fill {    ## no critic (ProhibitExcessComplexity)
    my $self        = shift;
    my $message     = shift;
    my $request     = undef;
    my $sent_header = undef;
    my $request_uri = undef;
    my $to          = undef;
    if ( $message->[TYPE] & TM_STORABLE ) {
        $request     = $message->payload;
        $request_uri = join q{},
            'http://', $request->{headers}->{host},
            $request->{uri};
        $to = $message->[FROM];
    }
    elsif ( $message->[TYPE] & TM_BYTESTREAM ) {
        $request_uri = $message->[PAYLOAD];
        $request     = { method => 'GET' };
        $sent_header = 'true';
        $to = $message->[FROM] if ( not $message->[TYPE] & TM_PERSIST );
    }
    else {
        return;
    }

    my $ua     = $self->{user_agent};
    my $method = $request->{method};
    my $req;
    ## no critic (RequireInitializationForLocalVars)
    local *STDIN;
    if ( $method eq 'POST' ) {

        # XXX: We would much prefer to actually use the temporary files.
        # We need to set $HTTP::Request::Common::DYNAMIC_FILE_UPLOAD
        # to a true value and then do something with a callback.
        my $content = undef;
        if ( $request->{tmp} ) {
            my $tmp_path = join q{/}, $self->{tmp_path}, 'post';
            my $tmp = ( $request->{tmp} =~ m{^($tmp_path/\w+$)} )[0];
            local $/ = undef;
            open my $fh, '<', $tmp or die "ERROR: couldn't open $tmp: $!";
            $content = <$fh>;
            close $fh   or die "ERROR: couldn't close $tmp: $!";
            unlink $tmp or die "ERROR: couldn't unlink $tmp: $!";
        }
        else {
            $content = $request->{body};
        }
        $req = POST(
            $request_uri,
            map { $_ => $request->{headers}->{$_} }
                grep { not $Exclude_Headers{$_} }
                keys %{ $request->{headers} },
            Content => $content
        );
    }
    elsif ( $method eq 'GET' ) {

        # XXX: What do we need to do with the request headers?
        $req = GET( $request_uri,
            map { $_ => $request->{headers}->{$_} }
                grep { not $Exclude_Headers{$_} }
                keys %{ $request->{headers} } );
    }
    elsif ( $method eq 'HEAD' ) {

        # XXX: What do we need to do with the request headers?
        $req = HEAD( $request_uri,
            map { $_ => $request->{headers}->{$_} }
                grep { not $Exclude_Headers{$_} }
                keys %{ $request->{headers} } );
    }
    else {
        if ( not $sent_header and $to ) {
            my $header = Tachikoma::Message->new;
            $header->[TYPE]    = TM_BYTESTREAM;
            $header->[TO]      = $to;
            $header->[STREAM]  = $message->[STREAM] . "\n";    # XXX: LB hack
            $header->[PAYLOAD] = join q{},
                "HTTP/1.1 501 NOT IMPLEMENTED\n\n",
                "Sorry, this method is not yet implemented.\n";
            $self->{sink}->fill($header);
            log_entry( $self, 501, $message );
        }
        $self->stderr( 'WARNING: method not implemented: ', $method );
    }

    # $req->authorization_basic($user, $pasword);
    # $self->stderr('fetching: ', $request_uri);

    my $sent_content = undef;
    my $res          = $ua->simple_request(
        $req,
        sub {
            my ( $payload, $r ) = @_;
            if ( not $sent_header and $to ) {

                # XXX: What do we need to do with the response headers?
                my $header = Tachikoma::Message->new;
                $header->[TYPE]    = TM_BYTESTREAM;
                $header->[TO]      = $to;
                $header->[STREAM]  = $message->[STREAM] . "\n"; # XXX: LB hack
                $header->[PAYLOAD] = join q{},
                    'HTTP/1.1 ', $r->{_rc}, q{ }, $r->{_msg}, "\n",
                    $r->{_headers}->as_string, "\n";
                $self->{sink}->fill($header);
                $sent_header = 'true';
            }
            return $self->stderr('no content') if ( $payload eq q{} );
            if ($to) {
                my $response = Tachikoma::Message->new;
                $response->[TYPE] = TM_BYTESTREAM;
                $response->[FROM] = $message->[FROM] if ( not $to );
                $response->[TO]   = $to;
                $response->[STREAM] =
                    $message->[STREAM] . "\n";    # XXX: LB hack
                $response->[PAYLOAD] = $payload;
                $self->{sink}->fill($response);
            }
            $sent_content = 'true';
            return 1;
        }
    );

    if ( not $sent_content ) {
        if ( not $sent_header and $to ) {

            # XXX: What do we need to do with the response headers?
            my $header = Tachikoma::Message->new;
            $header->[TYPE]    = TM_BYTESTREAM;
            $header->[TO]      = $to;
            $header->[STREAM]  = $message->[STREAM] . "\n";    # XXX: LB hack
            $header->[PAYLOAD] = join q{},
                join( q{ },
                $res->protocol || 'HTTP/1.1',
                $res->code, $res->message ),
                "\n",
                $res->headers->as_string,
                "\n";
            $self->{sink}->fill($header);
            $sent_header = 'true';
        }
        if ( $res->content and $to ) {
            my $response = Tachikoma::Message->new;
            $response->[TYPE] = TM_BYTESTREAM;
            $response->[FROM] = $message->[FROM] if ( not $to );
            $response->[TO]   = $to;
            $response->[STREAM]  = $message->[STREAM] . "\n";   # XXX: LB hack
            $response->[PAYLOAD] = $res->content;
            $self->{sink}->fill($response);
        }
        log_entry( $self, $res->code, $message ) if ($to);

        # if ($res->code >= 400) {
        #     use Data::Dumper;
        #     $self->stderr(Dumper({
        #         client_request => $request,
        #         proxy_request  => $req,
        #         response       => $res
        #     }));
        # }
        if ( $res->code >= 400 and $res->content ) {
            $self->stderr( join q{ }, $res->protocol || 'HTTP/1.1',
                $res->code, $res->message );
        }
    }
    elsif ($to) {
        log_entry( $self, $res->code, $message );
    }

    if ($to) {
        my $response = Tachikoma::Message->new;
        $response->[TYPE]   = TM_EOF;
        $response->[TO]     = $to;
        $response->[STREAM] = $message->[STREAM];
        $self->{sink}->fill($response);
    }
    else {
        $self->{sink}->fill($message);
    }
    return;
}

sub user_agent {
    my $self = shift;
    if (@_) {
        $self->{user_agent} = shift;
    }
    return $self->{user_agent};
}

sub tmp_path {
    my $self = shift;
    if (@_) {
        $self->{tmp_path} = shift;
    }
    return $self->{tmp_path};
}

1;
