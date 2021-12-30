#!/usr/bin/perl
# ----------------------------------------------------------------------
# tachikoma server authorized keys generation
# ----------------------------------------------------------------------
#

use strict;
use warnings;
our %Keys;
our %Ed25519_Keys;

our %workstation_keys;
our %server_keys;

%server_keys = (
    'tachikoma@server1' =>
q(-----BEGIN RSA PUBLIC KEY-----
...
-----END RSA PUBLIC KEY-----
),
);

%workstation_keys = (
    'tachikoma@workstation1' =>
q(-----BEGIN RSA PUBLIC KEY-----
...
-----END RSA PUBLIC KEY-----
),
);

my %authorized = ();
my %authorized_commands = ();

sub authorize {
    my $id = shift;
    $authorized{$id} = { map { $_ => 1 } @_ };
    return;
}

sub authorize_commands {
    my $id = shift;
    $authorized_commands{$id} = { map { $_ => 1 } @_ };
    return;
}

sub add_keys {
    my $keyring = shift;
    $Keys{$_} = $keyring->{$_} for (keys %$keyring);
    return;
}

sub add_ed25519_keys {
    my $keyring = shift;
    $Ed25519_Keys{$_} = $keyring->{$_} for (keys %$keyring);
    return;
}

sub generate_authorized_keys {
    my @ids = @_;
print <<"EOF";
#!/usr/bin/perl
# ----------------------------------------------------------------------
# tachikoma server authorized keys
# ----------------------------------------------------------------------
#

use strict;
use warnings;
use Tachikoma;

# Set our authorized keys

Tachikoma->configuration->public_keys( {
EOF
for my $id (sort keys %authorized) {
    die "no such key: $id" if (not $Keys{$id});
    my $tags = join("\n            ", sort keys %{ $authorized{$id} });
    print <<"EOF";
    '$id' => {
        allow => {map {\$_=>1} qw(
            $tags
        )},
EOF
    if ($authorized_commands{$id}) {
        my $commands = join("\n            ",
            sort keys %{ $authorized_commands{$id} }
        );
        print <<"EOF";
        allow_commands => {map {\$_=>1} qw(
            $commands
        )},
EOF
    }
    if ($Keys{$id}) {
        print <<"EOF";
        public_key =>
q($Keys{$id}),
EOF
    }
    if ($Ed25519_Keys{$id}) {
        print <<"EOF";
        ed25519 => pack('H*', q($Ed25519_Keys{$id})),
EOF
    }
    print "    },\n";
}

print <<'EOF';
} );

1;
EOF
}

1;
