#!/usr/bin/perl
use strict;
use warnings;
our %Keys;
our %Ed25519_Keys;

our %workstation_keys;
our %server_keys;
our %workstation_ed25519_keys;

%server_keys = (
);

%workstation_keys = (
    'tachikoma@nyx' =>
q(-----BEGIN RSA PUBLIC KEY-----
MIGJAoGBAL7HdQeC2zQOm61S7u5toPgDWvhOiBb5YZD/vsLBywBTeU4o6JGDEuVk
CcXCCedlZ4VQSfOMR0VCx0kuY+awzCac9WhQx88CWobAN2aoGuCJoNmD7mlbaG20
Qs+3gn+llLLhyOuCKPAUwMZagjbAa/aocufOeMJ770J+YZPdimrlAgMBAAE=
-----END RSA PUBLIC KEY-----
),
);

%workstation_ed25519_keys = (
    'tachikoma@nyx' => q(e7c36ace638140c88aec4bf295b10a35ece68a2b7f636e26de25f95b344e42d5),
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
print <<EOF;
#!/usr/bin/perl
# ----------------------------------------------------------------------
# \$Id\$
# ----------------------------------------------------------------------

use strict;
use warnings;
use Tachikoma::Config qw( %Keys );

# Set our authorized keys

\%Keys = (
EOF
for my $id (sort keys %authorized) {
    die "no such key: $id" if (not $Keys{$id});
    my $tags = join("\n            ", sort keys %{ $authorized{$id} });
    print <<EOF;
    '$id' => {
        allow => {map {\$_=>1} qw(
            $tags
        )},
EOF
    if ($authorized_commands{$id}) {
        my $commands = join("\n            ",
            sort keys %{ $authorized_commands{$id} }
        );
        print <<EOF;
        allow_commands => {map {\$_=>1} qw(
            $commands
        )},
EOF
    }
    if ($Keys{$id}) {
        print <<EOF;
        public_key =>
q($Keys{$id}),
EOF
    }
    if ($Ed25519_Keys{$id}) {
        print <<EOF;
        ed25519 => pack('H*', q($Ed25519_Keys{$id})),
EOF
    }
    print "    },\n";
}

print <<EOF;
);

1;
EOF
}

1;
