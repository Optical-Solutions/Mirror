
use Getopt::Long;

my $type_string;
my @types;

GetOptions(
    'type=s' => \$type_string
);

@types = split /,/, $type_string if defined $type_string;

foreach my $type (@types) {
    print "$type\n";
}
