#!/usr/local/mccs/perl/bin/perl
my $target = shift @ARGV;
print $target . "\n";
foreach my $inc(@INC){
	#print $inc . "\n";
	system "find", $inc, "-name", $target);
	}
