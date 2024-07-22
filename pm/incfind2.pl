#!/usr/local/mccs/perl/bin/perl
use strict;
use warnings;
foreach my $target(@ARGV){
	$target = $target . ".pm";
foreach my $dir (@INC) {
	system("find $dir -name $target 2> /dev/null"); 
	}
print "\n";
}
