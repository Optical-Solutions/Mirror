#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Soumya K Bhowmic
##Date      : 12/06/2023
##
##Brief Desc: This program looks at the last 1000 lines log file generated for 
##            Purchase Order file for Landair Inc 
## --------------------------------------------------------------------------  
use strict;
use IBIS::Email;
use Data::Dumper;

#TODO change debug back to 0
my $debug = 1;

my $logdir ='/usr/local/mccs/log/edi/edi_tms';
my $logfile = $logdir.'/poshipment_to_landair_log_file';
my $tmp_file = $logdir.'/'.'tmp_file';


my $tail_buffer  =`tail -n 1000 $logfile`;
print Dumper($tail_buffer) if $debug;


my @blocks = split(/\*\*\*\*\*\*\*\*\*\\*\*\*\*\*/, $tail_buffer);
my $size = @blocks;

if ($debug){
    print "size - 2\n";
    print $blocks[$size - 2];
    print "size - 1:\n";
    print $blocks[$size - 1];
}


my $total_buff = $blocks[$size - 2].'\n'.$blocks[$size - 1];
open(OUT, ">$tmp_file") || die "failed to open file: $tmp_file";
print OUT $total_buff;
close OUT;

my $buff1 = `grep 'NO DATA' $tmp_file`;
my $buff2 = `grep 'FTP sucessful' $tmp_file`;

if ($debug){
    print "Buff1\n";
    print $buff1;   
    print "Buff2\n";
    print $buff2;
}

my @no_data_ary = split(/\n/, $buff1);
my @success_ary = split(/\n/, $buff2);

my $n_size = @no_data_ary;
my $s_size = @success_ary;

## conditions for sending out warning
## no success, no 'No Data' twice, then something must be wrong

if($s_size > 0){
    exit();
}else{
    if($n_size >= 2){
	exit();
    }else{
	&send_warnings();
    }
}



sub send_warnings{
##if ((($n_size < 2)&&($s_size == 0)) || (($s_size <1)&&($n_size ==0)) ){
    my $list_str  ='rdistaff@usmc-mccs.org|';
    my $from_str ='rdistaff@usmc-mccs.org';
    my $subject ="(TEST) Warning: LANDAIR FILE TRANSFER TROUBLES!!!";
    my $body = $total_buff;
    
    &sendlistmail($list_str, $from_str, $subject, $body);
}


=head
my $cmd1 = "grep 'NO DATA'  " .$total_buff;
my $cmd2 = "grep 'FTP sucessful'  ". $total_buff;

print "Buffer1:";
print "$buffer1\n";

print "Buffer2:";
print "$buffer2\n";
=cut


## get the last part of the log
## split it by ****

## ?? confirm get time string

##  grep command 1
##  grep command 2

##  if nothing returned, send email. 
## sendlistmail
