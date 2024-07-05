#!/usr/local/mccs/perl/bin/perl
#--------------------------------------------------------------------------
#Ported by : Hung Nguyen
#Date      : 12/05/2023
#
#Brief Desc: This program is to check the rpos loading process.  It will
#            notify users if there are issues such as overwhelmed RMD BD   
#            job scheduler or TAS/upstream processes. 
# 
# Ported by : Kaveh Sari
# Date      : Friday, July 5, 2024 2:12:21 PM           
# Notes     : Verified Port...All appears to be Ok!
#           : No Data was found at the time of port.           
# --------------------------------------------------------------------------   

use strict;
use IBIS::Email;

my $debug = 0;
##sendmail(to, from, subject, contents);
my $dir1 ='/usr/local/mccs/data/rpos/archive/fms_load_daily/';
my $dir2 ='/usr/local/mccs/data/rpos/archive/iri_pos_daily/';
my $dir3 ='/usr/local/mccs/log/rpos/';
##my $dir4 ='/usr/local/mccs/log/rpos/';


my $string1 = `find $dir1 -mmin -180 | grep '.FMS_LOAD.EOD.' `;
my $string2 = `find $dir2 -mmin -180 | grep 'IRI_POS_' `;
my $string3 = `find $dir3 -mmin -180 | grep 'rpos_rms_log' `;
## my $string4 = `find $dir4 -mmin -360| grep 'rpos_fms_log' `;


if($debug){
    print "$string1\n";
    print "$string2\n";
    print "$string3\n";
}

my $red_flag1 = 0;
my $red_flag2 = 0;
my $red_flag3 = 0;
##my $red_flag4 = 0;

if(split(/\n/, $string1) == 0){
    ##print "No new fms files in 24 hrs";
    $red_flag1 = 1;
}

if(split(/\n/, $string2) == 0){
    ##print "No new sales files in last 24 hrs\n";
    $red_flag2 = 1;
}

### add another two checks on the log to see if there are log update in the past 6 hours, 
## if there is update, but no new files. something is wrong
## if there is no update, there may be database busy time.., need to wait..., and shut up


if(split(/\n/, $string3) > 0){
    ## there are rms rpos loading running in the last 6 hours
    $red_flag3 = 1;
}

##if(split(/\n/, $string4) > 0){
    ##print "No new sales files in last 24 hrs\n";
##    $red_flag4 = 1;
##}


if($red_flag1 && $red_flag2 && $red_flag3){
    &email_notification();
   ## This is to inform you that we have not received RPOS TAS files for 24 hours. 
}

###### subroutine ###################
sub email_notification{
    my $cc_str = 'yuc@usmc-mccs.org|Ashley.Robey@usmc-mccs.org|gearhartj@usmc-mccs.org|hq.help@usmc-mccs.org|Daniel.Duenas@usmc-mccs.org|billy.vanover@usmc-mccs.org|larry.d.lewis@usmc-mccs.org|Nora.Jansen@usmc-mccs.org|';
    ##my $cc_str_new  = 'yuc@usmc-mccs.org|MRI-RDIStaff@usmc-mccs.org|RPOSCoreTeam@usmc-mccs.org|esg@usmc-mccs.org|hq.help@usmc-mccs.org|';
    ## my $cc_str  = 'yuc@usmc-mccs.org|';
    my $content = 'If you are receiving this email, please contact the On-Call RPOS/MSST persons ASAP. Either no RPOS loading process was run for over 3 hours from an overwhelmed RMS DB job scheduler, Or TAS/upstream processes may have issues leading to no new sale files created in the past 3 hours. ';
    my $subject = 'NO RPOS SALES FILES HAVE BEEN RECEIVED IN THE LAST 3 HOURS';
    my $from    = 'rdistaff@usmc-mccs.org';

    my @list = split(/\|/, $cc_str);
    foreach my $e_addr (@list){
	if($e_addr =~ /\@/g){
	  ## &sendmail(to, from, subject, contents);  
	   &sendmail($e_addr, $from, $subject, $content);
	}
    }
}


