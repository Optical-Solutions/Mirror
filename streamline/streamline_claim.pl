#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use IBIS::Log::File;
use IBIS::Email;
use Sys::Hostname;
use Getopt::Long;
use IBIS::AP_Genex::Wrapper4claim;
use POSIX qw(strftime);
use IBIS::Log::File;
use Getopt::Long;

## preparations:
##sendmail(to, from, subject, contents);

## options
my ($config, $debug, $help, $hostname);

my $opt_result = GetOptions ( 
    "debug|d"    => \$debug,      # flag
    "help|h"     => \$help,
    );  


my $wrapper_obj = new IBIS::AP_Genex::Wrapper4claim();
#print Dumper($wrapper_obj);

## log file
my $now      = strftime( '%Y_%m_%d_%H_%M', localtime );
my $fileprefix = strftime( '%Y-%m%d%H%M', localtime ).'00';
my $log_filename   = $wrapper_obj->{LOG_DIR} . 
    "/" . "streamline_claim_" . $now;
$wrapper_obj->{'log_obj'} = IBIS::Log::File->new(
    {
        file   => $log_filename,
        append => 1
    }
    );

$wrapper_obj->{'log_obj'}->log_info( 
    "\n\n*************\n\tStarted at:$now\n" );
if($debug){
    print "Object Info:\n";
    print Dumper($wrapper_obj);
    $wrapper_obj->{'log_obj'}->info(
	"Running in debug mode for more infor in log.");
}


## 1, connect RMS db
$wrapper_obj->sget_dbh();
#print Dumper($wrapper_obj->{'dbh_obj'});

##exec yuc_create_cl_header;
##exec yuc_create_cl_detail2;

## 2, call two procedures to populate temp tables in db
eval{

# my $sth = $dbh->prepare('BEGIN my_proc(args); END;) || die $dbh->errstr;
# $sth->execute || die $dbh->errstr;

    my $sth1 = $wrapper_obj->{'dbh_obj'}->prepare('BEGIN rdiusr.create_cl_group_header(); END;');
    $sth1->execute || die $wrapper_obj->{'log_obj'}->info($wrapper_obj->{'dbh_obj'}->errstr);
    
    my $sth2 = $wrapper_obj->{'dbh_obj'}->prepare('BEGIN rdiusr.create_cl_group_detail();END;');
    $sth2->execute || die $wrapper_obj->{'log_obj'}->info($wrapper_obj->{'dbh_obj'}->errstr);
};

if($@){
    my $msg = "Database errors when exec claim db procedures:  $@";
    $wrapper_obj->{'log_obj'}->info($msg);
    ## sendmail(to, from, subject, contents);
    die;
}

## 3, call functions to generate the header and detail files
## clean up dir
my $clean_up ="rm $wrapper_obj->{'DATA_DIR'}"."* 2>/dev/null ";
my $sysout = system($clean_up ); 

my $hdr_file = $wrapper_obj->{'DATA_DIR'}.$fileprefix."CL_HEAD.DAT";
my $dtl_file = $wrapper_obj->{'DATA_DIR'}.$fileprefix."CL_DETAIL.DAT";

eval{
    my $h_sth = $wrapper_obj->get_claim_header_file();
    $wrapper_obj->print_header_file($h_sth,$hdr_file);
    
    my $d_sth = $wrapper_obj->get_claim_detail_file();
    $wrapper_obj->print_detail_file($d_sth, $dtl_file);
};

if($@){
    my $msg = "Database errors when running selection from claim views: $@";
    $wrapper_obj->{'log_obj'}->info($msg);
    ##sendmail(to, from, subject, contents);
    die;
}

## 4, scp header, details to FMS..
my $scp_success = 0;
if (( -s $hdr_file)&&(-s $dtl_file)){
    $scp_success =  $wrapper_obj->scp_head_detail_to_fmsserver();
}else{
    my $msg ="NO new claim header and/or detail files generated. 
             So, this could be from no new data. program will exit.";
    $wrapper_obj->{'log_obj'}->info($msg);
    ## sendmail(to, from, subject, contents);
    exit();
}

## 5, clean up current claim files out of the staging directory.
if($scp_success){    
    my $dest_file1 = $wrapper_obj->{'ARCHIVE_DIR'}.$fileprefix."CL_HEAD.DAT";
    my $dest_file2 = $wrapper_obj->{'ARCHIVE_DIR'}.$fileprefix."CL_DETAIL.DAT";
    my $cmd1 = "mv $hdr_file $dest_file1";
    my $cmd2 = "mv $dtl_file $dest_file2";
    my $ret1 = system($cmd1);
    unless($ret1){
	my $msg = "failed to move $hdr_file to archive dir.";
	$wrapper_obj->{'log_obj'}->info($msg);
    }
    my $ret2 = system($cmd2);
    unless($ret2){
	my $msg = "failed to move $dtl_file to archive dir.";
	$wrapper_obj->{'log_obj'}->info($msg);
    }
}else{
     my $msg ="SCP claim header/detail files failed. Please check scp permission etc...";
    $wrapper_obj->{'log_obj'}->info($msg);
    ## sendmail(to, from, subject, contents);  
}

## done on RMS

## Continue on FMS:

## 1, call process_claim_xml on FMS

## 2, call genex ksh for claim xml files

## 3, log result of genex ...

## emails:
#MAIL_TO=rdistaff@usmc-mccs.org
#MAIL_CC=rdistaff@usmc-mccs.org
#MAIL_FROM=yuc@usmc-mccs.org
