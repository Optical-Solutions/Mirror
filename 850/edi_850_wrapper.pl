#!/usr/local/mccs/perl/bin/perl

## Author: Chuck(Chunhui) Yu(yuc@usmc-mccs.org)

use strict;
use warnings;
use IBIS::EDI;
use Data::Dumper;
use Getopt::Std;
use POSIX qw(strftime WNOHANG);
use IBIS::Log::File;
use IBIS::DBI;
use Sys::Hostname;
use IBIS::Email;
use Fcntl qw(:flock);
getopts('dh');

our ( $opt_d, $opt_h,);

## ---Preparations----
## Global variables
my (
    $edi,$debug, $help, $config);         
my $hostname = hostname;

if ($opt_d) {
    $debug = 1;
}
else {
    $debug = 0;
}

if ($opt_h) {
    my $use_msg = qq(
"\n General Usage:
\n\t  perldoc $0 for more information.\n\n");
    print $use_msg;
    exit();
}
$config =  '/usr/local/mccs/etc/edi_850/edi_850_to_spscommerce.config';

## Object
$edi = IBIS::EDI->new( conf_file => $config );

if ($debug) {
    $edi->{'debug'} = 1;
    print Dumper($edi);
}

##  Log start time
my $now      = strftime( '%Y-%m-%d %H:%M:%S', localtime );
my $t_suffix = strftime( '%Y_%m_%d_%H_%M',    localtime );
my $log_filename   = $edi->{WRAPPER_LOG_DIR} . "/" . "edi_850_wrapper_log_" . $t_suffix;

$edi->{'log_obj'} = IBIS::Log::File->new(
    {
        file   => $log_filename,
        append => 1
    }
);

$edi->{'log_obj'}->log_info( 
    "\n\n*************\n\tStarted at:$now\n" );


## Set autocommit as 0 for transaction control
$edi->{'dbh'} = IBIS::DBI->connect(
    dbname  => $edi->{CONNECT_INSTANCE},
    attribs => { AutoCommit => 0 }
  )
  or $edi->{'log_obj'}->log_die(
    "Cannot connect database using: 
     $edi->{CONNECT_INSTANCE}\n"
  );


### 
## STEPS:
## 1, check last run status

my $last_run_status = $edi->check_last_run_status();

## 2 process status:
## If status = success, log & exit wrapper

my $re_run_flag = 0;

if($last_run_status){
    if($last_run_status eq 'S'){
	my $msg = "Last same day run succeeded: $last_run_status. No need to run again";
	$edi->{'log_obj'}->info($msg);
	$re_run_flag = 0;
    }elsif($last_run_status eq 'P'){
	my $msg = "Last same day run succeeded: $last_run_status. But more data in edi_850_po_reserved table.. need to run them..";
	$edi->{'log_obj'}->info($msg);
	$re_run_flag = 1;
	
        ## preparation for re-run in this case...:
	my $refresh_success = 0;
	$refresh_success =  $edi->refresh_and_delete();
	unless($refresh_success){
	    my $msg ="Failed in sub refres_and_delete";
	    $edi->{'log_obj'}->info($msg);
	    die(); ## if failed to refresh, exit the process. i.e. do not resend POs...
	}
    }else{
	my $msg = "last run status unknown: $last_run_status. program will exit";## only S or null 
	$edi->{'log_obj'}->info($msg);
	die;
    }
}else{ ## last run status is undef, i.e. last run failed, need to re-run...
    $re_run_flag = 1;
}


if($re_run_flag){
## If status not equal success:
## call re-run subrountine to re-run the cron with re-run option for sending out email
## the null status, may mean failure of last run, in that case, send out email at failure points. (NOTES: to identify all the failure points is not an easy task) 
## if the null status was resulted from the first process has not finished yet
## The flock machinism will lock out the second process from running.
    my $current_run_error = 0;
    eval{
	$edi->launch_edi_850(' -r ');
    };
    
    if($@){
	$edi->{'log_obj'}->info($@);
    }

## The note bellow is about re-run in the condition that last run status is P (some PO still pending in reserved table):

## if last run status is P, this run failed, send out message
## if last run status is P, this run success, do nothing. 

## if last run status is Null, and data in PO resrved, it means last run failed (EPT table, and PO reserved table both have data. 
## if this run status is S, then, sending out email about POs in reserved.
## if this run status is Null, then, seding out email about POs both in EPT, and PO reserved..


    my $this_run_status = $edi->check_last_run_status();
    unless($this_run_status){
	$current_run_error = 1;
	my $msg ="Re-launch of 850 is failed. run status is undef.";
	$edi->{'log_obj'}->info($msg);
    }else{
	$current_run_error = 0;
    }

    my $msg ='';
    if($last_run_status eq 'P'){
	if($current_run_error){
	    $msg ="Re-run 850 process failed, some unprocessed data in EPT/Staging ...";
	}else{
	    ## do nothing...
	}
    }else{ ## i.e. last run status is undef, (not success)
	my $po_in_reserved = $edi->count_850_po_in_reserved();
	
	if($current_run_error){
	    if($po_in_reserved > 0){
		$msg ="Re-run 850 failed, unprocessed data ($po_in_reserved rows) in EPT/staging, and PO_reserved ...";
	    }else{
		$msg ="Re-run 850 failed, unprocessing data in EPT/staging table...";
	    }
	}else{
	    if($po_in_reserved > 0){
		$msg = "Re-run 850 successful, but unprocessed data ($po_in_reserved rows) in PO_reservd table...";
	    }
	}
    }
    if($msg){
	## log msg;
	$edi->{'log_obj'}->info("sending email with message:$msg");
	## send email
	my $e_addr = 'rdistaff@usmc-mccs.org';
	my $from   = 'rdistaff@usmc-mccs.org';
	my $subject ='E850 Rerun Status';
	&sendmail($e_addr, $from, $subject, $msg);
    }
}

my $end_time     = strftime( '%Y_%m_%d_%H_%M', localtime );
$edi->{'log_obj'}->info(
	"Program finished at: $end_time");

=head

NAME
       edi_850_wrapper.pl

VERSION
      1.1

USAGE
    Make sure the same day's 850 process has been run, or finsihed before you 
    run this program:

      perl /usr/local/mccs/bin/edi_850_wrapper.pl 

REQUIRED ARGUMENTS
       None.

OPTIONS


       -d   Debug mode to print out more dumped data for details at some major steps in the code

       -h   Displays a brief help message and exit.

DESCRIPTION
       This process must only run after the daily edi_850_to_sps_by_views.pl is done. 
       This program must be run as rdiusr.
       The program is a relaunch of the 850 process in case the first run of the day is failed for
       some reason such as SFTP failure, or database handle broken during read/write process.

REQUIREMENTS
       None.

DIAGNOSTICS
       None.

CONFIGURATION
       same as edi_850_to_spscommerce.config

DEPENDENCIES
       * IBIS::EDI for funnctions
       * Getopt::Std for options
       * POSIX qw(strftime WNOHANG) for getting time string
       * IBIS::Log::File for using log

SEE ALSO


INCOMPATIBILITIES


BUGS AND LIMITATIONS
       Limitations:

BUSINESS PROCESS OWNER


AUTHOR
       Chunhui Yu<yuc@usmc-mccs.org│chunhui_at_tigr@yahoo.com>

ACKNOWLEDGEMENTS

LICENSE AND COPYRIGHT
       Copyright (c) 2008 MCCS. All rights reserved.

       This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

DISCLAIMER OF WARRANTY
       THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
