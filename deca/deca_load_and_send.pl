#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Soumya K Bhowmic
##Date      : 12/08/2023
##
##Brief Desc: The extracts the Wine and Beer Cost and pricing information in a 
##            csv file to be sent to Defense Commissary Agency (DeCA)
##
##            The program must run a rdiusr.
## --------------------------------------------------------------------------  

use warnings;
use strict;
use POSIX qw(strftime);
use MCCS::Config;
use IBIS::DBI;
use DBD::Oracle qw(:ora_types);
use IBIS::Log::File;
use Net::SFTP::Foreign;
use Carp;
use IBIS::RPT2DB;
##use Term::ANSIColor qw(:constants);
use Getopt::Long;
use Data::Dumper;
my $debug = 1;
use MCCS::WMS::Sendmail;

## variables needed
my (
    $g_cfg,
    $g_to_dir,
    $g_log_dir,
    $g_config_file,
    $g_file_pattern,
    $g_db_name,
    $g_log_file,
    $g_data_dir,
    $g_stage_dir,
    $cmd,
    $to_host,
    $g_arch_dir,
    $g_from_dir,
    $g_report_email,
    $g_rpt_dir,
     $g_host_name    
    );


my $time_str = strftime( "%Y%m%d%H%M", localtime() );
## get config value from config file
my $day_str = strftime( "%Y%m%d", localtime() );


$g_cfg = new MCCS::Config;
$g_config_file     = $g_cfg->DECA->{config_file};  ### required!!!
$g_file_pattern    = $g_cfg->DECA->{file_pattern};
##$g_file_pattern    =  'BWMCX';
$g_db_name         = $g_cfg->DECA->{db_name};
## $g_db_name         = 'rms_s';
$g_log_dir         = $g_cfg->DECA->{log_dir};
## $g_log_dir         = '/usr/local/mccs/log/deca/';
$g_log_file        = $g_log_dir."get_and_load_deca.log_".$time_str;

$g_report_email    =  $g_cfg->DECA->{tech_emails};
$g_stage_dir       = $g_cfg->DECA->{stage_dir};
$g_data_dir        = $g_stage_dir;
$g_log_dir         = $g_cfg->DECA->{log_dir};

$g_arch_dir         = $g_cfg->DECA->{archive_dir};
## my $archive_dir ='/usr/local/mccs/data/deca/archive/';

$g_host_name       = $g_cfg->DECA->{FTP_SERVER};
my $g_remote_dir = $g_cfg->DECA->{remote_dir};

## get log object
my $log_file = '/usr/local/mccs/log/deca/deca_load_and_send.log_'.$time_str;
my $g_log = new IBIS::Log::File( { file => $log_file, append => 1 } );


# NOTE in production, hard_coded rms_p_force will be replaced by $g_db_name, yuc
my $dbh  = IBIS::DBI->connect( dbname => 'rms_p_force',  attribs => { AutoCommit => 0 }  );

my $day_to_run = 0;
$day_to_run = &check_day_to_run();

if($day_to_run){
### 
    $g_log->info("Today is:". $day_str." and a day to run the process");
}else{
    $g_log->info("Today is:". $day_str." and NOT a day to run the process");
    $g_log->info("Program exiting...");
    exit;
}

## work from here:
my ($good_news, $good_news2, $bad_news, $bad_news2);

## get data from database into a file
my $lf_filename   = 'BWMCX'.$day_str.".csv";        #Why are we not using the global config file ($g_file_pattern)
my $output        = $g_stage_dir.$lf_filename;
my $remote_file   = $g_remote_dir.$lf_filename;
my $archive_file  = $g_arch_dir.$lf_filename;


## get data in a hash reference, and write flat file
$output  = &write_deca_flat($output, $dbh); 
unless(-s $output){
    my $msg = "Warning: DECA extraction return zero rows.";
    $g_log->info($msg);
    &send_mail_with_attachment ($msg, $msg, '');
}

## load data into a database table
 &load_deca_flat();


## sent file to fms server
my $sftp_error = 0;
if(-s $output){
    $sftp_error = &sftp_send_a_file ($output, $remote_file, $g_log);    
}

if ($sftp_error){
    my $msg = "Warning: scp failed:".$cmd;
    $g_log->info($msg);
    &send_mail_with_attachment ($msg, $msg, '');

}else{

    &send_mail_with_attachment('DeCA Beer and Wine Cost File Delivered  '.$time_str,
			       'A DeCA Beer and Wine Cost file has been generated and sent. Please review the attached file to ensure all required prices were included.',
                           $output);
}

print "about to archive $output \n";
my $mv_cmd = "mv $output  $archive_file";
my $ret_sys = system($mv_cmd);

if ($ret_sys){
    my $msg ="ERROR in moving files:".$mv_cmd;
    $g_log->info($msg);
    &send_mail_with_attachment ($msg, $msg, '');
}else{
    my $msg ="Succes in moving files:".$mv_cmd;
    $g_log->info($msg);
}

## send email after ftp the file away..
### &send_mail('DeCA Processing result '.$time_str, $good_news, $bad_news);

## disconnect the db once every type of files have been processed.
 $dbh->disconnect();
my $end_msg ="Program finished.";
$g_log->info($end_msg);

############################# SUBS ########################################

sub check_day_to_run{
    my $query ="select to_char(sysdate + 7, 'YYYYMMDD') as day_7_later from dual";
    $g_log->info("Query:".$query);
    my $date_str ='';
    my $is_day_to_run = 0;
    my $ret = $dbh->selectall_arrayref($query);
    if($ret){
        $date_str  = $ret->[0][0];
    }else{
        $date_str =  undef;
    }

    if ($day_str){
	if (substr($date_str, 6, 2) eq '01'){
	    $is_day_to_run = 1;
	    $g_log->info("this is the day to run the process.");
	}
    }
    #TODO remove next line uncomment after that
    return 1
    #return $is_day_to_run;
}


##-------------------------------------------------------------------------
sub write_deca_flat{
    my ($output, $dbh) = @_;
    open(OUT, ">$output") || die "failed to open file". $output;
    my $header =
	"CATEGORY_GROUP,VENDOR_NUMBER,UPC,DESCRIPTION,CASEPK,COST_STORE_OVERRIDE,UNIT_COST,COST_START_DATE\r\n";
    print OUT "$header";
    my $query = &get_query();

    $g_log->info("Query:".$query);
    my $sth;
    eval{
	$sth = $dbh->prepare($query);
	$sth->execute;
    };

    if($@){
	my $msg = "DB error:".$@;
	$g_log->info($msg);
	&send_mail_with_attachment ("DECA data extraction error", $msg, '');
    }
    
    my $line_ctr = 0;
    my $data_ref;
    while (my $r = $sth->fetchrow_hashref ){
        ## remove commas  from the description for csv file sake
	my $description = substr($r->{description}, 0, 60);
	$description =~ s/\,/ /g;
	
	my $line_data =
            $r->{category_group}.",".
            $r->{vendor_number}.",".
	    $r->{upc}.",". 
	    $description.",".
	    $r->{casepk}.",".
	    $r->{cost_store_override}.",".
	    $r->{unit_cost}.",".
	    $r->{cost_start_date}.
	    "\r\n";
	print OUT "$line_data";
    }
    close OUT;
    return $output;
}


#---------------------------------------------------------------------

sub get_query{

   my $query = "select CATEGORY_GROUP, VENDOR_NUMBER,UPC,DESCRIPTION,CASEPK,COST_STORE_OVERRIDE,UNIT_COST, COST_START_DATE from v_deca_combined_final where to_date(COST_START_DATE,'MM/DD/YYYY') >= sysdate + 1";
###my $query = "select CATEGORY_GROUP, VENDOR_NUMBER,UPC,DESCRIPTION,CASEPK,COST_STORE_OVERRIDE,UNIT_COST, '03/15/2022' as COST_START_DATE from v_deca_combined_final order by UPC asc";

    return $query;
}
###-------------------------------------------------------------------

sub sftp_send_a_file {
    my ($from_file, $to_file, $log) = @_;
    my $g_cfg = new MCCS::Config;
    my %arglist;

    # Retrieve destination server and directory
    my $dest     = $g_cfg->DECA->{FTP_SERVER};
    $arglist{user}     = $g_cfg->DECA->{USER};
    $arglist{password} = $g_cfg->DECA->{PSWD};
    # Log server name and directory
    $log->info('SFTP transfer started' );
    $log->info("FTP_SERVER: $dest");

    # Establish SFTP connection to MCL server
    my $sftp;
    my $num_retry      = 10;
    my $successful_ftp = 'N';
    my $attempt;

    while ( $num_retry-- ) {
	eval { $sftp = Net::SFTP::Foreign->new( $dest, %arglist ) };
	if ( !$@ ) { $successful_ftp = 'Y'; last }
	$attempt = 10 - $num_retry;
	$log->info("Attempt $attempt to connect to $dest failed!\n");
	sleep(10);
    }

    if ( $successful_ftp eq 'N' ) {
	$log->info("SFTP connection to Mclane server ($dest) failed!");
        die;
    }
    my $sftp_result;
    eval{
  
	$sftp_result = $sftp->put($from_file, $to_file);
    };
    my $sftp_error = 0;
    if ($@){
        print "error:".$@;
        $log->info("SFTP Error:".$@);
	$sftp_error = 1;
    }else{
        print "sftp successful";
        $log->info("SFTP succwssful");
    }
    return $sftp_error;
}


sub load_deca_flat{
    my $wms = IBIS::RPT2DB->new( conf_file => $g_config_file );
    $wms->{'dbh_obj'} = IBIS::DBI->connect(
        dbname  => $g_db_name,
        attribs => { AutoCommit => 0 }
        ) or die "failed to connect db";
    $wms->load_a_report_table($g_data_dir, $g_file_pattern, ',','MM/DD/YYYY');
}
#################################EOF#################################



sub send_mail_with_attachment {
    my $subject = shift;
    my $body = shift;
    my $file = shift;

    my $go_mail = MCCS::WMS::Sendmail->new();
    $go_mail->verboseLevel(1);
    my @emails = values( %{$g_report_email} );
    $g_log->info("Sending attachment to:");
    foreach my $e ( sort keys %{$g_report_email} ) {
	$g_log->info(" $e ($g_report_email->{$e})");
    }
    $g_log->info(" mail_report");
    $g_log->info(" Subject: $subject");
    $g_log->info(" Attachment: $file") if $file;
    
    $go_mail->logObj($g_log);
    $go_mail->subject($subject);
    $go_mail->sendTo($g_report_email);
    $go_mail->attachments($file) if $file;
    $go_mail->msg($body);
    
    if ($file) {
	$go_mail->send_mail_attachment();
    }
    else {
	$go_mail->send_mail('No mclane cost load file');
    }
}
