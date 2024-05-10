#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Soumya K Bhowmic
##Date      : 12/06/2023
##
##Brief Desc: This program generates a TMS interface file for Purchase Orders
##            and sends it to Landair Inc.
##            The TMS interface file is based on the data mainly from
##            IRO_PO_HEADERS
##            IRO_PO_DETAILS
##            PURCHASE_ORDERS
##            SPECIFIC_REMARKS
##            The program must run a rdiusr.
## --------------------------------------------------------------------------  
use strict;
use warnings;
##use IBIS::CronOnly;
use IBIS::EDI;
use IBIS::TMS;
use Data::Dumper;
use Getopt::Std;
use POSIX qw(strftime WNOHANG);
use IBIS::Log::File;
use IBIS::FTP;
use IBIS::DBI;
use Sys::Hostname;
use Fcntl qw(:flock);
use Getopt::Long;

## Ensure only a single process is running before anything
open( SELF, "<", $0 ) or die "Cannot open $0 - $!";
flock( SELF, LOCK_EX | LOCK_NB ) or die "Already running.";

## ---Preparations----
## Globals
my (
    $tms,            $noftp,           $help,  $config, 
    $local_ftp_file, $remote_ftp_file, $bkup_ftp_file,
    $skip_msg,       $rslt_struct,     $prefix_address, $prefix_zero,
    $po_ctr,         $po_list_buff,    $update_a,
    $db_op_errors1,  $file_buff_ref,   $email_file
);
my @ftp_result;
my $sftp_result;
my ($debug,  $db_success1, $db_success2, $ftp_success ) = 0;
my $resend_ponvsn_list  = '';
my $hostname = hostname;


## Options
my $opt_result = GetOptions ( 
    "list=s"   => \$resend_ponvsn_list,   # string
    "noftp"    => \$noftp,      # flag
    "debug"    => \$debug,      # flag
    "file"     => \$email_file,
    "prefix"   => \$prefix_address, ## flag to prefix zeros to address_to_site
    "help"     => \$help);  # flag

if(!$opt_result){
    print "Some options are wrong. Need resubmit the job $0\n";
}


if ($help) {
    my $use_msg = qq(
     "\n General Usage:\n\t  perldoc $0 for more information.\n\n");
    print $use_msg;
    exit();
}

if($prefix_address){
    $prefix_zero = 1;
}else{
    $prefix_zero = 0;
}


## The Object
$config = '/usr/local/mccs/etc/edi_tms/poshipment_to_landair.config';
$tms = IBIS::TMS->new( conf_file => $config );

if ($debug) {
    $tms->{'debug'} = 1;
    print Dumper($tms);
}

##  Log start time
my $t_day      = strftime( '%Y-%m-%d', localtime );
my $t_suffix = strftime( '%Y_%m_%d_%H_%M',    localtime );

## A single log file
my $log_filename   = $tms->{LOG_DIR} . "/" . "poshipment_to_landair_log_file";
$tms->{'log_obj'} = IBIS::Log::File->new(
    {
        file   => $log_filename,
        append => 1, 
        level  =>4
    }
);

$tms->{'log_obj'}->log_info("\n\n*************\n\tStarted at:$t_suffix\n");
## Set DB handle autocommit as 0 for transaction control
$tms->{'dbh'} = IBIS::DBI->connect(
    dbname  => $tms->{CONNECT_INSTANCE},
    attribs => { AutoCommit => 0 }
  )
  or $tms->{'log_obj'}->log_die(
    "Cannot connect database using: 
     $tms->{CONNECT_INSTANCE}\n"
  );


## resend POs by using ponvsn list
if ($resend_ponvsn_list) {
    $tms->{'log_obj'}->log_info("a list of PONVSNs for resending: $resend_ponvsn_list");
    $resend_ponvsn_list = $tms->clean_up_po_list($resend_ponvsn_list);

    if ($debug) {
        print "input ponvsn list for resending:\n";
        print Dumper($resend_ponvsn_list);
    }

    my ( $c_flag, $c_msg )= $tms->confirm_sent_tms_PONVSNs($resend_ponvsn_list);

    if ( $c_flag == 0 ) {  ## if no error
        $tms->{'log_obj'}
          ->log_info("All po in resending tms list is confirmed: $resend_ponvsn_list");
	my $err_ref = $tms->refresh_sent_tms_PONVSNs($resend_ponvsn_list);	
	my $refresh_success = $tms->commit_or_failure_notice( $err_ref,
	'DB error encounterred in updating edi_tms_po_tracking table. See log!!'
	    );

        ## commit_or_failure_notice function will return 0(failed) or 1(success)..
        ## if failed to update, send an email
	if(!$refresh_success){
	    $tms->{'log_obj'}->log_info('DB error in updating tms_po_tracking. See log!!');
	    &email_run_result( "TMS process failed to update tms_po_tracking table", 
			       "DB error in updating tms_po_tracking", 
			       $tms->{MAIL_CC} );
	    exit;
	}
    }
    else {
        $tms->{'log_obj'}->log_info(
            "Failed to confirm all or non of the POs in list: $resend_ponvsn_list. 
             Detailed po confirmation information: $c_msg\n"
        );
        print
            "Failed to confirm all or Non or the POs in list: $resend_ponvsn_list details: $c_msg 
             Please revise the list and submit again.\n";
        exit();
    }
}

## 1: Extract data from v_edi_tms view
$tms->pool_tms_details( $resend_ponvsn_list);

if ($debug) {
    print "name of the main view:\n";
    print "$tms->{'V_EDI_TMS'}";
    print "tms data from the view:\n";
    print Dumper($tms->{'tms_ary_ref'});
}



## 2:check if any data returned:
my $tms_ary = $tms->get_tms_array_ref();


print Dumper($tms_ary) if ($debug);


if (@$tms_ary == 0) {
    $tms->{'log_obj'}->log_info(
	"WARNING: NO DATA retrived from view v_edi_tms!!!\n");
    &email_run_result( "No New TMS Data", 
		       "No new Landair shipment data for transsmision.",
		       $tms->{MAIL_CC} );
    exit;
}
## change the data into hash reference

$rslt_struct = $tms->get_tms_hash_ref();
if (!$rslt_struct) {
    $tms->{'log_obj'}->log_info(
        "WARNING: trouble to create the hash reference!!!\n");
    exit;
}

print Dumper($rslt_struct) if ($debug);

## 3: FTP preparation and transmission
## a: file name
my $fn_sufx = &create_tms_ftp_filename();
$remote_ftp_file = $tms->{REMOTE_DIR_PROD} . $fn_sufx;
$bkup_ftp_file   = $tms->{FTP_STAGING_DIR_BKUP} . $fn_sufx;
$local_ftp_file  = $tms->{FTP_STAGING_DIR} . $fn_sufx;
my $remote_test_ftp_file = $tms->{REMOTE_DIR} . $fn_sufx;

## b: ftp 
($local_ftp_file, $file_buff_ref) =
  $tms->generate_tms_from_hash_ref($local_ftp_file,$rslt_struct,$prefix_zero);

$tms->{'log_obj'}->log_info(
    "sending files from local: $local_ftp_file to remote: $remote_ftp_file\n" );

if ($noftp) {
    ## $ftp_result  = '';
    $ftp_success = 1;
}
else {
    
    local $SIG{__WARN__} = sub{}; 
## sftp trying to set permission against the window server, caused the 'Couldn't fsetstat' error. 
## hopefully, This will silent the warning, yuc jan 25, 11
    
    $sftp_result = $tms->sftp_one_file_to_remote_server( $local_ftp_file, $remote_ftp_file );

    ## 
    ## this will be used after landair is ready for the 

##    my @tmp_hlder;
##    push (@tmp_hlder, $local_ftp_file);
##    @ftp_result   = IBIS::FTP->new( { 
##	destination => 'landair', 
##	files =>\@tmp_hlder, 
##	remote_dir=>'/ExternToTms/POFeed'} 
##	)->put();

}

##=> 

if (!$sftp_result) { ## i.e. sftp did not return any filename...
##if (!@ftp_result || !$ftp_result[0]){

    my $msg;
    if ($noftp) {
        $msg .= " You have chosen not to send the TMS ftp file by 
                    using option -n. This is just a notice.\n";
    }
    else {
        $msg .=
	    "FTP failed. TMS file generated: $local_ftp_file 
             You MUST manually send $local_ftp_file to $tms->{REMOTE_SERVER}.\n";
    }
    ## rename the ftp file if failed to send
    my $failed_fn = "$local_ftp_file"."_ftp_failed";
    my $scmd = "mv $local_ftp_file  $failed_fn";
    system($scmd);

    $tms->{'log_obj'}->log_info("$msg");
    $tms->sendnotice( $tms->{POC}, $tms->{MAIL_FROM},
		      "FTP to $tms->{REMOTE_SERVER} FAILED!", $msg );
    
}
else {
    $ftp_success = 1;
    my $msg_end =
	"FTP sucessful! local: $local_ftp_file remote: $remote_ftp_file";
    $tms->{'log_obj'}->log_info($msg_end);
}


## 4:  updating the tms_po_track table
if ($ftp_success) {
    ($update_a, $po_ctr, $po_list_buff) =
	$tms->insert_tms_po_track();    ## 
    push( @$db_op_errors1, @$update_a );
    $db_success1 = $tms->commit_or_failure_notice( $db_op_errors1,
    'DB error encounterred in updating edi_tms_po_tracking table. See log!!'
	);
}

## Reporting and Cleaning up
## Move ftp file to backup dir
if ($db_success1) {
    system("mv $local_ftp_file $bkup_ftp_file");
    system("chmod 755 $bkup_ftp_file");
}
$tms->{'dbh'}->disconnect();    ## disconnect after transactions are commited.

## Report to POC about the result or errors of this process
if ($db_success1) {
    my $title = "LANDAIR/TMS: $po_ctr PO Records were processed";
    $po_list_buff .= $skip_msg if ($skip_msg);
    &email_run_result( $title, $po_list_buff, $tms->{MAIL_CC} );
    if($email_file){
	&email_run_result( 'TMS FTP File', $$file_buff_ref, $tms->{MAIL_CC} );
    }
}
else {
    my $buffer =
'No changes for PO today, Or db processing errored out.  See log for details.';
    my $title = "No Data for TMS(Landair) today";
    $buffer .= $skip_msg if ($skip_msg);
    &email_run_result( $title, $buffer, $tms->{MAIL_CC} );
}

## Log finish time
my $end_time = strftime( '%Y-%m-%d %H:%M:%S', localtime );
$tms->{'log_obj'}->log_info("\nEnded at: $end_time\n*--------------*\n\n");
exit();

## ---End of Main----
################## subroutines ############################

## create ftp filenames in both local and remote directories:
sub create_tms_ftp_filename {
    my $time = strftime( '%Y%m%d%H%M', localtime );
    my $day  = strftime( '%Y%m%d',     localtime )
      ;    ##day and a 3 digits serial number
    my $name_prefix = "MCCS_TMS";
    my $pattern     = $name_prefix . $day;
    my $name_str =
      $tms->filename_with_increment( $tms->{FTP_STAGING_DIR_BKUP}, $pattern,
        0 );
    if ( $name_str eq '' ) {
        $name_str = '000';
        $tms->{'log_obj'}->log_info("filename_with_increment failed");
    }

    my $fn_sufx = "/"
      . $name_prefix
      . $time . "_"
      . $name_str;    ## yymmddhhmmss format was used for realflatfile name.
    return $fn_sufx;
}

## This takes care of all the local directories needed.
sub prepare_local_directories {
    &make_directory_tree( $tms->{FTP_STAGING_DIR_BKUP} );
    &make_directory_tree( $tms->{FTP_STAGING_DIR} );
    &make_directory_tree( $tms->{LOG_DIR} );
}

sub make_directory_tree {
    my ($dir) = @_;
    unless ( -d $dir ) {
        print "making directory: $dir\n";
        my $cmd = "mkdir -p $dir";
        system($cmd);
        my $cmd2 = "chmod -R 775 $dir";    ## group to write
        system($cmd2);
    }
    return $dir;
}

sub email_run_result {
    my ( $title, $content, $cc_string ) = @_;
    $tms->{'log_obj'}->log_info("cc_list: $cc_string");
    my @email_list = split( /\|/, $cc_string );
    foreach my $eaddress (@email_list) {
        $tms->{'log_obj'}->log_info("Sending email to: $eaddress\n");
        if ( $eaddress =~ /\@/g ) {
            $tms->sendnotice( $eaddress, '', $title, $content );
        }
    }
}

__END__

=pod

=head1 NAME

poshipment_to_landair.pl

=head1 VERSION

This documentation refers version 1.


=head1 REQUIRED ARGUMENTS

None.

=head1 OPTIONS

=over 6

=item -d or --debug

Debug mode to print out more dumped data for details at some major steps in the code

=item -h or --help

Displays a brief help message and exit.

=item -n or --noftp

If you do not want the file sent to Landair, use this option.
This option will make the program do NOT do ftp, just created files in data dir. 

=item -l or --list 

This option is for resending a list of POs. 
The parameter values is a list of po, concatinated with its version number.
For example, if po is 456801 and verison_no is 3,  this value would be: 4568013. 
For multiple POs, these values need to be separated with commas.

=item -f or --file 

This option is for getting email containing the content of ftp file to the sender.
So, the sender can examine the content of the file without going to the ftp file directory.

=back

=head1 USAGE

 only run on drlin057 as 'rdiusr' for production with mc2p
 only run on hqlin056 as 'rdiusr' for testing with mc2q

Examples:

  ./poshipment_to_landair.pl  --debug --noftp 

Not sending file to landair, and print debug informaiton

  ./poshipment_to_landair.pl 

Run the scripts, and extract all new PO shipment information, and sent to landair

=head1 DESCRIPTION

This program will generate a TMS interface file of purchase orders shipment information, and send the file to Landair Inc.
The TMS interface file is based on the data mainly from iro_po_headers, iro_po_details, purchase_orders and spcific_remarks. Two views are created for the purpose of collecting data. Following notes are given about theses Oracle views:
1, Only PO data started from a specific order_date are collected. The date is hard coded in the SQL to created the views.
2, If multiple versions of the same PO exist in iro tables, only the most recent version of PO will be selected.
3, Eleven data items are collected and included in the final view v_edi_tms, and the ftp file. The order of the fields are as follows:\
Vendor Number, Vendor Name, PO number, Destination(site_id), PO start date, PO Stop Date, PO Freight Term, Reference 1(PO version), Reference 2(Remarks), Reference 3 (Cancelled Indicator), and Buyer_Employee_ID. 

This program must be run as rdiusr.
If no new purchase order data in db, the script will send message.
Log messages goes to /usr/local/mccs/log/ftp/edi_tms/ 
Data file goes to: /usr/local/data/edi/ftp/tms_outbound_backup/

=head1 REQUIREMENTS

None.

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION

/usr/local/mccs/etc/edi_tms/poshipment_to_landair.config
                                                                                        
=head1 DEPENDENCIES

=over 4

=item * IBIS::EDI 

=item * IBIS::TMS

=item * Getopt::Std for options

=item * POSIX qw(strftime WNOHANG) for getting time string

=item * IBIS::Log::File for using log
 
=item * MCCS::DBI  for connecting to database server

=back

=head1 SEE ALSO 
wiki page for SPEC of the project wiki page:
http://ibisdev.usmc-mccs.org/wiki/index.php/PO_shipment_to_Landair

=head1 INCOMPATIBILITIES

Unknown.

=head1 BUGS AND LIMITATIONS

No known bugs found yet

=head1 BUSINESS PROCESS OWNERs

Gary Winstel, Karen Stuekergergen

=head1 AUTHOR

Chunhui Yu<yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2008 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITE
D TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SH
ALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTI
ON OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTH
ER DEALINGS IN THE SOFTWARE.

=cut


