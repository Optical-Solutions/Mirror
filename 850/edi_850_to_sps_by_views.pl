#!/usr/local/mccs/perl/bin/perl

## Author: Chuck(Chunhui) Yu(yuc@usmc-mccs.org)

use strict;
use warnings;
##use IBIS::CronOnly;
use version; our $VERSION = qv('0.0.1');
use IBIS::EDI;
use Data::Dumper;
#use Time::Elapse;
use HTML::Template;
use FileHandle;
use Getopt::Std;
use POSIX qw(strftime WNOHANG);
use IBIS::Log::File;
use IBIS::DBI;
use MCCS::POS::TE;
use Sys::Hostname;
use Net::SFTP;
use Fcntl qw(:flock);
use IBIS::EDI_XML;
getopts('tdhfnrx:l:');

our ( $opt_t, $opt_d, $opt_h, $opt_f, $opt_l, $opt_n, $opt_x, $opt_r);
## Ensure only a single process is running
open( SELF, "<", $0 ) or die "Cannot open $0 - $!";
flock( SELF, LOCK_EX | LOCK_NB ) or die "Already running.";

## ---Preparations----
## Global variables
my (
    $edi,               $debug,          $config,          $db_op_errors1,
    $db_op_errors2,     $checkpoint_A,   $checkpoint_B,    $group_ref,
    $t_ins,             $local_ftp_file, $remote_ftp_file, $bkup_ftp_file,
    $resend_po_list,    $po_to_skip,     $skip_msg,        $date_created,
    $dtls_data_reconst, $rslt_struct,    $ftp_result,      $NO_NEW_DATA,
    $lt_one_k
);
my ( $db_success1, $db_success2, $ftp_success ) = 0;
my $hostname = hostname;

#TODO remember to check on this with Larry as to whether this code will run on 0010 or drlin057
# if ($opt_t) {
#     if ( $hostname eq 'drlin057' ) {
#         print "Wrong server was used. Hostname: $hostname\n";
#         die;
#     }
# }

## Options
if ($opt_t) {
    $config = "/usr/local/mccs/etc/edi_850/test_edi_850_to_spscommerce.config";
}
else {
    $config = '/usr/local/mccs/etc/edi_850/edi_850_to_spscommerce.config';
}

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
if ($opt_x) {
    $date_created = $opt_x;
    $date_created =~ s/\s+//g;
}

## Object
$edi = IBIS::EDI->new( conf_file => $config );

if ($debug) {
    $edi->{'debug'} = 1;
    print Dumper($edi);
}

##  Log start time
my $now      = strftime( '%Y-%m-%d %H:%M:%S', localtime );
my $t_suffix = strftime( '%Y_%m_%d_%H_%M',    localtime );
my $log_filename   = $edi->{LOG_DIR} . "/" . "edi_log_file_" . $t_suffix;
my $flat_file_name = $edi->{LOG_DIR} . "/" . "Flat_file_" . $t_suffix;

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

##Directories for ftp, ftp file backup, and logs on local machine.
&prepare_local_directories($edi);

## this is to markoff POs that have been filtered by a matrix of conditions 
## (March 26, 15)
eval{
    $edi->e850_860_exclude_filtered_pos("From ".$hostname." at ".$t_suffix, '850'); 
};

if ($@) {
    $edi->{'log_obj'}->log_info("e850_860_exclude_filtered_pos errors");
    $edi->{'log_obj'}->log_info($@);
}

## This is to sync any POs that are on hold. 
## This could be troublesome for 850. May lose sending out file when some db errors happens..
my $sync_errors = $edi->sync_po_in_holding_post_modification();
if($sync_errors){
    if (@$sync_errors >0){
	$edi->{'log_obj'}->log_info("Failed to sync modified POs in edi_po_in_hold_t table. will exit... @$sync_errors ");
	die;
    }
}

## ----Starting the real stuffs from below ---------
## 1a: In the case of re-sending POs:
if ($opt_l) {
    $edi->{'log_obj'}->log_info("a list of POs for resending: $opt_l");

    $resend_po_list = $edi->clean_up_po_list($opt_l);

    if ($debug) {
        print "input po list for resending:\n";
        print Dumper($resend_po_list);
    }
    my ( $c_flag, $c_msg ) =
      $edi->confirm_sent_POs( $resend_po_list, $edi->{'TABLE_E8S'} );

## here, added date_created restrictions for same po being used multiple times
## 3, write functions to update E8S table, evaluate the update result before going forward
    if ( $c_flag == 0 ) {
        $edi->{'log_obj'}->log_info("All po in the list is confirmed: $opt_l");
        my $update_result =
          $edi->update_resending_po_in_e8s( $resend_po_list, $date_created );
        if ( $update_result == 0 ) {
            $edi->{'log_obj'}->log_info(
                "WARNINGS: attempt to update one or more POs failed 
                for po: $resend_po_list\n Not all PO will be resent!!!\n"
            );
        }
    }
    else {
        $edi->{'log_obj'}->log_info(
            "Failed to confirm all or non of the POs in list: $opt_l. 
             Detailed po confirmation information: $c_msg\n"
        );
        print
            "Failed to confirm all or Non or the POs in list: $opt_l details: $c_msg 
             Please revise the list and submit again.\n";
        exit();
    }
}

## 1b:  In the case of new POs (not resent),  extract info from base table for the views,
##      update EPT, Staging tables, which are necessary:

if ( !$opt_l ) {

    my $ret_status = $edi->put_status();
    if($ret_status){
	$edi->{'log_obj'}->log_info("Failed to update status table!!! Fatal!");
	die;
    }

## check new PO list, if larger than 1000, split them, and mark off some...
    my ($po_count, $po_ref, $seq_ref) = $edi->check_current_850_po_size($date_created);

    ## insert and mark...
    my $success_insert = 0;

    ## set a default limit for POs per transfer
    my $max4po = $edi->{'MAX_PO_SEND'};
    unless($max4po){
	$max4po = 999;
    }
    if($po_count > $max4po){
	## insert and mark...
	my $log_msg ="New POs ($po_count) is over max limit ($max4po) and will be splited";
	$edi->{'log_obj'}->info($log_msg);
	$success_insert = $edi->insert_and_mark($po_ref);  
	unless($success_insert){
	    my $msg ="failed in sub insert_and_mark, program exiting...";
	    $edi->{'log_obj'}->info($msg);
	    die();
	}else{
	    $lt_one_k = 1;
	}
    }else{
	my $msg = "New POs ($po_count) is less than $max4po  Will do the routine transferring";
	$edi->{'log_obj'}->info($msg);
    }
##   done with the mark off part of the POs

## Start extraction...
    my $value_ref = $edi->extract_edi_850($date_created);
    $checkpoint_A = $edi->check_empty_hash_ref($value_ref);

    if ( $checkpoint_A != 0 ) {

##   The function does 3 things:
##   group the data so data from the same PO, goes together, ## adding DDDDDate?????!!!!!
##   as 'date_created' only has information up to 'day'.
##   check if the PO has both 10 and 90 lines,
##   check if the PO need to be skiped

        ( $group_ref, $po_to_skip ) =
          $edi->grouping_e850_data_and_check_skip_pos($value_ref);
        print Dumper($group_ref) if ($debug);

        ##  do the inert of E8S, and update of ept, ie, db_process number 1
        foreach my $po_id ( keys %$group_ref ) {
            $edi->{'log_obj'}->log_info("Processing PO_ID: $po_id\n");
            my ( $ret_ref, $insert_ctr ) =
              $edi->insert_E8S_wo_transaction_data( $group_ref->{$po_id} );

            push( @$db_op_errors1, @$ret_ref );
            $t_ins = $t_ins + $insert_ctr
              if ( defined($t_ins) && defined($insert_ctr) );

            my $update_result = $edi->update_EPT_table( $group_ref->{$po_id} );
            push( @$db_op_errors1, @$update_result );
        }

        ## check if the above db operation is fine:
        $db_success1 = $edi->commit_or_failure_notice( $db_op_errors1,
            "First level DB operation errors, @$db_op_errors1" );
        ## $record_ref->{'insert_record'} = $t_ins;
    }    ## for checkpoint_A != 0 line
}

## 2, Empty the 850 staging table, no matter if ept has new data
## NOTE: if L option is used, only the resend pos in the list will go
my $edi_850_stack = $edi->collect_edi_850_stack2($resend_po_list);

if($debug){
    print "Dummped edi_850_stack\n";
    print Dumper($edi_850_stack);
}

my $group_ref2;
$checkpoint_B = $edi->check_empty_hash_ref($edi_850_stack);

if ( $checkpoint_B != 0 ) {

    ## extract, parse data from the TWO views, header view, and detail views:

    ## old code:
    ## ($parsed_data_ref, $group_ref2, $po_to_skip)
    ##  = $edi->parse_edi_850_data($edi_850_stack);

    my $hdr_data_ref  = $edi->extract_850_header($edi_850_stack, $opt_l, $date_created);
    my $dtls_data_ref = $edi->extract_850_details($edi_850_stack, $opt_l, $date_created);

    if ($debug) {
        print "header:\n";
        print Dumper($hdr_data_ref);
        print "Details:\n";
        print Dumper($dtls_data_ref);
    }

    $dtls_data_reconst =
      $edi->re_group_850_by_cost_rp_v3( $dtls_data_ref, $hdr_data_ref );

    $rslt_struct =
      $edi->data_structure_formation_for_xml( $hdr_data_ref,
        $dtls_data_reconst );

    if ($debug) {
        print "Combined header and details data:\n";
        print Dumper($dtls_data_reconst);
        print "Data structure for html_template:\n";
        print Dumper($rslt_struct);
    }

    if ( ($po_to_skip) && ( keys %$po_to_skip > 0 ) ) {
        $skip_msg .= $edi->handle_skip_POs($po_to_skip);
    }

    ## generate a ftp filename
    my $fn_sufx = &create_850_ftp_filename();
    $remote_ftp_file = $edi->{REMOTE_DIR_PROD} . $fn_sufx;
    $bkup_ftp_file   = $edi->{FTP_STAGING_DIR_BKUP} . $fn_sufx;
    $local_ftp_file  = $edi->{FTP_STAGING_DIR} . $fn_sufx;
    my $remote_test_ftp_file = $edi->{REMOTE_DIR} . $fn_sufx;

    ## write into a xml file and validate the xml file created
    $local_ftp_file =
      &generate_xml_by_HtmlTemplate( $rslt_struct, $local_ftp_file,
        $edi->{'HTML_TEMPLATE_FILE'} );

    ## validation ON XML file:
    $edi->{'log_obj'}->log_info(
        "sending files from local: $local_ftp_file to remote: $remote_ftp_file\n"
    );

    if ( -e $local_ftp_file ) {
        ##my $schema = '/usr/local/mccs/data/xml/schema/SPS_850.xsd';
	my $schema = $edi->{'SCHEMA'};
        my $success_flag;
        my $addr_dest_error;
        my $failed_step;
        ## validate 1 against schema:
        eval { MCCS::POS::TE::xml_validate( $local_ftp_file, $schema ); };
        if ($@) {
            $edi->{'log_obj'}
              ->log_info("Validation failed against schema: $schema");
            my $last_error = MCCS::POS::TE::get_last_xml_error();
            $edi->{'log_obj'}->log_info("$last_error");
            $failed_step = "Schema Validation";
        }
        else {
            $edi->{'log_obj'}
              ->log_info("Validation passed for file: $local_ftp_file");
        }
        ## validate 2 for address, destination1
        eval {
            ( $success_flag, $addr_dest_error ) =
              IBIS::EDI_XML::check_address_or_destination1($local_ftp_file);

        };
        if ( !$success_flag ) {
            if ($addr_dest_error) {
                $@ .= $addr_dest_error;
            }
        }
        if ($@) {
            $edi->{'log_obj'}
              ->log_info("Validation failed for Address-Destination");
            $edi->{'log_obj'}->log_info("$@");
            $failed_step = "Address, Destination Validation";
        }
        else {
            $edi->{'log_obj'}
              ->log_info("Validation passed on Address-Destination");
        }

        ###  Next step when validation checking become good enough, the die() in following code will be applied:
        if ($@) {
            $edi->{'log_obj'}
              ->log_info("Failed validation in validation step: $failed_step");
        }
    }

    ## Ftp to sps
    $edi->{'log_obj'}->log_info(
"sending files from local: $local_ftp_file to remote: $remote_ftp_file\n"
    );
    if ($opt_n) {
        $ftp_result  = '';
        $ftp_success = 1;
    }
    else {

        if ($opt_f) {    ## option to send to test directory:
            $edi->{'log_obj'}
              ->log_info("used option for FTP to test directory too");
            $ftp_result =
              $edi->ftp_one_file( $local_ftp_file, $remote_test_ftp_file );
            if ( $ftp_result eq '' ) {
                $edi->{'log_obj'}
                  ->log_info("FTP failed for file: $remote_test_ftp_file");
            }
            else {
                $edi->{'log_obj'}
                  ->log_info("FTP succeeded sending: $remote_test_ftp_file");
            }
        }
        else {
            #-----------------------
            # Put in PRODUCTION here
            #-----------------------
            $edi->{'log_obj'}->log_info("SFTP put: $local_ftp_file to");
            $edi->{'log_obj'}->log_info("          $remote_ftp_file");
            eval {
                $ftp_result =
		    $edi->ftp_one_file( $local_ftp_file, $remote_ftp_file );
    
                ## do it one more time if the first try failed...
	        unless($ftp_result){
    
                $edi->{'log_obj'}->log_info("          Put FAILED");
                $edi->{'log_obj'}->log_info("          2nd attempt $remote_ftp_file");
    
		    $ftp_result =
		        $edi->ftp_one_file( $local_ftp_file, $remote_ftp_file );
	        }
            };
            if($@){
                $edi->{'log_obj'}->log_info("FTP FAILED because\n$@");
            }

        }
    }
    unless ($ftp_result) {
        my $msg;
        if ($opt_n) {
            $msg .= " You have chosen not to send the ftp file by 
                    using option -n. This is just a notice.\n";
        }
        else {
            $msg .= "FTP failed. As some data in the database has been changed. 
                   You MUST manually send $local_ftp_file to $edi->{REMOTE_SERVER} ... 
                   connection? transmiting?\n";
        }
        $edi->{'log_obj'}->log_info("$msg");
	if($opt_r){
	    $edi->sendnotice( $edi->{POC}, $edi->{MAIL_FROM},
			      "FTP to $edi->{REMOTE_SERVER} FAILED!", $msg );
        }
	## if died here, need to trigger the re-run..
	if (!$opt_n ) {
            die($msg);
        }
    }
    else {
        $ftp_success = 1;
        my $msg_end =
           "FTP process has worked. local: $local_ftp_file remote: $remote_ftp_file";
        $edi->{'log_obj'}->log_info( $msg_end );
    }
}
else {    ## if edi_850_stack is empty

    my $msg = "Empty edi_850_staging stack. No new or resending POs.";
    $edi->{'log_obj'}->log_info($msg);
    my $buffer =
	'No new data for EDI 850 auto transmit. See log for details.';
    my $title = "No new data for EDI 850 auto transmit";
    &email_run_result( $title, $buffer, $edi->{MAIL_CC} );
    
## update to success when no data to send...
    if(!$opt_l){ ## only apply to cron job, not re-sending POs
	my $up_status_error = $edi->update_status('S', $edi->{'CUR_RUN_ID'} );
	if($up_status_error){
	    my $error_msg = "update status: $up_status_error\n";
	    $edi->{'log_obj'}->log_info("$error_msg");
	    die;
	}
	
    }
    $edi->{'dbh'}->disconnect();    ## disconnect after transactions are commited.
    my $end_time1 = strftime( '%Y-%m-%d %H:%M:%S', localtime );
    $edi->{'log_obj'}->log_info("\nEnded at: $end_time1\n*--------------*\n\n");
    exit();
}

## Update e8s table if ftp works fine

if ( $ftp_success) {
    for ( my $i = 0 ; $i < @{ $rslt_struct->{'PurchaseOrder'} } ; $i++ ) {
	my $po_id = 
	    $rslt_struct->{'PurchaseOrder'}[$i]->{'Header'}[0]->{'OrderHeader'}[0]->{'PurchaseOrderNumber'};
	if($po_id =~ /\d+/){
	    my $update_result = $edi->update_E8S_by_PO($po_id, $opt_l, $date_created);
	    push( @$db_op_errors2, @$update_result );
	}
    }
}else{
    
    $edi->{'log_obj'}->log_info("File sending failure... file generated. need re-run, or manual sending. program will exit...");
    my $buffer =
	'FTP sending failed. Maybe ftp authentication trouble...';
    my $title = "SPS FTP 850 failed ";
    if($opt_r){
	&email_run_result( $title, $buffer, $edi->{MAIL_CC} );
    }
    die;
}


$db_success2 = $edi->commit_or_failure_notice( $db_op_errors2,
    'Failure in update_E8S_table. See log for details' );

if ($db_success2) {
## move ftp file to backup directory
    system("mv $local_ftp_file $bkup_ftp_file");
    system("chmod 755 $bkup_ftp_file");
}


## report to poc about the result of this process
if ($db_success2) {
    my $buffer = &write_info_from_xml_struct( $rslt_struct, $flat_file_name );
    print "cclist: $edi->{'MAIL_CC'}\n";
    my $title = "EDI 850 Transmit to SPS Summary";
    $buffer .= $skip_msg if ($skip_msg);
    &email_run_result( $title, $buffer, $edi->{MAIL_CC} ) if (!$debug);
 
 ## update status table:
    if(!$opt_l){ ## only apply to cron job, not re-sending POs

	my $up_status_error;
	if($lt_one_k){ ## if larger than 1k, update status to Pending(P)
	    $up_status_error = $edi->update_status('P', $edi->{'CUR_RUN_ID'} );
	}else{
	    $up_status_error = $edi->update_status('S', $edi->{'CUR_RUN_ID'} );
	}
	if($up_status_error){
	    my $error_msg = "update status: $up_status_error\n";
	    $edi->{'log_obj'}->log_info("$error_msg");
	    die;
	}
    }
}
else {       
    my $buffer =
	'DB processing errored out.  See log for details.';
    my $title = "update e8s table errors out";
    if($opt_r){
	&email_run_result( $title, $buffer, $edi->{MAIL_CC} );
    }
}

$edi->{'dbh'}->disconnect();    ## disconnect after transactions are commited.

##Log finish time
my $end_time = strftime( '%Y-%m-%d %H:%M:%S', localtime );
$edi->{'log_obj'}->log_info("\nEnded at: $end_time\n*--------------*\n\n");
exit();
## ---End of Main----


################## subroutines ############################
sub generate_xml_by_HtmlTemplate {
    my ( $data_for_ht_ref, $local_ftp_fname, $template_fname ) = @_;
    ## my $rslt_struct = $edi->data_structure_formation_for_xml($hdr_data_ref, $dtls_data_reconst);
    ## print Dumper($rslt_struct);
    ## get the structure that fit the template:

    unless ($template_fname) {
        $template_fname =
          '/usr/local/mccs/etc/edi_850/e850_html_template.xml';
    }

    unless ($data_for_ht_ref) {
        die "no data for writting to html-template\n";
    }

    unless ($local_ftp_fname) {

        die "local_ftp_filename, and permission to write required!!\n";
    }

    my $template = HTML::Template->new(
        filename          => $template_fname,
        die_on_bad_params => '1',
        case_sensitive    => '0',
	default_escape    => 'HTML'
    );

    $template->param( $data_for_ht_ref );

    ## print out xml string:
    my $one_str = $template->output;

    ## filter blank lines and print file:
    open( OUT, ">$local_ftp_fname" )
      or die "can not open file to write to: $local_ftp_fname!!!\n";

    my @lines = split( /\n/, $one_str );
    foreach my $line (@lines) {
        if ( $line =~ /\w+/g ) {
            print OUT "$line\n";
        }
    }
    close OUT;
    return $local_ftp_fname;
}


sub write_info_from_xml_struct {
    my ( $xml_struct_ref, $flat_file_name ) = @_;

### loop through hash_ref and write output:
    my $header_buffer =
      "\nPartnership_id\t  PO_id \t Site_id \t Process_time\n";
    my $buffer = '';
    my $process_time = strftime( '%Y-%m-%d %H:%M', localtime );
    my @all_list;
    my $list_ref;

    for ( my $i = 0 ; $i < @{ $xml_struct_ref->{'PurchaseOrder'} } ; $i++ ) {
        my $bulk_po_flag;

        my $po_type =
          $xml_struct_ref->{'PurchaseOrder'}[$i]->{'Header'}[0]
          ->{'OrderHeader'}[0]->{'PurchaseOrderTypeCode'};
        my $tid =
          $xml_struct_ref->{'PurchaseOrder'}[$i]->{'Header'}[0]->{'OrderHeader'}[0]->{'TradingPartnerId'};
        ##print "tid: $tid\n";
        my $po_id =
          $xml_struct_ref->{'PurchaseOrder'}[$i]->{'Header'}[0]
          ->{'OrderHeader'}[0]->{'PurchaseOrderNumber'};
        ##print "tid: $tid  p_id: $po_id\n";
        if ( $po_type eq 'DS' ) {
            for (
                my $j = 0 ;
                $j <
                @{ $xml_struct_ref->{'PurchaseOrder'}[$i]->{'LineItems'} } ;
                $j++
              )
            {
		if( $xml_struct_ref->{'PurchaseOrder'}[$i]->{'LineItems'}[$j]->{'ShipDestinationQty'}[0]){
		    
		    foreach my $des (
			keys %{
			    $xml_struct_ref->{'PurchaseOrder'}[$i]
				->{'LineItems'}[$j]->{'ShipDestinationQty'}[0]
			}
			)
		    {
			if ( $des =~ /des/ig ) {
			    my $dest =
				$xml_struct_ref->{'PurchaseOrder'}[$i]
				->{'LineItems'}[$j]->{'ShipDestinationQty'}[0]
				->{$des};
			    push( @all_list, "$tid\t$po_id\t$dest" );
			    $list_ref->{$tid}->{$po_id}->{$dest} = 1;
			}
		    }
		}elsif($xml_struct_ref->{'PurchaseOrder'}[$i]->{'Header'}[0]->{'Address'}[0]->{'AddressLocationNumber'}){
		    my $bulk_ds_site_id = $xml_struct_ref->{'PurchaseOrder'}[$i]->{'Header'}[0]->{'Address'}[0]->{'AddressLocationNumber'}; 
		    push( @all_list, "$tid\t$po_id\t$bulk_ds_site_id" );
		    $list_ref->{$tid}->{$po_id}->{$bulk_ds_site_id} = 1;
		    
		}
            }
        }
        else { ## this is for the 'SA' case:
            my $bulk_site_id = 
		$xml_struct_ref->{'PurchaseOrder'}[$i]->{'Header'}[0]
		->{'Address'}[0]->{'AddressLocationNumber'};
            push( @all_list, "$tid\t$po_id\t$bulk_site_id" );
            $list_ref->{$tid}->{$po_id}->{$bulk_site_id} = 1;
        }
    }
    
    my $uniq_ref;
    foreach my $line (@all_list) {
        if ( defined( $uniq_ref->{$line} ) ) {
            if ( $uniq_ref->{$line} != 1 ) {
                $uniq_ref->{$line} = 1;
            }
            else {
                next;
            }
        }
    }

    foreach my $partner_id ( keys %$list_ref ) {
        foreach my $po ( keys %{ $list_ref->{$partner_id} } ) {
            foreach my $site ( sort { $a <=> $b }
                keys %{ $list_ref->{$partner_id}->{$po} } )
            {
                $buffer .= "$partner_id\t$po\t$site\t$process_time\n";
            }
        }
    }

    if ($buffer) {
        $buffer = $header_buffer . $buffer;
    }

    $edi->{'log_obj'}->log_info($buffer);
    return $buffer;
}

sub clean_up_po_list {
    my ($po_list_str) = @_;
    my @po_ary;
    $po_list_str =~ s/\s+//g;
    $po_list_str =~ s/[A-Za-z]+//g;
    @po_ary = split( /\,/, $po_list_str );
    return \@po_ary;
}

## create ftp filenames in both local and remote directories:
sub create_850_ftp_filename {
##  Do all the transactions in the following part:
## Get a file name for ftp and backup
    my $time = strftime( '%Y%m%d%H%M', localtime );
    my $day  = strftime( '%Y%m%d',     localtime )
      ;    ## only day used for checking serial number perday.
    my $name_prefix = "PO";
    my $pattern     = $name_prefix . $day;
    my $name_str =
      $edi->filename_with_increment( $edi->{FTP_STAGING_DIR_BKUP}, $pattern,
        0 );
    if ( $name_str eq '' ) {
        $name_str = '000';
        $edi->{'log_obj'}->log_info("filename_with_increment failed");
    }

    my $fn_sufx = "/"
      . $name_prefix
      . $time . "_"
      . $name_str;    ## yymmddhhmmss format was used for realflatfile name.
    return $fn_sufx;
}

## This takes care of all the local directories needed.
sub prepare_local_directories {
    &make_directory_tree( $edi->{FTP_STAGING_DIR_BKUP} );
    &make_directory_tree( $edi->{FTP_STAGING_DIR} );
    &make_directory_tree( $edi->{LOG_DIR} );
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
    $edi->{'log_obj'}->log_info("cc_list: $cc_string");
    my @email_list = split( /\|/, $cc_string );
    foreach my $eaddress (@email_list) {
        $edi->{'log_obj'}->log_info("Sending email to: $eaddress\n");
        if ( $eaddress =~ /\@/g ) {
            $edi->sendnotice( $eaddress, '', $title, $content );
        }
    }
}
__END__

=pod

=head1 NAME

edi_850_to_sps_by_views.pl

-- Extract and process Purchase Order data from views based on edi_pending_transactions,
and purchase_orders etc, create and send the purchase orders as an xml file to SPS. 

=head1 VERSION

This documentation refers version 2, which is a rewrite of edi_850_to_spscommerce.pl

=head1 USAGE

   edi_850_to_sps_by_views.pl (only run on drlin057 as 'rdiusr' for production with mc2p)

   edi_850_to_sps_by_views.pl -d -t  (only run on hqlin056 as 'rdiusr' for testing with mc2q)


=head1 REQUIRED ARGUMENTS

None.

=head1 OPTIONS

=over 5

=item -t

Testing mode using a test configuration file, containing a set of parameters including test database tables, views,ftp sever and log directories etc.

=item -d

Debug mode to print out more dumped data for details at some major steps in the code

=item -h

Displays a brief help message and exit.

=item -l <po_id value> separated by ','

The PO ids you want to re-send

=item -n

NO ftp, just created files in 850_outbound_backup dir. 
If you do not want the xml transmit to SPS use this option

=item -x

date_created in the format of '03-AUG-09' in edi_pending_transactions table.

Example:
   edi_850_to_sps_by_views.pl -t -d -n
--run test server, collect all unprocessed data in merch.edi_pending_transactions table, 
    not sending file to sps, result xml file will be in /usr/local/mccs/data/edi/850_outbound_backup/  
Note: This is the typical uses of the script for testing.

   edi_850_to_sps_by_views.pl -t  -l '354948, 234234' -n -x '05-AUG-09'   -d
-- run test server, to resend PO 354948,234234, no ftp, and  print out debug message

=back

=head1 DESCRIPTION

This is a rewitten of edi_850_to_spscommerce.pl. Two major summary views were created for purchase order information. The views are edi_850outbound_header, and edi_850outbound_details, the views replaced a lot of subroutines in edi_850_to_spscommerce.pl, that serves the purpose of collecting all necessary information for the purchase order file edi850s. Each night, a new purchase order number list and related data is extracted from edi_pending_transaction, edi_850_staging tables. All other data necessates the generation of edi850 were collected from the two views as mentioned ealier. These data were used to generate the edi850 xml file, and send to sps.

This program must be run as rdiusr.

If no new purchase order in db, the script will send message.

Log messages goes to /usr/local/mccs/log/ftp/edi_850/ with a time stamp on file name. 

=head1 REQUIREMENTS

None.

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION

=over 4

=item  Here is the list of details for production:
=item  Two views need to be created in production database.
=item  An edi_850 purchase order HTML::Template for edi850 is needed
=item  A config file contains log, ftp authentication info is required.                                       
 
=back
                                                                                        
=head1 DEPENDENCIES

=over 4

=item * IBIS::EDI for transaction data extraction and evaluation

=item * FileHandle for passing fh as parameters.

=item * Getopt::Std for options

=item * POSIX qw(strftime WNOHANG) for getting time string

=item * IBIS::Log::File for using log
 
=item * MCCS::DBI  for connecting to database server

=item * Net::SFTP for ftp flat files to nexcom

=item * Fcntl qw(:flock) for setting only single process

=back

=head1 SEE ALSO 

EDI 850 Project Specifications by Karen Stuekerjuergen and SPS data spec, ftp procedures. 

=head1 INCOMPATIBILITIES

Unknown.

=head1 BUGS AND LIMITATIONS

Limitations:

1, No validation program used for the content of the PO
Some fields required  may miss as RMS do not have all the data. 
2, Please report problems to the <MCCS Help Desk|mailto:help.desk@usmc-mccs.org>.

=head1 BUSINESS PROCESS OWNER

 Karen Stuekerjuergen<@usmc-mccs.org>

=head1 AUTHOR

Chunhui Yu<yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>

=head1 ACKNOWLEDGEMENTS

Thanks to my colleagues at MCCS, Eric Spencer, Hanny Januarious, Keren Stuekerjuergen, Mike Ganzalaz, Joel Mathews, Teresa Chao etc for all the help, suggestions and testings. 

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


