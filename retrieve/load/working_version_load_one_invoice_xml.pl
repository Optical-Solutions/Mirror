#!/usr/bin/perl
use warnings;
use strict;
use Getopt::Std;
use IBIS::Log::File;
use IBIS::DBI;
use Data::Dumper;
use File::Copy;
use Sys::Hostname;
use POSIX qw(strftime WNOHANG);
use lib qw (/usr/local/mccs/lib/perl5/IBIS);
use EDI_TWM::Invoice;
use EDI_TWM::EDI_PARSER;
use EDI_TWM::LINE_ITEM::LineItem;
use EDI_TWM::ADDRESS::Address;
##use EDI_TWM::EDI_ERR_HDL;
use EDI_Utils;
getopts('tdhs:f:');


our ($opt_t, $opt_d, $opt_h, $opt_f, $opt_s);
my ($success, $invoice_obj, $debug, $config, $size, $xmlfile, $hostname, $pro_id);
$hostname = hostname();
$pro_id = $$;

my $base_dir = "/usr/local/mccs/etc/edi_850";
if($opt_t){
    $config      = "$base_dir"."/test_edi_850_to_spscommerce.config";
}else{
    $config      = "$base_dir"."/edi_850_to_spscommerce.config";
}

## Options
my $help_flag = 0;
 my $usage = "\tUsage: ./$0 \n\t required:\n\t -f <invoice_xml file of full path> 
                       \n\t optionals:\n\t -d  <debug, print out objects>   
                       \n\t -t < Use it to load database >\n\n\t 
               example: ./load_multiple_invoices.pl -f /xxx/xxx/xxx/abc.xml -t \n\n";

if($opt_h){
    print "$usage\n";
    exit();
}

if($opt_d){
    $debug = 1;
}

if($opt_f){
    $xmlfile = $opt_f;
    unless( -e $opt_f){
	print "File $opt_f was not found. Full path?\n";
	exit();
    }
}

# Object  
$invoice_obj = EDI_TWM::Invoice->new(
    conf_file=>$config
    );
## connect to db:
$invoice_obj->_db_connect();

#Log
$invoice_obj->{LOG_DIR} = '/usr/local/mccs/log/edi/edi_810'; 
my $day = strftime( '%Y_%m_%d', localtime );
my $realtime = strftime( '%Y_%m_%d_%H_%M', localtime );
my $log_filename = $invoice_obj->{LOG_DIR}."/"."invo_xml_loading_log_".$day;
$invoice_obj->{'log_obj'} =
    IBIS::Log::File->new({
        file => $log_filename,
        append => 1
                         });
$invoice_obj->{'log_obj'}->log_info(
 "Start time: $realtime $0 host:$hostname 
  pid: $pro_id Loading: $xmlfile.\n");
print Dumper($invoice_obj) if ($debug);


## Is the xml file has been used before?
#my $fcheck = $invoice_obj->check_invo_xml_fn($xmlfile);
#if($fcheck){ ## return non zero number, exist file
#    my $cp_status = File::Copy::move($xmlfile, $invoice_obj->{REDUNDANT_INVO_DIR}."/");
#    if($cp_status == 1){
#	my $msg = "Same name file exists in IRDB: $xmlfile.
#               Moved to $invoice_obj->{REDUNDANT_INVO_DIR}.";
#	$invoice_obj->{'log_obj'}->log_info($msg);
#    }else{
#        $invoice_obj->{'log_obj'}->log_info("move $xmlfile to redundant directory failed. permission?");
#    }
#    exit();
#}

## parse a testing invoice data xml into a reference
my $data_ref2 = $invoice_obj->_parse_invoice_xml($xmlfile);
print Dumper($data_ref2) if($debug);


## use the parsed data to create an object
$invoice_obj->_invoice_obj_loader($data_ref2);


## set some default values:
$invoice_obj->set_INVOICE_ENTRY_TYPE('I');
if($invoice_obj->{'INVOICE_ID'}){
    $invoice_obj->{'log_obj'}->log_info("\t$0\t$invoice_obj->{'INVOICE_ID'}\n");	
}
my $currency_id = $invoice_obj->get_CURRENCY_ID;
if(undef $currency_id){
    $invoice_obj->set_CURRENCY_ID('USD');
}

## save filename:
my $source_xml;
if($xmlfile =~ /(IN\d+.7R3)/){
    $source_xml = $1;
    $invoice_obj->set_SOURCE_FILE($source_xml);
}

if(-s $xmlfile){ 
    my $load_isFine = 0;

    my $err_hdl_problem = 0;
    $invoice_obj->{'log_obj'}->log_info(
        "host:$hostname pid: $pro_id 
         vendor_duns: $invoice_obj->{'VENDOR_DUNS'}
         invoice_date: $invoice_obj->{'INVOICE_DATE'} 
         invoice_number: $invoice_obj->{'INVOICE_NUMBER'}");

    my $error_msg ='';
    eval{
	$error_msg .= $invoice_obj->load2();
    };

    print "$error_msg  " if($debug);
    if($error_msg){
        my $msg = "irdb loading process errored out for file: $xmlfile\n";
        $error_msg .= $msg;
	$invoice_obj->{'log_obj'}->log_info($msg);
	$invoice_obj->{'log_obj'}->log_info( 
           "host: $hostname pid: $pro_id"
	 );
	$invoice_obj->{'dbh'}->rollback;

	###############################
	## ERROR HANDLINGS June,09, 2009 
	###############################

	## grep error for 'duplicate invoice' pattern
	## find invoice_id, and source_file of the pattern
	#my @error_ary;
	#push(@error_ary, $error_msg);
	##my @grep_result1 = grep('RDIUSR.EDI_DUPLICATE_INVOICE', @error_ary);
	my $grep_result = 0;

	if($error_msg =~ /RDIUSR\.EDI\_DUPLICATE\_INVOICE/){
            ## To get previous redundant invoice xmlfile name
            ## by using vendorname, invoice_number, invoice_date from the invoice_obj (from xml file)
	    my ($f_vd, $f_in, $f_ivdt);
	    $f_vd    = $invoice_obj->get_VENDOR_DUNS();
            $f_in    = uc($invoice_obj->get_INVOICE_NUMBER());
            $f_ivdt  = $invoice_obj->get_INVOICE_DATE();
	    my $pre_invo_xml = '';
	    if(($f_vd ne '')&&($f_in ne '')){
                 ## NOTE: the invoice_date requires to be in the format of: yyyy-mm-dd
		$pre_invo_xml =
		    $invoice_obj->get_src_file_from_redundant_xml_info($f_in, $f_vd, $f_ivdt);		
	    }else{
                $error_msg .=
                "Missing vendor_name, or invoice_number to retrive redundant xml from db.";
		$err_hdl_problem = 1;
		warn($error_msg);
		exit;
	    }
	    my $pre_xml;
	    if($pre_invo_xml){
		$pre_xml = $invoice_obj->{INVO_XARCHIVE_DIR}."/".$pre_invo_xml;
	    }
            ## diff the two files
            print "old: $pre_xml new: $xmlfile\n" if $debug;	    
            my $diff_ref=[];
	    if((-e $xmlfile)&&($pre_invo_xml)){
		## assume system has 'diff' function
		$diff_ref = $invoice_obj->diff_output_to_array($pre_xml, $xmlfile);
	    }else{ ## this case is unlikely to happen
                my $d_msg = "File(s) diff are missing, $xmlfile, or archive file";
                if($pre_xml){$d_msg .= " $pre_xml";}
		$error_msg .= $d_msg;
                warn($error_msg);
		exit; ## retpnt 1, this will avoid checking on the diff array on next step
	    }

	    if(@$diff_ref == 0){ ## same file with different filename, move to redundant directory
		if($err_hdl_problem == 0){
		    my $ret_status = File::Copy::move($xmlfile, $invoice_obj->{REDUNDANT_INVO_DIR}.'/');               		
		    if($ret_status == 1){
			$error_msg .= "$pre_xml $xmlfile are identical files. moved to archive";
		    }else{
			$error_msg .= "File::Copy::move($xmlfile, $invoice_obj->{REDUNDANT_INVO_DIR}.'/') failed.";
			$err_hdl_problem = 1;
		    }	    
		}
	    }else{
		## if file contents different, processing the diff contents
		## check the diff tags to see if they are in the un-important list 
		## if all are in the unimportant list, log it, and copy file to redundant dir
		## if new tag fields are found, die with error
		
		my $similar =$invoice_obj->scan_diff_agxt_tags($diff_ref);
		
		if($debug){
		    print Dumper($diff_ref);
		}

		if($similar == 1){
                    my $ret_stat = File::Copy::move($xmlfile, $invoice_obj->{REDUNDANT_INVO_DIR}.'/');
		    if($ret_stat == 1){
			$error_msg .=
			"$pre_xml and $xmlfile have different tags, but not important. 
                         moved to redundant directory.";
		    }else{
		         $error_msg .=
			     "Failed move($xmlfile, $invoice_obj->{REDUNDANT_INVO_DIR}.'/'";
                         $err_hdl_problem = 1;
		    }
		}else{
		    my $tmp_msg ="$pre_xml  $xmlfile have tags not in the list to ignore. Manual checking needed.\n";
                    foreach my $d_line(@$diff_ref){
			$tmp_msg .= $d_line;
		    }

		    $error_msg .=  $tmp_msg;
	            $err_hdl_problem = 1;
		}
	    }
	}else{
	    ## other loading errors, like wrong_site_id, wrong_postal_code will be handled, 
            ## add error_msg, warn, return
	    $error_msg .= "Oracle error msg do not have 'EDI_DUPLICATE_INVOICE' by greping, other errors";
            $err_hdl_problem = 1;
	}
	$load_isFine = 0;
    }else{

	$invoice_obj->{'dbh'}->commit;
	$invoice_obj->{'log_obj'}->log_info(
            "Host: $hostname PID: $pro_id Successful in loading xmlfile: $xmlfile\n"
	    );
	$load_isFine = 1;
    }

    ## if loading works, move file to archive directory
    if($load_isFine == 1){
	my $temp_file = $xmlfile;
	$temp_file =~ s/invo_xml_staging/invo_xml_archive/g;
	my $m_status = File::Copy::move($xmlfile, $temp_file);
	my $cmd2 = "chmod 775 $temp_file";
	system($cmd2);
	$invoice_obj->{'log_obj'}->log_info(
            "Host: $hostname PID: $pro_id. File has been moved to $temp_file\n"
					    );
    }
    
    if($error_msg){
	 $invoice_obj->{'log_obj'}->log_info("LOG error_msg:\n $error_msg");
    }

    if($err_hdl_problem == 1){
      warn($error_msg);
    }
}

$invoice_obj->{'dbh'}->disconnect(); 
my $end_time  = strftime( '%Y_%m_%d_%H_%M', localtime );
$invoice_obj->{'log_obj'}->log_info("End_time:$end_time");
$invoice_obj->{'log_obj'}->log_info("\n******************\n\n");

######################## END #####################


=head1 DESCRIPTION

load_one_invoice_xml.pl is a loader function for loading invoice xml file from SPS to
IRDB database using IBIS::EDI_TWM::Invoice.pm package.
The process begins with parsing a single invoice xml file into an invoice object.
Then, load the object data into 3 IRDB tables, namely edi_invoices, edi_address, and edi_invoice_details. 
The loader function can also handle database loading error resulted from trying to load a redundant invoice.

=head1 EXAMPLES

IBIS::EDI_TWM::bin::load_one_invoice_xml.pl -t -f /usr/local/mccs/data/edi/invo_xml_staging/xxxx.xml

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

load_one_invoice_xml.pl is configured from /usr/local/mccs/etc/edi_850/test_edi_850_to_spscommerce.config.
 The configuration file is a set of key=value for diffination of db connection, ftp, and database table etc.

=head1 DEPENDENCIES

=over 4

    =item *  EDI_TWM::Invoice;

=item * EDI_TWM::EDI_PARSER;

=item *  EDI_TWM::LINE_ITEM::LineItem;

=item * EDI_TWM::ADDRESS::Address;

=item *  EDI_Utils;

=item * IBIS::DBI;

=back

=head1 INCOMPATIBILITIES

As if.

=head1 BUGS AND LIMITATIONS

The know issue of Net::SFTP is its slowness when compared with Net::FTP. So is th
is module. In my tests, sometimes it took about one minite to login. For transfer
 of a 20 MB file, it took about 7 mins. For transfer of a 150 MB file, it took ab
out 30 mins. It is very slow. 

=head1 AUTHOR

Chunhui Yu <yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>
Eric Spencer<spencere@usmc-mccs.org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2009 Chunhui Yu, All rights reserved.

    This module is free software; you can redistribute it and/or modify it under the 
same terms as Perl itself.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIE
D, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A 
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGH
T HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTI
ON OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
