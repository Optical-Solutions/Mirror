#!/usr/bin/perl
use warnings;
use strict;
use Getopt::Std;
use IBIS::Log::File;
use IBIS::DBI;
use Data::Dumper;
use Sys::Hostname;
use POSIX qw(strftime WNOHANG);
use lib qw (/usr/local/mccs/lib/perl5/IBIS);
use EDI_TWM::Invoice;
use EDI_TWM::EDI_PARSER;
use EDI_TWM::LINE_ITEM::LineItem;
use EDI_TWM::ADDRESS::Address;
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
 my $usage = "\tUsage: ./$0 \n\t required:\n\t -f <invoice_xml file of full path> \n\t optionals:\n\t -d  <debug, print out objects>   \n\t -t < Use it to load database >\n\n\t example: ./load_multiple_invoices.pl -f /xxx/xxx/xxx/abc.xml -t \n\n";

if($opt_h){
    print "$usage\n";
    exit();
}

if($opt_d){
    print "option d is used\n";
    $debug = 1;
}


if($opt_f){
    $xmlfile = $opt_f;
    unless( -e $opt_f){
	print "$opt_f was not found. Full path?\n";
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

$invoice_obj->{'log_obj'}->log_info("Start time: $realtime $0 host:$hostname pid: $pro_id\n");

print Dumper($invoice_obj) if ($debug);

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

## save filename:
my $source_xml;
if($xmlfile =~ /(IN\d+.7R3)/){
    $source_xml = $1;
    $invoice_obj->set_SOURCE_FILE($source_xml);
}

## ADDING ONE DEFAULT VALUE FOR CURRENCY:
my $currency_id = $invoice_obj->get_CURRENCY_ID;
if(undef $currency_id){
    $invoice_obj->set_CURRENCY_ID('USD');
}

print Dumper($invoice_obj) if ($debug);

if(($opt_t)&&($source_xml)){
    my $load_isFine = 0;
    ## log information
    $invoice_obj->{'log_obj'}->log_info("host:$hostname pid: $pro_id vendor_duns: $invoice_obj->{'VENDOR_DUNS'}\n invoice_date: $invoice_obj->{'INVOICE_DATE'}\n invoice_number: $invoice_obj->{'INVOICE_NUMBER'}\n");
    my $error_msg ='';

    eval{
	$error_msg .= $invoice_obj->load2();
    };

    print "BBBBBBBBBBBBBBBBBBb: $error_msg  " if($debug);

    if($error_msg){
        my $msg = "Failed to load xml file: $xmlfile\n";
        $error_msg .= $msg;
	$invoice_obj->{'log_obj'}->log_info($msg);
	$invoice_obj->{'log_obj'}->log_info( 
           "host: $hostname pid: $pro_id"
	 );

##	print "ERRRRRRRRRRRRRRRRRRRRRRRRRRRor message: $error_msg\n";
        $invoice_obj->{'dbh'}->rollback;
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
	my $cmd = "mv $xmlfile  $temp_file";
	system($cmd);
	my $cmd2 = "chmod 775 $temp_file";
	system($cmd2);
	$invoice_obj->{'log_obj'}->log_info(
            "Host: $hostname PID: $pro_id. File has been moved to $temp_file\n"
					    );
    }
    
    if($error_msg){
	 $invoice_obj->{'log_obj'}->log_info($error_msg);
    }
}

$invoice_obj->{'dbh'}->disconnect(); 
my $end_time  = strftime( '%Y_%m_%d_%H_%M', localtime );
$invoice_obj->{'log_obj'}->log_info("End_time:$end_time");
$invoice_obj->{'log_obj'}->log_info("\n******************\n\n");

######################## END #####################
