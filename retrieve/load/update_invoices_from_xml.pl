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
getopts('tdhcs:f:'); 

###
## This script will be used to update some field in the edi_invoices table from a xml file.
####


our ($opt_t, $opt_d, $opt_h, $opt_f, $opt_s, $opt_C, $opt_A);
my ($success, $invoice_obj, $debug, $config, $size, $xmlfile, $hostname, $pro_id);
$hostname = hostname();
$pro_id = $$;

my $base_dir = "/usr/local/mccs/etc/edi_850";
$config      = "$base_dir"."/edi_850_to_spscommerce.config";


## Options
my $help_flag = 0;
 my $usage = "\tUsage: ./$0 \n\t required:\n\t -f <invoice_xml file of full path> \n\t optionals:\n\t -d  <debug, print out objects>   \n\t -t < Use it to load database >\n\n\t example: ./update_invoices_from_xml.pl -f /xxx/xxx/xxx/abc.xml -t \n\n";

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

## use the same file to create aobject
$invoice_obj->_invoice_obj_loader($data_ref2);


## 
my $source_xml;
print Dumper($invoice_obj);


## update Charge fields:
if($opt_C){
    if($xmlfile =~ /(IN\d+.7R3)/g){
	$source_xml = $1;
	$invoice_obj->set_SOURCE_FILE($source_xml);
    }
    
    
    my $load_fine = 0;
    ## log information
    $invoice_obj->{'log_obj'}->log_info("host:$hostname pid: $pro_id vendor_duns: $invoice_obj->{'VENDOR_DUNS'}\n invoice_date: $invoice_obj->{'INVOICE_DATE'}\n invoice_number: $invoice_obj->{'INVOICE_NUMBER'}\n");

my @fields = qw/
SOURCE_FILE
ORIG_MERCHANDISE_AMOUNT
CHARGE_FREIGHT_DESCRIPTION
CHARGE_FREIGHT_AMOUNT
CHARGE_DISCOUNT_DESCRIPTION
CHARGE_DISCOUNT_AMOUNT
CHARGE_KEG_DESCRIPTION
CHARGE_KEG_AMOUNT
CHARGE_DEFECT_DESCRIPTION
CHARGE_DEFECT_AMOUNT
CHARGE_OTHER_DESCRIPTION
CHARGE_OTHER_AMOUNT
	      /;
    eval{
	$invoice_obj->update_fields(\@fields);
    };

    if($@){
        my $msg = "Failed to update some fields with data in file: $xmlfile\n";
        $@ .= $msg;
	$invoice_obj->{'log_obj'}->log_info( $msg);
        $invoice_obj->{'log_obj'}->log_info( $@);
	$invoice_obj->{'log_obj'}->log_info( "host: $hostname pid: $pro_id  $@ ");
        $invoice_obj->{'dbh'}->rollback;
      
    }else{

	$invoice_obj->{'dbh'}->commit;
	$invoice_obj->{'log_obj'}->log_info(
            "Host: $hostname PID: $pro_id Successful in update with data in: $xmlfile\n"
	    );
	$load_fine = 1;
    }

    if($@){
	warn($@);
    }
}


if($opt_A){

## get the 3 key infor from xml file:
    my ($i_num, $v_dun, $i_date);
    $i_num  = $invoice_obj->get_INVOICE_NUMBER;
    $v_dun  = $invoice_obj->get_VENDOR_DUNS;
    $i_date = $invoice_obj->get_INVOICE_DATE;
    my $i_id = $invoice_obj->get_a_field_value_from_the_3_keys('invoice_id', $i_num, $v_dun, $i_date);

    $invoice_obj->update_all_address();







}

$invoice_obj->{'dbh'}->disconnect(); 
my $end_time  = strftime( '%Y_%m_%d_%H_%M', localtime );
$invoice_obj->{'log_obj'}->log_info("End_time:$end_time");
$invoice_obj->{'log_obj'}->log_info("\n******************\n\n");

######################## END #####################
