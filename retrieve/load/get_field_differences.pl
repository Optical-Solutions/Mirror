#!/usr/bin/perl
use warnings;
use strict;
use Getopt::Std;
use IBIS::Log::File;
use IBIS::DBI;
use Data::Dumper;
use POSIX qw(strftime WNOHANG);
use lib qw (/usr/local/mccs/lib/perl5/IBIS);
use EDI_TWM::Invoice;
use EDI_TWM::EDI_PARSER;
use EDI_TWM::LINE_ITEM::LineItem;
use EDI_TWM::ADDRESS::Address;
getopts('tdhs:f:'); 


our ($opt_t, $opt_d, $opt_h, $opt_f, $opt_s);
my ($success, $invoice_obj, $debug, $config, $size, $xmlfile);

my $base_dir = "/usr/local/mccs/etc/edi_850";
if($opt_t){
    $config      = "$base_dir"."/test_edi_850_to_spscommerce.config";
}else{
    $config      = "$base_dir"."/edi_850_to_spscommerce.config";
}

## Options
my $help_flag = 0;
 my $usage = "\tUsage: $0 \n\t required:\n\t -f <invoice_xml file of full path> \n\t optionals:\n\t -d  <debug, print out objects>   \n\t -t < run it to get some print out for field differences >\n\n\t example: $0 -f /xxx/xxx/xxx/abc.xml -t \n\n";

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

$invoice_obj->{'log_obj'}->log_info("Start time: $realtime $0\n");

print Dumper($invoice_obj) if ($debug);

## parse a testing invoice data xml into a reference
my $data_ref2 = $invoice_obj->_parse_invoice_xml($xmlfile);

print Dumper($data_ref2) if($debug);

## use the same file to create aobject
$invoice_obj->_invoice_obj_loader($data_ref2);
$invoice_obj->set_INVOICE_ENTRY_TYPE('I');
##$invoice_obj->{'log_obj'}->log_info("\t$0\t$invoice_obj->{'INVOICE_ID'}\n");	

print Dumper($invoice_obj) if ($debug);


if($opt_t){
    my $load_fine = 0;
    ## log information
    $invoice_obj->{'log_obj'}->log_info("vendor_duns: $invoice_obj->{'VENDOR_DUNS'}\n invoice_date: $invoice_obj->{'INVOICE_DATE'}\n invoice_number: $invoice_obj->{'INVOICE_NUMBER'}\n");

    eval{
	$invoice_obj->load2();
    };
    $invoice_obj->{'dbh'}->rollback;
}

$invoice_obj->{'dbh'}->disconnect(); 
my $end_time  = strftime( '%Y_%m_%d_%H_%M', localtime );
$invoice_obj->{'log_obj'}->log_info("End_time:$end_time");
$invoice_obj->{'log_obj'}->log_info("\n******************\n\n");

######################## END #####################
