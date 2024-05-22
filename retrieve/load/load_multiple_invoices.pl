#!/usr/bin/perl
## Purpose: Parse and load one xml file many times with changed invoice_number only
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

my ($invoice_ary, $invoice_obj, $debug, $config, $size, $xmlfile);
my $base_dir = "/usr/local/mccs/lib/perl5/IBIS/EDI_TWM";
$config      = "$base_dir"."/Config/edi_invoices.config";

## Options
my $help_flag = 0;
 my $usage = "\tUsage: ./load_multiple_invoices.pl \n\t required:\n\t-s <repeated times of db insert, required>\n\t optionals:\n\t -f <invoice_xml file of full path> \n\t -d  <debug, print out objects>   \n\t -t < Use it to load database >\n\n\t example: to load 3 times of file ./abc.xml\n\t ./load_multiple_invoices.pl -s 3 -f ./abc.xml -t\n\n";

if($opt_h){
    print "$usage\n";
}

if($opt_d){
    print "option d is used\n";
    $debug = 1;
}

if($opt_s){
    $size = $opt_s;
    print "size: $size\n";
}else{
    $help_flag = 1;
}

if($opt_f){
    $xmlfile = $opt_f;
    unless( -e $opt_f){
	print "$opt_f was not found. Full path?\n";
	exit();
    }
}else{
   ### default xml invoice file:
    $xmlfile = "$base_dir/DATA/IN45863708.xml";
}

if($help_flag){
    if(!$opt_h){
	print $usage;
    }
    exit();
}

#--------

# Object  
$invoice_obj = EDI_TWM::Invoice->new(
    conf_file=>$config
    );
## connect to db:
$invoice_obj->_db_connect();

#Log
$invoice_obj->{LOG_DIR} = '/usr/local/mccs/log/edi/edi_810'; 
## in object for later accessing
my $t_suffix = strftime( '%Y_%m_%d_%H_%M', localtime );
my $log_filename = $invoice_obj->{LOG_DIR}."/"."edi_log_file_".$t_suffix;
$invoice_obj->{'log_obj'} =
    IBIS::Log::File->new({
        file => $log_filename,
        append => 1
                         });

$invoice_obj->{'log_obj'}->log_info("Start time: $t_suffix\t $0\n");

print Dumper($invoice_obj);

## parse a testing invoice data xml into a reference
my $data_ref2 = $invoice_obj->_parse_invoice_xml($xmlfile);

## use the same file to create an array of objects, and change invoice_number in the middle
for(my $i= 0; $i < $size; $i++){
   my $a_obj =  EDI_TWM::Invoice->new(
	conf_file => $config
	);
   $a_obj->_invoice_obj_loader($data_ref2);
   my $invo_num = $i."_"."$t_suffix";
   $invoice_obj->{'log_obj'}->log_info("creating invo_num: $invo_num\n");
   $a_obj->set_INVOICE_NUMBER($invo_num);
   push(@$invoice_ary, $a_obj);
}

if($debug){
    print Dumper($invoice_ary);
}

##########  VALIDATOR   ##########
## not implemented yet
#################################


######  LOADER   #######
## Loading object from the object array:

if($opt_t){
    foreach my $an_obj(@$invoice_ary){
	$an_obj->set_INVOICE_ENTRY_TYPE('I');
	$an_obj->_db_connect();
	$an_obj->{'log_obj'} = $invoice_obj->{'log_obj'}; 
	$an_obj->{'log_obj'}->log_info("\t$0\t$invoice_obj->{'INVOICE_NUMBER'}\n");	
	$an_obj->load2();
	$an_obj->{'dbh'}->disconnect();  ## commit for each good  object data set
    }
}

my $end_time  = strftime( '%Y_%m_%d_%H_%M', localtime );
$invoice_obj->{'log_obj'}->log_info("End_time:$end_time");
exit();
######################## END #####################
