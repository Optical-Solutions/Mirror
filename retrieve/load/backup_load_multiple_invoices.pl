#!/usr/bin/perl
## Purpose: Parse and load
use warnings;
use strict;
use Getopt::Std;
use IBIS::Log::File;
use IBIS::DBI;
use Data::Dumper;
use POSIX qw(strftime WNOHANG);
use lib qw (/home/yuc/ );
use EDI_TWM::Invoice;
use EDI_TWM::EDI_PARSER;
use EDI_TWM::LINE_ITEM::LineItem;
use EDI_TWM::ADDRESS::Address;

our ($opt_t, $opt_d, $opt_h, $opt_f);


my ($invoice_ary, $invoice_obj, $debug, $config);

my $base_dir = "/home/yuc/EDI_TWM";
$config ='/home/yuc/EDI_TWM/Config/edi_invoices.config';

## Options

if($opt_d){
    print "option d is used\n";
    $debug = 1;
}

$debug = 0;

# Object  
 $invoice_obj = EDI_TWM::Invoice->new(
    conf_file=>$config
    );
## connect to db:
$invoice_obj->_db_connect();

#Log
my $t_suffix = strftime( '%Y_%m_%d_%H_%M', localtime );
my $log_filename = $invoice_obj->{LOG_DIR}."/"."edi_log_file_".$t_suffix;
$invoice_obj->{'log_obj'} =
    IBIS::Log::File->new({
        file => $log_filename,
        append => 0
                         });


## parse a testing invoice data xml into a reference
##my $edi_parser = EDI_TWM::EDI_PARSER::new();

my $xmlfile = "$base_dir/DATA/IN45863708.xml";
my $data_ref2 = $invoice_obj->parse_invoice_xml($xmlfile);


if($debug){
    print "DDDDDDDDDDDDDDDDDDData\n";
    print Dumper($data_ref2);
}

for(my $i= 0; $i < 1000; $i++){
   my $a_obj =  EDI_TWM::Invoice->new(
	conf_file => $config
	);

   $a_obj->invoice_obj_loader($data_ref2);
   $a_obj->set_INVOICE_NUMBER($i);
   push(@$invoice_ary, $a_obj);
}

##########  VALIDATOR   ##########

######  LOADER   #######
## Loading object from the object array:

foreach my $invoice_obj(@$invoice_ary){
    ## insert a invoice entry, get new invoice id:
    $invoice_obj->set_INVOICE_ENTRY_TYPE('I');
    $invoice_obj->_db_connect();
    $invoice_obj->load_invoice_obj_values_to_irdb();
    $invoice_obj->{'dbh'}->disconnect();
}

##$invoice_obj->{'dbh_obj'}->disconnect();

