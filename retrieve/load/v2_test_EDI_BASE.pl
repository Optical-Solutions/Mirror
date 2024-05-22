#!/usr/bin/perl
## Purpose: Parse and load

## 1, create a template for a invoice object tree (buildup the template)
## a, create template for each table,
## b, adding proper data structure to hold possible data values 

## 2, load values to the tree from a data_structrue which will be mapped to the field in the tree.
## a, parse xml into a data structure tree
## b, traverse through all branches of the tree to 
## c, map all the data items from the data tree to object template tree
## d, load up the object tree

## 3, load data items in the tree into the database
## after loading up the values into the template tree
## a, check the validity of the data, type, size before writting sql query
## b, a generic loader needed to load table from the object data
## c, load up a complate set of data for each invoice data object.


## EDIT and Display
## 4, extract data items from the database, or object and setup a tree
## 5, display the extracted data items from the tree to a web interface
## 6, web application to modify the data node in the tree
## 7, update database data from the tree.

## REPORTING
## extract data items from the database, set up a tree, same as step 4
## create necessary data structures for pdf file package
## create pdf file and send pdf file etc..


#  AUTHOR:  Chunhui YU, <yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.org>
#  CREATED: July 2008

##use strict;
use warnings;
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

## Options
if($opt_t){
   ## $config ="/home/yuc/three_way_matching/EDI_TWM/test_irdb.config";
}else{
    ##$config ='/home/yuc/three_way_matching/EDI_TWM/test_irdb.config';
}

if($opt_d){
    print "option d is used\n";
    $debug = 1;
}

$debug = 0;

# Object  
 $invoice_obj = EDI_TWM::Invoice->new(
    conf_file=>$config
    );

my $dbh = $invoice_obj->_db_connect();


#Log
my $t_suffix = strftime( '%Y_%m_%d_%H_%M', localtime );
my $log_filename = $invoice_obj->{LOG_DIR}."/"."edi_log_file_".$t_suffix;
$invoice_obj->{'log_obj'} =
    IBIS::Log::File->new({
        file => $log_filename,
        append => 1
                         });


## print Dumper($dbh);
## my $query = "select column_name, data_type, data_length, nullable from all_tab_columns where table_name = 'edi_invoices'";
## my $ret = $dbh->selectall_arrayref($query);
## print Dumper($ret);

=header
## 1, buildup an object tree template
## a, create template for each major table that need to be loaded the first time.
## For now, only 3 tables:
my ($edi_invoice, $line_item, $address) = $invoice_obj->parse_template_files();

print Dumper($edi_invoice);
print Dumper($line_item);
print Dumper($address);

### b, build the template:
### put the reference into a proper data structure for holding all possible data
my $line_item_ary = [];
my $address_ary   = [];
push(@$line_item_ary, $line_item);
push(@$address_ary, $address);

## use autoload to set up the tree
$invoice_obj->set_EDI_INVOICES($edi_invoice);
$invoice_obj->set_EDI_LINE_ITEMS($line_item_ary);
$invoice_obj->set_EDI_ADDRESS($address_ary);

=cut

##$debug = 0;


=header
my $line_item_obj = EDI_TWM::LINE_ITEM::LineItem->new();
print Dumper($line_item_obj);
my $address_obj =  EDI_TWM::ADDRESS::Address->new();
print Dumper($address_obj);
my $invoice_obj = EDI_TWM::INVOICE::Invoice->new();

##$invoice_obj->construct_object_tree();
my $line_item_ary = [];                                                                                                              
my $address_ary   = [];          
push(@$line_item_ary, $line_item_obj);
push(@$address_ary, $address_obj);

$invoice_obj->set_EDI_LINE_ITEMS($line_item_ary);                                                                                       $invoice_obj->set_EDI_ADDRESS($address_ary); 
$invoice_obj->set_EDI_INVOICES($invoice_obj);
print Dumper($invoice_obj);
## 2, load values to the tree from a data_structrue which will be mapped to the field in the tree.                                                                
## a, parse xml into a data structure tree                                                                                                                        
## b, traverse through all branches of the tree to                                                                                                                
## c, map all the data items from the data tree to object template tree                                                                                           
## d, load up the object tree     


=cut

## a function with input of a xml filename: output a data reference
## parse a testing invoice data xml into a reference
##my $edi_parser = EDI_TWM::EDI_PARSER::new();
my $xmlfile = "$base_dir/DATA/IN45863708.xml";

### it is import to set 0 here for parsing to hash, and not force array in the output
#my $data_ref2 = $edi_parser->EDI_TWM::EDI_PARSER::parse_xml_to_ref($xmlfile, 0);

my $data_ref2 = $invoice_obj->parse_invoice_xml($xmlfile);

if($debug){
    print "DDDDDDDDDDDDDDDDDDData\n";
    print Dumper($data_ref2);
}


$invoice_obj->invoice_obj_loader($data_ref2);
print Dumper($invoice_obj);
push(@$invoice_ary, $invoice_obj);

## subroutine to load up a data tree
## input: a mapping file, a parsed_data_reference 
## output: object that are loaded with data
## create a dummy mapping file:
## a dummy mapping reference too:




=header
my $map_file = "$base_dir/Config/data_mapping_file.txt";
my $map_ref = $invoice_obj->get_mapping_reference_from_file($map_file);

if($debug){
    print "MMMMMMMMMMMMMMMMMMMMapping\n";
    print Dumper($map_ref);
}

## loading up a fake object:
## load line_item, address specifically:

my $marker1 = "LineItems";
my $marker2 = "Header";
my $marker3 = "Address";

##my $invoice_obj =  EDI_TWM::INVOICE::Invoice->new();
my $site_id_ref;
foreach  my $key1 (keys %$data_ref2){
    if($key1 eq 'LineItems') {
	if($debug){print "key1: $key1\n";}
	my $lt_ary =[];
	for (my $i=0; $i<@{$data_ref2->{$key1}->{LineItem}}; $i++){
	    my $line_obj = EDI_TWM::LINE_ITEM::LineItem->new();
	    foreach my $key2 (keys %{$data_ref2->{$key1}->{LineItem}->[$i]->{InvoiceLine}}){
		## load up object by matching keys
                my $m_key1 = $map_ref->{$key1};
                my $m_key2 = $map_ref->{$key2};
		if(($m_key1 ne '') &&($m_key2 ne '')&&($data_ref2->{$key1}->{LineItem}->[$i]->{InvoiceLine}->{$key2} =~ /\d+|\w+/g)){
		    my $temp_field = 'set_'.uc($m_key2); 
		    $line_obj->$temp_field($data_ref2->{$key1}->{LineItem}->[$i]->{InvoiceLine}->{$key2});
		}
	    }
	    push(@$lt_ary, $line_obj);
	}
	$invoice_obj->set_EDI_INVOICE_DETAILS($lt_ary);	
    }elsif(($key1 eq 'Header')||($key1 eq 'Summary')){
	my $add_ref = [];
	foreach my $key (keys %{$data_ref2->{$key1}}){
	    if($key eq 'Address'){
		for (my $i=0; $i<@{$data_ref2->{$key1}->{Address}}; $i++){
		    my $address_obj = EDI_TWM::ADDRESS::Address->new();
		    foreach my $key2 (keys %{$data_ref2->{$key1}->{Address}->[$i]}){

			if($key2 eq 'AddressLocationNumber'){$site_id_ref->{$i}->{$key2} = $data_ref2->{$key1}->{Address}->[$i]->{$key2};}
			if($key2 eq 'AddressTypeCode'){      $site_id_ref->{$i}->{$key2} = $data_ref2->{$key1}->{Address}->[$i]->{$key2};}
			if($key2 eq 'AddressName')    {      $site_id_ref->{$i}->{$key2} = $data_ref2->{$key1}->{Address}->[$i]->{$key2};}

			my $m_key1 = $map_ref->{'Address'};
			my $m_key2 = $map_ref->{$key2};		
			if(($m_key1 ne '') &&($m_key2 ne '')&&($data_ref2->{$key1}->{Address}->[$i]->{$key2} ne '')){
			    my $temp_field = 'set_'.uc($m_key2);
			    if($key2 eq 'AddressTypeCode'){
				my $re_mapped_value = $map_ref->{$data_ref2->{$key1}->{Address}->[$i]->{$key2}};
                                $address_obj->$temp_field($re_mapped_value);
			    }else{
				$address_obj->$temp_field($data_ref2->{$key1}->{Address}->[$i]->{$key2});		
			    }
			}
		    }
		    push(@$add_ref, $address_obj);
		}
		$invoice_obj->set_EDI_ADDRESS($add_ref);              
	    }elsif(($key eq 'InvoiceHeader')||($key eq 'Date')||($key eq 'PaymentTerms')||($key eq 'Totals')){
		foreach my $key2 (keys %{$data_ref2->{$key1}->{$key}}){
		    my $m_key1 = $map_ref->{$key};
		    my $m_key2 = $map_ref->{$key2};
		    print "=======================>>>>m_key1:$m_key1  key: $key \tm_key2: $m_key2\n";
		    my $temp_field = 'set_'.uc($m_key2);
		    if(($m_key1 ne '') && ($m_key2 ne '')&&($data_ref2->{$key1}->{$key}->{$key2} ne '')){
			$invoice_obj->$temp_field($data_ref2->{$key1}->{$key}->{$key2});
		    }
		}
	    }
	}	
    }else{ 

    }
}



## collect site_id, and  vendorname from the file.
my $invoice_site_id;
my $vendor_name;
foreach my $key(keys %$site_id_ref){
    if(($site_id_ref->{$key}->{'AddressTypeCode'} eq 'ST')||($site_id_ref->{$key}->{'AddressTypeCode'} eq 'S'))  {
	$invoice_site_id = $site_id_ref->{$key}->{'AddressLocationNumber'};
    }elsif(($site_id_ref->{$key}->{'AddressTypeCode'} eq 'RI')||($site_id_ref->{$key}->{'AddressTypeCode'} eq 'R')){
	$vendor_name =  $site_id_ref->{$key}->{'AddressName'}
    }
}

$invoice_obj->set_SITE_ID($invoice_site_id);
$invoice_obj->set_VENDOR_NAME($vendor_name);


## set orig_merchandise_amount as orig_invoice_amount (TotalAmount) if it is absent
my $orig_merch_amt = $invoice_obj->get_ORIG_MERCHANDISE_AMOUNT();
if($orig_merch_amt eq ''){
    my $orig_invoice_amt = $invoice_obj->get_ORIG_INVOICE_AMOUNT();
    $invoice_obj->set_ORIG_MERCHANDISE_AMOUNT($orig_invoice_amt);
}


## set charge_xxx_amout, charge_xxx_description default value as zero, or null
my @charge = qw(
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
);

foreach my $ch_e(@charge){
    my $get_name = "get_".$ch_e;
    my $set_name =  "set_".$ch_e;
    my $value = $invoice_obj->$get_name();
    if($value eq ''){
	if($ch_e =~ /desc/ig){
	    $invoice_obj->$set_name('');
	}
	
        if($ch_e =~ /amount/ig){
	    $invoice_obj->$set_name(0);
	}
    }
}

##======
print Dumper($invoice_obj);

die;
die;


push(@$invoice_ary, $invoice_obj);
print Dumper($invoice_obj);

##########  VALIDATOR   ##########

##print Dumper($invoice_obj) if($debug);
### ====> object loading is done, next week, 
## 1, re-process all the config files
## 2, mapping file confirmation
## 3, validation of required data for each table

my $v_tree_ref = $invoice_obj->build_validation_tree();
print Dumper($v_tree_ref) if ($debug);





## check validity of the data
my $lineitems_ref  = $invoice_obj->get_EDI_INVOICE_DETAILS();
my $address_ref    = $invoice_obj->get_EDI_ADDRESS();
my $invoice_ref    = $invoice_obj->get_EDI_INVOICES();

my ($ctr1, $li_valid_ret)      =  
    $edi_parser->EDI_TWM::EDI_PARSER::validate_array_data_with_standard($lineitems_ref, $v_tree_ref->{'EDI_INVOICE_DETAILS'});
my ($ctr2, $address_valid_ret) =  
    $edi_parser->EDI_TWM::EDI_PARSER::validate_array_data_with_standard($address_ref,   $v_tree_ref->{'EDI_ADDRESS'});
my ($ctr3, $invoice_valid_ret) =  
    $edi_parser->EDI_TWM::EDI_PARSER::validate_level2_hash_with_standard($invoice_ref,  $v_tree_ref->{'EDI_INVOICES'});

if($debug){
    print "validate result:\n";
    print Dumper($li_valid_ret);
    print Dumper($$address_valid_ret);
    print Dumper($invoice_valid_ret);
    
    $invoice_obj->{'log_obj'}->log_info("Validate Result:\n");
    $invoice_obj->{'log_obj'}->log_info("line item errors: $ctr1, list:\n $li_valid_ret");
    $invoice_obj->{'log_obj'}->log_info("address errors:   $ctr2, list:\n $address_valid_ret");
    $invoice_obj->{'log_obj'}->log_info("invoice errors:   $ctr3, list:\n $invoice_valid_ret");
### need function to add missing values identified by the validation process
}

## if validation passed, start to write query to load data:
if(($ctr1 ==0)&&($ctr2==0)&&($ctr3==0)){
    ##print "validate passed";
  ##  $invoice_obj->{'log_obj'}->log_info->("validation passed.");
}else{
##    $invoice_obj->{'log_obj'}->log_info->("validation failed.");
    print "validation failed\n";
}


if($debug){
    print Dumper($map_ref);
    print Dumper($v_tree_ref);
}
=cut


######  LOADER   #######
## Loading object from the object array:

foreach my $invoice_obj(@$invoice_ary){
    ## insert a invoice entry, get new invoice id:
    $invoice_obj->set_INVOICE_ENTRY_TYPE('I');
    ##$invoice_obj->set_INVOICE_NUMBER_VERSION('1');
    $invoice_obj->load_invoice_obj_values_to_ridb();    
}

$invoice_obj->{'dbh'}->disconnect();
