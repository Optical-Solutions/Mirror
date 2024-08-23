package EDI_TWM::Invoice;
##use strict;
use base ('Class::Accessor');
use IBIS::Config::Auto;
use Net::SFTP;
use IBIS::Crypt;
use IBIS::DBI;
use IBIS::EDI_Utils;
use Data::Dumper;

use constant VALID_ADDRESS_TYPES => qw(ST RI BI);

our $debug;
use vars '$AUTOLOAD';

my  $base_dir = '/usr/local/mccs/lib/perl5/IBIS/EDI_TWM';

#Name:          new()
#Input Para:    a config file contains a list of fieldnames for a table of irdb.
#Output Para:   invoice object
#Purpose:       constructor of the Invoice object
sub new {
    my ( $class, %args ) = @_;
    my $self = {};
    bless $self, $class;    

    if ( $args{conf_file} ) {
        $self->_make_accessors( \%args );
    }else{
        ## set a default config in the object:
	$args{conf_file} = "/usr/local/mccs/lib/perl5/IBIS/EDI_TWM/Config/edi_invoices.config";
=header
	if($args{conf_type} eq 'edi_invoice') {
	    $args{conf_file} = "$base_dir/Config/edi_invoices.config";
	}elsif($args{conf_type} eq 'line_item'){
	    $args{conf_file}= "$base_dir/Config/edi_invoice_details.config";
	}elsif($args{conf_type} eq 'address') {
	    $args{conf_file}= "$base_dir/Config/edi_address.config";
	}else{
	    print "else case: $args{conf_type}\n";
	    $args{conf_file} = "$base_dir/Config/edi_invoices.config";
	}
=cut	

	$self->_make_accessors( \%args );
    }
    return $self;
}

#Name:        _make_accessors()
#Input Para:  config file
#Output Para: none
#Purpose:     to create a hash with a list of keys with null values
sub _make_accessors {
    my ( $self, $args ) = @_;
    my $config = $self->_parse_config($args->{conf_file}, '=');
    foreach  my $key (keys %{$config}) {
	$self->{$key} = $config->{$key};
    }
}



sub _parse_config{
    my ($self, $config,  $delim) = @_;
    my $config_ref;
    open(IN, $config) || die "can not open config file:$config";
    my @ary = <IN>;
    foreach my $line(@ary){
	if($line =~ /\w+/g){
            chomp($line);
	    my @pair = split(/$delim/, $line);
	    $pair[0] =~ s/\s+//g;
	   $config_ref->{$pair[0]} = $pair[1]; 
	}
    }
    close IN;
    return $config_ref;
}

sub _db_connect{
    my ($self) = @_;
    my $dbh =  IBIS::DBI->connect(
	dbname  => 'irdb',
	attribs => {AutoCommit => 0}
	) or die("Cannot connect database using: 'irdb'.\n"); 
    $self->{'dbh'} = $dbh;
    return $self->{'dbh'};
}



sub AUTOLOAD{
    no strict "refs";
    my ($self, $newval) = @_;
 ###   print "autoload: $AUTOLOAD\n";
    if($AUTOLOAD  =~ /.*::get_(\w+)/){
	my $attr_name = $1;
	*{$AUTOLOAD} = sub { return $_[0]->{$attr_name}};
	return $self->{$attr_name};
    }else{

    }
    if($AUTOLOAD  =~ /.*::set_(\w+)/){
        my $attr_name = $1;
        *{$AUTOLOAD} = sub {$_[0]->{$attr_name} = $_[1]; return};
	$self->{$1} = $newval;
	return $newval;
    }
}


# create db query from an array

sub _get_current_invoice_id{
    my ($self) = @_;
    my $query = "select edi_utils.get_last_invoice_id() FROM dual";
    my $ret  =  $self->{dbh}->selectall_arrayref($query);
    return $ret->[0][0];
}

sub _get_current_line_item_id{
    my($self) = @_;
    my $query2 = "select edi_utils.get_last_invoice_item_id() from dual";
    my $ret2 =  $self->{'dbh'}->selectall_arrayref($query2);
    return $ret2->[0][0];
}



sub _parse_invoice_xml{
    my ($self, $xmlfile) = @_;
    my $edi_parser = EDI_TWM::EDI_PARSER::new();
##  it is import to set 0 here for parsing to hash, and not force array in the output                                                                           
    my $data_ref = $edi_parser->EDI_TWM::EDI_PARSER::parse_xml_to_ref($xmlfile, 0);
    return $data_ref;
}


sub _get_sps_mccs_invoice_mapping_ref{
    my ($self, $map_file) = @_;
    my $map_ref;
    if($map_file eq ''){
        $map_file = "$base_dir"."/Config/data_mapping_file.txt";
    }
    open(IN, $map_file) || die "can not open file to write: $map_file\n";
    while(my $line = <IN>){
        chomp($line);
        my @ary = split(/\|\|/, $line);
        $ary[0] =~ s/\s+//g;
        $ary[1] =~ s/\s+//g;
        if(($ary[0] ne '')&&($ary[1] ne '')){
            $map_ref->{$ary[0]} = $ary[1];
        }
    }
    close IN;
    return $map_ref;   
}


sub _invoice_obj_loader{
    my ($self, $data_ref2, $map_ref) = @_;

    my $data_ref3;

    if($map_ref eq undef){
	$map_ref = $self->_get_sps_mccs_invoice_mapping_ref();
    }
    if($data_ref2 eq undef || $map_ref eq undef){
	$self->{'log_obj'}->log_info("missing data reference, or sps_mccs invoice_data_mapping file");
	exit();
    }

    my $marker1 = "LineItems";
    my $marker2 = "Header";
    my $marker3 = "Address";

    my $site_id_ref; ## reference needed to collect some information from diversed places in the xml file.
    foreach  my $key1 (keys %$data_ref2){
	if($key1 eq 'LineItems') {
	    if($debug){print "key1: $key1\n";}
	    my $lt_ary =[];
            if(ref($data_ref2->{$key1}->{LineItem}) eq 'ARRAY'){
		for (my $i=0; $i<@{$data_ref2->{$key1}->{LineItem}}; $i++){
		    my $line_obj = EDI_TWM::LINE_ITEM::LineItem->new();
		    foreach my $key2 (keys %{$data_ref2->{$key1}->{LineItem}->[$i]->{InvoiceLine}}){
			my $m_key1 = $map_ref->{$key1};
			my $m_key2 = $map_ref->{$key2};
			
			if(($m_key1 ne '') &&($m_key2 ne '')){ 
			    my $temp_field = 'set_'.uc($m_key2);
			    $line_obj->$temp_field($data_ref2->{$key1}->{LineItem}->[$i]->{InvoiceLine}->{$key2});

			    if($data_ref2->{$key1}->{LineItem}->[$i]->{InvoiceLine}->{$key2} ne ''){

				$data_ref3->{$key1}->{LineItem}->[$i]->{InvoiceLine}->{$key2}->{'value'} = $data_ref2->{$key1}->{LineItem}->[$i]->{InvoiceLine}->{$key2};
				$data_ref3->{$key1}->{LineItem}->[$i]->{InvoiceLine}->{$key2}->{'value_used'} = 'true';
			    }

			}
		    }
		    push(@$lt_ary, $line_obj);
		}
		
	    }else{
		my $line_obj = EDI_TWM::LINE_ITEM::LineItem->new();
		foreach my $key2 (keys %{$data_ref2->{$key1}->{LineItem}->{InvoiceLine}}){
		    my $m_key1 = $map_ref->{$key1};
		    my $m_key2 = $map_ref->{$key2};
		    if(($m_key1 ne '') &&($m_key2 ne '')){
			my $temp_field = 'set_'.uc($m_key2);
			$line_obj->$temp_field($data_ref2->{$key1}->{LineItem}->{InvoiceLine}->{$key2});

			if($data_ref2->{$key1}->{LineItem}->{InvoiceLine}->{$key2} ne ''){
			    $data_ref3->{$key1}->{LineItem}->{InvoiceLine}->{$key2}->{'value'} = $data_ref2->{$key1}->{LineItem}->{InvoiceLine}->{$key2};
			    $data_ref3->{$key1}->{LineItem}->{InvoiceLine}->{$key2}->{'value_used'} = 'true';
			}


		    }   
		}
		push(@$lt_ary, $line_obj);
	    }
	    $self->set_EDI_INVOICE_DETAILS($lt_ary);
	    
	}elsif(($key1 eq 'Header')||($key1 eq 'Summary')){
	    my $add_ref = [];
	    foreach my $key (keys %{$data_ref2->{$key1}}){
		if($key eq 'Address'){
## if not array, log error,
		    if((ref($data_ref2->{$key1}->{Address}) ne 'ARRAY')|| (@{$data_ref2->{$key1}->{Address}} < 2)){
			my $d_tp = ref(\$data_ref2->{$key1}->{Address});
                        my $ad_msg = "\n\tAddress is not array reference: $d_ty. \n\tOr Address array has fewer than 2 addresses. \n\tInvoice file wrong.\n";
			$self->{'log_obj'}->log_info($ad_msg);	
                        die($ad_msg);		
		    }else{
			my $cur_addr_type = ''; 
			my %address_type_checker;
# 			my $st_type_ctr   = 0;
# 			my $ri_type_ctr   = 0;
# 			my $bi_type_ctr   = 0;
			for (my $i=0; $i<@{$data_ref2->{$key1}->{Address}}; $i++){
			    my $address_obj = EDI_TWM::ADDRESS::Address->new();
			    foreach my $key2 (keys %{$data_ref2->{$key1}->{Address}->[$i]}){
			      	
				if($key2 eq 'AddressLocationNumber'){$site_id_ref->{$i}->{$key2} = $data_ref2->{$key1}->{Address}->[$i]->{$key2};}
				if($key2 eq 'AddressTypeCode'){      
				    $site_id_ref->{$i}->{$key2} = $data_ref2->{$key1}->{Address}->[$i]->{$key2};
				    $cur_addr_type = $data_ref2->{$key1}->{Address}->[$i]->{$key2};
				   if( grep{ $cur_addr_type eq $_ } VALID_ADDRESS_TYPES ){
#				    if($cur_addr_type eq 'ST'){
#					$st_type_ctr++;
# 				    }elsif($cur_addr_type eq 'RI'){
# 					$ri_type_ctr++;
# 				    }elsif($cur_addr_type eq 'BI'){
# 					$bi_type_ctr++; 
				    }else{
					$self->{'log_obj'}->log_info("unknown address type: $cur_addr_type.");
				    }
				}
				if($key2 eq 'AddressName')    {      $site_id_ref->{$i}->{$key2} = $data_ref2->{$key1}->{Address}->[$i]->{$key2};}
				
				my $m_key1 = $map_ref->{'Address'}; ## mapped key
				my $m_key2 = $map_ref->{$key2};
				if(($m_key1 ne '') &&($m_key2 ne '')&&($data_ref2->{$key1}->{Address}->[$i]->{$key2} ne '')){
				    my $temp_field = 'set_'.uc($m_key2);
				    if($key2 eq 'AddressTypeCode'){
					my $re_mapped_value = $map_ref->{$data_ref2->{$key1}->{Address}->[$i]->{$key2}};
					$address_obj->$temp_field($re_mapped_value);
					
					    if($data_ref2->{$key1}->{Address}->[$i]->{$key2} ne ''){
						$data_ref3->{$key1}->{Address}->[$i]->{$key2}->{'value'} = $data_ref2->{$key1}->{Address}->[$i]->{$key2};
						$data_ref3->{$key1}->{Address}->[$i]->{$key2}->{'value_used'} = 'true';
					    }
					
				    }else{
					
					## remove space from postalcode data:
					if($key2 =~ /postal/i){$data_ref2->{$key1}->{Address}->[$i]->{$key2} =~ s/\s+//g;}
					
					$address_obj->$temp_field($data_ref2->{$key1}->{Address}->[$i]->{$key2});                                  
					if($data_ref2->{$key1}->{Address}->[$i]->{$key2} ne ''){
					    $data_ref3->{$key1}->{Address}->[$i]->{$key2}->{'value'} = $data_ref2->{$key1}->{Address}->[$i]->{$key2};
					    $data_ref3->{$key1}->{Address}->[$i]->{$key2}->{'value_used'} = 'true';
					}
					
				    }
				}
			    }

# 			    if($cur_addr_type eq 'ST'){
# 				if($st_type_ctr < 2){
# 				    push(@$add_ref, $address_obj);
# 				}
# 			    }
# 			    
# 			    if($cur_addr_type eq 'RI'){
# 				if($ri_type_ctr < 2){
# 				    push(@$add_ref, $address_obj);
# 				}
# 			    }
#   
# 			    if($cur_addr_type eq 'BI'){
# 				if($bi_type_ctr < 2){
# 				    push(@$add_ref, $address_obj);
# 				}
# 			    }
				##
				##Only one of every type of address type in the address list (it will be the first one we find)
				##
			    if( ! $address_type_checker{$cur_addr_type} ){
				push(@$add_ref, $address_obj);
				$address_type_checker{$cur_addr_type}=1;
			    }
			}	#end for loop
		    }	#end validation check on number of addresses
		    $self->set_EDI_ADDRESS($add_ref);
		}elsif(($key eq 'InvoiceHeader')||($key eq 'Date')||($key eq 'PaymentTerms')||($key eq 'Totals')){
		    foreach my $key2 (keys %{$data_ref2->{$key1}->{$key}}){
			my $m_key1 = $map_ref->{$key};
			my $m_key2 = $map_ref->{$key2};
			my $temp_field = 'set_'.uc($m_key2);
			if(($m_key1 ne '') && ($m_key2 ne '')){
			    $self->$temp_field($data_ref2->{$key1}->{$key}->{$key2});
			    
			    if($data_ref2->{$key1}->{$key}->{$key2} ne ''){
				$data_ref3->{$key1}->{$key}->{$key2}->{'value'} = $data_ref2->{$key1}->{$key}->{$key2};
				$data_ref3->{$key1}->{$key}->{$key2}->{'value_used'} = 'true';
			    }
			    
			}
		    }
		}elsif($key =~ /chargesallow/ig){ ### this is the code to remap sps's charge allowance to IRDB's chargeallowance fields:
		    ## 1, loop  through the key's, regenerat a structure for this part
		 
=head ## yuc aug 31,09 taken out this part as required by Eric, Martin

		    my $transferred_ref = $self->sps_allowance_transfer_to_irdb($data_ref2->{$key1}->{$key});
		    $data_ref2->{$key1}->{$key} = $transferred_ref;
		    
		    ## 2,  mapping back the transferred structure to irdb field names 
		    foreach my $key2 (keys %{$data_ref2->{$key1}->{$key}}){
			my $m_key1 = $map_ref->{$key};
			my $m_key2 = $map_ref->{$key2};
			my $temp_field = 'set_'.uc($m_key2);
			if(($m_key1 ne '') && ($m_key2 ne '')){
			    $self->$temp_field($data_ref2->{$key1}->{$key}->{$key2});  ## this is the function to set values
			    if($data_ref2->{$key1}->{$key}->{$key2} ne ''){
				$data_ref3->{$key1}->{$key}->{$key2}->{'value'} = $data_ref2->{$key1}->{$key}->{$key2};
				$data_ref3->{$key1}->{$key}->{$key2}->{'value_used'} = 'true';
			    }
			}
			
		    }

=cut

		}
	    }
	}else{
	    my $error_msg ="Keys are something other than, LineItems, header or Summary: what is $key1 ??\n";
	    $self->{'log_obj'}->log_info($error_msg);
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
    
    $self->set_SITE_ID($invoice_site_id);
    $self->set_VENDOR_NAME($vendor_name);
    
    
    ## set orig_merchandise_amount as orig_invoice_amount (TotalAmount) if it is absent
    my $orig_merch_amt = $self->get_ORIG_MERCHANDISE_AMOUNT();
    if($orig_merch_amt eq ''){
	my $orig_invoice_amt = $self->get_ORIG_INVOICE_AMOUNT();
	$self->set_ORIG_MERCHANDISE_AMOUNT($orig_invoice_amt);
    }
    
    ## contral payment_terms to maximum 60 characters
    my $payment_terms = $self->get_PAYMENT_TERMS();
    if(length($payment_terms) > 60){
        $payment_terms  =~ s/\s+/ /g; ## get rid of extra spaces, and use only the first 60, per Eric's request May 20, 09
        $self->set_PAYMENT_TERMS(substr($payment_terms, 0, 60));
    }
    return $self;
}

sub sps_allowance_transfer_to_irdb{
    my ($self, $allowance_ref) = @_;
    my $transferred_ref;
    
    if (ref ($allowance_ref) eq 'HASH'){
	$transferred_ref = $self->transfer_sps_allowance_hash($allowance_ref);
    }
    
    if( ref ($allowance_ref) eq "ARRAY"){
	my $total_other_charges = 0;
        my $total_desc ='';
        my $o_ctr = 0;
	
	foreach my $sgl_alw_ref(@$allowance_ref){
	    my $single_transferred_ref = $self->transfer_sps_allowance_hash($sgl_alw_ref);
            if($single_transferred_ref->{ChargeOtherAmt}){
		$o_ctr++;
		$total_other_charges = scalar($single_transferred_ref->{ChargeOtherAmt}) + $total_other_charges;
		$total_desc .= $single_transferred_ref->{'ChargeOtherDesc'}.';';
	    }
	    if(defined ($transferred_ref)&& defined($single_transferred_ref)){
		%$transferred_ref = (%$transferred_ref, %$single_transferred_ref);
	    }else{
		%$transferred_ref = %$single_transferred_ref;
	    }
	}
	
	if($o_ctr > 0){
	    $transferred_ref->{ChargeOtherAmt} = $total_other_charges;
	    if(length($total_desc) > 500){ ## this likely will never be true. just to adapt the db field size limit.
		$total_desc = substr($total_desc, 0, 500);
	    }
            $transferred_ref->{ChargeOtherDesc} = $total_desc;
	}
    }
    return $transferred_ref;
}


sub transfer_sps_allowance_hash{
    my ($self, $allowance_hash_ref) = @_;
    my $transferred_ref;

    my $allow_map_ref = {
        'C000'=>'ChargeDefect',
        'C310'=>'ChargeDiscount',
        'D240'=>'ChargeFreight',
        'G510'=>'ChargeKeg',
        'OTHER'=>'ChargeOther'
        };

    ## from allowchangecode, get types of charges, change into irdb type tag
    my $AllowChrgCode = $allowance_hash_ref->{AllowChrgCode};
    unless($AllowChrgCode){return undef;}
    
    my ( $charge_allowance_type, $charge_allow_amt, $charge_allow_desc);
    $charge_allow_amt    = $allowance_hash_ref->{AllowChrgAmt};
    $charge_allow_desc   = $allowance_hash_ref->{AllowChrgHandlingDescription};
    
    my $charge_allow_type;       
    if(($AllowChrgCode)&&($allow_map_ref->{$AllowChrgCode} eq '')){
	$AllowChrgCode = 'OTHER';
    }
    
    $charge_allow_type = $allow_map_ref->{$AllowChrgCode};
    my $key1 = $charge_allow_type."Amt";
    my $key2 = $charge_allow_type."Desc";
    $transferred_ref->{$key1} = $charge_allow_amt;
    $transferred_ref->{$key2} = $charge_allow_desc;
    return $transferred_ref;
}


sub marker_used_data_field{
    my ($self, $reference) = @_;
    $reference->{'VALUE_USED'} = 'TRUE';
}


sub update_fields{
    my ($self, $fields) = @_;

##1, get xml file values of
##  vendor_duns, invoice_number, and invoice_date

    my ($vendor_duns, $invoice_number, $invoice_date);
    $vendor_duns    = $self->get_VENDOR_DUNS();  
    $invoice_number = $self->get_INVOICE_NUMBER();
    $invoice_date   = $self->get_INVOICE_DATE();

    ## print Dumper( $vendor_duns, $invoice_number, $invoice_date );
    $vendor_duns = IBIS::EDI_Utils::get_prefixed_string(11, $vendor_duns, '0');

##2, get invoice_id from these values in the database

    my $invoice_id = $self->get_invoice_id_by_vDuns_invoNum($vendor_duns, $invoice_number);

##    print Dumper($invoice_id) ;
    
##3, update fields values from the invoice_id
##    return $self;
    
    if($invoice_id){

## get value for each field from object,
## if valid value, update the field with the value	
	
	my $update_qry = "update edi_invoices set ";
        my $field_value_str = '';
	foreach my $field(@$fields){
            $field =~ s/\s+$//;
            $field =~ s/^\s+//g;
	    my $func_name = "get_".uc($field);
	    my $value = $self->$func_name;
	    if($value){
		if($field eq 'ORIG_MERCHANDISE_AMOUNT'){
		    $field_value_str .= "$field "."="."\'$value\'".",";
		    $field_value_str .= "INVOICE_NET_SALES "."="."\'\'".",";
		}else{		    
		    $field_value_str .= "$field "."="."\'$value\'".",";
		}
	    }
	}
	
        if($field_value_str =~ /\d+|\w+/g){
            $field_value_str =~ s/\,$//g;
	    $update_qry .= $field_value_str;
	}        
	
        $update_qry .= "  where invoice_id = \'$invoice_id\'";
  
	###print "$update_qry\n";

	eval{
	    $self->{'dbh'}->do($update_qry);
	};
        
	if($@){
	    warn($@);
	}else{
	    $self->{'log_obj'}->log_info("update success: $update_qry\n");
	}

    }else{
	$self->{'log_obj'}->log_info(" Failed to get corresponding po_id from vendor_duns: $vendor_duns and invoice_number: $invoice_number");
    }    
}


sub get_invoice_id_by_vDuns_invoNum{
    my ($self, $vduns, $invo_numb, $invo_date) = @_;

    my $qry = "select invoice_id from edi_invoices where vendor_duns = \'$vduns\' and invoice_number = \'$invo_numb\'";
    if($invo_date){
	$qry .= "  and  invoice_date = \'$invo_date\'";
    }

    my $ret2 =  $self->{'dbh'}->selectall_arrayref($qry);
    return $ret2->[0][0];
}

sub load2{
    my ($self) = @_;
    my $error_buffer ='';

    $error_buffer .= $self->_insert_a_table('EDI_INVOICES');

    if(length($error_buffer) == 0){ ## if no error from the first step, then do the insert of the address and line items etc...
      ## use the new invoice_id to insert address, and invoice_details:
	my $invoice_id = $self->_get_current_invoice_id(); ## this will get the invoice_id that just inserted!!!
	my $address_ref    = $self->get_EDI_ADDRESS();
	my $lineitems_ref  = $self->get_EDI_INVOICE_DETAILS();
	
	$self->{'log_obj'}->log_info("invoice_id: $invoice_id\n");                                                                                                               
	foreach my $address_obj(@$address_ref){
	    $address_obj->set_INVOICE_ID($invoice_id);
	    $address_obj->{'dbh'} = $self->{'dbh'};
	    $error_buffer .= $address_obj->_insert_a_table('EDI_ADDRESS');
	}    
	
	foreach my $invoice_d_obj(@$lineitems_ref){
	    $invoice_d_obj->set_INVOICE_ID($invoice_id);
	    my $item_id = $self->get_current_line_item_id();
	    $invoice_d_obj->set_ITEM_ID($item_id);
	    $invoice_d_obj->{'dbh'} = $self->{'dbh'};
	    $error_buffer .= $invoice_d_obj->_insert_a_table('EDI_INVOICE_DETAILS');
	}
##
## Martin Lourduswamy
## Load DATA from XML into DB for 'ChargesAllowances' using PROC discount_allowance_entry
##
        my $ChargesAllowances = $self->{'parsed_xml'}->{Header}->{ChargesAllowances};
   	$ChargesAllowances = [ $ChargesAllowances ] if( ref($ChargesAllowances) eq "HASH" );
 
	foreach my $ChargesAllowance ( @{ $ChargesAllowances } ) {
            $self->{'log_obj'}->log_info( "INSERT into edi_invoice_charge_allowance for invoice_id: $invoice_id" ) 
	        if( $invoice_id );
	    $error_buffer .= $self->_insert_ChargesAllowances( $ChargesAllowance, $invoice_id );
        }
        
    }

    return $error_buffer;
}

sub _insert_ChargesAllowances { 
    my ( $self, $ChargesAllowance, $invoice_id ) = @_;

    my $ChargeIndicator = $ChargesAllowance->{AllowChrgIndicator};
    my $ChargeCode = $self->{'dbh'}->quote( $ChargesAllowance->{AllowChrgCode} );

## For Charges, just take the absolute value.
## For Allowance FORCE the data to be negative amount
    my $ChargeAmount = abs( $ChargesAllowance->{AllowChrgAmt} );
    $ChargeAmount =~ s/\s+//g;
    $ChargeAmount = 0 if( $ChargeAmount eq "" );

    $ChargeAmount = -1 * $ChargeAmount if(  uc( $ChargeIndicator ) eq 'A' );

    my $ChargeDescription = substr( $ChargesAllowance->{AllowChrgHandlingDescription}, 0, 60 );
    $ChargeDescription = $self->{'dbh'}->quote( $ChargeDescription );

    unless( $invoice_id && $ChargeCode ) {
        my $error = 'Missing Data for ChargesAllowances';
	return $error;
    }


    eval{
        $self->{'dbh'}->do( qq{BEGIN edi_utils.discount_allowance_entry( p_invoice_id=>$invoice_id, p_external_id=>$ChargeCode, p_amount=>$ChargeAmount, p_description=>$ChargeDescription ); END;} );
    };

    $self->{'log_obj'}->log_warn( $@ ) if( $@ );

return $@;
}

sub _insert_a_table{
    my ($self, $table_name) = @_;
    $table_name =~ s/\s+//g;
    $table_name = uc($table_name);
    my @field_list;    
    if($table_name eq 'EDI_INVOICES'){
	@field_list = qw(
INVOICE_ID
INVOICE_DATE
INVOICE_DUE_DATE
INVOICE_DISCOUNT_DUE_DATE
INVOICE_NUMBER
INVOICE_NUMBER_VERSION
INVOICE_ENTRY_TYPE
SITE_ID
PO_NUMBER
PAYMENT_TERMS
VENDOR_DUNS
VENDOR_NAME
ORDER_TICKET_NUMBER
SHIP_DATE
REQUESTED_DELIVERY_DATE
ROUTING
SHIP_CARRIER
CURRENCY_ID
ORIG_INVOICE_AMOUNT
ORIG_MERCHANDISE_AMOUNT
NUM_LINE_ITEMS
ORIG_TOTAL_QUANTITY_ORDERED
ORIG_TOTAL_QUANTITY_SHIPPED
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
CREATE_DATE
UPDATE_DATE
PAYMENT_DISCOUNT
INVOICE_AMOUNT_BY_TERMS
INVOICE_NET_SALES
INVOICE_TOTAL_DISCOUNT
INVOICE_TOTAL_UNIT
FOBPAYCODE
VENDOR_PO_DATE
TERMSBASISDATECODE
TERMS_TYPE
SOURCE_FILE
INVOICE_TYPE_CODE
);
    }elsif($table_name  eq "EDI_INVOICE_DETAILS"){
	
	@field_list = qw(
INVOICE_ID
ITEM_ID
ITEM_ORDER
BAR_CODE_ID
PRODUCT_QUALIFIER_UP
PRODUCT_QUALIFIER_VA
PRODUCT_QUALIFIER_VE
PRODUCT_QUALIFIER_SM
VENDOR_STYLE_NO
VENDOR_STYLE_COLOR
VENDOR_STYLE_SIZE
ITEM_DESCRIPTION
ITEM_UNIT
UNIT_COST
ITEM_COST
QUANTITY_ORDERED
QUANTITY_SHIPPED
);
    }elsif($table_name = "EDI_ADDRESS"){
	@field_list = qw(
INVOICE_ID
ADDRESS_TYPE_ID
ENTITY_ID
ADDRESS_LINE1
ADDRESS_LINE2
ADDRESS_LINE3
ADDRESS_LINE4
CITY
STATE
POSTAL_CODE
COUNTRY
);
    }

## form a query from the array of fieldname and values
    my $q_a = " INSERT INTO ".$table_name." (";
    my $q_b ='';
    
    my @values;
    
    foreach my $fieldname(@field_list){
	$fieldname =~ s/\s+//g; ## remove white spaces
	$q_a .= "$fieldname".",\n";
	$q_b .= " ?,";
	my $funct_name = "get_".uc($fieldname);
	my $value = $self->$funct_name();
        push(@values, $value);
	if($value eq ''){
	    ## print "missing value for field: $fieldname\n";
	}
    }
                                                                                                   
    $q_a =~ s/\,$//g;
    $q_a .= ")  VALUES (";

    $q_b =~ s/\,$//g;
    $q_b .= ")";

    my $q_c = $q_a.$q_b;
     ##print "q_c: $q_c\n";
     ##print Dumper(\@values);
    my $sth = $self->{'dbh'}->prepare($q_c);
    eval{
     $sth->execute(@values);
     };
    if($@){
        ##warn($@);
    }
    return "$@";
}


sub query_generator{
    my ($self, $fields, $table_name) = @_;

    my $q_a = " INSERT INTO ".$table_name." (";
    my $q_b ='';
    my @values;

    foreach my $fieldname(@$fields){
        $fieldname =~ s/\s+//g; ## remove white spaces                                                                                   
        $q_a .= "$fieldname".",\n";
        $q_b .= " ?,";
        my $funct_name = "get_".uc($fieldname);
        my $value = $self->$funct_name();
        push(@values, $value);
    }

    $q_a =~ s/\,$//g;
    $q_a .= ")  VALUES (";

    $q_b =~ s/\,$//g;
    $q_b .= ")";
## put together the two parts:                                                                      \                                    
    my $q_c = $q_a.$q_b;
##    print "q_c: $q_c\n";
##    print Dumper(\@values);

##    my $sth = $self->{'dbh'}->prepare($q_c);
##    $sth->execute(@values);                                                                                                           
##    return $self;
    return ($q_c, \@values);
}



sub load_obj_from_db{
    my ($self, $invoice_id) = @_;

    my $query = "select * from edi_invoices where invoice_id = $invoice_id";
    my $invo_ref = $self->{dbh}->selectall_hashref($query, 'invoice_id');

    my $query2 = "select * from edi_invoice_details where invoice_id = $invoice_id";
    my $lineitem_ref =  $self->{dbh}->selectall_hashref($query2, 'item_order');

    my $query3 = "select * from edi_address where invoice_id = $invoice_id";
    my $address_ref =  $self->{dbh}->selectall_hashref($query3,'address_type_id');
    
    if((keys %$invo_ref) == 0){
	print "warning: wrong invoice_id used. No such a invoice_id in the database.\n";
	return undef;
    }
    
    foreach my $invoice_id (keys %$invo_ref){
	foreach my $key (keys %{$invo_ref->{$invoice_id}}){
	    ##print "key: $key\n";
	    my $func_name = "set_".uc($key);
            $self->$func_name($invo_ref->{$invoice_id}->{$key});	    
	}
    }


    my $addr_ary_ref = [];
    my $ltms_ary_ref = [];

    foreach my $item_order_id(%$lineitem_ref){
	my $lineitem_obj = EDI_TWM::LINE_ITEM::LineItem->new();
	foreach my $key (keys %{$lineitem_ref->{$item_order_id}}){
	    my $func_name = "set_".uc($key);
            $lineitem_obj->$func_name($lineitem_ref->{$item_order_id}->{$key});
	}
	if($lineitem_obj->{'INVOICE_ID'} ne ''){
	    push(@$ltms_ary_ref, $lineitem_obj);
	}
    }


    foreach my $address_type_id(%$address_ref){
        my $address_obj = EDI_TWM::ADDRESS::Address->new();
        foreach my $key (keys %{$address_ref->{$address_type_id}}){
            my $func_name = "set_".uc($key);
            $address_obj->$func_name($address_ref->{$address_type_id}->{$key});
        }
	if($address_obj->{'INVOICE_ID'} ne ''){
	    push(@$addr_ary_ref, $address_obj);
	}
    }

   $self->set_EDI_ADDRESS($addr_ary_ref);
   $self->set_EDI_INVOICE_DETAILS($ltms_ary_ref);

    return $self;
}

sub set_charge_amount_description_default{
    my ($self) = @_;
    my @charge = qw(
CHARGE_FREIGHT_DESCRIPTION  CHARGE_FREIGHT_AMOUNT  CHARGE_DISCOUNT_DESCRIPTION  CHARGE_DISCOUNT_AMOUNT  CHARGE_KEG_DESCRIPTION  CHARGE_KEG_AMOUNT  CHARGE_DEFECT_DESCRIPTION  CHARGE_DEFECT_AMOUNT  CHARGE_OTHER_DESCRIPTION  CHARGE_OTHER_AMOUNT);
    foreach my $ch_e(@charge){
	my $get_name = "get_".$ch_e;
	my $set_name =  "set_".$ch_e;
	my $value = $self->$get_name();
	if($value eq ''){
	    if($ch_e =~ /desc/ig){
		$self->$set_name('');
	    }
	    
	    if($ch_e =~ /amount/ig){
		$self->$set_name(0);
	    }
	}
    }
    return $self;
}


sub _query_creater_from_array_data{
    my ($self, $invoice_id, $data_ref, $v_tree_ref, $table_name) = @_;
    $table_name = uc($table_name);

    for(my $i=0; $i<@{$data_ref}; $i++){
        $data_ref->[$i]->set_INVOICE_ID($invoice_id);

        if($table_name eq 'EDI_INVOICE_DETAILS'){
            my $item_id = $self->_get_current_line_item_id();
            $data_ref->[$i]->set_ITEM_ID($item_id);
        }

## get start of the two parts of a query                                                                                                                        
## formation of the first part:                                                                                                                                 
        my $q_a = " INSERT INTO ".$table_name." (";
## formation of the second part:                                                                                                                                
        my $q_b ='';
        my @fields;
        my @values;

#step2: extend both part and value                                                                                                                             
##loop the standard, check value in data, write:                                                                                                                
        foreach my $key(keys %{$v_tree_ref->{$table_name}}){

            ### print "key: $key\n";                                                                                                                            
            if($key ne ''){
                my $field_func = "get_".$key;
                my $value = $data_ref->[$i]->$field_func();
                ##print Dumper($data_ref->[$i]);                                                                                                                
                my $ind_pos = $v_tree_ref->{$table_name}->{$key}->{'order'};
                if($ind_pos eq ''){
                    print "TTTTTTTTTTTTTTTTTTTTTTrouble!!!!!!!!!!!!!: key: $key  value: $value\n";
                }
                $fields[$ind_pos] = $key;
                $values[$ind_pos] = $value;
            }
        }

        ## print after pairing keyname, ? and value:                                                                                                            
        my $ctr = 0;
        foreach my $fieldname(@fields){
            $q_a .= "$fieldname".",\n";
            $q_b .= " ?,";
            $ctr++;
        }

##step3, other processing                                                                                                                                       
## process before putting together:                                                                                                                             
        $q_a =~ s/\,$//g;
        $q_a .= ")  VALUES (";

        $q_b =~ s/\,$//g;
        $q_b .= ")";
## put together the two parts:                                                                                                                                  
        my $q_c = $q_a.$q_b;
        ## print "q_c: $q_c\n";
        ## print Dumper(\@values);

        my $sth = $self->{'dbh'}->prepare($q_c);
        $sth->execute(@values);
    }
}

sub construct_object_tree{
    my ($self) = @_;

    if($self eq undef){
        $self = EDI_TWM::Invoice::new();
    }

    my ($edi_invoice, $line_item, $address) = $self->_parse_template_files();
    my $line_item_ary = [];
    my $address_ary   = [];
    push(@$line_item_ary, $line_item);
    push(@$address_ary, $address);

    $self->set_EDI_INVOICES($edi_invoice);
    $self->set_EDI_INVOICE_DETAILS($line_item_ary);
    $self->set_EDI_ADDRESS($address_ary);

    return $self;
}

sub parse_field_names_3{
    my ($self, $file) = @_;
    my $ref;
    my $ctr = 0;
    open(IN, $file)|| die "file open error: $file\n";
    while(my $line =<IN>){
        chomp($line);  ## remove return character                                                                                                               
        my @ary = split(/\s+/, $line);
        my $size = @ary;
        if($ary[0] ne ''){
            $ref->{$ary[0]}->{'name'} = $ary[0];
            $ref->{$ary[0]}->{'type'} = $ary[$size-1];
            my $nullable ='';
            for (my $i=1; $i<$size-1; $i++){
                $nullable .= $ary[$i];
            }
            $ref->{$ary[0]}->{'nullable'} = $nullable;
            $ref->{$ary[0]}->{'order'} = $ctr;
            $ctr++;
        }
    }
    close IN;
    return $ref;
}


sub build_validation_tree{
    my ($self) = @_;
    my $v_tree_ref;
    my ($invoice_ref, $line_item_ref, $address_ref) = $self->parse_template_files();
    $v_tree_ref->{EDI_INVOICES}   = $invoice_ref;
    $v_tree_ref->{EDI_INVOICE_DETAILS}  = $line_item_ref;
    $v_tree_ref->{EDI_ADDRESS}   = $address_ref;
    return $v_tree_ref;
}

sub _parse_template_files{
    my ($self) = @_;
    my ($infile1, $infile2, $infile3);
    $infile1 = "$base_dir/Config/edi_invoices.txt";
    $infile2 = "$base_dir/Config/edi_invoice_details.txt";
    $infile3 = "$base_dir/Config/edi_address.txt";
    my ($ref1, $ref2, $ref3);
    $ref1 = $self->parse_field_names_3($infile1);
    $ref2 = $self->parse_field_names_3($infile2);
    $ref3 = $self->parse_field_names_3($infile3);
    return ($ref1, $ref2, $ref3);
}


sub remove_spaces_on_ends{
    my($self, $item) = @_;
    $item =~ s/\s+$//g;
    $item =~ s/^\s+//g;
    return $item;
}


sub second_select_from_v_edi_summary{
    my ($self, $invoice_number, $vendor_name, $vendor_duns, $date_from, $date_to, $unpaid, $sort_by) = @_;

### adding test_condition in here:
    $invoice_number  = '';
    $vendor_name  = '';
    $vendor_duns = '';
    $date_from = '';
    $date_to = '';
    $unpaid = '';
    $sort_by ='';


    $invoice_number  = $self->remove_spaces_on_ends($invoice_number);
    $vendor_name     = $self->remove_spaces_on_ends($vendor_name);
    $vendor_duns     = $self->remove_spaces_on_ends($vendor_duns);
    $date_from       = $self->remove_spaces_on_ends($date_from);
    $date_to         = $self->remove_spaces_on_ends($date_to);
    $unpaid          = $self->remove_spaces_on_ends($unpaid);

## case1, default:
    my $query = '';
    my $e_flag = 1;

    my $q_a = 'select vendor_name, invoice_number, payment_terms, invoice_date_pretty, invoice_due_date_pretty, create_date_pretty, released_for_payment                     
                  from v_edi_summary  ';

    my $q_b = '';
    my $q_c = "";

    if(($invoice_number eq '')&&($vendor_name eq '')&&($vendor_duns eq '')&&($date_from eq '')&&($date_to eq '')&&($unpaid eq '')){
	$q_b = '';
	$e_flag = 0;
    }

## case 2, between date, with or without vendor_duns
    if(($date_from ne '')&&($date_to ne '')&&($invoice_number eq '')&&($vendor_name eq '')&&($unpaid eq '')){

	if($vendor_duns eq ''){
	    $q_b = " where invoice_date >= \'$date_from\' and invoice_date <= \'$date_to\'  ";
	}else{
	    $q_b = " where invoice_date >= \'$date_from\' and invoice_date <= \'$date_to\'  and vendor_duns  = \'$vendor_duns\'";
	}
	$e_flag = 0;
    }
    

## case 3, unpaid only
    if(($unpaid == 1)&&($invoice_number eq '')&&($vendor_name eq '')&&($vendor_duns eq '')&&($date_from eq '')&&($date_to eq '')){
	$q_b = "  where released_for_payment = 'N'";
	$e_flag = 0;
    }


## case 4, on invoice_duns
    if(($invoice_number eq '')&&($vendor_name eq '')&&($vendor_duns ne '')&&($date_from eq '')&&($date_to eq '')&&($unpaid eq '')){
	$q_b = "   where invoice_duns = \'$vendor_duns\'";
        $e_flag = 0;
    }
    
    if($e_flag == 1){
	print "select condition wrong\n";
	##return undef;
    }else{
      
    }


    if($sort_by eq ''){
	$q_c =  " order by invoice_due_date, invoice_number desc";
    }else{
	$q_c = " order by $sort_by  desc";
    }

    $query = $q_a.$q_b.$q_c;

    if($e_flag == 1){
        print "select condition wrong\n";

    }else{

    }

    my $query ='select vendor_name, invoice_number, payment_terms, invoice_date_pretty, invoice_due_date_pretty, create_date_pretty, released_for_payment  from v_edi_summary';

    ##my $data_ref = $self->{'dbh'}->selectall_arrayref($query);

    my $sth = $self->{'dbh'}->prepare($query);
    $sth->execute();
    my $ret = $sth->fetchall_arrayref();    
    return ($ret, $query);

}


sub select_from_v_edi_summary_old{
    my ($self, $invoice_number, $vendor_name, $vendor_duns, $date_from, $date_to) = @_;
    
    my $query_header = "select * from v_edi_summary ";
    my $query_middle = " where ";

    $query_middle .= "invoice_number is not null";


    if($vendor_duns){
	##$query_middle .= " and  vendor_duns = \'$vendor_duns\'";	
    }

    if($invoice_number){
        ##$query_middle .= " and  invoice_number = \'$invoice_number\'";

    }

    if($vendor_name){
	##$query_middle .= " and vendor_name = \'$vendor_name\'"; 

    }    

    my $order_by = "  order by invoice_number, invoice_due_date";
    my $query = $query_header.$query_middle.$order_by;


    my $data_ref = $self->{'dbh'}->selectall_hashref($query, 'invoice_number');

    return ($data_ref, $query);
}

## input: invoice filename
## output: presence or absence of the file in IRDB
sub check_invo_xml_fn{
    my ($self, $xml_name) = @_;
    my $leafname;
    if($xml_name =~/.*\/(.*)$/){
        $leafname = $1;
    }else{
        $leafname = $xml_name;
    }
    print "leafname: $leafname\n" if $self->{'debug'};
    my $qry = "select count(*) from edi_invoices where source_file = \'$leafname\'";
    my $ret2 =  $self->{'dbh'}->selectall_arrayref($qry);
    return $ret2->[0][0];
}

## input: invoice_number, vendor_name, invoice_date(yyyy-mm-dd)
##  from redundant invo xml
## output: source file name of the most recent previous invoice

sub get_src_file_from_redundant_xml_info{
    my ($self, $invo_number, $vendor_duns, $invo_date) = @_;
    if(($invo_number eq '')||($vendor_duns eq '')||($invo_date eq '')){
	return undef;
    }
    $vendor_duns = IBIS::EDI_Utils::get_prefixed_string(11, $vendor_duns, '0');
    my @bind_values = ($invo_number, $vendor_duns, $invo_date);
    my $qry = "select source_file from edi_invoices 
               where invoice_number = ? 
                     and vendor_duns = ?  
                     and to_char(invoice_date, 'YYYY-MM-DD') = ? 
                     order by create_date desc";

    my $sth = $self->{'dbh'}->prepare($qry);
    my $rv = $sth->execute(@bind_values);
    my @row = $sth->fetchrow_array;
    return $row[0];
}

## input: invoice_number, vendor_name, invoice_date(yyyy-mm-dd)
##  from redundant invo xml
## output: source file name of the most recent previous invoice

sub get_a_field_value_from_the_3_keys{
    my ($self, $field_name,  $invo_number, $vendor_duns, $invo_date) = @_;
    if(($invo_number eq '')||($vendor_duns eq '')||($invo_date eq '')){
        return undef;
    }
    $vendor_duns = IBIS::EDI_Utils::get_prefixed_string(11, $vendor_duns, '0');
    my @bind_values = ($invo_number, $vendor_duns, $invo_date);
    my $qry = "select $field_name from edi_invoices
               where invoice_number = ?
                     and vendor_duns = ?
                     and to_char(invoice_date, 'YYYY-MM-DD') = ?
                     order by create_date desc";

    my $sth = $self->{'dbh'}->prepare($qry);
    my $rv = $sth->execute(@bind_values);
    my @row = $sth->fetchrow_array;
    return $row[0];
}


## input: $pattern, $string to check
## output: true or false

sub grep_a_pattern_return_boolean{
    my($self, $pattern, $string) = @_;
    ## $pattern = '';
    my @result_grep = grep {/$pattern/} $string;
    my $size = @result;
    return $size;
}

## input: xml files of  invoice a and b
## output: array of difference including < or > in the front
sub diff_output_to_array{
    my($self, $xfa, $xfb) = @_;
    my @diff_text;
    open (DIFFTEXT, "diff  $xfa $xfb |");
    while (<DIFFTEXT>){
	push(@diff_text, $_);
    }
    close DIFFTEXT;
    return \@diff_text;
}

##input: filename
##output: array of file content, each line per array element
sub get_filecontent_into_list{
    my ($self, $tag_file) = @_;
    open(TEXT, "cat $tag_file |");
    my @list = <TEXT>;
    close TEXT;
    return \@list;
}

## purpose: parse diff output against a list of tags to check if all 
## the lines in diff output are included in the tags
## input: list of tags to check, reference of array for diff returns
## output: return similar (1) or not (0)

sub scan_diff_agxt_tags{
    my ($self, $array_ref) = @_;
    my $tag_ref = $self->get_filecontent_into_list($self->{TAG_LIST_TO_IGNORE});
    my $similar = 1; ## assume it is similar at first
    foreach my $line(@$array_ref){
	chomp($line);
        $line =~ s/^\s+//g;
	if($line !~ /\>|\</g){next;}
        if($line !~ /\w+/g){next;}
	my $in_list = 0; ## assume it is not in the list
	foreach my $tag(@$tag_ref){
	    chomp($tag);
	    if($similar == 1){
		if($line =~ /$tag/ig){
		    $in_list = 1;
		}
	    }
	}
	
	## if after one round of search, it is not in the tag list:
	if(($in_list == 0)&&($similar ==1)) {
	    $similar =  0;
	}
    }
    return $similar;
}

sub DESTROY{
    my ($self) = @_;
    ##print "The object is destroyed\n";
}

1;
