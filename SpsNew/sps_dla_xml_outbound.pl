#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Kaveh sari 
##Date      : 03/06/2024
##
##Brief Desc: 
##            The program must run as rdiusr.
##	      
##            
##            
##	      
## --------------------------------------------------------------------------  
# Compliance Management Solution
use strict;
use warnings;
use XML::Simple;
use MCCS::WMS::Sendmail;
use MCCS::Config;
use File::Basename;
use File::Path;
use Readonly;
use Fcntl qw(:flock);
use Data::Dumper;
use Pod::Usage;
use DateTime;
use IBIS::Log::File;
use IBIS::DBI;
use Getopt::Long;
use Net::SFTP::Foreign;
use File::Basename;
use IO::File;

use Carp;
use IBIS::Crypt;

#- One process at a time ---------------------------------------------
my $lock_file = "/usr/local/mccs/tmp/" . basename($0) . ".lck";
my $SELF;
open ($SELF, ">", $lock_file) or croak "Could not create lock file $lock_file";
close $SELF;
#flock $SELF, LOCK_EX | LOCK_NB or croak "Another $0 process already running";
if (-e $lock_file) { croak "Another $0 process already running"; }

#------------DEFINE NECESSARY VARIABLES-----------------------------#
my $sm         = MCCS::WMS::Sendmail->new();
my $dt         = DateTime->now( time_zone => 'local' );
my $timestamp  = $dt->strftime('%Y%m%d%H%M%S');
my $reporttime = $dt->strftime('%Y%m%d%H%M');
my $error_lvl  = 0;
my $host       = 'hostname';
my $submit_id  = $timestamp . $$;

my $debug;
my $database      = 'rms_p';
my $suppress_send = 0;
my $archive_sw    = 0;
my $report        = undef;
my $log = "/usr/local/mccs/log/sps/dla_outbound/" . $timestamp . "_dla_xml.log";
my $g_long_date = 'date +"%D %r"';
my $file_path   = '/usr/local/mccs/data/sps/dla/';
##my $csv_file;
my @po_id;
my $date;
my $suppress_date;
my $cfg      = MCCS::Config->new();
my $g_dbname = $cfg->sps_dla_global->{DBNAME};
my $no_save;

my $options = (
	GetOptions(
		'debug'      => \$debug,
		'log=s'      => \$log,
		'database=s' => \$g_dbname,
		'nosend'     => \$suppress_send,
		'archive'    => \$archive_sw,
		'report=s'   => \$report,
		'po=s{,}'    => \@po_id,
		'date=s'     => \$date,
		'nodate'     => \$suppress_date,
		'nosave'     => \$no_save,
	)
);

my $log_obj = IBIS::Log::File->new( { file => $log, append => 1 } );

my $dbh = IBIS::DBI->connect( dbname => $g_dbname, )
  || fatal_error("Failed to connect to RMS database\n");

my $global_ctr = 0;
my $hash_level = 0;

my @files;
my @err_list;

my $Miscellanous = {
	Qualifier1   => 'DN',
	Description1 => 'D340',
	Qualifier2   => 'B5',
	Description2 => 'XP',
};

#----------------- XML LAYOUT AS A HASH----------------------------#
# For XML::Simple Hash References is what it likes
#
# I had issue getting hashref of hashrefs to group properly....
# so I got lazy and instead of forcing the entire
# hash of hash plus arrays (and some extra craziness) I broke things
# into header and LineItem xml.  I then wrote to a FileHandle
# and a little fairy dust and it worked <poof_end_magic_trick>
#
# Four Leaf Clover/Rabbit's Foot/Unicorn -- everything works
#
#    XML::SIMPLE and the hash refs created here will create the XML
#    that resembles the following xml sample
#-------------------------------------------------------------------#
#<PurchaseOrders>
#     <!-- Repeat for each PO header "purchaseorder"-->
#     <PurchaseOrder>
#       <Header>
#         <OrderHeader>  </OrderHeader>
#         <Address> </Address>
#       </Header>
#       <LineItems>
#         <!-- Repeat for each po detail line  "lineitem"-->
#         <LineItem>
#          <Address></Address>
#          <Address></Address>
#          <Date></Date>
#          <Miscellanous></Miscellanous>
#          <Notes></Notes>
#          <Notes></Notes>
#          <Notes></Notes>
#          <Notes></Notes>
#          <OrderLine></OrderLine>
#          <Reference></Reference>
#          <Reference></Reference>
#         </LineItem>
#       </LineItems>
#     </PurchaseOrder>
# </PurchaseOrder>
#-------------------------------------------------------------------#

sub mainLine {
	my $FH;

	log_debug(qq(Getting PO Header Records for vendor => '00069043032'));
	my $po_headers = get_dla_po_headers();

	if ($po_headers) {
		my $filename = 'RN' . $timestamp . '.xml';

		log_debug(qq(Creating XML records in $filename));
		my $output_file = $file_path . $filename;
		$FH = IO::File->new($output_file, "w") or croak "Can not open output\n";
		#open ($FH, ">",$output_file) or croak "Can not open output\n";
		push( @files, $filename );
		print $FH qq(<PurchaseOrders>\n\n);

		foreach my $rec_id ( sort keys %{$po_headers} ) {

			#needed to create new file if sequence ctr
			if ( $global_ctr > 9000 ) {
				print $FH qq(</PurchaseOrders>);
				close $FH;

				my $dt_x        = DateTime->now();
				my $timestamp_x = $dt->strftime('%Y%m%d%H%M%S');

				$filename = 'RN' . $timestamp_x . '.xml';

				$output_file = $file_path . $filename;
				$FH = IO::File->new($output_file, "w") or croak "Can not open output\n";
				#open ($FH, ">",$output_file) or croak "Can not open output\n";

				print $FH qq(<PurchaseOrders>\n\n);

				$global_ctr = 0;

				push( @files, $filename );
			}

			print $FH qq(<PurchaseOrder>\n);
			my $Header    = Header_element( $po_headers->{$rec_id} );
			my $xml_input = {};
			$xml_input->{Header} = $Header;
			my $XML_Header =
			  XMLout( $xml_input, RootName => '', KeepRoot => 1, NoAttr => 1, );
			print $FH $XML_Header;

			my $po_details = get_dla_po_details(
				$po_headers->{$rec_id}->{po_id},
				$po_headers->{$rec_id}->{version_no}
			);
			my $arrayLineItem = {};
			my $ctr           = 0;

			foreach my $rec_idx ( sort keys %{$po_details} ) {
				my $LineItem = LineItems_element( $po_details->{$rec_idx},
					$po_headers->{$rec_id} );
				$arrayLineItem->{LineItem}[$ctr] = $LineItem;
				$ctr++;
			}

			$xml_input = {};
			$xml_input->{LineItems}->{LineItem} =
			  $arrayLineItem->{LineItem};    #LineItem;
			my $XML_LineItems =
			  XMLout( $xml_input, RootName => '', KeepRoot => 1, NoAttr => 1, );
			print $FH $XML_LineItems;

			print $FH qq(</PurchaseOrder>\n\n);
		}

		print $FH qq(</PurchaseOrders>);
		close $FH;

		parse_xml( $file_path, \@files ) if (! $no_save);
		sftp_sps( $file_path, $filename );

	}

	#Gather all your error records and send an Email to whome ever...
	if ( @err_list && !$debug ) {
		unshift( @err_list,
			qq(Please review following log for detail errors => $log \n) );
		$sm->errorLevel('error');
		send_mail( body =>
			  ( "ERROR during " . __FILE__ . ' at ' . $g_long_date, @err_list )
		);
	}
	return;
}

sub Header_element {
	my $po_header = shift;
	my $hashRef   = {};
	$hashRef->{OrderHeader} = get_OrderHeader($po_header);
	$hashRef->{Address}     = $cfg->sps_dla_global->{HeaderAddress};

	return $hashRef;
}

sub LineItems_element {
	my $po_detail = shift;
	my $po_header = shift;

	my $hashRef = {};
	
	
	$hashRef->{OrderLine}     = get_OrderLine($po_detail);
	$hashRef->{Date}          = get_Date($po_header);
	$hashRef->{Reference}     = get_References($po_detail);
	$hashRef->{Notes}         = get_Notes($po_detail);
	$hashRef->{Address}       = get_LineItem_Address($po_detail);
	$hashRef->{Miscellaneous} = get_miscellaneuos($po_detail);

	save_po_odn(
		odn  => $hashRef->{Reference},
		po_d => $po_detail,
		po_h => $po_header
	) if (! $no_save);

	return $hashRef;

}

#--------------------------------------------------------------------
#--------------------------------------------------------------------
sub get_miscellaneuos {
 my $po_detail = shift;
 my $hashRef   = {};

 
	$hashRef->{Qualifier1}   = 'DN';
	$hashRef->{Description1} = 'D340';
	$hashRef->{Qualifier2}   = 'B5';
	$hashRef->{Description2} = $po_detail->{fund_code} ;

 
return $hashRef;

}




sub get_OrderHeader {
	my $po_header = shift;
	my $hashRef   = {};

	#-----HardCoded------#
	$hashRef->{TsetPurposeCode}         = "00";
	$hashRef->{PurchaseOrderTypeCode} = "A0";
	#--------------------#
	$hashRef->{PurchaseOrderDate} = $po_header->{order_date_yyymmdd};

	return $hashRef;
}

sub get_References {
	my $po_detail = shift;

	#---AoH = array of hashes
	my @AoH = ();

	#-----HardCoded------#
	$AoH[0]{ReferenceQual} = 'AN';
	$AoH[1]{ReferenceQual} = 'TN';

	#--------------------#

	$AoH[0]{ReferenceID} = $po_detail->{po_id};

	#Dodaac -> Julian Day ->
	$global_ctr++;
	
	my $doddac = ($po_detail->{is_footwear} ) ? $po_detail->{ship_to_dodaac} :  $po_detail->{dodaac};
	my $reference_id =
	   $doddac
	  . substr( $dt->year(), 3, 1 )
	  . sprintf( "%03d", $dt->doy() )
	  . sprintf( "%04d", $global_ctr );

	$AoH[1]{ReferenceID} = $reference_id;

	return \@AoH;
}

sub get_Notes {
	my $po_detail = shift;

	#AoH = array of hashes

	my @AoH = ();

	#-----HardCoded------#
	$AoH[0]{NoteInformationField} = '0';
	$AoH[0]{NoteFormatCode}       = 'AOA';

	$AoH[1]{NoteInformationField} = 'DE';
	$AoH[1]{NoteFormatCode}       = $po_detail->{signal_code} ;

	$AoH[2]{NoteInformationField} = 'DF';
	$AoH[2]{NoteFormatCode}       = 'S';

	$AoH[3]{NoteInformationField} = '79';

	$AoH[4]{NoteInformationField} = 'A9';

	#--------------------#

	#MCX Option 06,08,09,12,15
	$AoH[3]{NoteFormatCode} = $po_detail->{reason_id};
	$AoH[4]{NoteFormatCode} = ($po_detail->{is_footwear} ) ? 'M70000' : $po_detail->{ship_to_dodaac};
	#$AoH[4]{NoteFormatCode} = $po_detail->{ship_to_dodaac};

	return \@AoH;
}

sub get_OrderLine {
	my $po_detail = shift;
	my $hashRef   = {};

	#-----HardCoded------#
	$hashRef->{PartNumberQualifier1} = 'FS';
	$hashRef->{OrderQtyUOM}          = $po_detail->{cost_descriptor};

	#--------------------#

	$hashRef->{LineSequenceNumber} = $po_detail->{fin_line_no};
	$hashRef->{PartNumber1}        = $po_detail->{bar_code_id};
	$hashRef->{OrderQty}           = $po_detail->{qty_ordered_x};

	return $hashRef;
}

sub get_Date {
	my $po_header = shift;
	my $hashRef   = {};

	#-----HardCoded------#
	$hashRef->{DateTimeQualifier1} = 'BD';

	#--------------------#

	$hashRef->{Date1} = $po_header->{expected_delivery_yyymmdd};

	return $hashRef;
}

sub get_LineItem_Address {
	my $po_detail = shift;

	#AoH = array of Hash References
	#Three addresses needed 0,1,2

	my @AoH = ();

	#-- Found in ibis.cfg
	$AoH[0] = $cfg->sps_dla_global->{BillToAddress};
	$AoH[0]->{AddressLocationNumber} = $po_detail->{dodaac};

	# New changes made to use ship to as billing address
	#    $AoH[0]->{Address1} = $po_detail->{address2};
	#	$AoH[0]->{Address2} = $po_detail->{address3};
	#	$AoH[0]->{AddressName} = $po_detail->{address1};
	#	$AoH[0]->{AddressLocationNumber} = $po_detail->{dodaac} ;
	#	$AoH[0]->{AddressTypeCode} = 'BT';
	#	$AoH[0]->{City} = $po_detail->{city} ;
	#	$AoH[0]->{Country} = $po_detail->{country} ;
	#	$AoH[0]->{LocationCodeQualifier} = '10';
	#	$AoH[0]->{PostalCode} = substr($po_detail->{zip_code}, 0,5) ;
	#	$AoH[0]->{'State'} = $po_detail->{'state'} ;
	#end changes for new bill to

	$AoH[2] = $cfg->sps_dla_global->{Z4};

	#-- address is return in query data located dodaac_sites
	if ($po_detail->{address2}) {
	 $AoH[1]->{Address1}              = $po_detail->{address2};
	 $AoH[1]->{Address2}              = $po_detail->{address3};
	} else { 
	 $AoH[1]->{Address1}              = $po_detail->{address3};
	}
	$AoH[1]->{AddressName}           = $po_detail->{address1};
	$AoH[1]->{AddressLocationNumber} = $po_detail->{ship_to_dodaac};
	$AoH[1]->{AddressTypeCode}       = 'ST';
	$AoH[1]->{City}                  = $po_detail->{city};
	$AoH[1]->{Country}               = $po_detail->{country};
	$AoH[1]->{LocationCodeQualifier} = '10';
	$AoH[1]->{PostalCode}            = substr( $po_detail->{regex_zip_code}, 0, 5 );
	$AoH[1]->{'State'}               = $po_detail->{'state'};

	return \@AoH;
}

#-----------------------------------------------------------------------------
#  Data Stuff
#-----------------------------------------------------------------------------
sub get_dla_po_headers {

       my $sql = <<"ENDSQL1";
select i.*, to_char(i.expected_delivery_date, 'yyyy-mm-dd') expected_delivery_yyymmdd,to_char(i.order_date, 'yyyy-mm-dd') order_date_yyymmdd
from iro_po_headers i
join PURCHASE_ORDERS p on (p.business_unit_id = i.business_unit_id and p.po_id = i.po_id and p.version_no = i.version_no)
join reasons rn on (rn.business_unit_id = p.business_unit_id and rn.reason_id = p.reason_id and rn.sub_type = 'PURC_ORDER' and substr(rn.description, 1,3) = 'DLA')
where i.business_unit_id = '30' and i.vendor_id = '00069043032'  and i.cancelled_ind <> 'Y'
%s 
and i.version_no = (select max(version_no) From Iro_Po_Headers p Where  cancelled_ind <> 'Y'
%s 
and  i.po_id = p.po_id )
%s 
and '60001' not in (nvl(p.for_site_id,'0'), nvl(p.shipped_to_site_id,'0')) and '70001' not in (nvl(p.for_site_id,'0'), nvl(p.shipped_to_site_id,'0'))
ENDSQL1
	my $replace_po   = '';
	my $replace_date = 'and trunc(Ride_In_Date) = Trunc(Sysdate)';
	if (@po_id) {
		my $po_str = join( ',', @po_id );
		$replace_po = ' and i.po_id in (' . $po_str . ')';
		log_debug(qq(Getting PO IDs with po constraint => $replace_po));
	}
	if ($date) {
		$replace_date =
		  qq(and trunc(Ride_In_Date) = to_date('$date', 'yyyymmdd') );
	}
	if ($suppress_date) {
		$replace_date = '';
	}

	log_debug(qq(Getting PO IDs with date constraint => $replace_date));
	$sql = sprintf $sql, $replace_date, $replace_date, $replace_po;

	log_debug(qq(SQL HEADER =>\n $sql));

	my $sth = $dbh->prepare($sql);
	$sth->execute();

	my $results;
	my $ctr = 0;

	while ( my $record7 = $sth->fetchrow_hashref() ) {
		$ctr++;
		$results->{ 'rec_' . sprintf( '%.6d', $ctr ) } = $record7;
	}

	$sth->finish();

	log_debug(qq(Records Found => $ctr));

	return $results;
}

sub get_dla_po_details {
	my $po_id      = shift;
	my $version_no = shift;
        my $sql =<<"ENDSQL2";
      select  p.*,
           (select bar_code_id from bar_codes b where b.business_unit_id = p.business_unit_id and b.style_id = p.style_id and sub_type = 'UPCE' and rownum = 1) bar_code_id,
            sd.*,
            lpad(to_char(po.reason_id), 2,'0') reason_id,
             p.qty_ordered * nvl(dm.multiplier,1) qty_ordered_x,
             Is_Dla_Footwear(p.style_id) is_footwear,
              regexp_substr(sd.zip_code, '[^-]*') regex_zip_code  
     from IRO_PO_HEADERS  hdr
     join PO_VERSION_HEADERS h On (
                                    hdr.business_unit_id = h.business_unit_id and 
                                    Hdr.Po_Id = h.Po_Id And
                                    Hdr.Version_No = h.Version_No) 
     Left Join Po_Version_Details P  On (
                                    hdr.business_unit_id = p.business_unit_id and 
                                    Hdr.Po_Id = P.Po_Id And
                                    Hdr.Version_No = P.Version_No and 
                                    P.po_detail_status <> 'D') 
       left Join site_dodaac_new sd on (nvl(p.site_id,h.for_site_id) = sd.ship_to_site and
                                        sd.signal_code = (case when is_dla_footwear(p.style_id) = '1' then 'B' else 'J' end))
     Join Purchase_Orders Po   On (Hdr.Business_Unit_Id = Po.Business_Unit_Id And
                               Hdr.Po_Id = Po.Po_Id And
                               Hdr.Version_No = Po.Version_No)
         join reasons rn on (rn.business_unit_id = hdr.business_unit_id and
                             rn.reason_id = po.reason_id and
                             rn.sub_type = 'PURC_ORDER' and
                             substr(rn.description, 1,3) = 'DLA')
         left join dla_upc_qty_multiplier dm on (dm.style_id = p.style_id)
      where hdr.po_id = $po_id
      and hdr.version_no = $version_no
ENDSQL2
	my $sth = $dbh->prepare($sql);
	$sth->execute();

	my $results;
	my $ctr = 0;

	while ( my $record5 = $sth->fetchrow_hashref() ) {
		$ctr++;
		$results->{ 'rec_' . sprintf( '%.6d', $ctr ) } = $record5;
	}

	$sth->finish();
	if ($debug) {
		print "***********PO ID = $po_id\n";
		print Dumper $results;
	}
	return $results;
}

sub get_xml_id {
	my $sql = qq( select xml_id_seq.nextval xml_id from dual );
	my $sth = $dbh->prepare($sql);
	$sth->execute();
	my $record4 = $sth->fetchrow_hashref();
	$sth->finish();

	return $record4->{xml_id};
}

sub is_po_id {
	my $po_id = shift;
	my $sql   = qq( select  1 Po_Found From Purchase_Orders  Where Po_Id = (Case When Is_Number('$po_id') = 'Y' Then '$po_id' else '0' end));
	my $sth = $dbh->prepare($sql);
	$sth->execute();
	my $record1 = $sth->fetchrow_hashref();
	$sth->finish();

	return $record1->{po_found};
}

sub get_before_process {
 my $sql = <<"ENDSQL3";
 Select Po_Id,po_group_id,Version_No,Document_Type,Po_Type,For_Site_Id,Shipped_To_Site_Id,to_char(Order_Date, 'mm/dd/yyyy') order_date,Approved_Ind,origin,Reason_Id,Created_By,to_char(Creation_Date, 'mm/dd/yyyy') creation_date,case when (select count(*) from PO_STYLE_COLORS po where po.business_unit_id = p.business_unit_id and po.po_id = p.po_id and po.version_no = p.version_no and po.cost_descriptor not in ('BX','CS','EA','KT','PG','PR','SE')) > 0 then 'Error Found' else null end UOM_ERROR From Purchase_Orders p
Where Business_Unit_Id = '30' And Vendor_Id ='00069043032' And Cancelled_Ind <> 'Y' and Ride_Processed_Fin_Date Is Null order by 1
ENDSQL3

	log_debug(qq(SQL Before Process Reopt =>\n $sql));
	my $sth = $dbh->prepare($sql);
	$sth->execute();

	my $results;
	my $ctr = 0;

	while ( my $record2 = $sth->fetchrow_hashref() ) {
		$ctr++;
		$results->{ 'rec_' . sprintf( '%.6d', $ctr ) } = $record2;
	}

	$sth->finish();

	return $results;
}

sub get_after_process {
 my $sql = <<"ENDSQL4";
Select 
distinct
  p.Po_Id,
  p.Version_No,
  p.Document_Type,
  p.Po_Type,
  p.For_Site_Id,
  p.Shipped_To_Site_Id,
  to_char(p.Order_Date, 'mm/dd/yyyy') order_date,
  p.Approved_Ind,
  p.origin,
  p.Reason_Id,
  p.Created_By,
  to_char(p.Creation_Date, 'mm/dd/yyyy') creation_date,
  to_char(h.ride_in_date, 'mm/dd/yyyy') ride_in_date
From Purchase_Orders p
join IRO_PO_HEADERS h on (h.business_unit_Id = p.business_unit_id and h.po_id = p.po_id and h.version_no = p.VERSION_NO)
where
p.business_unit_id = '30' and
p.po_id in ( Select distinct  Xml.Element_Data po_id
             From ( Select Distinct  Xml_Id,  Element_Name,   Element_Data po_id, Xml_Source
                    From Rdiusr.Xml_Save_Parsed_Data
                    Where Alt_Element_Name = 'PO_ID'   And Xml_Type = 'DLA') Base
             Join Rdiusr.Xml_Save_Parsed_Data Xml On (Base.Xml_Id = Xml.Xml_Id)
             Where xml.xml_source in ( select xml_source from XML_SAVE_DATA
                                       where trunc(create_ts) = trunc(sysdate) )
             and  Xml.Alt_Element_Name =  'PO_ID')
order by 1
ENDSQL4

	log_debug(qq(SQL After Process Reopt =>\n $sql));
	my $sth = $dbh->prepare($sql);
	$sth->execute();

	my $results;
	my $ctr = 0;

	while ( my $record3 = $sth->fetchrow_hashref() ) {
		$ctr++;
		$results->{ 'rec_' . sprintf( '%.6d', $ctr ) } = $record3;
	}

	$sth->finish();

	return $results;
}

sub get_after_error_process {
 my $sql = <<"ENDSQL5";
select i.po_id,
 i.version_no,
 i.po_document_type document_type,
 i.po_type,
 p.created_by,
 p.creation_date,
 p.for_site_id,
 p.shipped_to_site_id,
 p.reason_id,
 to_char(i.expected_delivery_date, 'mm/dd/yyyy') expected_delivery_date,
 to_char(i.order_date, 'mm/dd/yyyy') order_date,
 to_char(i.ride_in_date, 'mm/dd/yyyy') ride_in_date
from iro_po_headers i
join PURCHASE_ORDERS p on (p.business_unit_id = i.business_unit_id and p.po_id = i.po_id and p.version_no = i.version_no)
left join reasons rn on (rn.business_unit_id = p.business_unit_id and rn.reason_id = p.reason_id and rn.sub_type = 'PURC_ORDER' and substr(rn.description, 1,3) = 'DLA')
where
   i.business_unit_id = '30'
   and i.vendor_id = '00069043032'
   and i.cancelled_ind <> 'Y'
   %s
   and i.version_no = (select max(version_no) From Iro_Po_Headers p
                      Where  cancelled_ind <> 'Y'
                      %s
                      and  i.po_id = p.po_id )
   %s
   and( '60001' in (p.for_site_id, p.shipped_to_site_id) or '70001'  in (p.for_site_id, p.shipped_to_site_id) or rn.reason_id is null ) 
ENDSQL5

	my $replace_po   = '';
	my $replace_date = 'and trunc(Ride_In_Date) = Trunc(Sysdate )';
	if (@po_id) {
		my $po_str = join( ',', @po_id );
		$replace_po = ' and po_id in (' . $po_str . ')';
		log_debug(qq(Getting PO IDs with po constraint => $replace_po));
	}
	if ($date) {
		$replace_date =
		  q(and trunc(Ride_In_Date) = to_date('$date', 'yyyymmdd') );
	}
	if ($suppress_date) {
		$replace_date = '';
	}

	log_debug(qq(Getting PO IDs with date constraint => $replace_date));
	$sql = sprintf $sql, $replace_date, $replace_date, $replace_po;

	log_debug(qq(SQL Error Reopt =>\n $sql));

	my $sth = $dbh->prepare($sql);
	$sth->execute();

	my $results;
	my $ctr = 0;

	while ( my $record6 = $sth->fetchrow_hashref() ) {
		$ctr++;
		$results->{ 'rec_' . sprintf( '%.6d', $ctr ) } = $record6;
	}

	$sth->finish();

	return $results;
}

#-----------------------------------------------------------------------------
#  Write to file and/or DB - Using oracle specific return val on insert
#-----------------------------------------------------------------------------
sub saved_parsed_xml {
	my %args = @_;
	my $rec  = $args{record};
	my $out  = ();
	
	my $sql = qq(insert into xml_save_parsed_data(XML_ID, PARENT_ELEMENT_ID, ELEMENT_TREE_LVL, ELEMENT_NAME,ELEMENT_TYPE, ELEMENT_DATA, CREATE_TS, XML_SOURCE, XML_TYPE, ALT_ELEMENT_NAME) values (?,?,?,?,?,?,sysdate,?,?,?) returning element_id into ?);
	my @params = (
		$rec->{xml_id},           $rec->{parent_element_id},
		$rec->{element_tree_lvl}, $rec->{element_name},
		$rec->{element_type},     $rec->{element_data},
		$rec->{xml_source},       $rec->{xml_type},
		$rec->{alt_element_name}
	);

	my $sth = $dbh->prepare($sql);
	$sth->bind_param( $_, $params[ $_ - 1 ] ) for ( 1 .. @params );
	$sth->bind_param_inout( @params + 1, \$out->{element_id}, 0 );
	$sth->execute();
	$sth->finish();

	return $out;
}

sub save_xml_db {
	my %args      = @_;
	my $xml       = $args{xml};
	my $file_name = $args{file_name};
	my $out       = ();

	my $sql = qq(insert into xml_save_data (create_ts, xml_source, xml_type, xml_data) values (sysdate,'$file_name','DLA',?));

	my $sth = $dbh->prepare($sql);

	$sth->execute($xml);
	$sth->finish();
	return;
}

sub save_po_odn {
	my %args      = @_;
	my $odn       = $args{odn};
	my $po_d      = $args{po_d};
	my $po_h      = $args{po_h};
	my $file_name = $args{file_name};
	my $out       = ();
       
       my $sql = qq(insert into dla_po_odn (PO_ID, VERSION_NO, ODN, BAR_CODE_ID, QTY_ORDERED, FIN_LINE_NO, COST_DESCRIPTOR, DODDAC, REASON_ID,RDER_DATE, EXPECTED_DELIVERY, CREATED_TS) values (?,?,?,?,?,?,?,?,?,?,?,sysdate));

	my @params = (
		$po_d->{po_id},           $po_d->{version_no},
		$odn->[1]{'ReferenceID'}, $po_d->{bar_code_id},
		$po_d->{qty_ordered_x},   $po_d->{fin_line_no},
		$po_d->{cost_descriptor}, $po_d->{dodaac},
		$po_d->{reason_id},       $po_h->{order_date_yyymmdd},
		$po_h->{expected_delivery_yyymmdd}
	);

	my $sth = $dbh->prepare($sql);
	$sth->bind_param( $_, $params[ $_ - 1 ] ) for ( 1 .. @params );

	#	   $sth->bind_param_inout( @params+1,    \$out->{element_id} , 0 );
	$sth->execute();
	$sth->finish();
	return;
}

#-----------------------------------------------------------------------------
#  XML Parsing and Recusrive handling of parsed data
#-----------------------------------------------------------------------------
sub parse_xml {
	log_debug(qq(Saving files to RMS database));
	my %args;
	( $args{file_path} ) = shift;
	( $args{files} ) = shift;

	#print Dumper %args;
	my $FH;
	foreach my $fn ( @{ $args{files} } ) {
		log_debug(qq( File $fn Sent to RMS database));
		my $input_file = $args{file_path} . $fn;

		#---------------------------------------------------------------
		#  Will slurp the entire file in one swoop into mem, like a slushie
		#---------------------------------------------------------------
		open( $FH, "<", $input_file) or  croak "Can not open $input_file\n";
		local $/ = undef;
		my $file_content = <$FH>;
		close $FH;
		
		save_xml_db( xml => $file_content, file_name => $fn ); 
		

		if ($@) {
			push( @err_list, "Could not save XML to DB.\n FileName=> $fn \n" );
			log_debug("Could not save XML to DB.\n FileName=> $fn \n");
		}

		my $xml_parsed = XMLin(
			$file_content,
			KeepRoot   => 1,
			ForceArray => ['PurchaseOrder']
		);
		foreach
		  my $rec ( sort @{ $xml_parsed->{PurchaseOrders}->{PurchaseOrder} } )
		{
			my $id = get_xml_id();
			recurse_xml_hash(
				record    => $rec,
				file_name => $fn,
				xml_type  => 'DLA',
				xml_id    => $id
			);
		}
	}
	return;
}

sub recurse_xml_hash {
	my %args = @_;
	my $rec  = $args{record};
	$hash_level++;

	foreach my $key_name ( sort keys %{$rec} ) {
		my $save = ();
		$save->{xml_id}            = $args{xml_id};
		$save->{xml_source}        = $args{file_name};
		$save->{xml_type}          = $args{xml_type};
		$save->{parent_element_id} = $args{parent_element_id};
		$save->{element_tree_lvl}  = $hash_level;
		$save->{element_name}      = $key_name;
		my $ret;

		if ( ref( $rec->{$key_name} ) eq "HASH" ) {
			$save->{element_type} =
			  ( %{ $rec->{$key_name} } ) ? "HASH" : "SCALAR";
			$ret = saved_parsed_xml( record => $save );
			recurse_xml_hash(
				record            => $rec->{$key_name},
				file_name         => $args{file_name},
				xml_type          => $args{xml_type},
				xml_id            => $args{xml_id},
				parent_element_id => $ret->{element_id}
			);

		}
		elsif ( ref( $rec->{$key_name} ) eq "ARRAY" ) {
			$save->{element_type} =
			  ( @{ $rec->{$key_name} } ) ? "ARRAY" : "SCALAR";

			foreach my $array_hash ( @{ $rec->{$key_name} } ) {
				$ret = saved_parsed_xml( record => $save );
				recurse_xml_hash(
					record            => $array_hash,
					file_name         => $args{file_name},
					xml_type          => $args{xml_type},
					xml_id            => $args{xml_id},
					parent_element_id => $ret->{element_id}
				);
			}
		}
		else {
			$save->{element_type} = "SCALAR";
			$save->{element_data} = $rec->{$key_name};
			if ( $key_name eq 'ReferenceID' && is_po_id( $rec->{$key_name} ) ) {
				$save->{alt_element_name} = 'PO_ID';
			}
			elsif ( $key_name eq 'ReferenceID'
				&& !is_po_id( $rec->{$key_name} ) )
			{
				$save->{alt_element_name} = 'ODN';
			}
			elsif ( $key_name eq 'PartNumber1' ) {
				$save->{alt_element_name} = 'BAR_CODE_ID';
			}
			else {
				$save->{alt_element_name} = '';
			}
			saved_parsed_xml( record => $save );
		}
	}
	$hash_level-- if ($hash_level);
	return;
}

#-----------------------------------------------------------------------------
#  SFTP Function      ---- This is not the code you were looking for ---
#-----------------------------------------------------------------------------
sub sftp_sps {

	my $file_path2 = shift;
	my $filename  = shift;
	log_debug("Sftp file to SPS!  path=> $file_path2   name=>$filename\n");

	if ( !$suppress_send ) {
		my %arglist;
		my $dest     = $cfg->sps_dla_global->{sftp_config}->{host};
		my $inputDir = $cfg->sps_dla_global->{sftp_config}->{input_dir};
		log_debug("SFTP Hostname => $dest");

		# Retrieve SPS user name and password
		#use IBIS::Crypt;
		my $enc  = IBIS::Crypt->new();
		my $pswd = 'xhjjVj5_VKF';

#eval { $pswd = $enc->decrypt( $cfg->sps_dla_global->{sftp_hash}->{password} ) };

		$arglist{user}     = $cfg->sps_dla_global->{sftp_config}->{user};
		$arglist{password} = $pswd;
		$arglist{port}     = $cfg->sps_dla_global->{sftp_config}->{port};
		$arglist{more}     = '-v';

		# Log server name and directory
		log_debug('SFTP transfer started ');
		log_debug("FTP_SERVER: $dest");

		# Establish SFTP connection to SPS server
		my $sftp;
		my $num_retry      = 10;
		my $successful_ftp = 'N';
		my $attempt;
		while ( $num_retry-- ) {
			
			$sftp = Net::SFTP::Foreign->new( $dest, %arglist );
			if ( !$@ ) { $successful_ftp = 'Y'; last }
			$attempt = 10 - $num_retry;
			log_debug("Attempt $attempt to connect to $dest failed!\n");
			sleep(10);
		}

		if ( $successful_ftp eq 'N' ) {
			fatal_error("SFTP connection to MCL server ($dest) failed!");
		}
		$sftp->put(
			$file_path2 . $filename, $inputDir . '/' . $filename,
			copy_perms => 0,
			copy_time  => 0,
			atomic     => 1
		);

		if ( $log_obj && $sftp->error ) {
			$log_obj->info("$filename could not be sent");
		}
        
        log_debug("SFTP transfer completed");
#		if ($log_obj) { $log_obj->info('SFTP transfer completed') }

	}
	else {
	log_debug("Skipping file send per command line switch --nosend\nFiles to be sent $filename");
	}
	return;
}

#-----------------------------------------------------------------------------
#  Reporting something not important to me but them
#-----------------------------------------------------------------------------
sub report_proc {
	before_process_rpt()      if ( $report == 1 );
	after_process_rpt()       if ( $report == 2 );
	after_error_process_rpt() if ( $report == 3 );
        return;
}

sub html_header {
  my $html = <<'ENDHTML1';
  <html><head><style>
  .e832_table a:hover {text-decoration: underline;}
  .e832_table {
  font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
  font-size: 11px;
  border-collapse: collapse;
  border: 1px solid #69c;
  margin-right: auto;
  margin-left: auto;
  margin-top: auto;
  margin-bottom: auto;
  }
  .e832_table caption {
  font-size: 15px;
  font-weight: bold;
  padding: 12px 17px 5px 17px;
  color: #039;
  }
  .e832_table th {
  padding: 1px 3px 1px 3px;
  background-color: RoyalBlue;
  font-weight: normal;
  font-size: 11px;
  color: #FFF;
  }
  .e832_table tr:hover td {
  color: #339;
  background: #d0dafd;
  }
  .e832_table td {
  padding: 2px 3px 1px 3px;
  color: #000;
  background: #fff;
  }
  .e832_table tr {
  padding: 0px 0px 0px 0px;
  }<\style><\head>
ENDHTML1
	return $html;
}

sub before_process_rpt {
	my $FH;
	my $pre_active_file =
	    "/usr/local/mccs/log/sps/dla_reports/"
	  . $reporttime
	  . "_dla_pre_activity.csv";
	my $rpt_hash = get_before_process();

	#-------------------------------------------------------------
	# Always print hash in same order...
	#  Hash is Hash and does what it wants
	#-------------------------------------------------------------
	my @order = (
		'Po_Id',              'Version_No',
		'Po_Group_id',        'Document_Type',
		'Po_Type',            'For_Site_Id',
		'Shipped_To_Site_Id', 'Order_Date',
		'Approved_Ind',       'Origin',
		'Reason_Id',          'UOM_Error',
		'Created_By',         'Creation_Date'
	);
	if ($rpt_hash) {
		open( $FH, ">" ,$pre_active_file) or croak "Can not open output\n";
		print $FH join( ', ', @order );    #Header
		print $FH "\n";
		foreach my $rec_id ( sort keys %{$rpt_hash} ) {
			print Dumper $rpt_hash->{$rec_id} if ($debug);
			print $FH
			  join( ', ', map { $rpt_hash->{$rec_id}->{ lc($_) } } @order );
			print $FH "\n";
		}
		close $FH;

		my $html_table;
		$html_table = html_header();
		$html_table .= qq(<span style="background-color: yellow;">The following is a list of DLA POs that are currently in process and need to be completed or cancel.</span><br><br>\n\n);
		$html_table .= qq(<body><table class="e832_table" width="100%">);
		$html_table .= join( '', map { "<th> $_ </th>" } @order );
		foreach my $rec_id ( sort keys %{$rpt_hash} ) {
			$html_table .= "<tr>";
			$html_table .= join('',
				map {"<td>". ucfirst( lc( $rpt_hash->{$rec_id}->{ lc($_) } ) ). "</td>" } @order
			 );
			$html_table .= "</tr>\n";
		}
		$html_table .= "</table></body><br><br>";

		$sm->message_type('TEXT/HTML');
		send_mail(
			body => ($html_table),
			subj => 'Pre Activity Report ',
			logs => ( $pre_active_file, )
		);
	}
	return;
}

sub after_process_rpt {
	my $FH;
	my $after_process_file =
	    "/usr/local/mccs/log/sps/dla_reports/"
	  . $reporttime
	  . "_dla_post_activity.csv";
	my $rpt_hash = get_after_process();
    if ($rpt_hash) {
	#-------------------------------------------------------------
	# Always print hash in same order...
	#  Hash is Hash and does what it wants
	#-------------------------------------------------------------
	my @order = (
		'Po_Id',         'Version_No',
		'Document_Type', 'Po_Type',
		'For_Site_Id',   'Shipped_To_Site_Id',
		'Order_Date',    'Approved_Ind',
		'Origin',        'Reason_Id',
		'Created_By',    'Creation_Date',
		'Ride_In_Date'
	);

	open ($FH, ">",$after_process_file) or croak "Can not open output\n";
	print $FH join( ', ', @order );    #Header
	print $FH "\n";
	foreach my $rec_id ( sort keys %{$rpt_hash} ) {
		print Dumper $rpt_hash->{$rec_id} if ($debug);
		print $FH join( ', ', map { $rpt_hash->{$rec_id}->{ lc($_) } } @order );
		print $FH "\n";
	}
	close $FH;

	my $html_table;
	$html_table = html_header();
	$html_table .= qq(<span style="background-color: lightgreen;">The following is a list of DLA POs that have been PROCESSED and SENT via Sftp.</span><br><br>\n\n);
	$html_table .= qq(<body><table class="e832_table" width="100%">);
	$html_table .= join( '', map { "<th> $_ </th>" } @order );
	foreach my $rec_id ( sort keys %{$rpt_hash} ) {
		$html_table .= "<tr>";
		$html_table .= join( '',
			map { "<td>" . lc( $rpt_hash->{$rec_id}->{ lc($_) } ) . "</td>" }
			  @order );
		$html_table .= "</tr>\n";
	}
	$html_table .= "</table></body><br><br>";

	$sm->message_type('TEXT/HTML');
	send_mail(
		body => ($html_table),
		subj => 'Records Sent ',
		logs => ( $after_process_file, )
	);
    }
    return;
}

sub after_error_process_rpt {
	my $FH;
	my $after_process_file =
	    "/usr/local/mccs/log/sps/dla_reports/"
	  . $reporttime
	  . "_dla_post_error_activity.csv";
	my $rpt_hash = get_after_error_process();
    if ($rpt_hash) {
	#-------------------------------------------------------------
	# Always print hash in same order...
	#  Hash is Hash and does what it wants
	#-------------------------------------------------------------
	my @order = (
		'Po_Id',         'Version_No',
		'Document_Type', 'Po_Type',
		'For_Site_Id',   'Shipped_To_Site_Id',
		'Order_Date',    'Reason_Id',
		'Created_By',    'Creation_Date',
		'Ride_In_Date'
	);

	open ($FH, ">",$after_process_file) or croak "Can not open output\n";
	print $FH join( ', ', @order );    #Header
	print $FH "\n";
	foreach my $rec_id ( sort keys %{$rpt_hash} ) {
		print Dumper $rpt_hash->{$rec_id} if ($debug);
		print $FH join( ', ', map { $rpt_hash->{$rec_id}->{ lc($_) } } @order );
		print $FH "\n";
	}
	close $FH;

	my $html_table;
	$html_table = html_header();
	$html_table .= qq(The following is a list of DLA POs that had <span style="background-color: red;">ERRORS.</span><br><br>\n\n);
	$html_table .= qq(<body><table class="e832_table" width="100%">);
	$html_table .= join( '', map { "<th> $_ </th>" } @order );
	foreach my $rec_id ( sort keys %{$rpt_hash} ) {
		$html_table .= "<tr>";
		$html_table .= join( '',
			map { "<td>" . lc( $rpt_hash->{$rec_id}->{ lc($_) } ) . "</td>" }
			  @order );
		$html_table .= "</tr>\n";
	}
	$html_table .= "</table></body><br><br>";

	$sm->message_type('TEXT/HTML');
	send_mail(
		body => ($html_table),
		subj => 'ERROR Records ',
		logs => ( $after_process_file, ));
    }
    return;
}

#-----------------------------------------------------------------------------
#  Email and logging Functions  ------ Boring Stuff ---------
#-----------------------------------------------------------------------------
sub send_mail {

	#my @body = @_; #Just going to put every thing in the body good or bad
	my %args = @_;

	my $emails;

#	$emails->{larry} ='larry.d.lewis@usmc-mccs.org';
	#$emails->{jessica} ='Jessica.Gearhart@usmc-mccs.org';
	$emails = $cfg->sps_dla_global->{emails};

	$sm->subject( 'SPS DLA Outbound XML ' . $args{subj} . $g_long_date );
	$sm->sendTo($emails);
	$sm->msg( $args{body} );

	#$sm->logObj($log_obj);
	#$sm->verboseLevel($debug);
	$sm->hostName($host);

	$sm->attachments( $args{logs} ) if ( defined $args{logs} );

	$sm->send_mail_attachment();
	return;
}

#-----------------------------------------------------------------------------
#  Other needed stuff  -- nothing of interest here --
#-----------------------------------------------------------------------------
sub log_debug {
	my $str = shift;
	my $log_entry = join( '', "(PID $$) ", $str );
	if ($log_obj) { $log_obj->info($log_entry); }
	debug($log_entry);
	return;
}

sub log_warn {
	my $str = shift;
	my $log_entry = join( '', "(PID $$) ", $str );
	$sm->errorLevel('warning');
	if ($log_obj) { $log_obj->warn($log_entry); }
	send_mail(
		body => ( "WARNING on " . __FILE__ . ' ' . $g_long_date, $log_entry ),
		logs => ( $log, )
	);
	debug($log_entry);
	return;
}

sub fatal_error {
	my $str = shift;
	$sm->errorLevel('error');
	my $log_entry = join( '', "(PID $$) ", $str );
	if ($log_obj) { $log_obj->error($log_entry); }
	send_mail(
		body => ( "ERROR on " . __FILE__ . ' ' . $g_long_date, $log_entry ),
		logs => ( $log, )
	);
	croak $log_entry;
}

sub debug {
	my $str = shift;
	if ($debug) {
		print "DEBUG: ", $str, "\n";
	}
	return;
}

#---------------------------------------------------------------------
# MAIN PROGRAM - Do not modify main below.
# Use the main above aka my_main
#---------------------------------------------------------------------

# Want to catch warning
#$SIG{__WARN__} = sub { log_warn("@_") };

# Execute the main

	if   ( defined $report ) { report_proc() }
	else                     { mainLine() }
if ($@) {
	fatal_error($@);
}
unlink($lock_file);
#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------
