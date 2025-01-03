package MCCS::CMS::Loads::RCPTHEADER;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::RcptHeaderRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return;
}

sub get_sql {
    my $self               = shift;

return "
select  
 hdr.receipt_id,
 to_char(hdr.po_id) po_id,
 --hdr.trans_num,
 sv.scac_code,
 case when rcpt.cartons_counted < 0 then '-' end rcvr_ctr_sign,
 case when rcpt.cartons_counted < 0 then (rcpt.cartons_counted * -1) else rcpt.cartons_counted end receiver_carton_cnt,
 rcpt.po_document_type, 
 rcpt.site_id,
 case when trunc(rcpt.qty_received) < 0 then '-' end total_units_sign,
 case when trunc(rcpt.qty_received)  < 0 then (trunc(rcpt.qty_received)* -1) else trunc(rcpt.qty_received)end total_units,
 case when rcpt.asn_id is not null then rcpt.received_date else rcpt.creation_date end creation_date,
 rcpt.received_date,
 po.order_date,
rcpt.receipt_type
from 
 (select * from iro_po_receipt_headers where trunc(ride_in_date) = trunc(to_date('rdi_date', 'yyyymmdd' ) -1 ) )  HDR
  join receipts rcpt on (hdr.po_id = rcpt.po_id and
                         hdr.receipt_id = rcpt.receipt_id)
  left join ship_vias\@mc2p sv on (sv.business_unit_id = '30' and sv.ship_via_id = rcpt.ship_via_id)
  left join purchase_orders po on (po.po_id = hdr.po_id and
                                   ((po.reason_id <> 1 or po.reason_id is null ) and 
                                    (po.reason_sub_type = 'PURC_ORDER' or po.reason_sub_type is null))  )
union all
(select  
 rcpt.receipt_id,
 'D'||substr(hdr.vendor_id,2,10) po_id,
 sv.scac_code,
 case when rcpt.cartons_counted < 0 then '-' end rcvr_ctr_sign,
 case when rcpt.cartons_counted < 0 then (rcpt.cartons_counted * -1) else rcpt.cartons_counted end receiver_carton_cnt,
 rcpt.po_document_type, 
 rcpt.site_id,
 case when trunc(rcpt.qty_received)< 0 then '-' end total_units_sign,
 case when trunc(rcpt.qty_received) < 0 then (trunc(rcpt.qty_received) * -1) else trunc(rcpt.qty_received) end total_units,
 case when rcpt.asn_id is not null then rcpt.received_date else rcpt.creation_date end creation_date,
 rcpt.received_date,
rcpt.creation_date order_date,
 'DD' receipt_type
from 
  
 (select * from iro_po_receipt_headers where trunc(ride_in_date) = trunc(to_date('rdi_date', 'yyyymmdd' ) -1 ) )  HDR
  join receipts rcpt on (rcpt.po_id is null and
                         hdr.receipt_id = rcpt.receipt_id)
  left join ship_vias\@mc2p sv on (sv.business_unit_id = '30' and sv.ship_via_id = rcpt.ship_via_id)
where 
rcpt.receipt_type = 'NO PO'  
--and trunc(rcpt.received_date) = trunc(to_date('rdi_date', 'yyyymmdd' ) - 1) 
)
"
;
}

sub make_record {
    my ($self,$ReceiptId,  $PoNumber,  $CarrierNumber, $RcvCartCtnSign, $RcvCartCtn, $Status,
        $ReceiverLocation, $TotalUnitsSign, $TotalUnits, $CreateDate, $CompletionDate,
        $PoUniqueAddenda, $ReceiverType
    ) = @_;
  
    my $obj =
        MCCS::CMS::RecordLayout::RcptHeaderRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   ReceiverNumber       => $ReceiptId,
        	PoNumber             => $PoNumber,
            CarrierNumber        => $CarrierNumber,
            RcvCartCtnSign       => $RcvCartCtnSign,
            RcvCartCtn           => $RcvCartCtn,
            Status               => $Status,
            ReceiverLocation     => $ReceiverLocation,
            TotalUnitsSign       => $TotalUnitsSign,
            TotalUnits           => $TotalUnits,
            CreateDate           => $CreateDate ? MCCS::CMS::DateTime->new($CreateDate) : undef,               
            CompletionDate       => $CompletionDate ? MCCS::CMS::DateTime->new($CompletionDate) : undef, 
            UniqueAddenda        => $CreateDate ? MCCS::CMS::DateTime->new($CreateDate) : undef, 
            PoUniqueAddenda      => $PoUniqueAddenda ? MCCS::CMS::DateTime->new($PoUniqueAddenda) : undef, 
            ReceiverType         => $ReceiverType,
           
        }
      );
 return  $obj->to_string();

}

sub get_filename {
    my $self = shift;
    my $date = shift;
    return $date . '_ReceiverHeader.dat';
}


1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::LOCATION - MCCS::SAS LOCATION (site) record extract

=head1 SYNOPSIS

MCCS::SAS::LOCATION->new( MCCS::SAS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the location (site) hiearchy in MCCS::SAS. This is a daily/weekly tracking load.

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>
Larry Lewis  L<larry.d.lewis@usmc-mccs.org|mailto:larry.d.lewis@usmc-mccs.org>

=cut
