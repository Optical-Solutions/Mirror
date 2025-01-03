package MCCS::CMS::Loads::RCPTDETAIL;
use strict;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::RcptDetailRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
}

sub get_sql {
    my $self               = shift;

"
select 
  hdr.receipt_id,
  to_char(hdr.po_id) po_id,
  rcpt.creation_date,
  rcv.Style_Id || rcv.Color_Id || Nvl(Lpad(rcv.Size_Id,5,'*'),'NOSIZ') || Nvl(Lpad(rcv.Dimension_Id,5,'*'),'NODIM') Sku_Key ,
  merch.get_bar_code(rcv.business_unit_id,rcv.style_id,rcv.color_id,rcv.dimension_id,rcv.size_id,null,null,null) bar_code_id,
  rcv.fin_line_no,
  case when rcv.estimated_landed_cost < 0 then '-' end item_cost_sign,
  case when rcv.estimated_landed_cost < 0 then ((round(rcv.estimated_landed_cost,2) * 100) * -1) else (round(rcv.estimated_landed_cost,2) * 100) end item_cost,
  case when rcv.retail_price < 0 then '-' end item_retail_sign,
  case when rcv.retail_price < 0 then ( (round(rcv.retail_price,2) * 100) * -1) else (round(rcv.retail_price,2) * 100) end item_retail,
  rcpt.site_id,
  case when rcv.estimated_landed_cost < 0 then '-' end store_cost_sign,
  case when rcv.estimated_landed_cost < 0 then ( (round(rcv.estimated_landed_cost,2) * 100) * -1) else (round(rcv.estimated_landed_cost,2) * 100) end store_cost,
  case when rcv.retail_price < 0 then '-' end store_retail_sign,
  case when rcv.retail_price < 0 then ( (round(rcv.retail_price,2) * 100) * -1) else (round(rcv.retail_price,2) * 100) end store_retail,
  case when trunc(rcv.qty_received) < 0 then '-' end store_rcv_units_sign,
  case when trunc(rcv.qty_received) < 0 then (trunc(rcv.qty_received) * -1) else trunc(rcv.qty_received) end store_rcv_units,
  case when rcpt.asn_id is not null then rcpt.received_date else rcpt.creation_date end creation_date,
  po.order_date
  
from 
 (select * from iro_po_receipt_headers where trunc(ride_in_date) = trunc(to_date('rdi_date', 'yyyymmdd' ) -1 ) )  HDR
  join receipts rcpt on (hdr.po_id = rcpt.po_id and 
                         hdr.receipt_id = rcpt.receipt_id )
  join received_items rcv on (hdr.receipt_id = rcv.receipt_id)
  /*
  Left Join (Select * From Bar_Codes Where Sub_Type In ('EAN','UPCA') ) Bc 
           On (rcv.Style_Id = Bc.Style_Id And
               rcv.Color_Id = Bc.Color_Id And
               rcv.Size_Id = Bc.Size_Id And
               Nvl(rcv.Dimension_Id, 'NODIM') = Nvl(Bc.Dimension_Id, 'NODIM') )
  */
  left join purchase_orders po on 
       (po.po_id = hdr.po_id and
        ((po.reason_id <> 1 or po.reason_id is null ) and 
        (po.reason_sub_type = 'PURC_ORDER' or po.reason_sub_type is null))  )
union all
(select 
  hdr.receipt_id,
  'D'||substr(hdr.vendor_id,2,10) po_id,
  rcpt.creation_date,
  rcv.Style_Id || rcv.Color_Id || Nvl(Lpad(rcv.Size_Id,5,'*'),'NOSIZ') || Nvl(Lpad(rcv.Dimension_Id,5,'*'),'NODIM') Sku_Key ,
  merch.get_bar_code(rcv.business_unit_id,rcv.style_id,rcv.color_id,rcv.dimension_id,rcv.size_id,null,null,null) bar_code_id,
  rcv.fin_line_no,
  case when rcv.estimated_landed_cost < 0 then '-' end item_cost_sign,
  (round(rcv.estimated_landed_cost,2) * 100) item_cost,
  case when rcv.retail_price < 0 then '-' end item_retail_sign,
  (round(rcv.retail_price,2) * 100) item_retail,
  rcpt.site_id,
  case when rcv.estimated_landed_cost < 0 then '-' end store_cost_sign,
  (round(rcv.estimated_landed_cost,2) * 100) store_cost,
  case when rcv.retail_price < 0 then '-' end store_retail_sign,
  (round(rcv.retail_price,2) * 100) store_retail,
  case when trunc(rcv.qty_received) < 0 then '-' end store_rcv_units_sign,
  case when trunc(rcv.qty_received) < 0 then (trunc(rcv.qty_received) * -1) else trunc(rcv.qty_received) end store_rcv_units,
  case when rcpt.asn_id is not null then rcpt.received_date else rcpt.creation_date end creation_date,
  rcpt.creation_date
  
from 
 (select * from iro_po_receipt_headers where trunc(ride_in_date) = trunc(to_date('rdi_date', 'yyyymmdd' ) -1 ) )  HDR
  join receipts rcpt on (rcpt.po_id is null and 
                         hdr.receipt_id = rcpt.receipt_id )
  join received_items rcv on (hdr.receipt_id = rcv.receipt_id)
  /*
  Left Join (Select * From Bar_Codes Where Sub_Type In ('EAN','UPCA') ) Bc 
           On (rcv.Style_Id = Bc.Style_Id And
               rcv.Color_Id = Bc.Color_Id And
               rcv.Size_Id = Bc.Size_Id And
               Nvl(rcv.Dimension_Id, 'NODIM') = Nvl(Bc.Dimension_Id, 'NODIM') )
  */
 where 
rcpt.receipt_type = 'NO PO'  
--and trunc(rcpt.received_date) = trunc(to_date('rdi_date', 'yyyymmdd' ) - 1) 
  )
  
"

;

}

sub make_record {
    my $self = shift;
    my ($ReceiverNumber, $PoNumber,  $UniqueAddenda, $Sku,  $Upc, $LineNumber, 
        $ItemCostSign,  $ItemCost, $ItemRetailSign, $ItemRetail, $StoreNumber, $StoreCostSign, $StoreCost, 
        $StoreRetailSign, $StoreRetail, $StoreRcvUnitsSign, $StoreRcvUnits,
        $ReceiverCreateDate, $PoUniqueAddenda
    ) = @_;
  
    my $obj =
        MCCS::CMS::RecordLayout::RcptDetailRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   ReceiverNumber      => $ReceiverNumber,
            PoNumber            => $PoNumber,
            UniqueAddenda       => $UniqueAddenda ? MCCS::CMS::DateTime->new($UniqueAddenda) : undef, 
            Sku                 => $Sku,
            Upc                 => $Upc,
            LineNumber          => $LineNumber,
            ItemCostSign        => $ItemCostSign,
            ItemCost            => $ItemCost,
            ItemRetailSign      => $ItemRetailSign,
            ItemRetail          => $ItemRetail,
            StoreNumber         => $StoreNumber,
            StoreCostSign       => $StoreCostSign,
            StoreCost           => $StoreCost,
            StoreRetailSign     => $StoreRetailSign,
            StoreRetail         => $StoreRetail,
            StoreRcvUnitsSign   => $StoreRcvUnitsSign,
            StoreRcvUnits       => $StoreRcvUnits,
            ReceiverCreateDate  => $ReceiverCreateDate ? MCCS::CMS::DateTime->new($ReceiverCreateDate) : undef, 
            PoUniqueAddenda     => $PoUniqueAddenda ? MCCS::CMS::DateTime->new($PoUniqueAddenda) : undef,      
        }
      );
    $obj->to_string();
}

sub get_filename {
    my $self = shift;
    my $date = shift;
    $date . '_ReceiverDetail.dat';
}


1;

__END__

=pod

=head1 NAME


=head1 SYNOPSIS



=head1 DESCRIPTION


=head1 AUTHOR
Larry Lewis  L<larry.d.lewis@usmc-mccs.org|mailto:larry.d.lewis@usmc-mccs.org>

=cut
