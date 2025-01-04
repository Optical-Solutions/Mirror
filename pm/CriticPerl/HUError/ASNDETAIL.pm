package MCCS::CMS::Loads::ASNDETAIL;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::AsnDetailRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return; ##To be determined
}

sub get_sql {
    my $self               = shift;

return "
select 
l40.PO_ID,
mstr.key_data, 
substr(mstr.partnership_id, 1,(length(mstr.partnership_id) -3)) vendor,
bc.Style_Id || bc.Color_Id || Nvl(Lpad(bc.Size_Id,5,'*'),'NOSIZ') || Nvl(Lpad(bc.Dimension_Id,5,'*'),'NODIM') Sku_Key ,
l50.CARTON_CODE,
case when l40.mark_for_site is not null then
l40.mark_for_site else l30.SHIP_TO_CODE end SHIP_TO_CODE,
l60.barcode_id,
case when l60.QUANTITY_SHIPPED < 0 then '-' end qty_sign,
case when l60.QUANTITY_SHIPPED < 0 then (l60.QUANTITY_SHIPPED * -1) else l60.QUANTITY_SHIPPED end QUANTITY_SHIPPED,
case when sv.estimated_landed_cost < 0 then '-' end cost_sign,
case when sv.estimated_landed_cost < 0 then ((round(sv.estimated_landed_cost,2) * 100) * -1) else (round(sv.estimated_landed_cost,2) * 100) end estimated_landed_cost,
'' retail_sign,
(round(Get_Permanent_Retail_Price('30', l30.ship_to_code, bc.Style_Id, bc.Color_Id, bc.Dimension_Id, bc.Size_Id, Sysdate, Null),2) * 100) retail_price,
case when OUTER_PACK_QTY < 0 then '-' end pack_sign,
sv.OUTER_PACK_QTY,
po.creation_date
from 
E856_MSTR_RECORDS mstr

left join E856_HEADER_RECORDS L10 /* lv10 */
 on (L10.emr_fk_id = mstr.emr_pk_id)

left join E856_SHIPMENT_RECORDS L20 /* lv20 */
 on (l20.emr_fk_id = l10.emr_fk_id and
     l20.ehr_fk_id = l10.ehr_pk_id)
     
left join E856_SHIP_TO_FROM_RECORDS L30 /* lv30 */
 on (l30.emr_fk_id = l10.emr_fk_id and
     l30.ehr_fk_id = l10.ehr_pk_id)

left join E856_ORDER_RECORDS L40 /* lv40 */
 on (l40.emr_fk_id = l10.emr_fk_id and
     l40.ehr_fk_id = l10.ehr_pk_id)
     
left join E856_PACK_RECORDS L50 /* lv50 */
 on (l50.emr_fk_id = l40.emr_fk_id and
     l50.ehr_fk_id = l40.ehr_fk_id and
     l50.eor_fk_id = l40.eor_pk_id)

left join E856_ITEM_RECORDS L60 /* lv60 */
 on (l60.emr_fk_id = l40.emr_fk_id and
     l60.ehr_fk_id = l40.ehr_fk_id and
     l60.eor_fk_id = l40.eor_pk_id and
     l60.epr_fk_id = l50.epr_pk_id)

left Join(Select * From Bar_Codes\@mc2p Where Sub_Type In ('EAN','UPCA') )  bc 
 on (bc.bar_code_id = l60.barcode_id ) 

left join style_vendors\@mc2p sv 
on (sv.business_unit_id = '30' and
    sv.style_id = bc.style_id and
    sv.vendor_id = substr(mstr.partnership_id, 1,(length(mstr.partnership_id) -3)) and
    (nvl(sv.site_id, 'null')= nvl(trim(l30.SHIP_TO_CODE),'null') or nvl(sv.site_id, 'null') = 'null') and 
    mstr.date_created between sv.start_date and sv.end_date )

left join purchase_orders\@mc2p po 
on (l40.PO_ID = po.po_id)


where 
--mstr.date_created = trunc(to_date('rdi_date', 'yyyymmdd' ) ) 
mstr.date_created > to_date('20220909', 'yyyymmdd' )
and mstr.date_created < to_date('20220923', 'yyyymmdd' )

"
;

}

sub make_record {
    my ($self, $PoNumber,  $SequenceNumber,  $VendorNumber, $SKU,  $UCC128,
        $Store, $UPC, $QtySign, $Qty, $CostSign, $Cost, $RetailSign, $Retail,
        $PackSign, $PackSize, $UniqueAddenda
    ) = @_;

  
    my $obj =
        MCCS::CMS::RecordLayout::AsnDetailRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   PoNumber          => $PoNumber,
            SequenceNumber    => $SequenceNumber,
            VendorNumber      => $VendorNumber,
            SKU      => $SKU, 
            UCC128               => $UCC128,
            Store              => $Store,
            UPC              => $UPC,
            QtySign              => $QtySign,
            Qty              => $Qty,
            CostSign        => $CostSign, 
            Cost   => $Cost,
            RetailSign       => $RetailSign,
            Retail         => $Retail,
            UM         => '',
            PackSign         => $PackSign,
            PackSize         => $PackSize,
            UniqueAddenda     => $UniqueAddenda ? MCCS::CMS::DateTime->new($UniqueAddenda) : undef, 
            Division          => ''
            
           
        }
      );
    return $obj->to_string();
}

sub get_filename {
#   my $date = `date '+%Y%m%d'`;
#    chomp($date);
#    $date . '_AsnDetail.dat';	
	my $self = shift;
    my $date = shift;
    return $date . '_AsnDetail.dat';
#
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
