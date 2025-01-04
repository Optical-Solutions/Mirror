package MCCS::CMS::Loads::AUDITDATA;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::AuditDataRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return;  ##TO BE DETERMINED
}

sub get_sql {
    my $self               = shift;

return "
select 
r.po_id,
to_char(p.CREATION_DATE, 'yyyymmdd') po_create_date,
r.site_Id,
to_char(sysdate, 'yyyymmdd') audit_date,
r.RECEIPT_ID || to_char(RECEIVED_DATE, 'yyyymmdd'),
r.RECEIVED_BY,
nvl(r.waybill ,r.asn_id ) ucc128,
merch.get_bar_code(ri.business_unit_id,ri.style_id,ri.color_id,ri.dimension_id,ri.size_id,null,null,null) bar_code_id,
ri.QTY_RECEIVED,
ri.Style_Id || ri.Color_Id || Nvl(Lpad(ri.Size_Id,5,'*'),'NOSIZ') || Nvl(Lpad(ri.Dimension_Id,5,'*'),'NODIM') Sku_Key, 
r.ship_via_id

from receipts r
left join purchase_orders p on (p.business_unit_id = r.business_unit_id and
                                p.po_id = r.po_id)
left join RECEIVED_ITEMS ri on (ri.business_unit_id = r.business_unit_id and
                                ri.receipt_id = r.receipt_id)
where r.site_id in ('70001','60001')
and trunc(r.RIDE_PROCESSED_FIN_DATE) = trunc(to_date('rdi_date', 'yyyymmdd' ) - 1 )
/*and trunc(r.RIDE_PROCESSED_FIN_DATE) in (  trunc(to_date('rdi_date', 'yyyymmdd' ) - 1 ),
                                         
                                         Trunc(To_Date('20161205', 'yyyymmdd' ) ),
                                         Trunc(To_Date('20161206', 'yyyymmdd' ) ) )*/
and r.adv_shipping_notice_ind = 'N'
and nvl(r.waybill ,r.asn_id ) is not null
and r.RECEIVED_BY not in ('WMS', 'MERCH')
  
"

;
}

sub make_record {
    my ($self, $PoNumber, $PoCreateDate, $WareHouse,  $AuditDate, $UniqueIdent, 
        $RecvBy, $Ucc128, $BarCode, $Qty, $Sku, $StoreLocator
    ) = @_;
  
    my $obj =
        MCCS::CMS::RecordLayout::AuditDataRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   PoCreateDate      => $PoCreateDate,
            PoNumber          => $PoNumber,
            UCC128            => $Ucc128, 
            UniqueIdent       => $UniqueIdent, 
            BarCode           => $BarCode,
            WareHouse         => $WareHouse,
            AuditDate         => $AuditDate,
            Auditor           => $RecvBy,
            Qty               => $Qty,
            Sku               => $Sku,
            StoreLocator      => $StoreLocator,
        }
      );
    return $obj->to_string();
}

sub get_filename {
    my $self = shift;
    my $date = shift;
    return $date . '_AuditData.dat';
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
