package MCCS::CMS::Loads::ASNHEADER;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::AsnHeaderRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return; ##TO BE DETERMINED.
}

sub get_sql {
    my $self               = shift;

return "
select 
l40.PO_ID,
mstr.key_data, 
substr(mstr.partnership_id, 1,(length(mstr.partnership_id) -3)) vendor,
mstr.date_created,
regexp_replace(l20.BOL_NUMBER, '[^[:print:]]', '' ) BOL_NUMBER,
l20.pro_number,
l20.SCAC_CARRIER_CODE,
l20.SHIPPED_DATE, 
case when l40.number_cartons < 0 then '-' end carton_sign,
case when l40.number_cartons < 0 then (l40.number_cartons *-1) else l40.number_cartons end number_cartons,
l30.ship_from_zip_code,
po.creation_date,
--mstr.date_created processed,
l30.SHIP_TO_CODE,
l20.shipped_date
from 
rdiusr.E856_MSTR_RECORDS mstr

left join rdiusr.E856_HEADER_RECORDS L10 /* lv10 */
 on (L10.emr_fk_id = mstr.emr_pk_id)
left join rdiusr.E856_SHIPMENT_RECORDS L20 /* lv20 */
 on (l20.emr_fk_id = l10.emr_fk_id and
     l20.ehr_fk_id = l10.ehr_pk_id)
left join rdiusr.E856_SHIP_TO_FROM_RECORDS L30 /* lv30 */
 on (l30.emr_fk_id = l10.emr_fk_id and
     l30.ehr_fk_id = l10.ehr_pk_id)
left join rdiusr.E856_ORDER_RECORDS L40 /* lv40 */
 on (l40.emr_fk_id = l10.emr_fk_id and
     l40.ehr_fk_id = l10.ehr_pk_id)
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
    my ($self, $PoNumber,  $SequenceNumber,  $VendorNumber, $ReceivedDate,  $BOL, $ProNumber,
        $SCAC, $PickUpDate, $CartonCountSign, $CartonCount, $OriginZip, $UniqueAddenda,
        $DropLoc
    ) = @_;
    
    my $obj =
        MCCS::CMS::RecordLayout::AsnHeaderRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   PoNumber          => $PoNumber,
            SequenceNumber    => $SequenceNumber,
            VendorNumber      => $VendorNumber,
            ReceivedDate      => $ReceivedDate ? MCCS::CMS::DateTime->new($ReceivedDate) : undef, 
            BOL               => $BOL,
            FreightBillNumber => substr($ProNumber, 0,20),
            SCAC              => $SCAC,
            PickUpDate        => $PickUpDate ? MCCS::CMS::DateTime->new($PickUpDate) : undef, 
            CartonCountSign   => $CartonCountSign,
            CartonCount       => $CartonCount,
            OriginZip         => substr($OriginZip, 0,10),
            UniqueAddenda     => $UniqueAddenda ? MCCS::CMS::DateTime->new($UniqueAddenda) : undef, 
            DropLoc          => $DropLoc,
            Division          => ''
            
           
        }
      );
    return $obj->to_string();
}

sub get_filename {
    my $self = shift;
    my $date = shift;
    return $date . '_AsnHeader.dat';
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
