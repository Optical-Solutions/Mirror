package MCCS::CMS::Loads::PODETAIL;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::PoDetailRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return;
}

sub get_sql {
    my $self               = shift;

"
Select 
  P.Po_Id, 
  p.Style_Id || p.Color_Id || Nvl(Lpad(p.Size_Id,5,'*'),'NOSIZ') || Nvl(Lpad(p.Dimension_Id,5,'*'),'NODIM') Sku_Key ,
  merch.get_bar_code(p.business_unit_id,p.style_id,p.color_id,p.dimension_id,p .size_id,null,null,null) bar_code_id, 
  p2.site_id, 
  --P.Trans_Num, 
  Case When P.Qty_Ordered < 0 Then '-' End Po_Qty_Sign,  
  case when P.Qty_Ordered < 0 then (P.Qty_Ordered * -1) else P.Qty_Ordered end Qty_Ordered,  
  case when p.estimated_landed_cost < 0 then '-' end cost_sign,
  case when p.estimated_landed_cost < 0 then ((round(p.estimated_landed_cost,2) * 100) * -1 )else (round(p.estimated_landed_cost,2) * 100) end  po_estimated_landed_cost,
  P.Fin_Line_No, 
  Po.Order_Date,
   round(Get_Permanent_Retail_Price(hdr.business_unit_id, p.site_id, p.style_id, p.color_id, p.dimension_id, p.size_id, p2.ride_in_date, ''),2) * 100  item_retail
From 
  (Select Business_Unit_Id, Po_Id, Version_No,  Trans_Num From Iro_Po_Headers i
  Where
  -- cancelled_ind <> 'Y' and
  trunc(Ride_In_Date) = Trunc(to_date('rdi_date', 'yyyymmdd' ) - 1) and
  version_no = (select max(version_no) From Iro_Po_Headers p
                Where  
                     --cancelled_ind <> 'Y' and
                       trunc(Ride_In_Date) = Trunc(to_date('rdi_date', 'yyyymmdd' ) - 1)
                       and  i.po_id = p.po_id and i.trans_num = p.trans_num)) Hdr
    
    Left Join Po_Version_Details\@Mc2p P  On (Hdr.Po_Id = P.Po_Id And
                                             Hdr.Version_No = P.Version_No and P.po_detail_status <> 'D') 
                                             
    Left Join Iro_Po_Details P2  On (P.Po_Id = P2.Po_Id And
                                     p.style_id = p2.style_id And
                                     P.Fin_Line_No = p2.fin_line_no) 

    Join Po_Version_Headers\@mc2p Po   On (Hdr.Business_Unit_Id = Po.Business_Unit_Id And
                               Hdr.Po_Id = Po.Po_Id And
                               Hdr.Version_No = Po.Version_No and
      ((po.reason_id <> 1 or po.reason_id is null ) and 
       (po.reason_sub_type = 'PURC_ORDER' or po.reason_sub_type is null)) )  
    left join v_po_same_day_cancel sdc on sdc.po_id = hdr.po_id
 where nvl(sdc.CANCELLED_IND, 'N') = 'N'
"

;
return;
}

sub make_record {
    my $self = shift;
    my ($PoNumber,  $Sku,  $Upc, $StoreNumber,  $StoreOrdUnitsSign,
        $StoreOrdUnits, $StoreCostSign, $StoreCost, $LineNumber, $UniqueAddenda, $ItemRetail
    ) = @_;
  
    my $obj =
        MCCS::CMS::RecordLayout::PoDetailRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   PoNumber           => $PoNumber,
            UniqueAddenda      => $UniqueAddenda ? MCCS::CMS::DateTime->new($UniqueAddenda) : undef, 
            Sku                => $Sku,
            Upc                => $Upc,               
            LineNumber         => $LineNumber,
            StoreOrdUnitsSign  => $StoreOrdUnitsSign,
            StoreOrdUnits      => $StoreOrdUnits,
            StoreNumber        => $StoreNumber,
            StoreCostSign      => $StoreCostSign,
            StoreCost          => $StoreCost,
            ItemRetail         => $ItemRetail,
           
        }
      );
    $obj->to_string();
    return;
}

sub get_filename {
    my $self = shift;
    my $date = shift;
    $date . '_PODetail.dat';
    return;
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
