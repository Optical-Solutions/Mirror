package MCCS::CMS::Loads::POHEADER;
use strict;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::PoHeaderRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
}

sub get_sql {
    my $self               = shift;

"
 
 Select
Po.Po_Id,
Ipd.Department_Id,
po.Vendor_id,
Po.Ship_Not_Before_Date,
Po.delivery_date,
Po.Order_Date,
po.origin,
ipd.shipped_to_site_id site_id,
po.cancelled_ind,
--po.document_type,
Po.Po_Type,
po.fob_point,
case when ipdd.po_qty_ordered < 0 then '-' end order_sign,
case when ipdd.po_qty_ordered < 0 then (ipdd.po_qty_ordered * -1) else ipdd.po_qty_ordered end po_qty_ordered ,
case when ipdd.po_estimated_landed_cost < 0 then '-' end cost_sign,
case when ipdd.po_estimated_landed_cost < 0 then ((round(ipdd.po_estimated_landed_cost,2) * 100) * -1) else (round(ipdd.po_estimated_landed_cost,2) * 100) end Estimated_landed_cost,
--ipdd.full_cost,
Hdr.Version_No,
--Po.Term_Id
t.description,
iph.ride_in_date,
Case When Iph.Cancelled_Ind = 'Y'
then Po.Cancelled_Date end Canellation_date ,
Case when po.EDI_IND = 'Y'
then po.record_created_date end edi_date,
po.event_id,
decode(lower(po.pre_ticket_type), 'no', 'N',
                           'vendor','Y',
                           'supply', 'S') pre_ticket_flag,
(select remark from specific_remarks
where REMARK_KEY = po.remark_key
and level_id = 'HEADER'
and GENERIC_REMARK_ID is null
and REMARK_LINE = 1
and remark not like '%'||chr(9)||'%') remark,
(select reason_id||' '||description from reasons
where sub_type = 'PURC_ORDER'
and reason_id = po.reason_id) reason_desc,
Case When Iph.Cancelled_Ind = 'Y'
then po.po_created_by end cancelled_by
From
 (
Select Business_Unit_Id, Po_Id, Version_No,  Trans_Num From Iro_Po_Headers i
  Where
  -- cancelled_ind <> 'Y' and
  trunc(Ride_In_Date) = Trunc(to_date('rdi_date', 'yyyymmdd' ) - 1) and
  version_no = (select max(version_no) From Iro_Po_Headers p
                Where  
                 --cancelled_ind <> 'Y' and
                       trunc(Ride_In_Date) = Trunc(to_date('rdi_date', 'yyyymmdd' ) - 1)
                       and  i.po_id = p.po_id and i.trans_num = p.trans_num)) Hdr


  Join Iro_Po_Headers Iph On (Hdr.Business_Unit_Id = Iph.Business_Unit_Id And
      Hdr.Po_Id = Iph.Po_Id And
      Hdr.Version_No = iph.Version_No)

  --Join Purchase_Orders Po
  Join po_version_headers Po
  On (Hdr.Business_Unit_Id = Po.Business_Unit_Id And
      Hdr.Po_Id = Po.Po_Id And
      Hdr.Version_No = Po.Version_No and
      ((po.reason_id <> 1 or po.reason_id is null ) and
       (po.reason_sub_type = 'PURC_ORDER' or po.reason_sub_type is null)) )

/* USE THE HIGHEST NET COST  */
  Join (SELECT Po_Id, max(department_id) department_id, shipped_to_site_id , trans_num
        from
         (SELECT  Po_Id, department_id, max(shipped_to_site_id) shipped_to_site_id, trans_num
          FROM
            (SELECT Po_Id, v.department_id, shipped_to_site_id, trans_num,
              rank() over ( partition BY po_id, trans_num order by SUM(po_qty_ordered * po_estimated_landed_cost) DESC ) rnk,
              rank() over ( partition BY po_id, trans_num order by shipped_to_site_id DESC ) rnk_site
             FROM Iro_Po_Details I
             JOIN Styles S  ON I.Style_Id = S.Style_Id
             JOIN v_dept_class_subclass v  ON (v.section_id = s.section_id)
             --WHERE po_id  = 1064479
            GROUP BY po_id, department_id, shipped_to_site_id, trans_num)
         WHERE  rnk = 1
         group by  Po_Id, department_id, trans_num) rt
group by Po_Id,  shipped_to_site_id , trans_num) Ipd
    On (Hdr.Po_Id = Ipd.Po_Id And
        Hdr.Trans_Num = Ipd.Trans_Num )
/* END HIGHEST NET COST */

/* get the group sum of data */
  Join  (SELECT
    Po_Id,  trans_num,
    po_qty_ordered, po_estimated_landed_cost, full_cost,
    ordered_retail, order_cost
  FROM
    (SELECT
      Po_Id, trans_num,
      SUM(po_qty_ordered) po_qty_ordered,
      SUM(po_estimated_landed_cost) po_estimated_landed_cost,
      SUM(po_qty_ordered * po_estimated_landed_cost) full_cost,
      MAX(on_order_cost) order_cost,
      SUM(ordered_retail_value) ordered_retail

    FROM Iro_Po_Details I
    JOIN Styles S
      ON I.Style_Id = S.Style_Id
    JOIN v_dept_class_subclass v
      ON (v.section_id = s.section_id)
    --WHERE po_id  = 996668
    GROUP BY po_id,  trans_num
    )
  ) Ipdd
   On (Hdr.Po_Id = Ipdd.Po_Id And
       Hdr.Trans_Num = Ipdd.Trans_Num)
/*  get the group sum of data */

  left Join Terms  T On (Po.Term_Id = T.Term_Id And
                   T.Business_Unit_Id = '30')
                 --  order by 1 desc
 left join v_po_same_day_cancel sdc on sdc.po_id = hdr.po_id
 where nvl(sdc.CANCELLED_IND, 'N') = 'N'
"
;

}

sub make_record {
    my $self = shift;
 my ($PoNumber,  $DeptNumber,  $VendorNumber, $ShipStartDate, $ShipStopDate,
        $PoCreateDate, $OrderType, $ShipToNumber, $OrderStatus, $DistMethod,
        $BillType, $TotalUnitsSign, $TotalUnits, $PoTotalCostSign, $PoTotalCost,
        $Version, $Terms, $RideDate, $CancelDate, $EdiDate, $EventId, $PreTicketFlag,
        $Remark, $Reason_id, $po_created_by
    ) = @_;
  
    my $obj =
        MCCS::CMS::RecordLayout::PoHeaderRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   PoNumber         => $PoNumber,
            DeptNumber       => $DeptNumber,
            VendorNumber     => $VendorNumber,
            ShipStartDate    => $ShipStartDate ? MCCS::CMS::DateTime->new($ShipStartDate) : undef,               
            ShipStopDate     => $ShipStopDate ? MCCS::CMS::DateTime->new($ShipStopDate) : undef, 
            PoCreateDate     => $PoCreateDate ? MCCS::CMS::DateTime->new($PoCreateDate) : undef, 
            PoApprovalDate   => $RideDate ? MCCS::CMS::DateTime->new($RideDate) : undef, 
            PoCancelDate     => $CancelDate ? MCCS::CMS::DateTime->new($CancelDate) : undef, 
            EdiSentDate      => $EdiDate ? MCCS::CMS::DateTime->new($EdiDate) : undef,
            OrderType        => $OrderType,
            ShipToNumber     => $ShipToNumber,
            OrderStatus      => $OrderStatus,
            DistMethod       => substr($DistMethod, 0,10),
            BillType         => substr($BillType, 0,1),
            TotalUnitsSign   => $TotalUnitsSign,
            TotalUnits       => $TotalUnits,
            PoTotalCostSign  => $PoTotalCostSign,
            PoTotalCost      => $PoTotalCost,
            Version          => $Version,
            Terms            => $Terms,
            PreTicketFlag    => $PreTicketFlag,
            AdEvent          => $EventId,
            PoComments       => $Remark,
            ReasonCode       => $Reason_id,
            CancelledBy      => $po_created_by
            
        }
      );
    $obj->to_string();
}

sub get_filename {
    my $self = shift;
    my $date = shift;
    $date . '_POHeader.dat';
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
