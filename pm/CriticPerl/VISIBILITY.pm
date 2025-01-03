package MCCS::CMS::Loads::VISIBILITY;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::VisibilityRecord;
use MCCS::CMS::DateTime;



sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return;
}

sub get_sql {
    my $self               = shift;
    

my $sql = 
qq(
select * from (
 select 'Receiving' process_type, jc.username,usr.user_name, to_char(rcv.creation_date, 'mm/dd/yyyy hh24:mi:ss')  date_1, 
        to_char(jc.time_completed, 'mm/dd/yyyy hh24:mi:ss')  updated_date,   RCV.RECEIPT_ID, RCV.PO_ID,   
        case when (po.reason_id = '1' and po.reason_sub_type='PURC_ORDER') 
        then 'Frustrated'  end FFreight,
        null TRANSFER_ID, rcv.site_id, rcv.creation_date
 from JOB_CONTROL JC  
 join JOB_PARAMETERS JP on (JC.JOB_ID = JP.JOB_ID AND JP.PARAMETER_ID = 'RECEIPT_ID')
 left join RECEIPTS RCV on (RCV.BUSINESS_UNIT_ID = '30' AND 
                            JP.VALUE = RCV.RECEIPT_ID AND
                            PO_ID IS NOT NULL AND
                            ASN_ID IS NULL AND
                            SITE_ID NOT IN ('60001','70001') )
 left join purchase_orders po on (po.business_unit_id = '30' and po.po_id = rcv.po_id)
 left join RAMS_USERS usr on (jc.username = usr.user_id)
 where JC.OBJECT_TYPE = 'PROCEDURE' AND
       JC.OBJECT_NAME = 'RECEIVING_UPDATE' AND
       JC.APPLICATION_ID = 'RAMS' AND
       JC.TIME_CREATED >= trunc(sysdate ) and
       RCV.RECEIPT_ID IS NOT NULL
 union all
 select 'Transfer' process_type, jc.username, usr.user_name,  to_char(trf.creation_date, 'mm/dd/yyyy hh24:mi:ss') date_1,  
         to_char(jc.time_completed, 'mm/dd/yyyy hh24:mi:ss'),  null RECEIPT_ID, null PO_ID, null FF, TRF.TRANSFER_ID, trf.to_site_id, trf.creation_date
 from JOB_CONTROL JC  
 join JOB_PARAMETERS JP on (JC.JOB_ID = JP.JOB_ID AND
                            JP.PARAMETER_ID = 'P_TRANSFER')
 left join TRANSFERS TRF on (TRF.BUSINESS_UNIT_ID = '30' AND
                             JP.VALUE = TRF.TRANSFER_ID AND
                             TRF.from_site_id in ('60001','70001') and
                             TRF.TRANSFER_TYPE = 'TRANSFER IN'  )
 left join RAMS_USERS usr on (jc.username = usr.user_id)
 where JC.OBJECT_TYPE = 'PROCEDURE' AND
 JC.OBJECT_NAME = 'TRANSFER_UPDATE' AND
 JC.APPLICATION_ID = 'RAMS' AND
 JC.TIME_CREATED >= trunc(sysdate ) AND
 TRF.TRANSFER_ID IS NOT NULL )
 where
 to_char(creation_date, 'yyyymmddhh24mi')  between 'last_file_date_time' and  'rdi_date_time'



)
;

# (created_date <= to_char(curr_date, 'yyyymmdd') and created_time <= to_char(curr_time, 'hh24mi')) )

return $sql;
}


sub make_record {
    my $self = shift;
    my ($processType, $jc_username, $fullname, $creationDate, $updateDate, $receiptId,
        $poId, $frustrated, $transferId, $siteId
    ) = @_;
    
    my $obj =
        MCCS::CMS::RecordLayout::VisibilityRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   ProcessType       => $processType,
            UserName          => $jc_username,   
            CreationDate      => $creationDate, 
            UpdateDate        => $updateDate,   
            ReceiptId         => $receiptId,
            PoId              => $poId,
            Freight           => $frustrated,
            TransferId        => $transferId,     
            SiteId        => $siteId     
            
           
        }
      );
    return $obj->to_string();

}

sub get_filename {
     my $self = shift;
     my $date = shift;
#    $date . '_AsnHeader.dat';
    
    my $ts = `date '+%H00'`;
    chomp($date);
    chomp($ts);
    return $date . '_scv_store_'.$ts.'.dat';

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
