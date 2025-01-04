package MCCS::CMS::Loads::ASNREJECTED;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::AsnErrorRecord;
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
distinct
 substr(Ert.Partnership_Id,1,length(Ert.Partnership_Id) -3) vendor_id, 
 ert.key_data ASN_ID, 
 trunc(ert.date_created) date_created,
 asn.po_id,
asn.mark_for_site,

 case when trim(SUBSTR(ert.transaction_data,17,2)) = '50'
  then trim(SUBSTR(ert.transaction_data,46,20)) end carton_code,

 decode(mt.message_id,  '710023', '21003A',
                        '21003',  '21003B',
                        '20184', '20184')  error_code,

null units,
 decode (trim(SUBSTR(ert.transaction_data,17,2)), 10, 'Header Line (10)',
                                                  20, 'Shipment Line (20)',
                                                  30, 'Ship To and From Line (30)',
                                                  40, 'Order Line (40)',
                                                  50, 'Package Line (50)',
                                                  60, 'Items Line (60)',
                                                  70, 'Prepackage Line (70)',
                                                  90, 'End of Record Line (90)',
                                                  'Unknown'
                                                  )  line_type 
                                                  

from edi_rejected_transactions_arc ert
/* edi_errors will get only the lines that are bad. change to left join to see all the lines */
join edi_errors ee on (ee.Edi_Sequence_Id = ert.Edi_Sequence_Id)
left join vendors vv on (vv.business_unit_id = 30 and 
                         Vv.Vendor_Id = substr(Ert.Partnership_Id,1,length(Ert.Partnership_Id) -3) )
left join messages mm on (mm.message_id = ee.message_id)
left join message_texts mt on (mt.message_id =  mm.message_id and 
                               mt.language_id ='AMERICAN')
left join (Select distinct cr.partnership_id, cr.asn_id, L40.Po_Id, l40.mark_for_site
           From rdiusr.E856_Ctrl_Records Cr 
           Join (Select  Max(Record_Id) Record_Id, Partnership_Id, Asn_Id, Asn_Date, Asn_Time
                 From rdiusr.E856_Ctrl_Records 
                  Group By Partnership_Id, Asn_Id, Asn_Date, Asn_Time) Base 
           On (Cr.Partnership_Id = Base.Partnership_Id And
               Cr.Asn_Id = Base.Asn_Id And
               Cr.Asn_Date = Base.Asn_Date And
               Cr.Asn_Time = Base.Asn_Time)
           Join rdiusr.E856_Line_40 L40 On (cr.Record_Id = L40.Record_Id) ) asn on (asn.partnership_id = Ert.Partnership_Id and 
                                                                                    asn.asn_id =  ert.key_data)   
                                                                              
where trunc(date_created)  = Trunc(to_date('rdi_date', 'yyyymmdd' ) - 1) 

order by 1, 4,5,7"
;
}

sub make_record {
    my ($self, $VendorNumber, $AsnId, $AsnDate, $PoNumber, $Store, 
        $ErrorData, $ErrorCode, $units, $ErrorDataB
    ) = @_;

 my $obj =
 MCCS::CMS::RecordLayout::AsnErrorRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   PoNumber          => $PoNumber,
            VendorNumber      => $VendorNumber ,
            Store             => $Store ,
            AsnId             => $AsnId ,
            ErrorCode         => $ErrorCode,
            ErrorData         => $ErrorData,
            AsnDate           => $AsnDate, 
            ErrorDataB        => $ErrorDataB         
        }
      );  

    return $obj->to_string();

}

sub get_filename {
    my $self = shift;
    my $date = shift;
#    my $date = `date '+%Y%m%d'`;
#    my $ts = `date '+%H00'`;
#    chomp($date);
#    chomp($ts);
    #$date . '_AsnRejected_'.$ts.'.dat';
    return $date . '_AsnRejected.dat';

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
