package MCCS::CMS::Loads::ASNERROR;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::AsnErrorRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return; #TO BE DETERMINED
}

sub get_sql {
    my $self               = shift;

return "
select 
 case when message_id = '3674' 
  then null
  else  vendor_id
  end vendor_id, 
 asn_id, to_char(asn_date,'YYYYMMDDHH24MISS') asn_date, po_id, site_id, error_data, message_id,
 case when message_id = '4725' then qty_shipped end qty_shipped
 from
  (select 
     vendor_id, asn_id, asn_date, po_id, site_id, error_data, message_id, qty_shipped,
    rank() over ( partition BY  vendor_id, asn_id order by err_priority  ) rnk
   from 
    (select hdr.vendor_id, hdr.asn_id, hdr.asn_date, hdr.message_id, hdr.po_id, hdr.site_id, hdr.error_data, ep.err_priority, qty_shipped from 
     (select * from rdiusr.E856_ASN_SUSP_CNS_DATA\@mc2p 
      where 
       create_ts between 
            (select time_completed 
             from ( select *
                    from ( select * from merch.job_control where time_created > trunc(sysdate - 3) )
                    where object_name = 'EDI_INBOUND_SCHEDULER'
                      and Time_completed is not null
                    order by time_completed desc    
                   ) 
             where rownum =1) 
        and 
             (select curr_endtime from e856_asn_susp_runtime\@mc2p where id = 'main')
      --where trunc(asn_date) = trunc(to_date('24-JUL-14','dd-mon-yy'))
      ) hdr
      join rdiusr.e856_asn_susp_err_priority\@mc2p ep on (ep.message_id = hdr.message_id)
    ) q1
 )q2
where q2.rnk = 1
"
;
}


sub make_record {
    my ($self, $VendorNumber, $AsnId, $AsnDate, $PoNumber, $Store, 
        $ErrorData, $ErrorCode, $units 
    ) = @_;
  
    my $obj =
        MCCS::CMS::RecordLayout::AsnErrorRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   PoNumber          => $PoNumber,
        	VendorNumber      => $VendorNumber,
            Store             => $Store,
            AsnId             => $AsnId,
            ErrorCode         => $ErrorCode,
            ErrorData         => $ErrorData,
            #AsnDate           => $AsnDate ? MCCS::CMS::DateTime->new($AsnDate) : undef,
            AsnDate           => $AsnDate , 
            QtyUnits          => $units,
            
           
        }
      );
    return $obj->to_string();
}

sub get_filename {
    my $date = `date '+%Y%m%d'`;
    my $ts = `date '+%H00'`;
    chomp($date);
    chomp($ts);
    return $date . '_AsnError_'.$ts.'.dat';
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
