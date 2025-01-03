package MCCS::CMS::Loads::SITE;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::SiteRecord;
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
  s.site_id,
  s.name,
  To_Char(S.Date_Opened,'YYYYMMDD') Open_Date,
  decode(s.site_id, '09700', 'SDM-REC',
                    '05400', 'CLM',
                    '09400', 'SDM',
                    (Decode(Substr(S.Site_Id, 1,2), '08', 'ALM',
                                                    '12', 'BAM',
                                                    '05', 'CLM',
                                                    '04', 'CPM',
                                                    '03', 'ELM',
                                                    '01', 'HHM',
                                                    '16', 'IWM',
                                                    '15', 'KBM',
                                                    '11', 'MRM',
                                                    '10', 'PNM',
                                                    '02', 'QUM',
                                                    '18', 'SCM',
                                                    '09', 'SDM',
                                                    '13', 'TWM',
                                                    '14', 'YUM', 
                                                    ''
                           )
                   )
          ) Command,   
  to_char(s.date_closed,'YYYYMMDD') close_date,
  CASE WHEN sub_type = 'W' THEN 'Y' ELSE 'N' END is_warehouse,
s.address_1,
s.address_2,
s.city,
s.state_id,
s.zip_code,
s.telephone
FROM sites s
Where
 s.business_unit_id = 30 and  
 S.Site_Id In (Select Distinct Z.Site_Id from SECTION_GL_GROUPS z 
                 where z.business_Unit_id = 30
                   And Z.Business_Unit_Id = S.Business_Unit_Id
                   And Z.Site_Id = S.Site_Id
                   And (S.Date_Closed Is Null Or 
						S.Date_Closed > to_date('01-JAN-11' ,'DD-MON-yy'))) 
"

;
}

sub make_record {
    my ($self,$site,     $name,     $open_date, $command, $close_date, $warehouse,
        $address1, $address2, $city,      $state,   $zip, $telephone
    ) = @_;

    my $obj =
        MCCS::CMS::RecordLayout::SiteRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   LocationNumber    => $site,
            LocationName      => substr($name,0,30),
            SiteOpenDate      => $open_date ? MCCS::CMS::DateTime->new($open_date) : undef,
                
            SiteCloseDate     => $close_date ? MCCS::CMS::DateTime->new($close_date) : undef,
            AddressLine1      => substr($address1, 0,40),
            AddressLine2      => substr($address2, 0,40),
            LocationCity      => $city,
            LocationState     => $state,
            LocationZip       => $zip,
            CompanyName       => $command,
            Phone             => substr($telephone, 0,13),
        }
      );
    return $obj->to_string();
}

sub get_filename {
    my $self = shift;
    my $date = shift;
    return $date . '_Site.dat';

#my $date = `date '+%Y%m%d'`;
#    chomp($date);
#    $date . '_Site.dat';
}


1;

__END__

=pod

=head1 NAME

MCCS::CMS::Loads::SITE - 

=head1 SYNOPSIS



=head1 DESCRIPTION

This plugin extracts the data necessary for the Site Core File for Compliance Network

=head1 AUTHOR


Larry Lewis  L<larry.d.lewis@usmc-mccs.org|mailto:larry.d.lewis@usmc-mccs.org>

=cut
