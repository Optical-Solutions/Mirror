package MCCS::CMS::Loads::VENDOR;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::VendorRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return;
}

sub get_sql {
    my $self               = shift;

return "
select VENDOR_ID, NAME, null factory_number, CURRENCY_ID, ADDRESS_1, ADDRESS_2, ADDRESS_3, CITY, STATE_ID, ZIP_CODE, COUNTRY_ID, note_1, note_2 ,
t.term_id, T.Description
from vendors v
left join terms t on t.business_unit_id = v.business_unit_id and
                t.term_id = v.term_id
where v.business_unit_id = '30' and v.VENDOR_STATUS ='A'


"
;

}

sub make_record {
    my ($self,$vendor,     $name,     $factor_number, $currency_code, 
    $address1, $address2, $address3,
    $city, $state, $zip, $country, $vendor_type, $vendor_minimum,
    $term_id, $t_description
    ) = @_;

    my $obj =
        MCCS::CMS::RecordLayout::VendorRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   VendorNumber    => $vendor,
            VendorName      => substr($name,0,30),
            FactorNumber    => $factor_number,
            CurrencyCode    => $currency_code,
            RemitAddress1   => $address1,
            RemitAddress2   => $address2,
            RemitAddress3   => $address3,
            City            => $city,
            State           => $state,
            ZipCode         => $zip,
            CountryCode     => $country,
            VendorType      => $vendor_type,
            VendorMinimum   => $vendor_minimum,
            TermId          => $term_id,
            Description     => $t_description
            
        }
      );
    return $obj->to_string();
}

sub get_filename {
    my $self = shift;
    my $date = shift;
    return $date . '_Vendor.dat';
}


1;

__END__

=pod

=head1 NAME

MCCS::CMS::Loads::VENDOR - 

=head1 SYNOPSIS



=head1 DESCRIPTION

This plugin extracts the data necessary for the Vendor Core File for Compliance Network

=head1 AUTHOR


Larry Lewis  L<larry.d.lewis@usmc-mccs.org|mailto:larry.d.lewis@usmc-mccs.org>

=cut
