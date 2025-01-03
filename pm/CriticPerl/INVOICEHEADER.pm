package MCCS::CMS::Loads::INVOICEHEADER;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::InvoiceHeaderRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return;
}

sub get_sql {
    my $self = shift;

return "
select
invoice_number,
--invoice_date,
CREATE_DATE,
invoice_due_date,
case when get_invoice_orig_site_id(ei.invoice_id) is null
then site_id else get_invoice_orig_site_id(ei.invoice_id) end site_id,
po_number,
(select order_date from purchase_orders po
  where po.business_unit_id = '30'
    and po.po_id = ei.po_number
    and rownum = 1) vendor_po_date,
vendor_duns,
num_line_items,
ship_date,
payment_terms,
(round(orig_invoice_amount,2) * 100) orig_invoice_amount,
--address_line1,
address_line2,
address_line3,
address_line4,
city,
state,
postal_code,
case when (select count(*) from Country_Code_Listing
           where Alpha_2 = vr.country or
          Alpha_3 =  vr.country) > 0
  then country end  country,
case when invoice_type_code = 'DR' then
    case when (select count(*) from purchase_orders ppo where ppo.business_unit_id = '30' and  ppo.po_id = ei.po_number) > 0
    then null else invoice_type_code end
end invoice_type_code
from edi_invoices\@aims.usmcmccs.org ei
left join edi_address\@aims.usmcmccs.org vr
 on (vr.invoice_id = ei.invoice_id
and vr.address_type_id = 'R')
where
 ei.INVOICE_ENTRY_TYPE <> 'T' and
 trunc(ei.CREATE_DATE)= Trunc(to_date('rdi_date', 'yyyymmdd' ) - 1)
"
;

}

sub make_record {
    my ($self,$InvoiceNo,  $InvoiceDate,  $InvoiceDueDate, $site_id, $PoNumber, $PoDate,
        $VendorNumber, $NumberLines, $ShipDate, $PaymentTerms, $invoiceAmount,
        $address1, $address2, $address3,
        $city, $state, $zip, $country, $invoiceType
    ) = @_;
    
    $InvoiceNo =~ s/[^[:print:]]//g;
    $InvoiceNo =~ s/\///g;
  
    my $obj =
        MCCS::CMS::RecordLayout::InvoiceHeaderRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   InvoiceNo        => substr($InvoiceNo, 0,20),
            InvoiceDate      => $InvoiceDate ? MCCS::CMS::DateTime->new($InvoiceDate) : undef,               
            InvoiceDueDate   => $InvoiceDueDate ? MCCS::CMS::DateTime->new($InvoiceDueDate) : undef,               
            PoNumber         => $PoNumber,
            PoDate           => $PoDate ? MCCS::CMS::DateTime->new($PoDate) : undef,               
            VendorNum        => $VendorNumber,
            PayVendorNum     => $VendorNumber,
            DUN              => $VendorNumber,
            NumberLines      => $NumberLines,
#            BillTo           => $BillTo,
            #RemitTo          => $RemittTo,
            ShipTo           => $site_id,
            TermType         => $PaymentTerms,
            RemitAddress1   => substr($address1, 0, 50),
            RemitAddress2   => $address2,
            RemitAddress3   => $address3,
            City            => $city,
            State           => $state,
            ZipCode         => $zip,
            TotalInvoiceAmt  => $invoiceAmount,
            CountryCode      => $country,
            InvoiceTypeCode  => $invoiceType, 
                        
        }
      );
        return $obj->to_string();

}

sub get_filename {
    my $self = shift;
    my $date = shift;
return $date . '_810_Header.dat';

}

#sub database {
# my $self = shift;
# my $database = '';
# return $database;
#}

1;

__END__

=pod

=head1 NAME


=head1 SYNOPSIS


=head1 DESCRIPTION


=head1 AUTHOR

Larry Lewis  L<larry.d.lewis@usmc-mccs.org|mailto:larry.d.lewis@usmc-mccs.org>

=cut
