package MCCS::CMS::Loads::INVOICEDETAIL;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::InvoiceDetailRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return;
}

sub get_sql {
    my $self = shift;

"
 select 
sub.invoice_number,
--sub.invoice_date,
sub.create_date,
sub.po_number,
sub.vendor_po_date,
base.bar_code_id,
get_cms_sku(base.bar_code_id) sku_key,
round(quantity_shipped),
item_unit,
unit_cost
from 
 Edi_Invoice_Details\@aims.usmcmccs.org base 
 join (select * from edi_invoices\@aims.usmcmccs.org ei
       where  ei.INVOICE_ENTRY_TYPE <> 'T' and 
         trunc(ei.CREATE_DATE)= Trunc(to_date('rdi_date', 'yyyymmdd' ) - 1)
       )  sub on sub.invoice_id = base.invoice_id
"

;
return; 
}

sub make_record {
    my $self = shift;
    my ($InvoiceNo,  $InvoiceDate,  $PoNumber, $PoDate,
        $BarCode, $Sku, $Qty, $ItemUnit, $UnitCost
    ) = @_;
  
  $InvoiceNo =~ s/[^[:print:]]//g;
  
    my $obj =
        MCCS::CMS::RecordLayout::InvoiceDetailRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   InvoiceNo   => substr($InvoiceNo, 0,20),
#            InvoiceDate => $InvoiceDate ? MCCS::CMS::DateTime->new($InvoiceDate) : undef,               
            PoNumber    => $PoNumber,
            PoDate      => $PoDate ? MCCS::CMS::DateTime->new($PoDate) : undef,               
            UPC         => $BarCode,
            SKU         => $Sku,
            QTY         => $Qty,
            UOM         => $ItemUnit,
            Price       => $UnitCost,
            
                        
        }
      );
    $obj->to_string();
    return;
}

sub get_filename {
    my $self = shift;
    my $date = shift;
    $date . '_810_Detail.dat';
    return;
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
