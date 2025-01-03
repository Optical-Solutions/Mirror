package MCCS::CMS::Loads::ITEM;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::ItemRecord;
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
  base.style_id || base.color_id || nvl(lpad(base.size_id,5,'*'),'NOSIZ') || nvl(lpad(base.Dimension_id,5,'*'),'NODIM') sku_key_b ,
  Base.Bar_Code_Id, 
  Base.Style_Id, 
  regexp_replace(s.description, '[^[:print:]]', '') ,
  base.color_id,
  base.size_id,
  decode(base.sub_type, 'EAN', 'E',
                         'UPCA', 'U',
                         'O')  upc_type,
                         
  Case When S.Estimated_Landed_Cost < 0
  Then '-' End Cost_Sign,
  (round(S.Estimated_Landed_Cost,2) * 100) Estimated_landed_cost,
  Case When P.Retail_Price < 0 
  then '-' end retail_sign,
  (round(P.Retail_Price,2) * 100) Retail_price,
  Sv.Vendor_Style_No,
  V.Department_Id,
  v.class_id,
  Sv.Vendor_Id
From
  (select lp_bc bar_code_id, style_id, color_id, dimension_id, size_id, primary_ind, sub_type from 
    (select 
      lpad(bar_code_id,13,'0') lp_bc, style_id, color_id, dimension_id, size_id, primary_ind, sub_type,
      rank() over ( partition BY lpad(bar_code_id,13,'0'), style_id, color_id, dimension_id, size_id order by primary_ind, sub_type desc, date_created desc, expiry_date desc  ) rnk
     from 
      (select * from  bar_codes b1  
       where
        b1.business_unit_id = '30' 
        and length(Bar_Code_Id) >= 11
        and sub_type in ('UPCA','EAN') 
     ) q1 
    ) q2
   where 
     q2.rnk = 1 )  Base
  Join Styles S On (S.Business_Unit_Id = '30' And
                    S.Style_Id = Base.Style_Id)
  Join (select distinct business_unit_id, style_id, vendor_id, vendor_style_no 
        from (select * from Style_Vendors where Sysdate Between Start_Date And End_Date) ) 
                      Sv On (Sv.Business_Unit_Id  = '30' And
                            Sv.Style_Id = Base.Style_Id )
  Join Prices P On (P.Business_Unit_Id = '30' And
                    P.Site_Id = Chr(255) And P.Zone_Id = Chr(255) And
                    P.Dimension_Id = Chr(255) And P.Size_Id = Chr(255) And
                    P.Color_Id = Chr(255) And P.Price_Sub_Type = 'PERM' And
                    Sysdate Between P.Start_Date And P.End_Date And
                    P.Style_Id = Base.Style_Id)
   join v_dept_class_subclass v on (s.section_id = v.section_id)
   
"
;
return;
}

sub make_record {
    my $self = shift;
    my ($sku, $barcode, $style_id, $style_desc,$color, $size, $type, $cost_sign, $cost, $retail_sign,
        $retail, $vendor_style, $dept_id, $class_id, $vendor_id
    ) = @_;

    $style_desc =~ s/\t+/ /g;
    
    my $obj =
        MCCS::CMS::RecordLayout::ItemRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   Sku             => $sku,
            Upc             => $barcode,
            StyleId         => $style_id,
            StyleDesc       => substr($style_desc, 0,30),               
            ItemCostSign    => $cost_sign,
            ItemCost        => $cost,
            ItemRetailSign  => $retail_sign,
            ItemRetail      => $retail,
            #Vpn             => $vendor_style,
            Dept            => $dept_id,
            VendorNum       => $vendor_id,
            ColorId         => $color,
            SizeId          => $size,
            UpcType         => $type,
            Class           => $class_id,
        }
      );
    $obj->to_string();
    return;
}

sub get_filename {
    my $self = shift;
    my $date = shift;
    $date . '_Item.dat';
    return;
}


1;

__END__

=pod

=head1 NAME

MCCS::CMS::Loads::ITEM - 

=head1 SYNOPSIS



=head1 DESCRIPTION

This plugin extracts the data necessary for the Item Core File for Compliance Network

=head1 AUTHOR


Larry Lewis  L<larry.d.lewis@usmc-mccs.org|mailto:larry.d.lewis@usmc-mccs.org>

=cut
