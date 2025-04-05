package MCCS::SAS::Loads::INVENTORY_SAS_PROD;

# Modified by: Armando Someillan  03/14/2013
# Implement new way of calculating Shrink Cost
# Modified by: Armando Someillan  03/21/2013
# Remove site exclusion based on Closing date.
# 06/12/14  Reverse sign of trans_in_units, trans_out_cost and trans_out_units
# to force them to always be positive. The objective being to eliminate the need
# to change the SAS metadata
use strict;
use warnings;
use base qw(MCCS::SAS::Loads::INVENTORY);
use MCCS::SAS::InventoryRecord;
use Memoize;

sub init {
	my $self = shift;
        return $self;
}

sub get_sql {
	return "
SELECT
    i.sku_key,
    i.site_id,
    i.merchandising_year,
    i.merchandising_week,
    nvl(i.retail_on_week,0)  retail_on_week,
    i.cost_on_week,
    nvl(trunc(i.inventory_on_week),0) inventory_on_week,
    nvl(i.prev_retail_on_week,0) retail_on_week_prev,
    nvl(i.prev_cost_on_week,0) cost_on_week_prev,
    nvl(trunc(i.prev_inv_on_week),0) inventory_on_week_prev,
    nvl(i.receipts_retail,0) receipt_retail,
    nvl(i.receipts_cost,0) receipt_cost,
    nvl(trunc(i.receipts_qty),0) receipt_qty,
    case when 12 = (select cal.merchandising_period from merchandising_calendars cal
                                      where cal.merchandising_year = i.merchandising_year 
                                      and cal.merchandising_week = i.merchandising_week)
     then 0
     else 
    (SELECT ROUND((nvl(i.sales_sold_price,0) * -1) * (shrinkage_rate / 100),2) 
     From Sections 
     Where Section_Id = S.Section_Id 
       AND business_unit_id = 30
    )  
    end shrink_retail,
    case when 12 = (select cal.merchandising_period from merchandising_calendars cal
                                      where cal.merchandising_year = i.merchandising_year 
                                      and cal.merchandising_week = i.merchandising_week)
     then 0
     else 
   (SELECT ROUND((nvl(i.sales_sold_price*(shrinkage_rate / 100),0) *-1)- ((nvl(i.sales_sold_price*(shrinkage_rate / 100),0) * -1) * (calc_cumulative_markon_rate / 100)),2) 
     FROM sections 
     Where Section_Id = S.Section_Id 
       AND business_unit_id = 30)  
    end shrink_cost,
    0 shrink_units,
    nvl(i.returns_retail * -1,0) claim_retail,
    nvl(i.returns_cost * -1,0) claim_cost,
    nvl(trunc(i.returns_qty) * -1,0) claim_units,
    nvl(i.Mark_Down_Perm,0) - nvl(i.Mark_Up_Perm,0) perm_markdown,
    nvl(i.transfers_retail, 0) trans_retail,
    nvl(i.transfers_cost, 0) trans_cost,
    nvl(trunc(i.transfers_qty), 0) trans_units,
    nvl(T.transfer_retail_in, 0) trans_in_retail,
    nvl(T.transfer_cost_in, 0) trans_in_cost,
    nvl(trunc(T.transfer_qty_in), 0) trans_in_units,
    nvl(T.transfer_retail_out * -1, 0) trans_out_retail,
    nvl(T.transfer_cost_out * -1, 0) trans_out_cost,
    nvl(trunc(T.transfer_qty_out * -1), 0) trans_out_units
From
  --sas_prod_complete I 
  sas_prod_complete_2012 I 
  Join Sas_Product_Master Ma On (I.Sku_Key = Ma.Sku_Key)
  Join Styles S on (s.style_id = ma.style_id and s.business_unit_id = '30') 
  Left Outer Join sas_transfers T on (T.site_id = I.site_id and T.sku_key = I.sku_key and
                           T.merchandising_year = I.merchandising_year and
                           T.merchandising_week = I.merchandising_week)
WHERE
    I.Merchandising_Year = ?
    And I.Merchandising_Week = ?
and  s.section_id not in (select v.section_id from v_dept_class_subclass v
                      Join Departments D On (V.Department_Id = D.Department_Id)
                     Where V.Department_Id = 0853 And V.Class_Id = 5000)
 
  And i.Site_Id In (Select Distinct Z.Site_Id 
                 from SECTION_GL_GROUPS\@mc2p z,sites\@mc2p s 
                 where z.business_Unit_id = 30
                   and z.business_Unit_id = s.business_Unit_id
                   and z.site_id = s.site_id
                   )";

}

sub make_record { ## no critic qw(Subroutines::RequireArgUnpacking)
	my $self = shift;
	my (
		$upc,            $site,           $year,              $week,          $retail,
		$cost,           $num_items,      $bop_retail,        $bop_cost,      $bop_num,
		$rec_retail,     $rec_cost,       $rec_qty,           $shrink_retail, $shrink_cost,
		$shrink_units,   $claim_retail,   $claim_cost,        $claim_units,   $perm_markdown,
		$trans_retail,   $trans_cost,     $trans_units,       $trans_in_retail,
		$trans_in_cost,  $trans_in_units, $trans_out_retail,  $trans_out_cost, $trans_out_units
	  )
	  = @_;

	my $obj =
	  MCCS::SAS::InventoryRecord->new(
		filehandle => $self->{'util'}->{'filehandle'} );
	$obj->set(
		{
			product_id          => $upc,
			store_id            => $site,
			Week                => $week,
			Year                => $year,
			inv_EOP_retail      => $retail,
			inv_EOP_cost        => $cost,
			inv_EOP_items       => $num_items,
			inv_receipts_retail => $rec_retail,
			inv_receipts_cost   => $rec_cost,
			inv_receipts_items  => $rec_qty,
			inv_retail_1        => $shrink_retail,
			inv_cost_1          => $shrink_cost,
			inv_items_1         => $shrink_units,
			inv_retail_2        => $claim_retail,
			inv_cost_2          => $claim_cost,
			inv_items_2         => $claim_units,
			inv_retail_3        => $trans_retail,
			inv_cost_3          => $trans_cost,
			inv_items_3         => $trans_units,
			inv_retail_4        => $trans_in_retail,
			inv_cost_4          => $trans_in_cost,
			inv_items_4         => $trans_in_units,
			inv_retail_5        => $trans_out_retail,
			inv_cost_5          => $trans_out_cost,
			inv_items_5         => $trans_out_units,
			inv_markdown_1      => $perm_markdown
		}
	);

	$obj->set(
		{
			inv_BOP_retail => $bop_retail || 0,
			inv_BOP_cost   => $bop_cost   || 0,
			inv_BOP_items  => $bop_num    || 0
		}
	);

	return $obj->to_string();
}

sub site_field { return; }

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::INVENTORY_DEV - MCCS::SAS INVENTORY extract for the DEV environment

=head1 SYNOPSIS

MCCS::SAS::INVENTORY_DEV->new( MCCS::SAS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the inventory data (by merchandising year/week) in MCCS::SAS.
This plugin is specifically for the DEV environment. It probably will not be needed after go-live.

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
