package MCCS::SAS::Loads::INVENTORY;
use strict;
use warnings;
use base qw(MCCS::SAS::Loads::Base);
use MCCS::SAS::InventoryRecord;
use Memoize;

my $last_merch_sth;

sub init{
	my $self = shift;
	
	my $db = $self->{'util'}->get_database();

	$self->{'previous_week_sth'} = $db->prepare("
		SELECT retail, cost, items FROM sas_inventory_eop_tracking
		WHERE product_id = ? AND site_id = ? AND merch_year = ? AND merch_week = ?
	");

	$self->{'this_week_insert_sth'} = $db->prepare("
		BEGIN
		/*
			sku,site,year,week,cost,retail,units
		*/
			sas_utils.mark_inventory_eop(?,?,?,?,?,?,?);
		END;
	");

	$last_merch_sth = $db->prepare("
		SELECT sas_utils.last_merch_week(?,?) FROM dual
	");

	return memoize('last_merch');
}

sub get_sql{
	return;

}

sub make_record{ ## no critic qw(Subroutines::RequireArgUnpacking)
	my $self = shift;
	my($upc,$site,$year,$week,$retail,$cost,$num_items) = @_;
	my $obj = MCCS::SAS::InventoryRecord->new( filehandle => $self->{'util'}->{'filehandle'} );
	$obj->set(
		{
			product_id => $upc,
			store_id => $site,
			Week => $week,
			Year => $year,
			inv_EOP_retail => $retail,
			inv_EOP_cost => $cost,
			inv_EOP_items => $num_items
		}
	);

	my ($last_year, $last_week) = last_merch($year,$week);	#calc last merch week before this one
	$self->{'previous_week_sth'}->execute($upc,$site,$last_year,$last_week);	#lookup values from last week to get eop to copy to bop of this record
	my ($bop_retail,$bop_cost,$bop_num) = $self->{'previous_week_sth'}->fetchrow_array();
	$obj->set(
		{
			inv_BOP_retail => $bop_retail,
			inv_BOP_cost => $bop_cost,
			inv_BOP_items => $bop_num
		}
	);

	$self->{'this_week_insert_sth'}->execute($upc,$site,$year,$week,$retail,$cost,$num_items);		#insert current eop into my tracking (to be used as bop next week)
	return $obj->to_string();
}


#static
sub last_merch{ ## no critic qw(Subroutines::RequireArgUnpacking)

	my ($merch_year, $merch_week) = @_;
	$last_merch_sth->execute($merch_year, $merch_week);
	my ($last) = $last_merch_sth->fetchrow_array();
	return (substr($last,0,4),substr($last,4));
}

sub get_filename{ ## no critic qw(Subroutines::RequireArgUnpacking)
	my $self = shift;
	my ($year,$week) = @_;
	$week = sprintf("%02d",$week);
	my $year_week = $year . $week;
	return "MINVENTORY_" . $year_week . "_001.txt";
}

sub site_field{ return 'si.site_id' }
sub week_limiting {return 1;}

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::INVENTORY - MCCS::SAS INVENTORY extract

=head1 SYNOPSIS

MCCS::SAS::INVENTORY->new( MCCS::SAS::Util  );

=head1 DESCRIPTION

This plugin extracts the data necessary for the inventory data (by merchandising year/week) in MCCS::SAS.
This plugin would be run for a week for the past week.

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
