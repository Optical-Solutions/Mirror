package MCCS::RMS::StoredProcedures;

#1/9/2008 Eric Spencer 	- added is_jesta routine and modified query_exception_cost to use it for a single version of this module
#			(Database stored procedure changed arguments between Essentus 5.04 and Jesta 9.2)
#			- updated query_exception_cost() routine to use the is_jesta() function. Previous 9.2 version was incorrect.

use strict;
use warnings;

use version; our $VERSION = qv('0.0.1');

sub query_exception_cost
{
    my ($dbh, $style_id, $zone_id) = @_;
    my $estimated_landed_cost;
	# 'describe query_exception_cost' in jesta 9.2
	# shows a different set of parameters for this function (i.e. zone_id not used and added a date param)
    my $jesta = is_jesta($dbh);
    my $columns = $jesta ? '
	null,
	null,
	null,
	SYSDATE
	' : 
	'
	:zone_id,
	null,
	null,
	null
	';

    my $r = $dbh->prepare_cached(qq{
            BEGIN
                :elc := query_exception_cost(30,
                                             '00001707694',
                                             :style_id,
                                             null,
					    $columns
                );
            END;
    });

    $r->bind_param(':style_id', $style_id);

    if(! $jesta){ $r->bind_param(':zone_id', $zone_id); }

    $r->bind_param_inout(':elc', \$estimated_landed_cost, 1000);

    $r->execute();

    return $estimated_landed_cost ? $estimated_landed_cost : undef;
}

sub get_permanent_retail_price
{
    my ($dbh, $style_id) = @_;

    my $retail_price;

    my $r = $dbh->prepare_cached(q{
            BEGIN
                :retail_price := get_permanent_retail_price(30,
                                                            null,
                                                            :style_id,
                                                            null,
                                                            null,
                                                            null,
                                                            null,
                                                            null
                );
            END;
    });

    $r->bind_param(':style_id', $style_id);

    $r->bind_param_inout(':retail_price', \$retail_price, 1000);

    $r->execute();

    return $retail_price ? $retail_price : undef;
}

sub update_inven_move_summary
{
    my ($dbh) = @_;
    
    my $rows_inserted;

    my $r = $dbh->prepare_cached(q{
            BEGIN
                :rows_inserted := load_inven_move_summary.f_nightly_insert;
            END;
    });

    $r->bind_param_inout(':rows_inserted', \$rows_inserted, 1000);

    $r->execute();

    return $rows_inserted ? $rows_inserted : undef;
}

#this is not a stored procedure... but a helper routine... can't seem to find a better place to put it
sub is_jesta{
	my ($dbh) = @_;
	my $st;
    my $ret;
	eval{ 
		$st = $dbh->prepare_cached('SELECT MAX(version_number) max_ver FROM application_build_information');
	};
	if($st && ! $@){
		$st->execute();
		my ($res) = $st->fetchrow_array();
		$st->finish();
		$ret = substr($res,0,1) == 9 ? 1 : 0;
        if (wantarray()) {
		return ($ret,$res);
        }
	} else {
		return undef;
	}
}

1;

__END__

=pod

=head1 NAME

StoredProcedures.pm - Interfaces to Oracle stored procedures and functions.

=head1 VERSION

This documentation refers to MCCS::RMS::StoredProcedures version 0.0.1.

=head1 SYNOPSIS

    use MCCS::RMS::StoredProcedures;
    my $elc    = MCCS::RMS::query_exception_cost($dbh, $style_id, $zone_id)
    my $retail = MCCS::RMS::get_permanent_retail_price($dbh, $style_id)

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=over 4

=item query_exception_cost()

Returns a number representing the Estimated Landed Cost (ELC) for a particular style ID.

=item get_permanent_retail_price()

Returns a number representing the current Marine Corps Exchange (MCX) retail price.

=item update_inven_move_summary()

Returns a number representing the number of records inserted into the inven_move_summary table for the daily update.

=item is_jesta(database_handle)

In scalar context: returns a boolean indicating whether the connected database is Jesta 9.x version or not (as oppossed to 5.04)
In list context: returns that same boolean and the actual version value from the database

=back

=head1 EXAMPLES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to the MCCS Help Desk B<help.desk@usmc-mccs.org>.
Patches are welcome.

=head1 AUTHOR

Fred Isler B<islerwi@usmc-mccs.org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2007 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut

