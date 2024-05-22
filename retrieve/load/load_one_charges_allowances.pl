#!/usr/bin/perl

use strict;
use warnings;
use version;

use File::Basename;
##use Net::Domain;
use Pod::Usage;
use XML::Simple;

use MCCS::Utils;
# This script must be run as "rdiusr". 
die( "This script MUST be run as rdiusr" ) unless MCCS::Utils::is_rdiusr();

our $VERSION = qv('0.0.1');
##
## Version  0.0.1       :       Martin lourduswamy
##
## Get computer hostname that this script is running on and date for later use
##my $HOSTNAME = Net::Domain::hostfqdn;
##my $DATE = localtime;

use IBIS::Log::File;
use IBIS::DBI;
use Getopt::Long;

################ Main Program ################

## Read in the Commandline options
my $DEBUG = 0;
my $xml_file_path ="";
my $help = "";

GetOptions (
        'help'       => \$help,
        'debug!'     => \$DEBUG,                # Optional
        'file=s'     => \$xml_file_path, 	# Required
) or pod2usage(1);

pod2usage( -verbose => 2 ) if( $help );

## Read Config file for Globals
$DEBUG = 1 if( $DEBUG );
my $IRDB_DATABASE = 'irdb';

my ( $xml_filename, $xml_dir, $xml_suffix ) = fileparse( $xml_file_path, qr/\.[^.]*/ );
$xml_filename = $xml_filename . $xml_suffix;

die( "Input XML File is empty" )
    unless( -e $xml_file_path && -s $xml_file_path );

## Log info into logfile load_charges_allowances.log
##
my ( $log_filename, $dir, $suffix ) = fileparse( __FILE__, qr/\.[^.]*/ );

my $log_file = '/usr/local/mccs/log/' . $log_filename . '.log';

my $log = IBIS::Log::File->new(
                { file => $log_file,
                  append => 1,
                  level => 4
                }
          );

my $dbh = IBIS::DBI->connect( 'dbname' => $IRDB_DATABASE )
    or $log->die( 'Could not connect to RMS database: $RMS_DATABASE' );
$dbh->{AutoCommit} = 0;


## Check if INVOICE is already in edi_invoices table
## If it is not present, then do not process
my $get_invoice_id_sql = <<"__SQL__";
SELECT invoice_id FROM edi_invoices
WHERE source_file = ?
__SQL__

my $sth = $dbh->prepare( $get_invoice_id_sql );
$sth->execute( $xml_filename );
my( $invoice_id ) = $sth->fetchrow_array;
die( "Missing Invoice in edi_invoices, Can not process exiting" )
    unless( $invoice_id );

## Check if INVOICE is already in edi_invoice_charge_allowance table 
## If it is present, then do not process
my $check_invoice_id_sql =<<"__SQL__";
SELECT invoice_id FROM edi_invoice_charge_allowance
WHERE invoice_id = ?
__SQL__

$sth = $dbh->prepare( $check_invoice_id_sql );
$sth->execute( $invoice_id );
my( $charges_allowances_invoice_id ) = $sth->fetchrow_array;
die( "Invoice in edi_invoice_charge_allowance, No need to process, exiting" )
    if( $charges_allowances_invoice_id );


## Parse the XML and STORE the data into edi_invoice_charge_allowance table
##
my $parsed_data = XMLin( $xml_file_path );

## Load DATA from XML into DB for 'ChargesAllowances' using PROC discount_allowance_entry
my $ChargesAllowances = $parsed_data->{Header}->{ChargesAllowances};
$ChargesAllowances = [ $ChargesAllowances ] if( ref($ChargesAllowances) eq "HASH" );

foreach my $ChargesAllowance ( @{ $ChargesAllowances } ) {
    $log->log_info( "INSERT into edi_invoice_charge_allowance for invoice_id: $invoice_id" )
        if( $invoice_id );
    my $message .= _insert_ChargesAllowances( $ChargesAllowance, $invoice_id );
    print "$message\n";
}

exit 0;
##------------------------------------------------------------------------------
sub _insert_ChargesAllowances {
    my ( $ChargesAllowance, $invoice_id ) = @_;

    my $ChargeIndicator = $ChargesAllowance->{AllowChrgIndicator};
    my $ChargeCode = $dbh->quote( $ChargesAllowance->{AllowChrgCode} );

## For Charges, just take the absolute value.
## For Allowance FORCE the data to be negative amount
    my $ChargeAmount = abs( $ChargesAllowance->{AllowChrgAmt} );
    $ChargeAmount =~ s/\s+//g;
    $ChargeAmount = 0 if( $ChargeAmount eq "" );
    $ChargeAmount = -1 * $ChargeAmount if(  uc( $ChargeIndicator ) eq 'A' );

    my $ChargeDescription = substr( $ChargesAllowance->{AllowChrgHandlingDescription}, 0, 60 );
    $ChargeDescription = $dbh->quote( $ChargeDescription );

    unless( $invoice_id && $ChargeCode ) {
        my $error = 'Missing Data for ChargesAllowances';
        return $error;
    }

    my $info =<<"TEXT";
FILE : $xml_file_path
ChargeIndicator : $ChargeIndicator
ChargeCode      : $ChargeCode
ChargeAmount    : $ChargeAmount
ChargeDescription : $ChargeDescription
TEXT
    print $info if( $DEBUG );

    my $rows_inserted = 0;
    eval{
        $rows_inserted = $dbh->do( qq{BEGIN edi_utils.discount_allowance_entry( p_invoice_id=>$invoice_id, p_external_id=>$ChargeCode, p_amount=>$ChargeAmount, p_description=>$ChargeDescription ); END;} );
    };

    if( $@ ) {
        $log->log_warn( "Error in Inserting" );
        $log->log_warn( $@ ) if( $@ );
        $dbh->rollback();
    } else {
        $dbh->commit();
    }


return $@;
}

__END__

=pod

=head1 NAME

Load Charges and Allowances for Invoices

=head1 VERSION

This documentation refers to load_charges_allowances.pl version 0.0.1.

=head1 SYNOPSIS

load_charges_allowances.pl [--options]

=head1 REQUIRED ARGUMENTS

=over 4

=item --file <Path To Input Invoice_xml File>

=back

=head1 OPTIONS

=over 4

=item --debug

This argument runs the program in debug mode, does not load data into Database.

=item --help

This argument displays this screen.

=back

=head1 DESCRIPTION

This script will read in an XML file and find its invoice_id from table edi_invoices
If found, it will read in the charges and allowances(if any) from the XML file and load
them into the table edi_invoice_charge_allowance. This script will record in a logfile at 
/usr/local/mccs/log/load_charges_allowances.log everytime it runs.

=head1 REQUIREMENTS

Just the XML file to process

=head1 CONFIGURATION

No seperate Configuration file for this script.
All the configuration are coded within the script. 

=head1 HISTORY

=over 4

=item '11-12-2009' Martin Lourduswamy

=back

=head1 BUGS AND LIMITATIONS

There are no known bugs in this script
Please report problems to the L<MCCS Help Desk|mailto:help.desk@usmc-mccs.org>.

=head1 AUTHOR

Martin Lourduswamy

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2006 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
