#!/usr/local/mccs/perl/bin/perl
#--------------------------------------------------------------------------
#Ported by : Soumya K Bhowmic
#Date      : 11/30/2023
#
#Brief Desc: This program gets the retail data for Nexcom for zones 
#            983, 984 and 987   
#            Thereafter it loads them into RMS Database table 
#            NEX_RETAIL_LOAD (insert and updates)
# --------------------------------------------------------------------------     
use strict;
use warnings;

use version; our $VERSION = qv('0.0.1');

use Data::Dumper;
use Getopt::Std;
use POSIX qw(strftime WNOHANG);
use Readonly;
use MCCS::Utils;
use MCCS::RMS::CostLoad;
use MCCS::RMS::StoredProcedures;
use IBIS::Log::File;
use IBIS::Mail;
use IBIS::DBI;
use IBIS::SFTP;
use English qw(-no_match_vars);

Readonly::Scalar my $DATA_DIR         => '/usr/local/mccs/data/rms/retail_load';
Readonly::Scalar my $REMOTE_DIR       => '/u5/retekdata/prd/rms/mcx/amc';
Readonly::Scalar my $NEX_VENDOR_ID    => '00001707694';
Readonly::Scalar my $LOGFILE          => '/usr/local/mccs/log/nexcom/rms_nex_retail_load.log';
Readonly::Scalar my $TABLE_NAME       => 'NEX_RETAIL_LOAD';
Readonly::Scalar my $MAIL_CC          => q{};
Readonly::Scalar my $CONNECT_INSTANCE => 'rms_p';
Readonly::Scalar my $FTP_CONNECT      => 'nexcom_production';

my $server = $ARGV[0] || `hostname`;
chomp($server);

if ( not MCCS::Utils::is_rdiusr() ) { die "Must run this process as rdiusr\n"; }

# Signal handler for child processes
$SIG{CHLD} = sub {
    while ( waitpid( -1, WNOHANG ) > 0 ) { }
};

my (%opt);

getopts( 'disv', \%opt );

my $debug = $opt{d};

my $insert_only = $opt{i};

# This variable contains the files to be retrieved from NEXCOM Richter. Update
# this list to add new warehouses.
my @rfiles = qw(AMC_LOC_983.dat AMC_LOC_984.dat AMC_LOC_995.dat);

# Standardized error messages
my %error = (
    1 => 'No style/vendor record',
    2 => 'Record already exists',
    3 => 'Multiple costs',
    4 => 'Zero AMC value',
    5 => 'Single',
    6 => 'High cost difference',
    7 => 'AMC greater than retail',
    8 => 'Multiple retail',
);

# makes '01-MAY-2006'
my $date = uc( strftime( '%d-%b-%Y', localtime ) );

# Pricing zones keyed to warehouses. Add warehouses and zones here.
my %zone = (
    983 => [ 'PZ008', 'PZ018' ],
    984 => [ 'PZ001', 'PZ002', 'PZ003', 'PZ004', 'PZ005' ],
    995 => [ 'PZ009', 'PZ010', 'PZ011', 'PZ013', 'PZ014', 'PZ015', 'PZ016' ],
);

my $log = IBIS::Log::File->new( { file => $LOGFILE, append => 1 } );

$log->log_info("NEX Retail Load initiated\n");

if ( not -d $DATA_DIR ) {
    $log->log_die("Retail Load $DATA_DIR data directory does not exist\n");
}

# Maybe permissions are too tight?
chdir $DATA_DIR
    or $log->log_die("Cannot change to retail load $DATA_DIR data directory\n");

# Start the program logic ------------------------------------------------------

if ( not $opt{s} )    # Don't fetch files if we are in dev mode
{
    unlink MCCS::RMS::CostLoad::get_file_names($DATA_DIR);

    # Fetch the files from NEXCOM's host
    IBIS::SFTP->new(
        {   destination => $FTP_CONNECT,
            remote_dir  => $REMOTE_DIR,
            files       => \@rfiles
        }
    )->get();
}

my @files;

eval { @files = MCCS::RMS::CostLoad::get_file_names($DATA_DIR); }
    or $log->log_die("Cannot get data file names\n");

if (@files) {
    for (@files) {
        if ( $_ =~ m/([^\/]*)$/xms ) {
            my $file = $1;
            $log->log_info("Retrieved $file from NEXCOM\n");
        }
        else {
            $log->log_info("Skipped unwanted file $_ in the NEXCOM file list\n");
        }
    }
}
else {
    $log->log_die("No files retrieved from NEXCOM\n");
}

my $dbh = IBIS::DBI->connect( dbname => $CONNECT_INSTANCE )
    or $log->log_die("Cannot connect to RAMS database\n");

# If user uses "insert-only" commandline option, ensure table has no records
if ($insert_only) {
    if ( my $count = MCCS::RMS::CostLoad::get_row_count( $dbh, $TABLE_NAME ) ) {
        $log->log_die(
            "Cannot run program using -i command line option (insert-only mode) because there are already $count records in $TABLE_NAME\n"
        );
    }
}

# Cache some data before we start ----------------------------------------------

my $nex_styles = MCCS::RMS::CostLoad::get_nex_styles($dbh);
$log->log_info("Line 144 \n");

#my $nex_retail_load; # = get_nex_retail_load($dbh);

# End data caching -------------------------------------------------------------

$dbh->disconnect();

# Loop through warehouse files -------------------------------------------------
my $pid;

for (@files) {
$log->log_info("$_ Line 156 \n");
    # XXX fork here

    if ( $pid = fork ) {
        next;
    }
    elsif ( defined($pid) ) {
        $log->log_info("$pid Line 163 \n");
        $dbh = IBIS::DBI->connect( dbname => $CONNECT_INSTANCE )
            or $log->log_die("Cannot connect to RAMS database\n");

        # Cache nex_retail_load data.  Must be cached before each
        # warehouse file is processed because data is committed after each
        # file is processed.  No need to cache data if inserting into empty
        # table (commandline option "-i").
        my %nex_retail_load;

        if ( not $insert_only ) {
            %nex_retail_load = get_nex_retail_load($dbh);
        }

        my $nrl_hashref = \%nex_retail_load;

        # Set the warehouse number. Filenames are of the form AMC_LOC_xxx
        # where xxx is the warehouse number. This enables the addition of
        # new warehouses by updating the warehouse file list @rfiles.
        # See above.
        my ($wh_num) = $_ =~ m/AMC_LOC_(\d{3})\.dat$/xms;

        my ( $data, $total ) = process_file( $dbh, $_, $wh_num, %nex_retail_load );

        update_database( $dbh, $wh_num, $data, $nrl_hashref );

        my $now = strftime( '%Y-%m-%d %H:%M:%S', localtime );

        $log->log_info("Warehouse $wh_num  NEX Retail Load complete: $now\n");

        # Report Stats
        # $total{amc_records}
        # $total{styles}
        # $total{invalid_upcs}

        # Set a default value in case some are undefined. It could happen.
        $total->{amc_records}  = $total->{amc_records}  || 'N/A';
        $total->{invalid_upcs} = $total->{invalid_upcs} || 'N/A';
        $total->{styles}       = $total->{styles}       || 'N/A';

        my $msg = success_message( $now, $wh_num, $total );
        MCCS::RMS::CostLoad::mail_notify( $MAIL_CC,
            $server . ' NEX Retail Load Complete', $msg );
        $dbh->disconnect();
        exit;
    }
    else {
        $log->log_die("Cannot fork: $_\n");
    }
}

# End warehouse file loop ------------------------------------------------------

# Subroutines ------------------------------------------------------------------
#-------------------------------------------------------------------------------
# Parse records from the warehouse file and eliminate invalid barcodes
sub process_file {
    my ( $dbh, $file, $wh_num, %nex_retail_load ) = @_;

    my ( @data, $total );

    $log->log_info("  Processing for warehouse $wh_num\n");

    # Should this be warn or die? If we warn, we can keep processing the
    # rest of the files.
    if ( not -e $_ ) { next; }

    # --------------------------------------------
    # Do not want to process blank file!
    my $file_size = -s $_;
    $log->log_info("  File $_ $file_size bytes\n");

    if ( $file_size == 0 ) {
        $log->log_warn("File $_ is zero bytes");

        # Continue to process next file
        next;
    }

    # --------------------------------------------

    open my $fh, '<', $_
        or $log->log_die("Cannot read data file for $wh_num\n");

    while (<$fh>) {

        # UPC, Average Moving Cost, Retail Price

        if ( $_ =~ m/^(\d*),\s*([\d\.]*),\s*([\d\.]*)$/smx ) {
            my $upc    = $1;
            my $amc    = $2;
            my $retail = $3;

            $total->{amc_records}++;

            if ( MCCS::RMS::CostLoad::invalid_upc($upc) ) {
                $total->{invalid_upc}++;
                $log->log_debug("\tinvalid UPC $upc");
                next;
            }

            # Get the style id for the UPC
            my $style_id = MCCS::RMS::CostLoad::style_id( $dbh, $upc );

            if ($style_id) {
                $total->{styles}++;
            }
            else {
                $log->log_debug("\tCan not Get the style id for the UPC $upc");
                next;
            }

            # Skip styles in the style_vendors table where
            # the vendor_id != '00001707694' (the vendor is not NEXCOM)
            if ( not exists $nex_styles->{$style_id} ) {
                $log->log_debug(
                    "\tSkip styles in the style_vendors table where the vendor_id != '00001707694' (the vendor is not NEXCOM)"
                );
                next;
            }

            push @data,
                {
                warehouse    => $wh_num,
                upc          => $upc,
                amc          => $amc,
                retail_price => $retail,
                style_id     => $style_id
                };
        }
    }

    close $fh;

    # Dump some data useful for debugging --------------------------------------
    if ($debug) {
        open my $fh, '>', "./nex_styles_$wh_num.dbg" or die "$!\n";
        print {$fh} Dumper($nex_styles);
        close $fh;

        open $fh, '>', "./nex_retail_load_$wh_num.dbg" or die "$!\n";
        print {$fh} Dumper(%nex_retail_load);
        close $fh;

        open $fh, '>', "./data_$wh_num.dbg" or die "$!\n";
        print {$fh} Dumper( \@data );
        close $fh;
    }

    # End debug dumps ----------------------------------------------------------

    return \@data, $total;
}

#-------------------------------------------------------------------------------
# Insert and/or update records in the NEX_RETAIL_LOAD table
sub update_database {
    my ( $dbh, $wh_num, $data, $nex_retail_load ) = @_;

    my %dupestyles;

    my $processed = 0;

    open my $fh, '>', "./NEX_RETAIL_LOAD_$wh_num" or die "$!\n";

    $dbh->begin_work or $log->log_die("$wh_num: $dbh->errstr\n");

    for my $r ( @{$data} ) {
    ZONE_ID:
        for my $zone_id ( @{ $zone{ $r->{warehouse} } } ) {

            # Only allow one record for each combination of style_id
            # and zone_id
            if ( exists $dupestyles{ $r->{style_id} }{$zone_id} ) { next ZONE_ID }

            update_nex_retail_load(
                $dbh, $fh,
                {   style_id     => $r->{style_id},
                    zone_id      => $zone_id,
                    retail_price => $r->{retail_price},
                },
                $nex_retail_load,
            );

            $dupestyles{ $r->{style_id} }{$zone_id} = 1;

            $processed++;

            if ( $debug && ( $processed % 5000 == 0 ) ) {
                $log->log_info("$processed Warehouse $wh_num records processed...\n");
            }
        }
    }

    close $fh;

    eval { $dbh->commit } or $log->log_die("$wh_num: $dbh->errstr\n");

##    print "$processed records processed for Warehouse $wh_num\n";

    if ($debug) {
        open $fh, '>', "./dupestyles_$wh_num.dbg" or die "$!\n";
        print {$fh} Dumper(%dupestyles);
        close $fh;
    }

    return 1;
}

#------------------------------------------------------------------------------
# Insert/update NEX_RETAIL_LOAD.
#
# NEX_RETAIL_LOAD has a primary key contstraint for columns STYLE_ID and
# PRICE_ZONE_ID.
#
# If a record with the incoming STYLE_ID and PRICE_ZONE_ID (zone_id) exists,
# and if the incoming NEX retail price is different from NEX_CURRENT_RETAIL,
# then update columns NEX_PREVIOUS_RETAIL_PRICE, NEX_CURRENT_RETAIL
# and LAST_NEX_CHANGE_DATE.
#
# If no record with the incoming STYLE_ID and PRICE_ZONE_ID (zone_id) exists,
# then insert a record for this style_id and price_zone_id into
# NEX_RETAIL_LOAD.
#
# Values for MCX columns ESTIMATED_LANDED_COST and RETAIL_PRICE are computed on
# insert or update using database functions QUERY_EXCEPTION_COST and
# GET_PERMANENT_RETAIL_PRICE respectively.
#
# Table structure for NEX_RETAIL_LOAD:
#   STYLE_ID                      NOT NULL  VARCHAR2(20)
#   PRICE_ZONE_ID                 NOT NULL  VARCHAR2(5)
#   ESTIMATED_LANDED_COST                   NUMBER(12,4)
#   RETAIL_PRICE                            NUMBER(12,2)
#   NEX_PREVIOUS_RETAIL_PRICE               NUMBER(12,2)
#   NEX_CURRENT_RETAIL                      NUMBER(12,2)
#   LAST_NEX_CHANGE_DATE                    DATE

sub update_nex_retail_load {

    my ( $dbh, $fh, $nex, $nrl_ref ) = @_;

    my $key = $nex->{style_id} . q{|} . $nex->{zone_id};

    # Bypass record comparison if commandline -i option is set for "insert
    # only" (nex_retail_load has no records).
    if ( not $insert_only ) {

        # If the record is already in nex_retail_load...
        if ( exists ${$nrl_ref}{$key} ) {

            # ...compare nex_retail_load.nex_current_retail
            # to the latest NEX retail coming in from dat file. If different...
            if ( ${$nrl_ref}{$key}->{nex_current_retail} != $nex->{retail_price} ) {

                # ...then update this record:
                my $sth_update = $dbh->prepare(
                    q{
                        UPDATE NEX_RETAIL_LOAD SET
                        ESTIMATED_LANDED_COST = ?,
                        RETAIL_PRICE = ?,
                        NEX_PREVIOUS_RETAIL_PRICE = ?,
                        NEX_CURRENT_RETAIL = ?,
                        LAST_NEX_CHANGE_DATE = ?
                        WHERE
                        STYLE_ID = ?
                        AND PRICE_ZONE_ID = ?
                        }
                );

                # ...compute MCX ELC using database function:
                my $estimated_landed_cost =
                    MCCS::RMS::StoredProcedures::query_exception_cost(
                    $dbh,
                    ${$nrl_ref}{$key}->{style_id},
                    ${$nrl_ref}{$key}->{price_zone_id}
                    );

                # ...compute MCX retail_price using database function:
                my $retail_price =
                    MCCS::RMS::StoredProcedures::get_permanent_retail_price( $dbh,
                    ${$nrl_ref}{$key}->{style_id} );

                #  ...set NPRP equal to the old nex_current_retail value
                my $nex_previous_retail_price = ${$nrl_ref}{$key}->{nex_current_retail};

                # ...set NCRP equal to the NEX value from data file
                my $nex_current_retail = $nex->{retail_price};

                # ...set LNCD to today's date
                my $last_nex_change_date = $date;

                if ($debug) {
                    print {$fh}
                        "RECORD UPDATED: ${$nrl_ref}{$key}->{style_id}\t${$nrl_ref}{$key}->{price_zone_id}\t$estimated_landed_cost\t$retail_price\t$nex_previous_retail_price\t$nex_current_retail\t$last_nex_change_date\n";
                }

                eval {
                    $sth_update->execute(
                        $estimated_landed_cost,
                        $retail_price,
                        $nex_previous_retail_price,
                        $nex_current_retail,
                        $last_nex_change_date,
                        ${$nrl_ref}{$key}->{style_id},
                        ${$nrl_ref}{$key}->{price_zone_id}
                    );
                }
                    or $log->log_error(
                    "nex_retail_load update failed Style: ${$nrl_ref}{$key}->{style_id}, Price Zone ID: ${$nrl_ref}{$key}->{price_zone_id}, ELC: $estimated_landed_cost, Retail Price: $retail_price, NEX Previous Retail Price: $nex_previous_retail_price, NEX Current Retail Price: $nex_current_retail, Last NEX Change Date: $last_nex_change_date\n"
                    );

                if ($EVAL_ERROR) {
                    $log->log_debug("$EVAL_ERROR\n");
                }
                else {
                    ;
                }

                return 1;

            }    # update record

            # Record exists but has not been updated
            if ($debug) {
                print {$fh}
                    "RECORD UNMODIFIED for STYLE_ID ${$nrl_ref}{$key}->{style_id} and ZONE_ID ${$nrl_ref}{$key}->{price_zone_id}\n";
            }

            return 1;

        }    # if exists

    }    #unless insert only run

    # insert record if it doesn't already exist
    my $sth_insert = $dbh->prepare_cached(
        q{
            INSERT INTO NEX_RETAIL_LOAD VALUES(?, ?, ?, ?, ?, ?, ?)
            }
    );

    my $estimated_landed_cost =
        MCCS::RMS::StoredProcedures::query_exception_cost( $dbh, $nex->{style_id},
        $nex->{zone_id} );
    my $retail_price =
        MCCS::RMS::StoredProcedures::get_permanent_retail_price( $dbh,
        $nex->{style_id} )
        || 0;

    my $nex_previous_retail_price = $nex->{retail_price};

    my $nex_current_retail = $nex->{retail_price};

    my $last_nex_change_date = $date;
    #TODO remove next line
    $log->log_info("estimated landed cosgt is . $estimated_landed_cost");
    if ($debug) {
        print {$fh}
            "RECORD INSERTED: $nex->{style_id}\t$nex->{zone_id}\t$estimated_landed_cost\t$retail_price\t$nex_previous_retail_price\t$nex_current_retail\t$last_nex_change_date\n";
    }

    eval {
        $sth_insert->execute( $nex->{style_id}, $nex->{zone_id}, $estimated_landed_cost,
            $retail_price, $nex_previous_retail_price, $nex_current_retail,
            $last_nex_change_date );
    }
        or $log->log_error(
        "nex_retail_load insert failed Style: $nex->{style_id}, Prize Zone ID: $nex->{zone_id}, ELC: $estimated_landed_cost, Retail Price: $retail_price, NEX Previous Retail Price: $nex_previous_retail_price, NEX Current Retail Price: $nex_current_retail, Last NEX Change Date: $last_nex_change_date\n"
        );

    if ($EVAL_ERROR) {
        $log->log_debug("$EVAL_ERROR\n");
    }
    else {
        ;
    }

    return 1;
}

#------------------------------------------------------------------------------
# Cache NEX_RETAIL_LOAD style_id, zone_id, retail_price, nex_current_retail
# for comparison against each warehouse's data file.

sub get_nex_retail_load {
    my ($dbh) = @_;

    my $sth = $dbh->prepare(
        q{
            SELECT
                style_id,
                price_zone_id,
                retail_price,
                nex_current_retail
            FROM
                nex_retail_load
         }
    );

    $sth->execute();

    my $nex_retail_load = $sth->fetchall_arrayref(
        {   style_id           => 1,
            price_zone_id      => 1,
            retail_price       => 1,
            nex_current_retail => 1
        }
    );

    # convert array of anonymous hashes into a hash of hashes; works a lot
    # more quickly
    my %nrl_hash;

    foreach my $r ( @{$nex_retail_load} ) {

        # use pipe for separator in debug files
        my $key = $r->{style_id} . q{|} . $r->{price_zone_id};

        $nrl_hash{$key} = {
            style_id           => $r->{style_id},
            price_zone_id      => $r->{price_zone_id},
            retail_price       => $r->{retail_price},
            nex_current_retail => $r->{nex_current_retail}
        };
    }

    return %nrl_hash;
}

#-------------------------------------------------------------------------------
sub success_message {
    my ( $now, $wh_num, $total ) = @_;

    return <<"END_MSG";
<?xml version="1.0" encoding="UTF-8"?>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3c.org/TR/xhtml1/xhtml1-strict.dtd">

<html xmlns="http://www.w3c.org/1999/xhtml" xml:lang="en" lang="en">

<head>
    <title>NEX Retail Load ($wh_num)</title>

    <style type="text/css">
    body{ font-family: arial, helvetica, sans; }
    .bold{ font-weight: bold; }
    </style>

</head>

<body>
    <div style="text-align: center;">
        <h1>NEX Retail Load</h1>
        <h2>Warehouse $wh_num</h2>
        <h3>Successful</h3>
    </div>

    <div>
        Processing completed: <span class="bold">$now</span>
    </div>

    <div>
    Please contact the <a href="mailto:help.desk\@usmc-mccs.org?subject=NEX Retail Load Help Request&cc=winstelgp\@usmc-mccs.org">MCCS Help Desk</a> if you experience any problems.
    </div>

    <div style="text-align: right;">
        <img src="http://ibis.usmc-mccs.org/lib/images/logos/perl_power.gif" />
    </div>
</body>
</html>
END_MSG
}

__END__

=pod

=head1 NAME

rms_nex_retail_load.pl - Load NEX retail data from NEXCOM Richter into MCX RAMS

=head1 VERSION

This documentation refers to rms_nex_retail_load.pl version 0.0.1.

=head1 USAGE

    /usr/local/mccs/bin/rms_nex_retail_load.pl

=head1 REQUIRED ARGUMENTS

None.

=head1 OPTIONS

=over 4

=item -d

Enable debugging. This should not be enabled in production due to the performance impact of extensive logging. This switch will also create several files in $DATA_DIR that contain dumps of certain data structures.

=item -i

Insert-only mode. To be used when there are no existing records in nex_retail_load, for greater performance.

=item -s

Do not download files. Use files alrady located in the $DATA_DIR. This is mostly useful for development.

=back

=head1 DESCRIPTION

In today's environment, Marine Corps (MCX) pulls merchandise using VRR from three Navy Exchange Command (NEXCOM) warehouses. These warehouses are Chino, CA (995), Norfolk, VA (992), and Pensacola, FL (983). Visual Rapid Replenishment (VRR) creates distributions on Richter (NEXCOM merchandising system) and passes the data to the appropriate warehouse management system. Merchandise is picked, shipped and the transactions are closed. Financial information is passed to Lawson (NEXCOM financial system), an invoice is created and sent to MCX. Invoicing is done using the Richter Average Moving Cost (AMC) by warehouse.

At MCX, site merchandise is received on the Richter Automated Merchandising System (RAMS). The receipt transaction uses the Style/Vendor Estimated Landed Cost (ELC). The receipt transaction cost information is passed to the MCX financial system, AXS-One. In most cases the Richter AMC and the RAMS ELC do not match. Therefore, invoices from NEXCOM do not match invoices from MCX.

This program enables the RAMS ELC to stay in sync with the Richter AMC.

=head1 REQUIREMENTS

=over 4

=item 1. Download files from NEXCOM Richter.

=over 4

=item A. Login to NEXCOM host hqimb3 (164.226.186.89) via FTP.

=item B. Remote directory is /rmdata/retek/rams_to_retek.

=item C. File names are:

        AMC_LOC_983.dat
        AMC_LOC_984.dat
        AMC_LOC_995.dat

B<NOTE:> If warehouses are added or deleted, this list will change.

=item D. Local directory is /usr/local/mccs/data/rms/retail_load.

=item E. Process is scheduled to run at 0500 daily.

=back

=item 2. Get BAR_CODES.STYLE_ID for each UPC.

A. The STYLE_ID is retrieved by querying the BAR_CODES table. Skip records where no STYLE_ID is associated with a UPC.

=item 3. Validation checks.

A. If the UPC is 13 digits and the leading position is 0, strip the zero and attempt to match in  If the leading digit is not zero, attempt to match on the whole UPC. Discard all others.

B. Skip records where the STYLE_VENDORS.VENDOR_ID does not equal 00001707694 for that STYLE_ID. This is the NEXCOM vendor ID.

J. Remove records with duplicate styles. Duplicates are tracked by style_id and zone_id.

=item 4. Convert Richter warehouse number to RAMS price zones.

A. Create a separate record for each zone. Each warehouse number will map to one or more pricing zones as follows:

    983 = PZ008, PZ018
    984 = PZ001, PZ002, PZ003, PZ004, PZ005
    995 = PZ009, PZ010, PZ011, PZ013, PZ014, PZ015, PZ016

=item 5. Load NEX retail. The NEX_RETAIL_LOAD table is updated with STYLE_ID, PRICE_ZONE_ID, ESTIMATED_LANDED_COST, RETAIL_PRICE, NEX_PREVIOUS_RETAIL_PRICE, NEX_CURRENT_RETAIL AND LAST_NEX_CHANGE_DATE.

=over 4

=item A. The initial load will create a new record in this table for each record represented by an entry in the Richter data files, as long as they pass the validation checks listed above.

=item B. Each load after the initial will load/change the record as follows:

=over 4

=item a. If the NEX retail price has changed, update NEX_RETAIL_LOAD.NEX_CURRENT_RETAIL with new retail price. Update NEX_PREVIOUS_RETAIL_PRICE with the old value from NEX_CURRENT_RETAIL. Update RETAIL_PRICE by running the database function GET_PERMANENT_RETAIL_PRICE to compute the current MCX retail price for this style_id and price_zone_id. Update LAST_NEX_CHANGE_DATE to date of current process.

=item b. If retail price does not exist, add record with date of current process.

=item c. B<DO NOT PURGE THIS TABLE>

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION

Requires ORACLE_HOME and TNS_ADMIN environment variables to be set. This is done by IBIS::DBI.

=head1 EXIT STATUS

None.

=head1 DEPENDENCIES

=over 4

=item * L<Data::Dumper>

=item * L<Getopt::Std>

=item * L<IBIS::Log::File>

=item * L<IBIS::Mail>

=item * L<IBIS::DBI>

=item * L<IBIS::SFTP>

=item * L<POSIX> qw(strftime)

=item * L<Readonly>

=item * L<version>

=item * L<English> qw(-no_match_vars)

=back

=head1 INCOMPATIBILITIES

None.

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to the L<MCCS Help Desk|mailto:help.desk@usmc-mccs.org>.
Patches are welcome.

=head1 BUSINESS PROCESS OWNER

Karen Stuekerjuergen L<stuekerjuergenk@usmc-mccs.org|mailto:stuekerjuergenk@usmc-mccs.org>

=head1 AUTHOR

William F. Isler L<islerwi@usmc-mccs.org|mailto:islerwi@usmc-mccs.org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2006 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut

