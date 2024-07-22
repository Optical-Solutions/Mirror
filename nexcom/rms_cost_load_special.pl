#!/usr/local/mccs/perl/bin/perl
#--------------------------------------------------------------------------
#Ported by : Soumya K Bhowmic
#Date      : 11/30/2023
#
#Brief Desc: This program gets the retail data for Nexcom for zones 
#            983 and 995   
#            Thereafter it loads them into RMS Database table 
#            NEX_RETAIL_LOAD (insert and updates)
#
#Ported by  :Kaveh Sari
#Date       :Mon Jul 22 14:49:16 EDT 2024
#           :Tested the process by copying data files from xxx56 to 0010 and running 
#           :the same.
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
use IBIS::Log::File;
use IBIS::Mail;
use IBIS::DBI;
use IBIS::SFTP;
Readonly::Scalar my $DATA_DIR         => '/usr/local/mccs/data/rms/cost_load';
Readonly::Scalar my $REMOTE_DIR       => '/u5/retekdata/prd/rms/mcx/amc';
Readonly::Scalar my $NEX_VENDOR_ID    => '00001707694';
Readonly::Scalar my $LOGFILE          => '/usr/local/mccs/log/nexcom/rms_cost_load_special.log';
#TODO, uncomment next line and delete line after that.
#Readonly::Scalar my $MAIL_TO          => 'rdistaff@usmc-mccs.org|';
Readonly::Scalar my $MAIL_TO          => 'kaveh.sari@usmc-mccs.org|';
Readonly::Scalar my $MAIL_CC          => q{};
Readonly::Scalar my $CONNECT_INSTANCE => 'rms_p';
Readonly::Scalar my $FTP_CONNECT      => 'nexcom_production';

my $dbh;

my $server = `hostname`;
chomp($server);

die "Must run this process as rdiusr\n" unless MCCS::Utils::is_rdiusr();

# Signal handler for child processes
#-----------------------------------
$SIG{CHLD} = sub {
    while ( waitpid( -1, WNOHANG ) > 0 ) { }
};

my (%opt);

getopts( 'dsv', \%opt );

my $debug = $opt{d};

# This variable contains the files to be retrieved from NEXCOM Richter. Update
# this list to add new warehouses.
#------------------------------------------------------------------------------
#EMER FIX my @rfiles = qw(AMC_LOC_983.dat AMC_LOC_984.dat AMC_LOC_995.dat);
my @rfiles = qw(AMC_LOC_983.dat AMC_LOC_995.dat);

# Standardized error messages
#----------------------------
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

#my $date = strftime( '%Y-%m-%d', localtime );
# makes '01-MAY-2006'
my $date = uc( strftime( '%d-%b-%Y', localtime ) );

# Pricing zones keyed to warehouses. Add warehouses and zones here.
#------------------------------------------------------------------
my %zone = (
##             983 => [ 'PZ008', 'PZ018' ],
##             984 => [ 'PZ001', 'PZ002', 'PZ003', 'PZ004', 'PZ005' ],
             983 => [ 'PZ008', 'PZ018', 'PZ001', 'PZ002', 'PZ003', 'PZ004', 'PZ005' ],
             995 => [ 'PZ009', 'PZ010', 'PZ011', 'PZ013', 'PZ014', 'PZ015', 'PZ016' ],
           );

## 984 => [ 'PZ001', 'PZ002', 'PZ003', 'PZ004', 'PZ005', 'PZ017' ],
## PZ017 was removed as requested on Aug 27, 14, yuc

my $log = IBIS::Log::File->new( { file => $LOGFILE, append => 1 } );

$log->log_info("RMS Cost Load initiated\n");

$log->log_die("Cost Load $DATA_DIR data directory does not exist\n")
  unless -d $DATA_DIR;

# Maybe permissions are too tight?
#---------------------------------
chdir $DATA_DIR
  or $log->log_die("Cannot change to cost load data directory\n");

# Start the program logic ------------------------------------------------------

unless ( $opt{s} )    # Don't fetch files if we are in dev mode
{
    unlink MCCS::RMS::CostLoad::get_file_names($DATA_DIR);

    # Fetch the files from NEXCOM's host
    IBIS::SFTP->new(
                     {
                       destination => $FTP_CONNECT,
                       remote_dir  => $REMOTE_DIR,
                       files       => \@rfiles
                     }
      )->get()
      or $log->log_die("Cannot retrieve files from NEXCOM FTP server\n");
} ## end unless ( $opt{s} )

my @files = MCCS::RMS::CostLoad::get_file_names($DATA_DIR);

if (@files) {
    for (@files) {
        my ($file) = $_ =~ m/([^\/]*)$/xms;
        $log->log_info("Retrieved $file from NEXCOM\n");
    }
} ## end if (@files)
else {
    $log->log_die("No files retrieved from NEXCOM\n");
}

# TODO Don't forget to update this for production or development

$dbh = IBIS::DBI->connect( dbname => $CONNECT_INSTANCE )
  or $log->log_info("Cannot connect to RAMS database\n");

# if failed to connect to db the first time, do 3 attempts:
# if still dose not connect, die, and sending out email messages.
#-----------------------------------------------------------------
unless ($dbh) {
    $dbh = &multiple_db_connecting_attempts();
}

# Cache some data before we start ----------------------------------------------

my $nex_styles = MCCS::RMS::CostLoad::get_nex_styles($dbh);

my $style_singles = get_style_singles($dbh);

# End data caching -------------------------------------------------------------

## $dbh->disconnect();

# Loop through warehouse files -------------------------------------------------
my $pid;

for (@files) {
    ##my $fdbh = IBIS::DBI->connect(dbname => $CONNECT_INSTANCE );

    # Set the warehouse number. Filenames are of the form AMC_LOC_xxx
    # where xxx is the warehouse number. This enables the addition of
    # new warehouses by updating the warehouse file list @rfiles.
    # See above.

    my ($wh_num) = $_ =~ m/AMC_LOC_(\d{3})\.dat$/xms;

    my ( $data, $multi_cost, $multi_price, $total ) = process_file( $dbh, $_, $wh_num );
    update_database( $dbh, $wh_num, $data, $multi_cost, $multi_price );

    my $now = strftime( '%Y-%m-%d %H:%M:%S', localtime );

    $log->log_info("Warehouse $wh_num RMS Cost Load complete: $now\n");

    # Report Stats
    # $total{amc_records}
    # $total{amc_greater_than}
    # $total{singles}
    # $total{high_cost}
    # $total{styles}
    # $total{multiples}
    # $total{zero_amc}
    # $total{nexcom_upcs}
    # $total{invalid_upcs}

    # Set a default value in case some are undefined. It could happen.
    $total->{amc_records}      = $total->{amc_records}      || 'N/A';
    $total->{invalid_upcs}     = $total->{invalid_upcs}     || 'N/A';
    $total->{amc_greater_than} = $total->{amc_greater_than} || 'N/A';
    $total->{singles}          = $total->{singles}          || 'N/A';
    $total->{zero_amc}         = $total->{zero_amc}         || 'N/A';
    $total->{high_cost}        = $total->{high_cost}        || 'N/A';
    $total->{styles}           = $total->{styles}           || 'N/A';

    my $msg = success_message( $now, $wh_num, $total );

    MCCS::RMS::CostLoad::mail_notify( $MAIL_CC, $server . ' RMS Cost Load Complete', $msg );

    ##$fdbh->disconnect();

} ## end for (@files)

# End warehouse file loop ------------------------------------------------------

#===============================================================================
# Subroutines 
#===============================================================================

#---------------------------------------------------------------------
# Parse records from the warehouse file and eliminate invalid barcodes
#---------------------------------------------------------------------
sub process_file {
    my ( $dbh, $file, $wh_num ) = @_;

    my ( @data, $multi_cost, $multi_price, $total );

    $log->log_debug("Processing for warehouse $wh_num\n") if $debug;

    # Should this be warn or die? If we warn, we can keep processing the
    # rest of the files.
    #-------------------------------------------------------------------
    open my $fh, '<', $_
      or $log->log_die("Cannot read data file for $wh_num\n");

    next unless -e $_;

    while (<$fh>) {

        # UPC, Average Moving Cost, Retail Price
        #---------------------------------------
        my ( $upc, $amc, $retail ) = $_ =~ m/^(\d*),\s*([\d\.]*),\s*([\d\.]*)$/smx;

        $total->{amc_records}++;

        if ( zero_average_moving_cost($amc) ) {
            $total->{zero_amc}++;
            next;
        }

        if ( MCCS::RMS::CostLoad::invalid_upc($upc) ) {
            $total->{invalid_upc}++;
            next;
        }

        # 6/9/2009 ERS per MIke T, remove this condition
        #
        # Skip records where the AMC is greater than the retail cost
        #        if ( $amc > $retail )
        #        {
        #            $log->log_info("$wh_num: $error{7} for UPC $upc\n");
        #            $total->{amc_greater_than}++;
        #            next;
        #        }

        # Get the style id for the UPC
        #-----------------------------
        my $style_id = MCCS::RMS::CostLoad::style_id( $dbh, $upc );

        if ($style_id) {
            $total->{styles}++;
        }
        else {
            next;
        }

        # Identify style_id's with multiple costs. If there are multiple
        # records for the same style for a UPC from the inbound file,
        # these are probably styles with dimensions/colors/sizes and we
        # cannot load. These items must be handled manually. Multi-price
        # styles are removed later in this program.
        #----------------------------------------------------------------

        $multi_cost->{$wh_num}{$style_id}{$amc}++;

        if ( keys %{ $multi_cost->{$wh_num}{$style_id} } > 1 ) {
            $log->log_info("$wh_num: $error{3} for UPC $upc style $style_id amc $amc\n");
            next;
        }

        # New condition. Multiple prices assigned to a style id.
        $multi_price->{$wh_num}{$style_id}{$retail}++;

        if ( keys %{ $multi_price->{$wh_num}{$style_id} } > 1 ) {
            $log->log_info("$wh_num: $error{8} for UPC $upc style $style_id retail $retail\n");
            next;
        }

        # Skip styles in the style_vendors table where
        # the vendor_id != '00001707694' (the vendor is not NEXCOM)
        next unless exists $nex_styles->{$style_id};

        # Skip styles in the styles table with style_type = 'SINGLE'.
        if ( exists $style_singles->{$style_id} ) {
            $log->log_info("$wh_num: $error{5} for UPC $upc style $style_id\n");
            $total->{singles}++;
            next;
        }

        # Skip styles where the cost variance is greater than 50%.
        if ( high_cost_difference( $dbh, $style_id, $amc ) ) {
            $log->log_info("$wh_num: $error{6} for UPC $upc ($amc) style $style_id\n");
            $total->{high_cost}++;
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

    } ## end while (<$fh>)

    close $fh;

    # Dump some data useful for debugging --------------------------------------
    if ($debug) {
        open my $fh, '>', "./nex_styles_$wh_num.dbg" or die "$!\n";
        print {$fh} Dumper($nex_styles);
        close $fh;

        open $fh, '>', "./style_singles_$wh_num.dbg" or die "$!\n";
        print {$fh} Dumper($style_singles);
        close $fh;

        open $fh, '>', "./multiple_cost_$wh_num.dbg" or die "$!\n";
        print {$fh} Dumper($multi_cost);
        close $fh;

        open $fh, '>', "./multi_price_$wh_num.dbg" or die "$!\n";
        print {$fh} Dumper($multi_price);
        close $fh;

        open $fh, '>', "./data_$wh_num.dbg" or die "$!\n";
        print {$fh} Dumper( \@data );
        close $fh;
    } ## end if ($debug)

    # End debug dumps ----------------------------------------------------------

    return \@data, $multi_cost, $multi_price, $total;

} ## end sub process_file

#-------------------------------------------------------------------------------
# Write records to the IRI_WHSLE_STYLE_VENDORS table
#-------------------------------------------------------------------------------
sub update_database {
    my ( $dbh, $wh_num, $data, $multi_cost, $multi_price ) = @_;
    my %dupestyles;

    my $fh1;

    if ($debug) {
        open $fh1, '>', "./IRI_WHSLE_STYLE_VENDORS_$wh_num" or die "$!\n";
    }

    $dbh->begin_work or $log->log_die("$wh_num: $dbh->errstr\n");

    for my $r ( @{$data} ) {
        for my $zone_id ( @{ $zone{ $r->{warehouse} } } ) {

            # warehouse | style_id | amc
            unless (    ( keys %{ $multi_cost->{ $r->{warehouse} }{ $r->{style_id} } } > 1 )
                     or ( keys %{ $multi_price->{ $r->{warehouse} }{ $r->{style_id} } } > 1 ) )
            {
                # UPC | ZONE | STYLE_ID | RETAIL_PRICE | DATE
                # May need to add checks to make sure the first three values are
                # defined.

                next if ( exists $dupestyles{ $r->{style_id} }{$zone_id}{ $r->{amc} }{ $r->{retail_price} } );

                update_iri_whsle_style_vendors(
                                                $dbh, $fh1,
                                                {
                                                   business_unit_id         => '30',
                                                   job_id                   => '999999999999',
                                                   style_id                 => $r->{style_id},
                                                   vendor_id                => $NEX_VENDOR_ID,
                                                   region_district_sub_type => 'DISTRICT',
                                                   site_group_id            => $zone_id,
                                                   start_date               => $date,
                                                   end_date                 => '01-JAN-2525',
                                                   primary_vendor_ind       => 'Y',
                                                   local_first_cost         => $r->{amc},
                                                   cost_descriptor          => 'EA',
                                                   cost_factor              => 1,
                                                   landed_cost_id           => '0000',
                                                   exclude_vendor_rebate    => 'N',
                                                   rtv_allowed_ind          => 'Y',
                                                   date_created             => $date,
                                                   created_by               => 'MERCH',
                                                   status                   => 'N',
                                                   process_status           => 'N'
                                                },
                                              );

                $dupestyles{ $r->{style_id} }{$zone_id}{ $r->{amc} }{ $r->{retail_price} } = 1;
            } ## end unless ( ( keys %{ $multi_cost...}))
        } ## end for my $zone_id ( @{ $zone...})
    } ## end for my $r ( @{$data} )

    if ($debug) {
        close $fh1;
    }

    eval { $dbh->commit } or $log->log_die("$wh_num: $dbh->errstr\n");

    return 1;

} ## end sub update_database

#-------------------------------------------------------------------------------
# If inbound cost (AMC from file) is +/- 50% difference, do not load. These need
# to be reviewed and loaded manually.
#
# Compare the incoming AMC zone cost to
# style_vendor_exception_costs.estimated_landed_cost. If the record exists and
# the cost difference is more than 50% or if the record does not
# exist then compare the incoming AMC zone cost to
# style_vendors.estimated_landed_cost.
#
# This subroutine calls get_elc() and exceeds_cost_variance(). Update
# exceeds_cost_variance() to change the parameters.

sub high_cost_difference {

    my ( $dbh, $style_id, $amc ) = @_;

    my $elc = get_elc( $dbh, $style_id );

    if ($elc) {
        if ( exceeds_cost_variance( $amc, $elc, $style_id ) ) {
            if ($debug) {
                $log->log_debug(
"High Cost Diff: AMC $amc svec_elc $elc for style $style_id. Style Vendor Exception Cost is too high.\n" );
            }

            return 1;
        } ## end if ( exceeds_cost_variance...)

        return;
    } ## end if ($elc)

    # Couldn't find style_vendor_exception_cost.estimated_landed_cost
    # so use style_vendors.estimated_landed_cost
    #-----------------------------------------------------------------
    elsif ( defined $nex_styles->{$style_id}{estimated_landed_cost} ) {
        $elc = $nex_styles->{$style_id}{estimated_landed_cost};

        if ( exceeds_cost_variance( $amc, $elc, $style_id ) ) {
            if ($debug) {
                $log->log_debug(
"High Cost Diff: AMC $amc sv_elc $elc for style $style_id. Style Vendor Estimated Landed Cost is too high.\n"
                );
            }

            return 1;
        } ## end if ( exceeds_cost_variance...)

        return;
    } ## end elsif ( defined $nex_styles...)

    # Ooops, we lose
    else {
        $log->log_error("Could not determine ELC for style $style_id\n");
        return 1;
    }

} ## end sub high_cost_difference

#-------------------------------------------------------------------------------
sub exceeds_cost_variance {
    my ( $amc, $elc, $style_id ) = @_;
    my $d;

    # Catchem nasty divide by zero mothers
    eval { $d = ( $elc - $amc ) / $elc; };

    if ($@) {
        $log->log_error("Divide by zero elc $elc for style $style_id!\n")
          if $opt{v};
        return 1;
    }

    return ( ( $d >= .5 ) or ( $d <= -.5 ) ) ? $d : undef;

} ## end sub exceeds_cost_variance

#-------------------------------------------------------------------------------
# Unfortunately, we have to use two steps to determine the ELC per requirements.
#-------------------------------------------------------------------------------
sub get_elc {
    my ( $dbh, $style_id ) = @_;

    my $sth = $dbh->prepare_cached(
        qq{
        SELECT
            ESTIMATED_LANDED_COST
        FROM
            STYLE_VENDOR_EXCEPTION_COSTS
        WHERe
            BUSINESS_UNIT_ID = '30'
            AND STYLE_ID = ?
            AND VENDOR_ID = $NEX_VENDOR_ID
    }
                                  );

    my $r = $dbh->selectrow_arrayref( $sth, undef, $style_id );

    return $r->[0] ? $r->[0] : undef;
} ## end sub get_elc

#-------------------------------------------------------------------------------
# AMC's with zero are no good
#-------------------------------------------------------------------------------
sub zero_average_moving_cost {
    no warnings 'uninitialized';
    my ($amc) = @_;

    return $amc == 0 ? 1 : undef;

} ## end sub zero_average_moving_cost

#-------------------------------------------------------------------------------
# Insert records into IRI_WHSLE_STYLE_VENDORS
#
# Table structure IRI_WHSLE_STYLE_VENDORS
#   BUSINESS_UNIT_ID           NUMBER(2)
#   JOB_ID                     NUMBER(12)
#   STYLE_ID                   VARCHAR2(14)
#   VENDOR_ID                  VARCHAR2(11)
#   REGION_DISTRICT_SUB_TYPE   VARCHAR2(10)
#   SITE_GROUP_ID              VARCHAR2(5)
#   SITE_ID                    VARCHAR2(5)
#   START_DATE                 DATE
#   END_DATE                   DATE
#   VENDOR_STYLE_ID            VARCHAR2(25)
#   PRIMARY_VENDOR_IND         VARCHAR2(1)
#   COUNTRY_OF_ORIGIN_ID       VARCHAR2(10)
#   COUNTRY_OF_SUPPLY_ID       VARCHAR2(10)
#   CURRENCY_ID                VARCHAR2(10)
#   PURCHASING_TYPE            VARCHAR2(10)
#   FOREIGN_FIRST_COST         NUMBER(12,2)
#   LOCAL_FIRST_COST           NUMBER(12,2)
#   COST_DESCRIPTOR            VARCHAR2(5)
#   COST_FACTOR                NUMBER(11,3)
#   LANDED_COST_ID             VARCHAR2(4)
#   ESTIMATED_LANDED_COST      NUMBER(12,4)
#   INNER_PACK_QTY             NUMBER(11,3)
#   OUTER_PACK_QTY             NUMBER(11,3)
#   MINIMUM_LEVEL              NUMBER(5)
#   DISCOUNT_ID                VARCHAR2(5)
#   FREE_UNITS_CONDITION       NUMBER(11,3)
#   FREE_UNITS_QUANTITY        NUMBER(11,3)
#   CONSIGNMENT_RATE           NUMBER(6,3)
#   PO_REPL_TYPE               VARCHAR2(30)
#   EXCLUDE_VENDOR_REBATE      VARCHAR2(1)
#   RTV_ALLOWED_IND            VARCHAR2(1)
#   DATE_CREATED               DATE
#   CREATED_BY                 VARCHAR2(15)
#   STATUS                     VARCHAR2(2)
#   PROCESS_STATUS             VARCHAR2(2)
#   REJECTED_ID                NUMBER(10)
#   STATE_OF_SUPPLY_ID         VARCHAR2(10)
#   STATE_OF_ORIGIN_ID         VARCHAR2(10)
#   VENDOR_STYLE_NO            VARCHAR2(25)
#   DISTRIBUTED_BY             VARCHAR2(1 CHAR)
#   PO_BY                      VARCHAR2(1 CHAR)
#   PALLET_TIER                NUMBER(3)
#   PALLET_HEIGHT              NUMBER(3)
#   GROUP_SALES_FLAG           VARCHAR2(1 CHAR)
#   ITEM_TYPE                  VARCHAR2(1 CHAR)
#   PRICE_REQUIRED_IND         VARCHAR2(1 CHAR)
#   NOTE_1                     VARCHAR2(30 CHAR)
#   NOTE_2                     VARCHAR2(30 CHAR)
#   NOTE_3                     VARCHAR2(30 CHAR)

sub update_iri_whsle_style_vendors {
    my ( $dbh, $fh, $h ) = @_;

    my $insert_sql = q{
        INSERT  /*+ APPEND */ 
        INTO IRI_WHSLE_STYLE_VENDORS
        VALUES(
         ?,    ?,    ?,    ?,    ?, 
         ?,    NULL, ?,    ?,    NULL, 
         ?,    NULL, NULL, NULL, NULL, 
         NULL, ?,    ?,    ?,    ?, 
         NULL, 
               mccs_plsql_functions.get_inner_pack_qty(?,?,?), 
                     mccs_plsql_functions.get_outer_pack_qty(?,?,?,null), 
                           NULL, NULL, 
         NULL, NULL, NULL, NULL, ?, 
         ?,    ?,    ?,    ?,    ?, 
         NULL, NULL, NULL, NULL, 'O', 
         'O',  NULL, NULL, NULL, NULL, 
         NULL, NULL, NULL, NULL
         )
    };

    my $sth = $dbh->prepare_cached($insert_sql);

    if ($debug) {
        print {$fh}
                   "$h->{business_unit_id}\t$h->{job_id}\t$h->{style_id}\t$h->{vendor_id}\t$h->{region_district_sub_type}\t$h->{site_group_id}\t$h->{start_date}\t$h->{end_date}\t$h->{primary_vendor_ind}\t$h->{local_first_cost}\t$h->{cost_descriptor}\t$h->{cost_factor}\t$h->{landed_cost_id}\t$h->{exclude_vendor_rebate}\t$h->{rtv_allowed_ind}\t$h->{date_created}\t$h->{created_by}\t$h->{status}\t$h->{process_status}\n";
    }

    # There is a unique key constraint on this table. We may see records
    # where the same style is mapped to multiple UPCs. This will cause an
    # insert failure.
    #
    eval {
        $sth->execute(
            $h->{business_unit_id},
            $h->{job_id},
            $h->{style_id},
            $h->{vendor_id},
            $h->{region_district_sub_type},
            $h->{site_group_id},
            $h->{start_date},
            $h->{end_date},
            $h->{primary_vendor_ind},
            $h->{local_first_cost},
            $h->{cost_descriptor},
            $h->{cost_factor},
            $h->{landed_cost_id},
            $h->{style_id}, $h->{vendor_id}, $h->{site_group_id},    #inner_pack_qty
            $h->{style_id}, $h->{vendor_id}, $h->{site_group_id},    #outer_pack_qty
            $h->{exclude_vendor_rebate},
            $h->{rtv_allowed_ind},
            $h->{date_created},
            $h->{created_by},
            $h->{status},
            $h->{process_status}
                     );
      }
      or $log->log_error(
"iri_whsle_style_vendors update failed\n$insert_sql\nStyle: $h->{style_id}, Vendor: $h->{vendor_id}, Zone: $h->{site_group_id}, AMC: $h->{local_first_cost}, Price: $h->{local_first_cost}\n"
      );

    if ($@) {
        $log->log_debug("$insert_sql\n$@\n");
    }

    return;

} ## end sub update_iri_whsle_style_vendors

#-------------------------------------------------------------------------------
# Cache all 'SINGLE' type styles
#-------------------------------------------------------------------------------
sub get_style_singles {
    my ($dbh) = @_;

    return $dbh->selectall_hashref(
        q{
            SELECT
                style_id,
                style_type
            FROM
                styles
            WHERE
                business_unit_id = '30'
                and style_type = 'SINGLE'
        },
        'style_id'
                                  );
} ## end sub get_style_singles

#-------------------------------------------------------------------------------
sub success_message {
    my ( $now, $wh_num, $total ) = @_;

    return <<"END_MSG";
<?xml version="1.0" encoding="UTF-8"?>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3c.org/TR/xhtml1/xhtml1-strict.dtd">

<html xmlns="http://www.w3c.org/1999/xhtml" xml:lang="en" lang="en">

<head>
    <title>RMS Cost Load Message</title>

    <style type="text/css">
    body{ font-family: arial, helvetica, sans; }
    .bold{ font-weight: bold; }
    </style>

</head>

<body>
    <div style="text-align: center;">
        <h1>RMS Cost Load</h1>
        <h2>Warehouse $wh_num</h2>
        <h3>Successful</h3>
    </div>

    <div>
        Processing completed: <span class="bold">$now</span>
    </div>

    <div>
    Processing Statistics
        <ul>
            <li>Inbound Records: <span class="bold">$total->{amc_records}</span></li>
            <li>Styles Loaded: <span class="bold">$total->{styles}</span></li>
            <li>Invalid UPC: <span class="bold">$total->{invalid_upcs}</span></li>
            <li>AMC Greater Than Retail: <span class="bold">$total->{amc_greater_than}</span></li>
            <li>Single Styles: <span class="bold">$total->{singles}</span></li>
            <li>High Cost Variance: <span class="bold">$total->{high_cost}</span></li>
            <li>Zero AMC: <span class="bold">$total->{zero_amc}</span></li>
        </ul>
    </div>

    <div>
    Please contact the <a href="mailto:help.desk\@usmc-mccs.org?subject=RMS Cost Load Help Request&cc=winstelgp\@usmc-mccs.org">MCCS Help Desk</a> if you experience any problems.
    </div>

    <div style="text-align: right;">
        <img src="http://ibis.usmc-mccs.org/lib/images/logos/perl_power.gif" />
    </div>
</body>
</html>
END_MSG

} ## end sub success_message

sub email_to_ccstr_pip_dlmt {
    my ( $cc_str, $subject, $body ) = @_;
    my @cc_list = split( /\|/, $cc_str );
    foreach my $to_str (@cc_list) {
        if ($to_str) {
            my $hdrstr = '';
            $hdrstr .= "To: $to_str\n";
            $hdrstr .= "From: rdistaff\@usmc-mccs.org\n";
            ##$hdrstr .= "Cc: $cc_str\n" if $cc_str;
            $hdrstr .= "Subject: $subject\n";
            $body   .= "\n\temail from host: $server\n";
            open MAIL, "|/usr/sbin/sendmail -t" or die "Can not open sendmail\n";
            print MAIL $hdrstr . "\n";
            print MAIL $body . "\n";
            close MAIL;
        } ## end if ($to_str)
    } ## end foreach my $to_str (@cc_list)
} ## end sub email_to_ccstr_pip_dlmt

sub multiple_db_connecting_attempts {
    my $reconnection_ctr = 1;
    my $max_attempts     = 3;
    my $dbh;
    my $error_msg;
    while ( $reconnection_ctr <= $max_attempts ) {
        unless ($dbh) {
            sleep(30);
            $reconnection_ctr++;
            eval {
                     $dbh = IBIS::DBI->connect( dbname => $CONNECT_INSTANCE )
                  or $log->log_info("Cannot connect to RAMS database in attempt: $reconnection_ctr\n");

            };
            if ($@) {
                $error_msg .= $@ . "\n";
            }
        } ## end unless ($dbh)
    } ## end while ( $reconnection_ctr...)

    unless ($dbh) {
        my $subject = "RMS COST LOAD Failed to Connect to DB";
        my $body =
"Program rms_cost_load.pl failed to connect to RMS database for data loading. Program terminiated. Details: $error_msg";
        &email_to_ccstr_pip_dlmt( $MAIL_TO, $subject, $body );
        $log->log_info("$subject $body. Email sent and program will terminate.");
        die;
    } ## end unless ($dbh)

    return $dbh;

} ## end sub multiple_db_connecting_attempts

__END__

=pod

=head1 NAME

rms_cost_load.pl - Load cost data from NEXCOM into MCX RAMS

=head1 VERSION

This documentation refers to rms_cost_load.pl version 0.0.1.

=head1 USAGE

    /usr/local/mccs/bin/rms_cost_load.pl

=head1 REQUIRED ARGUMENTS

None.

=head1 OPTIONS

=over 4

=item -d

Enable debugging. This should not be enabled in production due to the performance impact of extensive logging. This switch will also create several files in $DATA_DIR that contain dumps of certain data structures.

=item -s

Do not download files. Use files alrady located in the $DATA_DIR. This is mostly useful for development.

=item -v

Enable verbose logging. This option reports AMC greater than retail and  divide by zero errors.

=back

=head1 DESCRIPTION

In today's environment, Marine Corps (MCX) pulls merchandise using VRR from three Navy Exchange Command (NEXCOM) warehouses. These warehouses are Chino, CA (995), Norfolk, VA (992), and Pensacola, FL (983). Visual Rapid Replenishment (VRR) creates distributions on Richter (NEXCOM merchandising system) and passes the data to the appropriate warehouse management system. Merchandise is picked, shipped and the transactions are closed. Financial information is passed to Lawson (NEXCOM financial system), an invoice is created and sent to MCX. Invoicing is done using the Richter Average Moving Cost (AMC) by warehouse.

=head1 REQUIREMENTS

=over 4

=item 1. Download files from NEXCOM.

=over 4

=item A. Login to NEXCOM host hqimb3 (164.226.186.89) via FTP.

=item B. Remote directory is /rmdata/retek/rams_to_retek.

=item C. File names are:

        AMC_LOC_983.dat
        AMC_LOC_984.dat
        AMC_LOC_995.dat

B<NOTE:> If warehouses are added or deleted, this list will change.

=item D. Local directory is /usr/local/mccs/data/rms/cost_load.

=item E. Process is scheduled to run at 0330 daily.

=back

=item 2. Get BAR_CODES.STYLE_ID for each UPC.

A. The STYLE_ID is retrieved by querying the BAR_CODES table. Skip records where no STYLE_ID is associated with a UPC.

=item 3. Validation checks.

A. Skip records where the AMC is greater than the retail cost. REMOVED 6/9/2009 ERS

B. Skip records with local NEXCOM UPCs. These are generic UPCs created by NEXCOM for internal use. NEXCOM UPCs are identified by the first two digits of 04.

C. If the UPC is 13 digits and the leading position is 0, strip the zero and attempt to match in RAMS. If the leading digit is not zero, attempt to match on the whole UPC. Discard all others.

D. Skip records where the STYLE_VENDORS.VENDOR_ID does not equal 00001707694 for that STYLE_ID. This is the NEXCOM vendor ID.

E. Skip records that have corresponding records in IRI_WHSLE_STYLE_VENDORS where IRI_WHSLE_STYLE_VENDORS.STATUS equals 'N'.

F. Skip records with multiple costs. If there are multiple records for the same style in the inbound AMC file, these records are probably styles with dimensions/colors/sizes. This data is not passed between systems, therefore these items must be loaded manually.

G. Skip records with a zero AMC.

H. Skip records where STYLES.STYLE_TYPE equals 'SINGLE'.

I. Skip records where the AMC is +/- 50% of the ELC. These records must be reviewed and loaded manually.

J. Remove records with duplicate styles. Duplicates are tracked by warehouse, style_id, and AMC.

=item 4. Convert Richter warehouse number to RAMS price zones.

A. Create a separate record for each zone. Each warehouse number will map to one or more pricing zones as follows:

    983 = PZ008, PZ018
    984 = PZ001, PZ002, PZ003, PZ004, PZ005, PZ017
    995 = PZ009, PZ010, PZ011, PZ013, PZ014, PZ015, PZ016

=item 5. Load IRI_WHSLE_STYLE_VENDORS table. 

=back

=back

=item 6. Report Errors

=over 4

=item No Style/Vendor Record - See 3.D

=item Record already exists - See 3.E

=item Multiple costs - See 3.F

=item Zero AMC value - See 3.G

=item Single - See 3.H

=item High Cost Difference - See 3.I

=back

=item 7. Send email notification when processing is complete.

=back

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

=item * L<IBIS::FTP>

=item * L<POSIX> qw(strftime)

=item * L<Readonly>

=item * L<version>

=back

=head1 INCOMPATIBILITIES

None.

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to the L<MCCS Help Desk|mailto:help.desk@usmc-mccs.org>.
Patches are welcome.

=head1 HISTORY

2/5/2008 - ERS - Minor changes to before turning on in testing for Jesta 9.2. 9.2 code was already substantially there.
11/17/2008 - ERS - Added usage of Oracle function rdiusr.mcc_utils.get_inner_pack_qty() and rdiusr.mcc_utils.get_outer_pack_qty().
		per Karen S, pack quantities are being handled incorrectly by Jesta, we need to fix for them by looking up from style_vendors. 
6/9/2009 ERS per MIke T, Gary W, Karen S, remove AMC > retail condition
8/25/2009 ERS removed fork... its pointless and overly complicated. Changed to use IBIS::SFTP instead of IBIS::FTP

=head1 BUSINESS PROCESS OWNER

Karen Stuekerjuergen L<stuekerjuergenk@usmc-mccs.org|mailto:stuekerjuergenk@usmc-mccs.org>

=head1 AUTHOR

Trevor S. Cornpropst L<cornpropstt@usmc-mccs.org|mailto:cornpropstt@usmc-mccs.org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2006 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
