#!/usr/bin/perl
#---------------------------------------------------------------------
# Program:  wms_item_add_update.pl
# Author:   Hanny Januarius
# Created:  Tue May 26 07:34:07 EDT 2015
# Description:
#  Build sku/item flat file for WMS
#  Outfiles are located at /usr/local/wms/data/flatfile directory.
#
#  This program is using parameters table at MC2P database,
#  table WMS_PARAMETERS.
#---------------------------------------------------------------------
use strict;
use IBIS::DBI;
use IBIS::Log::File;
use File::Basename;
use File::Path;
use Readonly;
use MCCS::Config;
use MCCS::WMS::Query;
use Getopt::Std;
use Fcntl qw(:flock);

# Flush output
$| = 1;

#- One process at a time ---------------------------------------------
my $lock_file = "/usr/local/wms/tmp/" . basename($0) . ".lck";
open SELF, "> $lock_file" or die "Could not create lock file $lock_file";
flock SELF, LOCK_EX | LOCK_NB or die "Another $0 process already running";

#- Configuration files -----------------------------------------------
my $g_cfg    = new MCCS::Config;
my $g_emails = $g_cfg->rws_cron_check->{emails};
my $g_dbname = $g_cfg->wms_global->{DBNAME};

#- Global variables --------------------------------------------------
my $g_verbose = 0;
Readonly my $g_logfile => '/usr/local/wms/log/' . basename(__FILE__) . '.log';
my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);
my $g_log = IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );
my $g_host = `hostname`;
chomp($g_host);

# prefix g_  = global
# prefix go_ = global object
my $g_object_logfile  = '/usr/local/wms/log/' . basename(__FILE__) . '_Query_ba.log';
my $g_object_logfile2 = '/usr/local/wms/log/' . basename(__FILE__) . '_Query_ta.log';

# This object for flat file
# based on BAR_CODES_ACTIVITY table
#----------------------------------
my $go_wms_ba = MCCS::WMS::Query->new(
    dbname  => $g_dbname ,
    outfile => 'ITEM_ADD_UPDATE' #, logfile => $g_object_logfile
);
sleep 2;

# This object for flat file
# based on TABLE_AUDITS table
#----------------------------------
my $go_wms_ta = MCCS::WMS::Query->new(
    dbname  => $g_dbname ,
    outfile => 'ITEM_ADD_UPDATE' #, logfile => $g_object_logfile2
);

my $g_verbose   = $go_wms_ba->get_param('sku_debug_flag');
my $g_days_past = $go_wms_ba->get_param('sku_days_past');
my $g_params;

#---------------------------------------------------------------------
# SUBS
#---------------------------------------------------------------------
sub send_mail {
    my $msg_sub  = shift;
    my $msg_bod1 = shift;
    my $msg_bod2 = shift || '';
    if ($g_verbose) {

        # Dont want to send email if on verbose mode
        $g_log->info("Not Sending any email out on \$g_verbose = $g_verbose");
        return;
    } ## end if ($g_verbose)
    foreach my $name ( sort keys %{$g_emails} ) {
        $g_log->info( "Sent email to $name (" . $g_emails->{$name} . ")" );
        $g_log->info("  Sbj: $msg_sub ");
        $g_log->debug("  $msg_bod1 ");
        $g_log->debug("  $msg_bod2 ");
        open( MAIL, "|/usr/sbin/sendmail -t" );
        print MAIL "To: " . $g_emails->{$name} . " \n";
        print MAIL "From: rdistaff\@usmc-mccs.org\n";
        print MAIL "Subject: $msg_sub \n";
        print MAIL "\n";
        print MAIL $msg_bod1;
        print MAIL $msg_bod2;
        print MAIL "\n";
        print MAIL "Server: $g_host";
        print MAIL "\n";
        print MAIL "\n";
        close(MAIL);
    } ## end foreach my $name ( sort keys...)
} ## end sub send_mail

#---------------------------------------------------------------------
sub fatal_error {
    my $msg = shift;
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
    $g_log->error($msg);
    die $msg;
} ## end sub fatal_error
#---------------------------------------------------------------------
sub build_main_sql {

    my $sql = <<EOFSQL;
select     distinct
           'SKU',
           '',
           s.hazardous_id,
           s.description,
           'USA',
           s.weight,
           s.cube,
           tih.lobid,
           tih.lobdesc,
           '',
           s.estimated_landed_cost,
           b.style_id,
           b.color_id,
           b.size_id,
           nvl(b.dimension_id,''),
           '1'  -- This is ADD
           --decode(bca.operation_type, 'ADD', '1',
           --                           'UPDATE', '2',
           --                           'DELETE', '3', ''
           --      ) activity_type
from       
           bar_codes          b,
           bar_codes_audit    bca,
           styles             s,
           te_item_hier       tih
where  
           b.bar_code_id        = bca.bar_code_id
       and b.sub_type           in ('UPCA','EAN')
       and
        (
          ( (b.sub_type = 'UPCA') AND  ( length(b.bar_code_id) = 12) )
           OR
          ( (b.sub_type = 'EAN') AND  ( length(b.bar_code_id) = 13) )
        )
       and bca.operation_type           = 'ADD'
       and bca.business_unit_id         = '30'
       and trunc(bca.operation_date)    =  trunc(sysdate)
       and bca.style_id                 = s.style_id
       and s.business_unit_id           = '30'
       and s.section_id not in ('20343','20344') -- exclude department 0098
       and tih.styleid                  = s.style_id
       and not  exists (
       -- Do not want to include the same barcode, same color size dim
       -- ADD and DELETE type
            select 'y'
            from 
                    bar_codes_audit zz
            where   zz.bar_code_id = bca.bar_code_id
            and     zz.color_id    = bca.color_id
            and     zz.size_id     = bca.size_id
            and     zz.style_id    = bca.style_id
            and     zz.operation_type in ('ADD','DELETE')
            and     nvl(zz.dimension_id, 'zxcv') = nvl(bca.dimension_id, 'zxcv')
            and     trunc(zz.operation_date)   =  trunc(sysdate)
            group by 
                    zz.style_id, zz.bar_code_id, zz.color_id, zz.size_id, nvl(zz.dimension_id, 'zxcv')
            having mod(count(*),2) = 0
       )
EOFSQL

    # set the SQL into our object so it can build the flat file
    #----------------------------------------------------------
    $go_wms_ba->set_sql($sql);
    $g_log->info( "bar_codes_activity:\n" . $sql );

    my $sql2 = <<EOFSQL2;
select     distinct
           'SKU',
           '',
           s.hazardous_id,
           s.description,
           'USA',
           s.weight,
           s.cube,
           tih.lobid,
           tih.lobdesc,
           '',
           s.estimated_landed_cost,
           b.style_id,
           b.color_id,
           b.size_id,
           nvl(b.dimension_id,''),
           '2' -- this is UPDATE
from       
           bar_codes          b,
           styles             s,
           te_item_hier       tih
where  
           s.style_id       in 
           (
              -- Find all styles that have its description changed
              --
              select substr(ta.primary_key,4)
              from   table_audits ta
              where  ta.business_unit_id = '30'
              and    ta.application_id   = 'RAMS'
              and    ta.table_name       = 'STYLES'
              and    ta.column_name      = 'DESCRIPTION'
              and    ta.operation        = 'UPDATE'
              and    trunc(ta.operation_date)   =  trunc(sysdate)
           )
       and b.style_id           = s.style_id
       and b.sub_type           in ('UPCA','EAN')
       and
        (
          ( (b.sub_type = 'UPCA') AND  ( length(b.bar_code_id) = 12) )
           OR
          ( (b.sub_type = 'EAN') AND  ( length(b.bar_code_id) = 13) )
        )
       and s.business_unit_id   = '30'
       and b.business_unit_id   = '30'
       and tih.styleid          = s.style_id
       and s.section_id not in ('20343','20344') -- exclude department 0098
EOFSQL2

    # set the SQL into our object so it can build the flat file
    #----------------------------------------------------------
    $go_wms_ta->set_sql($sql2);
    $g_log->info( "table_audits:\n" . $sql2 );
} ## end sub build_main_sql

#---------------------------------------------------------------------
sub spool_flat_file {
    $g_log->info('Spool flat file based on BAR_CODES_ACTIVITY');
    $go_wms_ba->spool();
    sleep 1;
    $g_log->info('Spool flat file based on TABLE_AUDITS');
    $go_wms_ta->spool();

    $g_log->info("Since we send only one file, I concat file ta into ba and we send ba file to NFI.");

    my $ba_file = $go_wms_ba->get_flatfilename();
    my $ta_file = $go_wms_ta->get_flatfilename();

    # Read the ta (file that contains records from TABLE_AUDITS)
    my $fin;
    open($fin, $ta_file) or fatal_error("Could not read $ta_file because $!");
    my @ta_data = <$fin>;
    close $fin;
    
    my $fout;
    open($fout, ">>", $ba_file) or fatal_error("Could not concat to $ba_file because $!");
    print $fout @ta_data;
    close $fout;

    $g_log->info("Send " . basename($ba_file). " to NFI");
    $go_wms_ba->send();
     
    $g_log->info("Remove " . basename($ta_file)) ;
    $g_log->info("No need to archive it.");

    unlink($ta_file);

} ## end sub spool_flat_file

#---------------------------------------------------------------------
# Pseudo Main which is called from MAIN
#---------------------------------------------------------------------
sub my_main {
    my $sub = __FILE__ . " the_main";
    if ($g_verbose) {

        # Record Everything
        $g_log->level(5);
    }
    else {

        # Record Everything, except debug logs
        $g_log->level(4);
    }

    $g_log->info("-- Start ----------------------------------------");
    $g_log->info("Database = $g_dbname");
    build_main_sql();
    spool_flat_file();
    $g_log->info("-- End ------------------------------------------");
} ## end sub my_main

#---------------------------------------------------------------------
# MAIN PROGRAM - Do not modify main below.
# Use the main above aka my_main
#---------------------------------------------------------------------

# Want to catch warning
$SIG{__WARN__} = sub { $g_log->warn("@_") };

# Execute the main
eval { my_main() };
if ($@) {
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, "Untrapped Error:\n\n", " $@" );
    $g_log->info($@);
}

#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------
