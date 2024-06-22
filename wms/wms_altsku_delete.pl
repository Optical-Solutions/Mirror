#!/usr/bin/perl
#---------------------------------------------------------------------
# Program:  wms_altsku_delete.pl
# Author:   Hanny Januarius
# Created:  Tue May 26 08:39:14 EDT 2015
# Description:
#  Build altsku flat file for WMS
#  Outfiles are located at /usr/local/wms/data/flatfile directory.
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

# This object for flat file
# based on BAR_CODES_ACTIVITY table
#----------------------------------
my $go_wms_ba = MCCS::WMS::Query->new(
                                       dbname  => $g_dbname,
                                       outfile => 'ALTSKU_DELETE' #,  logfile => $g_object_logfile
                                     );
my $g_verbose   = $go_wms_ba->get_param('altsku_debug_flag');
my $g_days_past = $go_wms_ba->get_param('altsku_days_past');
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
           'ALTSKU',
           '',
           bca.bar_code_id,
           bca.style_id,
           bca.color_id,
           bca.size_id,
           bca.dimension_id,
           '3' -- this is DELETE
from       
           bar_codes_audit bca
where  
           bca.sub_type in ('UPCA','EAN')
       and      
        (
          ( (bca.sub_type = 'UPCA') AND ( length(bca.bar_code_id) = 12) ) 
           OR
          ( (bca.sub_type = 'EAN') AND ( length(bca.bar_code_id) = 13) )
        )
       and bca.OPERATION_type   = 'DELETE'
       and bca.business_unit_id = '30'
       and bca.business_unit_id   = '30'
       and trunc(bca.operation_date)   =  trunc(sysdate)
       and (    
               exists(
                 select '1'
                 from styles s
                 where s.style_id = bca.style_id
                 and   s.business_unit_id = '30'
                 and   s.section_id not in ('20343','20344')
               )
               OR
               exists(
                 select '1'
                 from STYLES_DELETED_REC s
                 where s.style_id = bca.style_id
                 and   s.business_unit_id = '30'
                 and   s.section_id not in ('20343','20344')
               )
           )
       and ( 
            not  exists (
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
           OR
            exists (
                        select 'x'
                        from v_wms_barcode_swapped v
                        where
                        v.sub_type           in ('UPCA','EAN')
                   and
                    (
                      ( (v.sub_type = 'UPCA') AND  ( length(v.bar_code_id) = 12) )
                       OR
                      ( (v.sub_type = 'EAN') AND  ( length(v.bar_code_id) = 13) )
                    )
                   and v.operation_type           = 'DELETE'
                   and v.business_unit_id         = '30'
                   and trunc(v.operation_date)    = trunc( sysdate  )
                   and v.style_id                 = bca.style_id
                   and v.bar_code_id              = bca.bar_code_id
                   and v.color_id                 = bca.color_id
                   and v.size_id                  = bca.size_id
                   and v.business_unit_id         = '30'
          )
       )
EOFSQL

    # set the SQL into our object so it can build the flat file
    #----------------------------------------------------------
    $go_wms_ba->set_sql($sql);

} ## end sub build_main_sql

#---------------------------------------------------------------------
sub spool_flat_file {
    $g_log->info('Spool flat file based on BAR_CODES_ACTIVITY');
    $go_wms_ba->spool();
    $go_wms_ba->send();

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
