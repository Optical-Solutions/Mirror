#!/usr/local/mccs/perl/bin/perl
#---------------------------------------------------------------------
# Program    : missing_item_report.pl
# Author     : Hanny Januarius
# Created    : Fri Dec 29 07:26:39 EST 2017
#
# Description: Send email to MSST and others notifiying wms items that
#              are missed due to records were created after item send
#              cron job at 7pm.  Any record created after 7 pm should
#              be listed in this report.
#
# Requestor  : Alicia Morrison (POC)
#
# Ported by: HJ
# Date: Wed Dec  6 08:14:28 EST 2023
#---------------------------------------------------------------------
use strict;
use IBIS::DBI;
use IBIS::Log::File;
use File::Basename;
use File::Path;
use Readonly;
use MCCS::Config;
use Data::Dumper;
use Fcntl qw(:flock);
use MCCS::WMS::Sendmail;
use Getopt::Std;
use DateTime;
use Net::SFTP::Foreign

# Flush output
$| = 1;

#- One process at a time ---------------------------------------------
my $lock_file = "/usr/local/mccs/tmp/" . basename($0) . ".lck";
open SELF, "> $lock_file" or die "Could not create lock file $lock_file";
flock SELF, LOCK_EX | LOCK_NB or die "Another $0 process already running";

#- Get option switches -----------------------------------------------
our %g_opt = ( d => 0 );
getopts( 'd', \%g_opt );
my $DEBUG = $g_opt{d};

#- Configuration files -----------------------------------------------
my $g_cfg = new MCCS::Config;

my $g_emails      = $g_cfg->wms_missing_item->{tech_emails};        #TODO
my $g_cust_emails = $g_cfg->wms_missing_item->{customer_emails};    #TODO
my $g_dbname      = $g_cfg->wms_missing_item->{dbname};             #TODO

if ($DEBUG) {
    print Dumper $g_cfg->wms_missing_item;
}

#- Global variables --------------------------------------------------
my $g_verbose = 0;
if ($DEBUG) {
    $g_verbose = 1;
}
my $progname = basename(__FILE__);
$progname =~ s/\.\w+$//;
Readonly my $g_logfile => '/usr/local/mccs/log/BOA/' . $progname . '.log';

my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);
my $g_log = IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );
my $g_host = `hostname`;
chomp($g_host);
my $go_mail = MCCS::WMS::Sendmail->new();
my $g_dbh   = my $g_ba_sql;
my $g_ba_sth;

#---------------------------------------------------------------------
# SUBS
#---------------------------------------------------------------------
sub send_mail_html {
    my $msg_sub  = shift;
    my $msg_bod1 = shift;
    my $msg_bod2 = shift || '';

    return if $g_verbose;    # Dont want to send email if on verbose mode

    my $css = <<ECSS;
<style>
p, body {
    color: #000000;
    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
}

.e832_table_nh {
    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
    font-size: 11px;
    border-collapse: collapse;
    border: 1px solid #69c;
    margin-right: auto;
    margin-left: auto;
}
.e832_table_nh caption {
    background-color: #FFF;
    font-size: 11pt;
    font-weight: bold;
    padding: 12px 17px 5px 17px;
    color: #039;
}
.e832_table_nh th {
    padding: 1px 4px 0px 4px;
    background-color: RoyalBlue;
    font-weight: normal;
    font-size: 11px;
    color: #FFF;
}
.e832_table_nh tr:hover td {
    /*
    color: #339;
    background: #d0dafd;
    padding: 2px 4px 2px 4px;
    */
}
.e832_table_nh td {
    padding: 2px 3px 1px 3px;
    color: #000;
    background: #fff;
}
</style>
ECSS

    open( MAIL, "|/usr/sbin/sendmail -t" );
    print MAIL "To: " . $g_cust_emails->{'Hanny Januarius'} . " \n";
    print MAIL "From: rdistaff\@usmc-mccs.org\n";
    print MAIL "Cc: " . $g_emails->{'Hanny Januarius'} . " \n";
    print MAIL "Subject: $msg_sub \n";
    print MAIL "Content-Type: text/html; charset=ISO-8859-1\n\n"
        . "<html><head>$css</head><body>$msg_bod1 $msg_bod2</body></html>";

    #print MAIL $msg_bod2;
    print MAIL "\n";
    print MAIL "\n";
    print MAIL "Server: $g_host\n";
    print MAIL "\n";
    print MAIL "\n";
    close(MAIL);

} ## end sub send_mail_html

#---------------------------------------------------------------------
sub send_mail {
    my $msg_sub  = shift;
    my $msg_bod1 = shift;
    my $msg_bod2 = shift || '';
    my @body     = ( $msg_bod1, $msg_bod2 );

    return if $g_verbose;    # Dont want to send email if on verbose mode

    $go_mail->logObj($g_log);
    $go_mail->subject($msg_sub);
    $go_mail->sendTo($g_emails);
    $go_mail->msg(@body);
    $go_mail->hostName($g_host);
    $go_mail->send_mail();
} ## end sub send_mail

#---------------------------------------------------------------------
sub fatal_error {
    my $msg = shift;
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
    $g_log->error($msg);
    die $msg;
} ## end sub fatal_error

#---------------------------------------------------------------------

#---------------------------------------------------------------------

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
    my $local_d = '/usr/local/mccs/data/boa_daily_files';
    unless ( -d $local_d ) {mkpath($local_d)}
    chdir($local_d) or fatal_error("Could not cd to $local_d, $!");

    $g_log->info(" local dir:  $local_d");

    #my $host = '171.162.109.30';
    my $host = '171.162.110.18';
    my $u = 'usmcpers';
    my $p = 'asdf!@#$';
    my $dir = 'outgoing';
    $g_log->info(" Open SFTP to BoA at $host");
    $g_log->info(" user: $u");


    my $sftp = Net::SFTP::Foreign->new( $host, user=> $u, password => $p);
    $sftp->die_on_error("Unable to establish SFTP connection to $host user $u ");

    $sftp->setcwd($dir) or fatal_error( "Could not set to remote dir to $dir: " . $sftp->error );

    my $list_ref = $sftp->ls();

   foreach my $e ( @{$list_ref} ) {
     my $f = $e->{filename};
     $g_log->info(" " . $e->{longname});
     #print Dumper $e;
   }
    
   foreach my $e ( @{$list_ref} ) {
     my $f = $e->{filename};
     if ( -e $f ) {
        #$g_log->info(" skipped $f file exists");
     } else {
        $g_log->info(" get $f");
	$sftp->get($f) or fatal_error( "Could not get $f :" . $sftp->error );
     }
   }
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
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, 
        "Untrapped Error:\n\n", " $@" );
    $g_log->info($@);
}

#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------
