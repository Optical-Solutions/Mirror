#!/usr/local/mccs/perl/bin/perl
#---------------------------------------------------------------------
# Program    : "schedules_with_no_parameters_report.pl"
# Author     : Hanny Januarius
# Created    : Wed Sep  7 12:00:04 EDT 2022
# Description: Hi RDI, 
#
#              I have a request for a new cron from RSG.  Over the weekend, 
#              I was called about an issue with a scheduler.  A user setup 
#              a Replenishment Scheduler without adding any parameters.  
#              The problem this causes: the scheduler will run for EVERY stock generation.  
#              I was able to kill the scheduler to stop the PO creations and the Replenishment i
#              team deleted the scheduler so we solved the short term problem.  
#              Long term they’d like a Cron to run every day that would list out a scheduler 
#              setup without the generation ID.  
#
#              Below is a query of the object for the Replenishment Schedulers.  
#              I’d like this to run every day at 3:15pm and sent to the below group of people.  
#              I’ve submitted an SR for this (SR768613). 
#
#              The below is a quick query but really this is all that should be needed.  
#              If there is no parameter_selection_id associated with a schedule, that means 
#              there are null parameters and null randoms.  
#
#              Nora Jansen
#
# Ported by  : Hanny Januarius
# Date       : Mon Dec 11 09:03:18 EST 2023
# 
# Ported by  : Kaveh Sari
# Date       : Mon Jun 24 14:32:22 EDT 2024
#            : No Changed Required...Tested Query / and email functionality.
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
use warnings;

# Flush output
$| = 1;

#- One process at a time ---------------------------------------------
my $lock_file = "/usr/local/mccs/tmp/" . basename($0) . ".lck";
open(my $fh, ">", $lock_file) or die "Could not create lock file $lock_file";
flock $fh, LOCK_EX | LOCK_NB or die "Another $0 process already running";

#- Get option switches -----------------------------------------------
our %g_opt = (
    d => 0
);
getopts('d', \%g_opt);
my $DEBUG = $g_opt{d};

#- Configuration files -----------------------------------------------
my $g_cfg = new MCCS::Config;

my $g_emails = $g_cfg->schedules_with_no_parameters_report->{emails};
my $g_dbname = $g_cfg->schedules_with_no_parameters_report->{dbname};

print Dumper $g_emails if $DEBUG;

#- Global variables --------------------------------------------------
my $g_verbose = 0;
Readonly my $g_logfile => '/usr/local/mccs/log/' . basename(__FILE__) . '.log';
my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);
my $g_log = IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );
my $g_host = `hostname`; 
chomp($g_host);
my $go_mail = MCCS::WMS::Sendmail->new();
my $g_dbh = IBIS::DBI->connect(dbname=>$g_dbname);

#---------------------------------------------------------------------
# SUBS
#---------------------------------------------------------------------
sub send_mail {
    my $msg_sub  = shift;
    my $msg_bod1 = shift;
    my $msg_bod2 = shift || '';
    my @body = ($msg_bod1, $msg_bod2);

    return if $g_verbose;    # Dont want to send email if on verbose mode
    my $css     = <<ECSS;
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
    font-size: 11pt;
    padding: 12px 17px 5px 17px;
    color: #black;
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
    #$go_mail->logObj($g_log);
    #$go_mail->subject($msg_sub);
    #$go_mail->sendTo($g_emails);
    #$go_mail->msg(@body);
    #$go_mail->hostName($g_host);
    #$go_mail->send_mail();
    foreach my $user (sort keys %{$g_emails}) {
    open( my $mail, "-|","/usr/sbin/sendmail -t" );
        print $mail "To: " . $g_emails->{$user} . " \n";
        print $mail "From: rdistaff\@usmc-mccs.org\n";
        print $mail "Cc: " . $g_emails->{RDI} . " \n";
        print $mail "Subject: $msg_sub \n";
        print $mail "Content-Type: text/html; charset=ISO-8859-1\n\n"
          . "<html><head>$css</head><body>$msg_bod1 $msg_bod2</body></html>";
        print $mail "\n";
        print $mail "<br>\n";
        print $mail "<br>\n";
        print $mail "Server: $g_host\n";
        print $mail "\n";
        print $mail "\n";
    close($mail);
    }

}

#---------------------------------------------------------------------
sub fatal_error {
    my $msg = shift;
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
    $g_log->error($msg);
    die $msg;
}

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

    my $sql = <<END1;
select schedule_id, object_name, description, date_created, parameter_selection_id 
from   job_schedules
where  
       parameter_selection_id is null
-- and    object_name ='SRJS0010'
and object_name not in ('SRJS0240','SRJS0280','SRJS0290','SRJS0170')



END1
    my $sth = $g_dbh->prepare($sql);

    $sth->execute();
    my @data = ();
    
    while (my $row = $sth->fetchrow_hashref) {
	push(@data, $row);
    }
    $g_dbh->disconnect;
    my $str = "";
    # print Dumper \@data;
    #
    if ( @data ) {
	$str .= qq(<table class="e832_table_nh">\n);
	$str .= qq(<tr>\n);
	$str .= qq(<th>Schedule Id</th>\n);
	$str .= qq(<th>Object Name</th>\n);
	$str .= qq(<th>Description</th>\n);
	$str .= qq(<th>Created Date</th>\n);
	$str .= qq(<th>Parameter Selection Id</th>\n);
	$str .= qq(</tr>\n);

        foreach my $e (@data) {
	   $str .= qq(<tr>\n);
	   $str .= qq(<td>$e->{schedule_id}</td>\n);
	   $str .= qq(<td>$e->{object_name}</td>\n);
	   $str .= qq(<td>$e->{description}</td>\n);
	   $str .= qq(<td>$e->{date_created}</td>\n);
	   $str .= qq(<td>$e->{parameter_selection_id}</td>\n);
	   $str .= qq(</tr>\n);
        }
	$str .= qq(</table><br>\n);

	$str .= qq(<p>database = $g_dbname</p>\n );
	$str .= qq(<p>Help Desk please contact <b>MSST On Call</b> if you receive outside normal business hours.</p>\n );

        send_mail("Schedules with no Parameters",$str);
    } else {
        #send_mail("Schedules with no Parameters","<h4>Schedules with no Parameters</h4>\n<p>No row selected</p>");
    }
    $g_log->info("-- End ------------------------------------------");
}

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
