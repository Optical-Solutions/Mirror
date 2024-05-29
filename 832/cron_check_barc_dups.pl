#!/usr/local/mccs/perl/bin/perl
#---------------------------------------------------------------------
# Program:
# Author:   Hanny Januarius
# Created:  Tue Mar 13 09:46:23 EDT 2012
# Description: Check duplicate barcode on 832 tables
#
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
use IBIS::DB_Utils qw(rms_p_connect);

# Flush output
$| = 1;

#- One process at a time ---------------------------------------------
my $lock_file = "/usr/local/mccs/tmp/" . basename($0) . ".lck";
open SELF, "> $lock_file" or die "Could not create lock file $lock_file";
flock SELF, LOCK_EX | LOCK_NB or die "Another $0 process already running";

#- Configuration files -----------------------------------------------
my $g_cfg = new MCCS::Config;
my $g_emails = $g_cfg->RDI_group->{emails};

#- Global variables --------------------------------------------------
my $g_verbose = 0;
Readonly my $g_logfile => '/usr/local/mccs/log/832/' . basename(__FILE__) . '.log';
my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);
my $g_log = IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );
my $g_host = `hostname`; 
chomp($g_host);
my $go_mail = MCCS::WMS::Sendmail->new();

#---------------------------------------------------------------------
# SUBS
#---------------------------------------------------------------------
sub send_mail {
    my $msg_sub  = shift;
    my $msg_bod1 = shift;
    my $msg_bod2 = shift || '';
    my @body = ($msg_bod1, $msg_bod2);

    return if $g_verbose;    # Dont want to send email if on verbose mode

    $go_mail->logObj($g_log);
    $go_mail->subject($msg_sub);
    $go_mail->sendTo($g_emails);
    $go_mail->msg(@body);
    $go_mail->hostName($g_host);
    $go_mail->send_mail();
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
    my $sql =<<ENDSQL;
    select style_id, bar_code_id, color_id, size_id, dimension_id, count(*) C
    from bar_codes
    where business_unit_id = '30'
    group by
    style_id, bar_code_id, color_id, size_id, dimension_id
    having count(*) > 1
ENDSQL
    my $dbh = rms_p_connect();
    my $sth = $dbh->prepare($sql);

    $sth->execute();
    my $dat = "";
    while ( my $row = $sth->fetchrow_hashref) {
        $dat .= '<tr>' 
                . '<td>' . $row->{style_id} . '</td>'
                . '<td>' . $row->{bar_code_id} . '</td>'
                . '<td>' . $row->{color_id} .'</td>'
                . '<td>' . $row->{size_id}  .'</td>';
        if ( $row->{dimension_id} =~ m/\w/ ) {
        $dat .= '<td>' . $row->{dimension_id} . '</td>';
        } else {
        $dat .= '<td>n/a</td>';
        }
        $dat .= '<td>' . $row->{c} . '</td>'
                ."</tr>\n";
    }

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
    my $timestamp = `date`; chomp($timestamp);
    my $g_host = `hostname`; chomp($g_host);
    my $msg = <<EOM;
<table class="e832_table_nh">
        <tr>
            <th>Style</th>
            <th>Barcode</th>
            <th>Color</th>
            <th>Size</th>
            <th>Dim</th>
            <th>Dups count</th>
        </tr>
        $dat
</table>
<p style="font-size: 11px;">$timestamp</p>
<p style="font-size: 10px;">server: $g_host</p>
EOM
    my $from = 'rdistaff@usmc-mccs.org';
    if ( $dat ) {
        my $subject = "Duplicate detected on RMS database BAR_CODES table.";
        open( MAIL, "|/usr/sbin/sendmail -t" );
        ## Mail Header
        #print MAIL "To: " . 'rdistaff@usmc-mccs.org' . "\n";
        print MAIL "To: " . 'kaveh.sari@usmc-mccs.org' . "\n";
        print MAIL "From: $from\n";
        print MAIL "Subject: $subject\n";

        ## Mail Body
        print MAIL "Content-Type: text/html; charset=ISO-8859-1\n\n"
          . "<html><head>$css</head><body>$msg</body></html>";
        close(MAIL); 

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
