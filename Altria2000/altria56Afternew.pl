#!/usr/bin/perl
#---------------------------------------------------------------------
# Program    : altria.pl
# Author     :
# Created    : Wed Aug  9 09:19:32 EDT 2023
#
# Description:
#
# Requestor  :
#---------------------------------------------------------------------
use strict;
use Date::Manip::Date;
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
use Date::Calc qw(Add_Delta_YMD);
use Net::SFTP::Foreign;


# Flush output
$| = 1;

#- One process at a time ---------------------------------------------
# my $lock_file = "/usr/local/mccs/tmp/" . basename($0) . ".lck";
# open SELF, "> $lock_file" or die "Could not create lock file $lock_file";
# flock SELF, LOCK_EX | LOCK_NB or die "Another $0 process already running";

#- Get option switches -----------------------------------------------
our %g_opt = ( d => 0 );
getopts( 'd', \%g_opt );
my $DEBUG = $g_opt{d};  # if you pass -d on the command line it is on debug mode

#- Configuration files -----------------------------------------------
my $g_cfg = new MCCS::Config;

my $g_emails      = $g_cfg->Altria->{tech_emails};
my $g_cust_emails = $g_cfg->Altria->{customer_emails};
my $g_dbname      = $g_cfg->Altria->{RMS};
my $g_base_d      = "/tmp/altria2";
my $g_txt_d       = "/tmp/altria2";
my $g_count       = "";

#if ($DEBUG) {
    print Dumper $g_cfg->Altria;
#}
unless ( -d $g_txt_d ) {
    mkpath($g_txt_d);
}
#- Global variables --------------------------------------------------
# Any variable in all CAPS or have prefix g_ are global variables
# --------------------------------------------------------------------
my $g_verbose = 0;
if ($DEBUG) {
    $g_verbose = 1;
}
my $g_progname = basename(__FILE__);
$g_progname =~ s/\.\w+$//;
Readonly my $g_logfile => '/tmp/altria2/' . $g_progname . '.log';
my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);    # chop the carriage return or end of line.

my @timeFields = split(' ', localtime());
$timeFields[1] = uc($timeFields[1]); # make month uppercase

my @months = qw(JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC);
my @days   = qw(SUN MON TUE WED THU FRI SAT);
my $startMonth = 0;
# translate month string to index position in @months array
for( my $i = 0; $startMonth == 0 && $i < 12; $i++ ) {
    if($timeFields[1] eq $months[$i]) {
        $startMonth = $i + 1;
    }
}

my $startYear  = $timeFields[4] + 0; # as integer
my $startDay = $timeFields[2] + 0; # as integer
#Transalate day of week into a string which is a number
my $g_day_of_week = -1;
$timeFields[0] = uc($timeFields[0]);
for( my $i = 0; $g_day_of_week == -1 && $i < 7; $i++ ) {
    if($timeFields[0] eq $days[$i]) {
        $g_day_of_week = $i;
    }
}
#TODO Setting day to Sunday on next line for testing.
$g_day_of_week = 1;

my @endDates = Add_Delta_YMD($startYear, $startMonth, $startDay, 0, 0, -($g_day_of_week+1)); # Adjust to last Saturday

# pad month to min 2 chars with leading 0 if needed
if(length($endDates[1]) < 2) {
    $endDates[1] = '0' . $endDates[1];
}
# pad day of month to min 2 chars with leading 0 if needed
if(length($endDates[2]) < 2) {
    $endDates[2] = '0' . $endDates[2];
}

my $g_yyyymmdd = join('', @endDates);

chomp($g_yyyymmdd);     # chop the carriage return or end of line.
#my $today = Date::Manip::Date->new("today");
#my $g_day_of_week = $today->printf("%w");    # day of week as #number  we now will use 6 as Saturday, so we added the #line #below.
#my $g_day_of_week = '6';
my $g_log =
  IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );
my $g_host = `hostname`;
chomp($g_host);
my $g_mail = MCCS::WMS::Sendmail->new();

# Establish DB connection to RMS database
my $g_dbh = IBIS::DBI->connect( dbname => $g_dbname );
my $g_ba_sql;
my $g_ba_sth;

#---------------------------------------------------------------------
# SUBS
#---------------------------------------------------------------------
# sub send_mail_html {
#     my $msg_sub  = shift;
#     my $msg_bod1 = shift;
#     my $msg_bod2 = shift || '';

#     return if $g_verbose;    # Dont want to send email if on verbose mode

#     my $css = <<ECSS;
# <style>
# p, body {
#     color: #000000;
#     font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
# }

# .e832_table_nh {
#     font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
#     font-size: 11px;
#     border-collapse: collapse;
#     border: 1px solid #69c;
#     margin-right: auto;
#     margin-left: auto;
# }
# .e832_table_nh caption {
#     background-color: #FFF;
#     font-size: 11pt;
#     font-weight: bold;
#     padding: 12px 17px 5px 17px;
#     color: #039;
# }
# .e832_table_nh th {
#     padding: 1px 4px 0px 4px;
#     background-color: RoyalBlue;
#     font-weight: normal;
#     font-size: 11px;
#     color: #FFF;
# }
# .e832_table_nh tr:hover td {
#     /*
#     color: #339;
#     background: #d0dafd;
#     padding: 2px 4px 2px 4px;
#     */
# }
# .e832_table_nh td {
#     padding: 2px 3px 1px 3px;
#     color: #000;
#     background: #fff;
# }
# </style>
# ECSS

#     open( MAIL, "|/usr/sbin/sendmail -t" );
#     print MAIL "To: " . $g_cust_emails->{'rdi'} . " \n";
#     print MAIL "From: rdistaff\@usmc-mccs.org\n";
#     print MAIL "Cc: " . $g_emails->{'Hanny'} . " \n";
#     print MAIL "Cc: " . $g_emails->{'kaveh'} . " \n";
#     print MAIL "Cc: " . $g_emails->{'Mike'} . " \n";
#     print MAIL "Subject: $msg_sub \n";
#     print MAIL "Content-Type: text/html; charset=ISO-8859-1\n\n"
#       . "<html><head>$css</head><body>$msg_bod1 $msg_bod2</body></html>";
#     print MAIL "\n";
#     print MAIL "\n";
#     print MAIL "Server: $g_host\n";
#     print MAIL "\n";
#     print MAIL "\n";
#     close(MAIL);

# } ## end sub send_mail_html

#---------------------------------------------------------------------
# sub send_mail {
#     my $msg_sub  = shift;
#     my $msg_bod1 = shift;
#     my $msg_bod2 = shift || '';
#     my @body     = ( $msg_bod1, $msg_bod2 );

#     return if $g_verbose;    # Dont want to send email if on verbose mode

#     $g_mail->logObj($g_log);
#     $g_mail->subject($msg_sub);
#     $g_mail->sendTo($g_emails);
#     $g_mail->msg(@body);
#     $g_mail->hostName($g_host);
#     $g_mail->send_mail();
# } ## end sub send_mail

#---------------------------------------------------------------------
sub fatal_error {
    my $msg = shift;
    # send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
    $g_log->error($msg);
    die $msg;
} ## end sub fatal_error

#---------------------------------------------------------------------
# Make detail
# use $g_dbh  global database handle for RMS database
# prepare you SQL object here, execute and fetchrow array, hash, or all
# here.
# Do the clean up if needed here
# Return array reference (do not return full blown array)
#---------------------------------------------------------------------
sub make_detail {
    $g_log->info("Make detail records");

    my $data;



    my $sql = q{
select '64549' || '|' || TO_CHAR(
     m.week_ending_date, 'YYYYMMDD') || '|' || TO_CHAR(SD.SALE_DATE, 'YYYYMMDD') || '|' || TO_CHAR(SA.SALE_DATE_TIME, 'HH24:MI:SS') || '|' || SA.SITE_ID || SA.SLIP_NO || SA.REGISTER_ID || '|' || SA.SITE_ID || '|' || A.NAME || '|' || A.ADDRESS_2 || '|' || A.CITY || '|' || A.STATE_ID || '|' || A.ZIP_CODE || '|' || V.SUB_CLASS_DESCR || '|' || CV.DESCRIPTION || '|' || SD.STYLE_ID || '|' || SD.BAR_CODE_ID || '|' || S.DESCRIPTION || '|' || CASE
          WHEN v.department_id = '0991'
   
          and V.CLASS_ID = '1100'
          AND STYLE_TYPE = 'MULTI' THEN 'CARTON'
          WHEN v.department_id = '0991'
          and V.CLASS_ID = '1100'
          AND STYLE_TYPE = 'SINGLE' THEN 'PACK'
          WHEN v.department_id = '0991'
          and V.CLASS_ID = '1200'
          AND STYLE_TYPE = 'MULTI' THEN 'SLEEVE'
          WHEN v.department_id = '0991'
          and V.CLASS_ID = '1200'
          AND STYLE_TYPE = 'SINGLE' THEN 'CAN'
          wHEN V.DEPARTMENT_ID = '0992'
          AND V.CLASS_ID = '2000'
          AND STYLE_TYPE = 'MULTI' THEN 'CARTON'
          WHEN V.DEPARTMENT_ID = '0992'
          AND V.CLASS_ID = '2000'
          AND STYLE_TYPE = 'SINGLE' THEN 'EA'
          ELSE 'EA'
     END || '|' || SD.QTY || '|' || CASE
          WHEN STYLE_TYPE = 'MULTI' THEN (
               SELECT distinct TO_CHAR(X.MULTI_QTY)
               FROM STYLES X
               WHERE BUSINESS_UNIT_ID = 30
                    AND X.MULTI_STYLE_ID = S.STYLE_ID
          )
          else '1'
     END || '|' || NULL || '|' || --Multi Unit Indicator
     NULL || '|' || --Multi Unit Required Quantity
     NULL || '|' || --Multi Unit Discount Amount
     NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || SD.EXTENSION_AMOUNT || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || 'In Store' || '|' || NULL || '|' || NULL || '|' || NULL
FROM STYLES S,
     SALES SA,
     SALE_DETAILS SD,
     rsg.SITES_ALTRIA @mc2p A,
     V_DEPT_CLASS_SUBCLASS V,
     CHARACTERISTIC_VALUES cv,
     STYLE_CHARACTERISTICS SC,
     MERCH_CAL_MIKE_WEEK M
WHERE S.BUSINESS_UNIT_iD = 30
     AND S.BUSINESS_UNIT_ID = SA.BUSINESS_UNIT_ID
     AND S.BUSINESS_UNIT_ID = SD.BUSINESS_UNIT_ID
     AND S.BUSINESS_UNIT_ID = A.BUSINESS_UNIT_ID
     AND S.BUSINESS_UNIT_ID = V.BUSINESS_UNIT_ID
     AND S.BUSINESS_UNIT_ID = CV.BUSINESS_UNIT_iD
     AND S.BUSINESS_UNIT_ID = SC.BUSINESS_UNIT_iD
     AND S.BUSINESS_UNIT_ID = M.BUSINESS_UNIT_ID
     AND S.STYle_ID = SD.STYLE_ID
     AND SA.SITE_ID = SD.SITE_ID
     AND SA.SALE_DATE = SD.SALE_DATE
     AND SA.SLIP_NO = SD.SLIP_NO
     AND SA.REGISTER_ID = SD.REGISTER_ID
     AND SA.SITE_ID = A.SITE_ID
     AND S.SECTION_ID = V.SECTION_ID
     AND S.STYLE_ID = SC.STYLE_ID
     AND SC.CHARACTERISTIC_TYPE_ID = CV.CHARACTERISTIC_TYPE_ID
     AND CV.CHARACTERISTIC_TYPE_ID = 'BRAND'
     AND SC.CHARACTERISTIC_VALUE_ID = CV.CHARACTERISTIC_VALUE_ID
     AND Sa.SALE_DATE BETWEEN M.WEEK_STARTING_DATE AND M.WEEK_ENDING_DATE
     AND (
          (
               (
                    V.DEPARTMENT_ID = '0991'
                    and v.class_id IN ('1100', '1200')
               )
               or (
                    v.department_id = '0992'
                    and v.class_id = '2000'
                    and style_type != 'REGULAR'
               )
          )
     )
AND sa.sale_date between trunc(sysdate-(dow+7)) and trunc(sysdate -(dow+1))
ORDER BY 
SA.SITE_ID,
SA.SLIP_NO,
SA.REGISTER_ID
    };
    $sql =~s/dow/$g_day_of_week/g;
                    # Change this to the actual SQL

    my $sth = $g_dbh->prepare($sql) or fatal_error("Cannot prepare $sql");
    $g_log->info("prepare statement succeeded");
    $sth->execute;
    $g_log->info("execute completed");    
    my @array = ();
    if ($DEBUG) {
        print "LOOK here detail\n";
    }
    while (my $row = $sth->fetchrow_array) {
        push(@array, $row);
        print $row." \n"  if ($DEBUG); 
    }
        $g_log->info("push to detail completed");    

    $data = \@array;    # get array reference
    return $data;
}
#---------------------------------------------------------------------
# Make Header
# use $g_dbh  global database handle for RMS database
# prepare you SQL object here, execute and fetchrow array, hash, or all
# here.
# Do the clean up if needed here
# Return array reference (do not return full blown array)
#---------------------------------------------------------------------
#---------------------------------------------------------------------
## Make Header
## use $g_dbh  global database handle for RMS database
## prepare you SQL object here, execute and fetchrow array, hash, or all
## here.
## Do the clean up if needed here
## Return array reference (do not return full blown array)
###-------------------------------------------------------------------
sub make_header {
    $g_log->info("Make header records");

    my $data;
    my $sql = q{ 
select              COUNT(*) ||'|'||
                    SUM(SD.QTY)||'|'||

                    to_char(SUM(SD.EXTENSION_AMOUNT),'fm9999999.90')||'|'||
                    'MARINECORPSPERSONALFAMILYREAD'
                    
                    
FROM                STYLES S,
                    SALES SA,
                    SALE_DETAILS SD,
                    rsg.SITES_ALTRIA @mc2p A,
                    V_DEPT_CLASS_SUBCLASS V,
                    CHARACTERISTIC_VALUES cv,
                    STYLE_CHARACTERISTICS SC,
                    MERCH_CAL_MIKE_WEEK M
                    
WHERE               S.BUSINESS_UNIT_iD = 30 AND
                    S.BUSINESS_UNIT_ID = SA.BUSINESS_UNIT_ID AND
                    S.BUSINESS_UNIT_ID = SD.BUSINESS_UNIT_ID AND
                    S.BUSINESS_UNIT_ID = A.BUSINESS_UNIT_ID AND
                    S.BUSINESS_UNIT_ID = V.BUSINESS_UNIT_ID AND
                    S.BUSINESS_UNIT_ID = CV.BUSINESS_UNIT_iD AND
                    S.BUSINESS_UNIT_ID = SC.BUSINESS_UNIT_iD AND
                    S.BUSINESS_UNIT_ID = M.BUSINESS_UNIT_ID AND
                    S.STYle_ID = SD.STYLE_ID AND
                    SA.SITE_ID = SD.SITE_ID AND
                    SA.SALE_DATE = SD.SALE_DATE AND
                    SA.SLIP_NO = SD.SLIP_NO AND
                    SA.REGISTER_ID = SD.REGISTER_ID AND
                    SA.SITE_ID = A.SITE_ID AND
                    S.SECTION_ID = V.SECTION_ID AND
                    S.STYLE_ID = SC.STYLE_ID AND
                    SC.CHARACTERISTIC_TYPE_ID = CV.CHARACTERISTIC_TYPE_ID AND
                    CV.CHARACTERISTIC_TYPE_ID = 'BRAND' AND
                    SC.CHARACTERISTIC_VALUE_ID = CV.CHARACTERISTIC_VALUE_ID AND
                    Sa.SALE_DATE BETWEEN M.WEEK_STARTING_DATE AND M.WEEK_ENDING_DATE AND
                    ( ((V.DEPARTMENT_ID = '0991' and
                    v.class_id IN ('1100','1200')) or
                    (v.department_id = '0992' and v.class_id = '2000' and style_type!='REGULAR'))) and
                    AND sa.sale_date between trunc(sysdate-(dow+7)) and trunc(sysdate -(dow+1)) 
                    }; 
   #and rownum < 10
    $sql =~s/dow/$g_day_of_week/g;
                    # Change this to the actual SQL
#$sql = "select sysdate from dual";
    my $sth = $g_dbh->prepare($sql) or fatal_error("Cannot prepare $sql");
    $sth->execute;
    my @array = ();

    while ( my $row = $sth->fetchrow_array ) {
        push( @array, $row );
    }

    if ($DEBUG) {
        print "---------------------------------------------\n";
        print "LOOK here HEADER\n";
        print @array;
        print " \n";
        print "---------------------------------------------\n";
    }
    $g_count = substr($array[0],0,index($array[0],'|'));
    $data = \@array;    # get array reference

    return $data;
}

sub create_record {
    my $filename =
      "MARINE CORPS PERSONAL & FAMILY READ" . $g_yyyymmdd . ".TXT";
      #"MARINE_CORPS_PERSONAL & FAMILY_READ" . $g_yyyymmdd . $$. ".txt";
    my $fileout = $g_txt_d . '/' . $filename;
    my $header  = make_header();
    my $detail  = make_detail();
    my $fh;
    open($fh, '>', $fileout)
      or fatal_error("Could not write $filename due to $!");
    foreach my $h (@{$header}) {
        print $fh $h."\n";
    }
    foreach my $d (@{$detail}) {
        print $fh $d."\n";
    }
    close ($fh);
    my $fsize = -s $fileout;
    $g_log->info("$filename created");
    $g_log->info("$fsize bytes ");

    return $filename;
}

#---------------------------------------------------------------------
# sub push_to_altria {
#     my $file = shift;
#     $g_log->info("Push $file to Altria sftp server");

# # Server and authentication details
#  my $hostname = $g_cfg->Altria->{sftpserver};
#  #print "hostname is $hostname \n";
#  my $username = $g_cfg->Altria->{user};
#  my $password = $g_cfg->Altria->{pw};

#  # Local file path and remote destination
#  my $local_path =   $g_txt_d . '/' . $file;
#  my $remote_path = $g_cfg->Altria->{sftp_dir} . '/' . $file;
#  print sftp_dir;

#  # Create an SFTP object
#  #my $sftp = Net::SFTP::Foreign->new($hostname, user => $username, password => $password);


#  # Check for errors in creating the SFTP object
#  die "Could not establish SFTP connection: " . $sftp->error unless defined $sftp;

#  # Upload the local file
#  #$sftp->put($local_path, $remote_path) or die "Failed to upload file: " . $sftp->error;

#  print "File uploaded successfully.\n";

#  # Disconnect from the SFTP server
#  #$sftp->disconnect;
# #
# }

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

    my $filename = create_record();
    #push_to_altria($filename);
    # send_mail_html(
    #     "Altria Successful Transmision Notification " . `date`,
    #     "Number of Records = " . $g_count,
    #     ""
    # );

    # example error bit
    # fatal_error("ERROR HERE!\n");

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
    # send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date,
    #     "Untrapped Error:\n\n", " $@" );
    $g_log->info($@);
}

#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------
