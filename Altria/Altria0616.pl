#!/usr/bin/perl
#---------------------------------------------------------------------
# Program    : altria.pl
# Author     : Kaveh Sari
# Created    : Wed Aug  9 09:19:32 EDT 2023
#
# Description:
#
# Requestor  :
# Ported by  : Hanny Januarius
# Date       : Thu Dec  7 08:11:38 EST 2023
# Tested     : For Cloud Deployment.
#---------------------------------------------------------------------
use strict; 
use warnings;
use Date::Manip::Date;
use IBIS::DBI;
use File::Basename;
use File::Path;
use File::Copy;
use Readonly;
use MCCS::Config;
use Data::Dumper;
use Fcntl qw(:flock);
use MCCS::WMS::Sendmail;
use Getopt::Long;
use DateTime;
use Date::Calc qw(Add_Delta_YMD);
use Net::SFTP::Foreign;
use Carp;
use MCCS::MCE::Util;
use Sys::Hostname qw(hostname);

local $| = 1;
my $util = MCCS::MCE::Util->new();

use Getopt::Long;

# Declare these before using them
my $DEBUG = 0;
my $param_dow_plus_7;
my $param_dow_plus_1;
my ($param_start_date, $param_end_date);
my $help;
my $nosend;
my $param_po;

GetOptions(
    'd'   => \$DEBUG,
    'w=i' => \$param_dow_plus_7,
    'x=i' => \$param_dow_plus_1,
    's=s' => \$param_start_date,  # Start date mm/dd/yyyy
    'e=s' => \$param_end_date,    # End date mm/dd/yyyy
    'po=s'     => \$param_po,  
    'h|help' => \$help,
    'nosend' => \$nosend,    
       
) or die "Error in command line arguments\n";

if ($help) {
    print <<"USAGE";
Usage: $0 [options]

Options:
  -d           Enable debug output
  -w <int>     Days offset from today for start (e.g., 7 = a week ago)
  -x <int>     Days offset from today for end   (e.g., 1 = yesterday)
  -s <mm/dd/yyyy>  Start date (format: mm/dd/yyyy)
  -e <mm/dd/yyyy>  End date   (format: mm/dd/yyyy)
  -h, --help   Show this help message and exit

Examples:
  $0 -s 05/15/2024 -e 05/22/2024
  $0 -w 7 -x 1
USAGE
    exit 0; 
}

print "DEBUG: -w = ", ($param_dow_plus_7 // 'undef'), ", -x = ", ($param_dow_plus_1 // 'undef'), "\n";
print "DEBUG: -s = ", ($param_start_date // 'undef'), ", -e = ", ($param_end_date // 'undef'), "\n";
print "DEBUG: --nosend = ", ($nosend ? "yes" : "no"), "\n";
# my $DEBUG = $GetOptins{d};

# my $param_dow_plus_7 = defined($GetOptions{w}) ? GetOptions{w} : undef;
# my $param_dow_plus_1 = defined($GetOptions{x}) ? GetOptions{x} : undef;


#- Configuration files -----------------------------------------------
my $g_cfg       = MCCS::Config->new();
my $g_base_d    = "/usr/local/mccs/data/altria";
my $g_txt_d     = "/usr/local/mccs/data/altria/txt";
#TODO remove /tmp from line below.
my $g_remote_path = "/incoming";
my $g_count     = "";
my $g_dt        = DateTime->now;
my $g_ymd       = $g_dt->ymd('');
my $g_log_dir   = "/usr/local/mccs/log/altria";
my $g_verbose   = $DEBUG ? 1 : 0;
mkpath($g_txt_d);
mkpath($g_log_dir);

#- Global variables --------------------------------------------------
# Any variable in all CAPS or have prefix g_ are global variables
# --------------------------------------------------------------------
my $g_progname = basename(__FILE__);
$g_progname =~ s/\.\w+$//x;

Readonly my $g_logfile => '/usr/local/mccs/log/altria/' . $g_progname . '.log';

my $g_long_date = $g_ymd;
chomp($g_long_date);

my @timeFields = split( ' ', localtime() );
$timeFields[1] = uc( $timeFields[1] );

my @months     = qw(JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC);
my @days       = qw(SUN MON TUE WED THU FRI SAT);
my $startMonth = 0;

# translate month string to index position in @months array
for ( my $i = 0 ; $startMonth == 0 && $i < 12 ; $i++ ) {
   if ( $timeFields[1] eq $months[$i] ) {
      $startMonth = $i + 1;
   }
}

my $startYear = $timeFields[4] + 0;
my $startDay  = $timeFields[2] + 0;
#Transalate day of week into a string which is a number

my $g_day_of_week = -1;
$timeFields[0] = uc( $timeFields[0] );
for ( my $i = 0 ; $g_day_of_week == -1 && $i < 7 ; $i++ ) {
   if ( $timeFields[0] eq $days[$i] ) {
      $g_day_of_week = $i;
   }
}
my @endDates = Add_Delta_YMD( $startYear, $startMonth, $startDay, 0, 0, -( $g_day_of_week + 1 ) );

# pad month to min 2 chars with leading 0 if needed

if ( length( $endDates[1] ) < 2 ) {
   $endDates[1] = '0' . $endDates[1];
}

# pad day of month to min 2 chars with leading 0 if needed

if ( length( $endDates[2] ) < 2 ) {
   $endDates[2] = '0' . $endDates[2];
}

my $g_yyyymmdd = join( '', @endDates );

chomp($g_yyyymmdd);  # pad day of month to min 2 chars with leading 0 if needed

my $g_host = hostname;
chomp($g_host);
my $super_dbh = IBIS::DBI->connect( dbname => 'MVMS-Middleware-RdiUser' );

#---------------------------------------------------------------------
# SUBS
#---------------------------------------------------------------------
sub zprint {
   my @arg = @_;
   print("@arg \n");
   return;
}

#---------------------------------------------------------------------
sub fatal_error {
   my $msg = shift;
   croak($msg);
}

#---------------------------------------------------------------------
# Make detail
# use $g_dbh  global database handle for RMS database
# prepare you SQL object here, execute and fetchrow array, hash, or all
# here.
# Do the clean up if needed here
# Return array reference (do not return full blown array)
#---------------------------------------------------------------------
sub make_detail {
   zprint("Make detail records");

   my ($start_date, $end_date);
   my $use_date_range = 0;

if (defined $param_start_date && defined $param_end_date) {
   # Parse mm/dd/yyyy format (instead of dd/mm/yyyy)
   my ($smon, $sday, $syear) = split('/', $param_start_date);
   my ($emon, $eday, $eyear) = split('/', $param_end_date);

   # No formatting or normalization needed if using literal string as 'MM/DD/YYYY'
   $start_date = "$smon/$sday/$syear";
   $end_date   = "$emon/$eday/$eyear";

   $use_date_range = 1;
}
 else {
      # Fall back to original DOW logic if -s/-e are not used
      # These are integers, so safe to interpolate directly
      my $dow_plus_7 = defined($param_dow_plus_7) ? $param_dow_plus_7 : ($g_day_of_week + 7);
      my $dow_plus_1 = defined($param_dow_plus_1) ? $param_dow_plus_1 : ($g_day_of_week + 1);
   }

   my $sql = q{

select              '64549' ||'|'||
                    TO_CHAR(m.week_ending_date,'YYYYMMDD')||'|'||
                    TO_CHAR(SD.SALE_DATE,'YYYYMMDD')||'|'||
                    TO_CHAR(SA.SALE_DATE_TIME,'HH24:MI:SS')||'|'||
                    SA.SITE_ID||SA.SLIP_NO||SA.REGISTER_ID ||'|'||
                    SA.SITE_ID||'|'||
                    A.NAME||'|'||
                    A.ADDRESS_2||'|'||
                    A.CITY||'|'||
                    A.STATE_ID||'|'||
                    A.ZIP_CODE||'|'||
                    V.SUB_CLASS_DESCR||'|'||
                    CV.DESCRIPTION ||'|'||
                    SD.STYLE_ID||'|'||
                    SD.BAR_CODE_ID||'|'||
                    S.DESCRIPTION||'|'||
                    CASE WHEN v.department_id = '0991' and V.CLASS_ID = '1100' AND STYLE_TYPE = 'MULTI' THEN 'CARTON'
                         WHEN v.department_id = '0991' and V.CLASS_ID = '1100' AND STYLE_TYPE = 'SINGLE' THEN 'PACK'
                         WHEN v.department_id = '0991' and V.CLASS_ID = '1200' AND STYLE_TYPE = 'MULTI' THEN 'SLEEVE'
                         WHEN v.department_id = '0991' and V.CLASS_ID = '1200' AND STYLE_TYPE = 'SINGLE' THEN 'CAN'
                         WHEN V.DEPARTMENT_ID = '0992' AND V.CLASS_ID = '2000' AND STYLE_TYPE = 'MULTI' THEN 'CARTON'
                         WHEN V.DEPARTMENT_ID = '0992' AND V.CLASS_ID = '2000' AND STYLE_TYPE =  'SINGLE' THEN 'EA'
                         ELSE 'EA'
                    END ||'|'||
                    SD.QTY||'|'||
                    CASE WHEN STYLE_TYPE = 'MULTI' THEN 
                        (SELECT distinct TO_CHAR(X.MULTI_QTY) FROM STYLES X WHERE BUSINESS_UNIT_ID = 30 AND
                         X.MULTI_STYLE_ID = S.STYLE_ID) else '1' END ||'|'||
                    NULL||'|'||--Multi Unit Indicator
                    NULL||'|'||--Multi Unit Required Quantity
                    NULL||'|'||--Multi Unit Discount Amount
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    SD.EXTENSION_AMOUNT||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    'In Store'||'|'||
                    NULL||'|'||
                    NULL||'|'||
                    NULL

FROM                STYLES S,
                    SALES SA,
                    SALE_DETAILS SD,
                    SITES_ALTRIA A,
                    V_DEPT_CLASS_SUBCLASS V,
                    CHARACTERISTIC_VALUES cv,
                    STYLE_CHARACTERISTICS SC,
                    MERCH_CAL_MIKE_WEEK M

WHERE               S.BUSINESS_UNIT_ID = 30 AND
                    S.BUSINESS_UNIT_ID = SA.BUSINESS_UNIT_ID AND
                    S.BUSINESS_UNIT_ID = SD.BUSINESS_UNIT_ID AND
                    S.BUSINESS_UNIT_ID = A.BUSINESS_UNIT_ID AND
                    S.BUSINESS_UNIT_ID = V.BUSINESS_UNIT_ID AND
                    S.BUSINESS_UNIT_ID = CV.BUSINESS_UNIT_ID AND
                    S.BUSINESS_UNIT_ID = SC.BUSINESS_UNIT_ID AND
                    S.BUSINESS_UNIT_ID = M.BUSINESS_UNIT_ID AND
                    S.STYLE_ID = SD.STYLE_ID AND
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
                    SA.SALE_DATE BETWEEN M.WEEK_STARTING_DATE AND M.WEEK_ENDING_DATE AND
                    (
                      (V.DEPARTMENT_ID = '0991' and v.class_id IN ('1100','1200'))
                      or (v.department_id = '0992' and v.class_id = '2000' and style_type != 'REGULAR')
                    )
                    AND
   };

   # Add safe date filtering:
   if ($use_date_range) {
      $sql .= " sa.sale_date BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY') ";
   } else {
      my $dow_plus_7 = defined($param_dow_plus_7) ? $param_dow_plus_7 : ($g_day_of_week + 7);
      my $dow_plus_1 = defined($param_dow_plus_1) ? $param_dow_plus_1 : ($g_day_of_week + 1);
      $sql .= " sa.sale_date BETWEEN trunc(sysdate - $dow_plus_7) AND trunc(sysdate - $dow_plus_1) ";
   }

   $sql .= " ORDER BY SA.SITE_ID, SA.SLIP_NO, SA.REGISTER_ID ";

   my $sth = $super_dbh->prepare($sql) or fatal_error("Cannot prepare $sql");

   if ($use_date_range) {
      $sth->execute($start_date, $end_date);
   } else {
      $sth->execute();
   }

   my @array = ();
   while (my $row = $sth->fetchrow_array) {
      push @array, $row;
   }

   my $data = \@array;  # return array reference
   return $data;

} ## end sub make_detail

#---------------------------------------------------------------------
# Make Header
# use $g_dbh  global database handle for RMS database
# prepare you SQL object here, execute and fetchrow array, hash, or all
# here.
# Do the clean up if needed here
# Return array reference (do not return full blown array)
#---------------------------------------------------------------------

sub make_header {
   zprint("Make header records");

   my ($start_date, $end_date);
   my $use_date_range = 0;

if (defined $param_start_date && defined $param_end_date) {
   # Parse mm/dd/yyyy format (instead of dd/mm/yyyy)
   my ($smon, $sday, $syear) = split('/', $param_start_date);
   my ($emon, $eday, $eyear) = split('/', $param_end_date);

   # No formatting or normalization needed if using literal string as 'MM/DD/YYYY'
   $start_date = "$smon/$sday/$syear";
   $end_date   = "$emon/$eday/$eyear";

   $use_date_range = 1;
}
 else {
      # Use DOW offsets as fallback - these are integers, safe to interpolate
      my $dow_plus_7 = defined($param_dow_plus_7) ? $param_dow_plus_7 : ($g_day_of_week + 7);
      my $dow_plus_1 = defined($param_dow_plus_1) ? $param_dow_plus_1 : ($g_day_of_week + 1);
   }

   # Base SQL with a placeholder for date filtering condition:
   my $sql = q{
      select
         COUNT(*) ||'|'||
         SUM(SD.QTY)||'|'||
         to_char(SUM(SD.EXTENSION_AMOUNT),'fm9999999.90')||'|'||
         'MARINECORPSPERSONALFAMILYREAD'
      FROM
         STYLES S,
         SALES SA,
         SALE_DETAILS SD,
         SITES_ALTRIA A,
         V_DEPT_CLASS_SUBCLASS V,
         CHARACTERISTIC_VALUES cv,
         STYLE_CHARACTERISTICS SC,
         MERCH_CAL_MIKE_WEEK M
      WHERE
         S.BUSINESS_UNIT_ID = 30
         AND S.BUSINESS_UNIT_ID = SA.BUSINESS_UNIT_ID
         AND S.BUSINESS_UNIT_ID = SD.BUSINESS_UNIT_ID
         AND S.BUSINESS_UNIT_ID = A.BUSINESS_UNIT_ID
         AND S.BUSINESS_UNIT_ID = V.BUSINESS_UNIT_ID
         AND S.BUSINESS_UNIT_ID = CV.BUSINESS_UNIT_ID
         AND S.BUSINESS_UNIT_ID = SC.BUSINESS_UNIT_ID
         AND S.BUSINESS_UNIT_ID = M.BUSINESS_UNIT_ID
         AND S.STYLE_ID = SD.STYLE_ID
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
         AND SA.SALE_DATE BETWEEN M.WEEK_STARTING_DATE AND M.WEEK_ENDING_DATE
         AND (
            (V.DEPARTMENT_ID = '0991' AND v.class_id IN ('1100','1200'))
            OR (v.department_id = '0992' AND v.class_id = '2000' AND style_type != 'REGULAR')
         )
         AND
   };

   # Add date filtering with bind variables or direct interpolation for offsets:
   if ($use_date_range) {
      $sql .= " sa.sale_date BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY') ";
   } else {
      my $dow_plus_7 = defined($param_dow_plus_7) ? $param_dow_plus_7 : ($g_day_of_week + 7);
      my $dow_plus_1 = defined($param_dow_plus_1) ? $param_dow_plus_1 : ($g_day_of_week + 1);
      $sql .= " sa.sale_date BETWEEN trunc(sysdate - $dow_plus_7) AND trunc(sysdate - $dow_plus_1) ";
   }

   my $sth = $super_dbh->prepare($sql) or fatal_error("Cannot prepare $sql");

   # Execute with bind values if using start/end dates:
   if ($use_date_range) {
      $sth->execute($start_date, $end_date);
   } else {
      $sth->execute();
   }

   my @array = ();
   while (my $row = $sth->fetchrow_array) {
      push @array, $row;
   }

   $g_count = substr($array[0], 0, index($array[0], '|'));
   my $data = \@array;

   return $data;
}## end sub make_header
sub create_record {
   my $filename = "TEST_DO_NOT_PROCESS_MARINE_CORPS_PERSONAL_FAMILY_READ_" . $g_yyyymmdd . ".TXT";
   my $fileout  = $g_txt_d . '/' . $filename;
   my $header   = make_header();
   my $detail   = make_detail();
   my $fh;
   open( $fh, '>', $fileout )
     or fatal_error("Could not write $filename due to $!");
   foreach my $h ( @{$header} ) {
      print $fh $h . "\n";
   }
   foreach my $d ( @{$detail} ) {
      print $fh $d . "\n";
   }
   close($fh);
   my $fsize = -s $fileout;
   zprint("$filename created $fsize bytes");
   return $filename;
} ## end sub create_record

#---------------------------------------------------------------------
sub push_to_altria {
   my $file = shift;
   print "DEBUG: Entered push_to_altria with file: $file\n";
   my $basename = basename($file);
   my $outfile = $g_remote_path . "/" . $basename;
   my $name = 'MVMS-Middleware-Altria-SFTP';
   print "DEBUG: Getting secret for '$name'\n";
   my $secret = $util->get_secret($name);
   unless (defined $secret) {
        die ">>> push_to_sas: Secret $name not found\n";
    }
    print ">>> push_to_sas: got secret\n";

    my $sftp_server = $secret->{'host'};
    $util->print_secret($name);
    my $user = $secret->{'username'} // $secret->{'user'};
    my $pw;

    if (defined $secret->{'pw'}) {
        $pw = $secret->{'pw'};
        print "on pw key\n";
    }
    if (defined $secret->{'password'}) {
        $pw = $secret->{'password'};
        print "on password key\n";
    }

    if (defined $pw) {
        $pw =~ s/\\//g;  # Remove backslashes from password
    } else {
        die ">>> push_to_altria: Password not found in secret\n";
    }

    my $port = $secret->{'port'} || '22';
    print("port = $port\n");

   my $local_path = "$g_txt_d/$file";
   print "DEBUG: Connecting to $sftp_server\n";

    my $sftp = Net::SFTP::Foreign->new(
      $sftp_server,
      user     => $secret->{username} || $secret->{user},
      password => $secret->{password} || $secret->{pw},
       port     => $secret->{port}     || '22',
      more     => [qw(-o PreferredAuthentications=password -o PubkeyAuthentication=no)]
    );
    
   $sftp->die_on_error("Unable to establish SFTP connection to $sftp_server\n");
   print "DEBUG: Connected. Starting file upload...\n";
   zprint("put $local_path to $outfile");
   $sftp->put( $local_path, $outfile ) or croak("Could not put: " . $sftp->error);
   zprint("Directory contents:");
   print "DEBUG: Listing remote directory...\n";
   my @array = $sftp->ls($g_remote_path);
   for my $e (@array) {
      for my $i (@$e) {
         zprint($i->{longname});
      }
   }
   
   zprint('SFTP transfer completed');
   $sftp->disconnect;
   return;
} ## end sub push_to_altria

#---------------------------------------------------------------------
# MAIN PROGRAM - Do not modify main below.
# Use the main below aka my_main
#---------------------------------------------------------------------

sub my_main {
    my $filename = create_record();

if ($nosend) {
        print "INFO: --nosend specified, skipping FTP upload.\n";
    } else {
        print "INFO: Sending $filename to Altria via SFTP...\n";
        push_to_altria($filename);
    }

    return;
} ## end sub my_main

#--------------------------------------------------------------------
# MAIN PROGRAM - Do not modify main below.
# Use the main above aka my_main

my_main();
1;

#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------