#!/usr/local/bin/perl
#---------------------------------------------------------
# Ported by:
# Date:
# Desc:
#---------------------------------------------------------

use strict;
use warnings;
use Getopt::Long;
use DateTime;
use Data::Dumper;
use Class::Inspector;
use Pod::Usage;
use Net::SFTP::Foreign;
use File::Path   qw(make_path);
use File::Copy   qw(move);
use File::Remove qw(remove);
use File::Spec;
use File::Basename;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use Carp;
use Readonly;
use Fcntl qw(:flock);
use MCCS::Config;
use MCCS::WMS::Sendmail;
use IBIS::Log::File;
use IO::File;

#-----------------------------------------------
# Define Variables
#-----------------------------------------------
my  $util = MCCS::MCE::Util->new();
my $cfg      = MCCS::Config->new();
#my $g_dbname = $cfg->empowerit_v2->{DBNAME};
my $g_dbname = IBIS::DBI->connect( dbname =>'MVMS-Middleware-RdiUser');
my $dt        = DateTime->now();
my $year      = $dt->strftime('%Y');
my $yearMonth = $dt->strftime('%Y%m');
my $date      = $dt->strftime('%Y%m%d');
my $ts        = $dt->strftime('%H%M');
my $timestamp = $dt->strftime('%Y%m%d %H%M%S');

my $sm        = MCCS::WMS::Sendmail->new();
my $error_lvl = 0;

#my $host    = "hqm04ibisvr0010";
my $baseDir = "/usr/local/mccs/data/empower_it/";
my $filename;
my $path_file;
my $log = "/usr/local/mccs/log/empower_it/" . $date . "_empower_sales.log";
my $g_long_date = $timestamp;

my $debug;
my $suppress_send = 0;
my $archive_sw    = 0;
my $force_sw      = 0;
my $merch_sw      = 0;
my $merch_year;
my $merch_week;


#my $token= 'MVMS-Middleware-EmpowerIT-SFTP';
#my $secret=$Util->getsecret($token);
my $options = (
    GetOptions(
        'debug'      => \$debug,
        'log=s'      => \$log,
        'database=s' => \$g_dbname,
        'week=s'     => \$merch_week,
        'year=s'     => \$merch_year,
        'nosend'     => \$suppress_send,
        'archive'    => \$archive_sw,
        'force'      => \$force_sw,
    )
);


unless (-d ($gbasedir . "/archive") ) {          # Verify that we need the sub directory for Empower_it exists after moving to cloud.
    make_path(($gbasedir . "/archive"));
}
unless (-d "/usr/local/mccs/log/empower_it" ) {  # Verify that we need the sub directory for Empower_it exists after moving to cloud.
    make_path("/usr/local/mccs/log/empower_it");
}
unless (-d "/usr/local/mccs/tmp") {              # Verify that we need the sub directory for Empower_it exists after moving to cloud.
    make_path("/usr/local/mccs/tmp");
}
make_path($directory, {
    mode => 0755
});
#- One process at a time ---------------------------------------------
my $lock_file = "/usr/local/mccs/tmp/" . basename($0) . ".lck";
my $SELF = IO::File->new($lock_file, 'w' ) or croak "Could not create lock file $lock_file";
flock $SELF, LOCK_EX | LOCK_NB or croak "Another $0 process already running";
if ( -e $lock_file ) { croak "Another $0 process already running" }


#- Log and DB connection
my $log_obj = IBIS::Log::File->new( { file => $log, append => 1 } );

#----MY IBIS::DBI ---#
use IBIS::EmpowerIT::Query;
my $dataAccess = IBIS::EmpowerIT::Query->new(
    AutoCommit => 1,
    logFileRef => $log_obj,
    dbname     => $g_dbname,
    debug      => $debug
);

#--------------------#
log_debug("Started");
log_debug( "Database: ", Dumper $dataAccess );

sub mainLine {
    log_debug( "Logfile: ", $log );

    if ( $merch_week && $merch_year ) {
        $filename = $merch_year . '-' . sprintf( "%02d", $merch_week ) . '.csv';
        $path_file = $baseDir . $filename;
        $merch_sw  = 1;

    }
    else {
        my $currWeek =
          $dataAccess->get_record( dataSource => 'getCurrMerchWeek', );
        log_debug( "Current Week: ", Dumper $currWeek );

        my $closedWeek =
          $dataAccess->get_record( dataSource => 'getLastClosedWeek' );
        log_debug( "Closed Week: ", Dumper $closedWeek );

        $filename = $year . '-'
          . sprintf( "%02d", $closedWeek->{merchandising_week} ) . '.csv';

        $path_file = $baseDir . $filename;

        if ( incompleteSalesBoolean($closedWeek) ) {
            if ( !$debug ) {    #warn will send email
                log_warn(
qq(Not all sales have processed for $closedWeek->{week_start_date} thru $closedWeek->{week_ending_date} )
                );
            }
            log_debug(
qq(Not all sales have processed for $closedWeek->{week_start_date} thru $closedWeek->{week_ending_date} )
            );

            my $timeout      = 1;                                    #set true
            my $retry        = $cfg->empowerit_v2->{retries};
            my $sleepTimeSec = $cfg->empowerit_v2->{sleepTimeSec};
            $sleepTimeSec = 2300 if ( !$sleepTimeSec );

            while ($retry) {
                my $dt_1        = DateTime->now();
                my $timestamp_1 = $dt_1->strftime('%Y-%m-%d %H:%M:%S');
                log_debug(qq(Retries left $retry \@ $timestamp_1 ));

                if ( !incompleteSalesBoolean($closedWeek) ) { #-all sales are in
                    $timeout = 0;
                    last;
                }

                if ($force_sw) {
                    $timeout = 0;
                    log_debug(
qq(Force switch was set gathering data as is regardless of missing Sales)
                    );
                    last;
                }

                $retry--;
                sleep $sleepTimeSec;
            }

            if ($timeout) {
                fatal_error(
qq(The alotted time has expired for Sales to complete befor running EmpowerIT Sales extract please contact RDI group)
                );
            }
        }

        log_debug(
qq( All Sales have been processed for $closedWeek->{week_start_date} thru $closedWeek->{week_ending_date}  )
        );
    }
    createFile();

    #     my @zipFileNames;
    #     foreach my $file (@fileNames) {
    #       push(@zipFileNames, zip_files($file) );
    #     }
    #     log_debug( "Zipped file: ",     @zipFileNames );
sftpFiles
    sftpFiles($filename);
    archiveFiles($filename);
    return;
}

sub incompleteSalesBoolean {
    my $closedWeek      = shift;
    my $incompleteSales = $dataAccess->get_record(
        dataSource => 'boolIncompleteSale',
        week_start_date  => $closedWeek->{week_ending_date},
        week_ending_date => $closedWeek->{week_ending_date},
    );

    #--returns true or greater than zero if incomplete sales are found for closed week--#
    return $incompleteSales->{incomplete_sales};
}

sub createFile {
    my $salesData;
    if ($merch_sw) {
        $salesData = $dataAccess->get_record(
            dataSource => 'getSalesDataMerch',
            merch_week => $merch_week,
            merch_year => $merch_year,
        );
    }
    else {
        $salesData = $dataAccess->get_record( dataSource => 'getSalesData', );
        ### ^^NEVER DEBUG SO MUCH DATA print Dumper $salesData if ($debug)^^;
    }

    #Want the same order everytime.. Never trust a hash and its ordering
    my @colOrder =
      qw(merchandising_year merchandising_period merchandising_week week_ending_date site_id
      name bar_code_id description department_id dept_name class_id class_descr sub_class_id
      sub_class_descr qty extension_amount);
    my $fh;
    $fh = IO::File->new( $path_file, 'w' )
      or croak "Could not write $path_file because $!";

    foreach my $rec_id ( keys %{$salesData} ) {
        my @data;
        for my $colname (@colOrder) {
            my $value = $salesData->{$rec_id}->{$colname};
            $value = qq("$value")
              if ( ( $salesData->{$rec_id}->{punct_flg} )
                && ( lc($colname) eq 'description' ) );
            push @data, qq($value);
        }
        my $line = join( '|', @data );
        print $fh $line;
        print $fh qq(\n);
    }

    log_debug(qq(Done writing $path_file ));
    return;
}

sub sftpFiles {
    my (@zFiles);
    push(@zFiles, shift);
    my $sftpInput_Hash = $cfg->empowerit_v2->{sftp};
    #my $sftpHash=$Util->getsecret('sftp');
    my $token= 'MVMS-Middleware-EmpowerIT-SFTP';
    my $sftpHash=$Util->getsecret($token);
    if ( !$suppress_send ) {
        my %arglist;
        my $dest     = $sftpHash->{host};
        my $inputDir = $sftpInput_Hash->{input_dir};

        # Retrieve MCL user name and password
        $arglist{user}     = $sftpHash->{user};
        $arglist{password} = $sftpHash->{password};

        #$arglist{port}     = $sftpHash->{port};

        # Log server name and directory
        log_debug('SFTP transfer started');
        log_debug("FTP_SERVER: $dest");

        # Establish SFTP connection to server
        my $sftp;
        my $num_retry      = 10;
        my $successful_ftp = 'N';
        $sftp = Net::SFTP::Foreign->new( $dest, %arglist ) or croak "SFTP failed $!";

        if ( $successful_ftp eq 'N' ) {
            fatal_error("SFTP connection to NFI server ($dest) failed!");
        }

        foreach my $zFile (@zFiles) {
            $sftp->put(
                $baseDir . $zFile, $inputDir . '/' . $zFile,
                copy_perms => 0,
                copy_time  => 0,
                atomic     => 1
            );
            if ( $sftp->error ) {
                log_warn("$zFile could not be sent");
            }
        }
        log_debug('SFTP transfer completed');
    }
    else {
        log_debug(
"Skipping file send per command line switch --nosend\nFiles to be sent @zFiles"
        );
    }
    return;
}

sub archiveFiles {
    my @archiveFiles = @_;
    if ($archive_sw) {

        my $loc_baseDir = $baseDir;
        my $archive_path =
          "/usr/local/mccs/data/empower_it/archive/" . $yearMonth . "/" . $date;

        log_debug("Archiving Files to $archive_path => @archiveFiles");

        make_path($archive_path);

        foreach my $file (@archiveFiles) {
            move( $loc_baseDir . $file, $archive_path . '/' . $file );
        }

        #--extra clean up from zipping of files
        #      foreach my $file (@fileNames){
        #        remove($baseDir.$file)
        #      }
    }
    return;
}

sub zip_files {
    chdir($baseDir);

    my $unzippedFile = shift;

    my $zip = Archive::Zip->new();

    $zip->addFile($unzippedFile);
    ( my $zipFile = $unzippedFile ) =~ s/\.csv/\.zip/x;

    unless ( $zip->writeToFileNamed($zipFile) == AZ_OK ) {
        fatal_error('write error trying to zip file');
    }

    return $zipFile;
}

# sub send_mail {
#     my @body = @_;    #Just going to put every thing in the body good or bad

#     my $emails;

#     #$emails->{larry} ='larry.d.lewis@usmc-mccs.org';
#     $emails = $cfg->empowerit_v2->{emails};

#     $sm->subject( 'EmpowerIT - RMS Sales Data ' . $g_long_date );
#     $sm->sendTo($emails);
#     $sm->msg(@body);
#     $sm->logObj($log_obj);
#     $sm->verboseLevel($debug);
#     $sm->hostName($host);
#     $sm->attachments($log);

#     $sm->send_mail_attachment();
#     return;
# }

sub log_debug {
    my $str       = shift;
    my $log_entry = join( '', "(PID $$) ", $str );
    if ($log_obj) { $log_obj->info($log_entry); }
    debug($log_entry);
    return;
}

sub log_warn {
    my $str       = shift;
    my $log_entry = join( '', "(PID $$) ", $str );
    $sm->errorLevel('warning');
    #send_mail( "WARNING on " . __FILE__ . ' ' . $g_long_date, $log_entry );
    if ($log_obj) { $log_obj->warn($log_entry); }
    debug($log_entry);
    return;
}

sub fatal_error {
    my $str = shift;
    $sm->errorLevel('error');
    my $log_entry = join( '', "(PID $$) ", $str );
    #send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $log_entry );
    if ($log_obj) { $log_obj->error($log_entry); }
    croak $log_entry;
}

sub debug {
    my $str = shift;
    if ($debug) {
        print "DEBUG: ", $str, "\n";
    }
    return;
}

#---------------------------------------------------------------------
# MAIN PROGRAM - Do not modify main below.
# Use the main above aka my_main
#---------------------------------------------------------------------

# Want to catch warning
#$SIG{__WARN__} = sub { log_warn("@_") };

# Execute the main
mainLine();
unlink($lock_file);

#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------
