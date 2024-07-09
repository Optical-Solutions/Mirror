#!/usr/local/mccs/perl/bin/perl

use strict;
use warnings;
use IBIS::EDI;
use IBIS::TMS;
use Data::Dumper;
use Getopt::Std;
use POSIX qw(strftime WNOHANG);
use IBIS::Log::File;
use IBIS::SFTP;
use Sys::Hostname;
use Fcntl qw(:flock);
use Getopt::Long;
use DateTime;
use DateTime::Format::Strptime;
use File::Basename;
##use IBIS::DBI;

## Ensure only a single process is running before anything
open( SELF, "<", $0 ) or die "Cannot open $0 - $!";
flock( SELF, LOCK_EX | LOCK_NB ) or die "Already running.";

## ---Preparations----
## Globals
my (
    $tms,            
    $help,  
    $config, 
    $debug,  
    $sftp_success,
    $sftp_result, 
    $hostname
    );
my @ftp_result;
$hostname = hostname;

## Options
my $opt_result = GetOptions ( 
    "debug"    => \$debug,      # flag
    "help"     => \$help);  # flag

if ($help) {
    my $use_msg = qq(
     "\n General Usage:\n\t  perldoc $0 for more information.\n\n");
    print $use_msg;
    exit();
}


## The Object
$config = '/usr/local/mccs/etc/edi_tms/poshipment_to_landair.config';
$tms = IBIS::TMS->new( conf_file => $config );

if ($debug) {
    $tms->{'debug'} = 1;
    print Dumper($tms);
}

##  Log start time
## A single log file
my $log_filename   = $tms->{LOG_DIR} . "/" . "fetch_mercurygate_track_files.log";
$tms->{'log_obj'} = IBIS::Log::File->new(
    {
        file   => $log_filename,
        append => 1, 
    });

my $start_time = strftime( '%Y_%m_%d_%H_%M',    localtime );
$tms->{'log_obj'}->info($start_time);

## a: file name
my ($ymd_str, $hms_str) = &get_yesterday_time_string();

my $track_file_end   = $tms->{TRACK_FILE_END};
my $fn_sufx          = $ymd_str.$track_file_end; ## strange file name with a space in the front!!!
my $remote_ftp_file  = $tms->{REMOTE_OUTBOUND} . $fn_sufx;
my $local_ftp_file   = $tms->{TEMP_TRACK_FILE_DIR} . $fn_sufx;
my $scp_user_host    = $tms->{COGNOS_SERVER_STR};
my $remote_dir2_file =  $tms->{COGNOS_INBOUND_DIR};


## b: fetch
eval{    
    ## space in file names are stripped off in sftp process:
    $sftp_result = $tms->sftp_list_and_get($remote_ftp_file,$local_ftp_file, $tms->{REMOTE_OUTBOUND} );
};


if ($@) {
    sleep(100);
    eval{    
	## space in file names are stripped off in sftp process:
	$sftp_result = $tms->sftp_list_and_get($remote_ftp_file,$local_ftp_file, $tms->{REMOTE_OUTBOUND} );
    };
    if($@){
	$sftp_success = 0;
	my $msg =
	    "FTP failed in the second attempt.";
	$tms->{'log_obj'}->log_info($msg);
    }else{
	$sftp_success = 1;
	my $msg_end =
	    "FTP sucess in second try! local: $local_ftp_file remote: $remote_ftp_file";
	$tms->{'log_obj'}->log_info($msg_end);
    }
}else {
    $sftp_success = 1;
    my $msg_end =
	"FTP sucessful! local: $local_ftp_file remote: $remote_ftp_file";
    $tms->{'log_obj'}->log_info($msg_end);
}


##c: sent the fetched file to hqlin79, or drlin79

if($sftp_success){
    $local_ftp_file =~ s/\s+//g; ## strip possible space from file names 
    my $c_cmd = "chmod 775 $local_ftp_file";
    my $t_cmd = "touch $local_ftp_file"; ## update time stamp to the process time
    my $c_ret = system($c_cmd);
    my $t_ret = system($t_cmd);
    if(($c_ret > 0)|| ($t_ret > 0)){
	my $msg =
	    "permission setting, or touching error for $local_ftp_file";
	$tms->{'log_obj'}->log_info($msg);	
    }
    my $scp_cmd = "scp -p \'$local_ftp_file\'   "."\'$scp_user_host\'".":$tms->{COGNOS_INBOUND_DIR}";
    print "scp command: $scp_cmd\n" if ($debug);
    my $ret = system($scp_cmd);
    
    my $reslt_msg  = '';
    if($ret){
	$reslt_msg = "some trouble to scp file to $scp_user_host";
    }else{
	$reslt_msg = "SCP success for  $scp_user_host.";
	
    }
    $tms->{'log_obj'}->log_info($reslt_msg);

## -----------------quick fix on the problem of ftp 
    my $scp_user_host2 = 'rdiusr@drlin079.usmc-mccs.org';
    my $scp_cmd2 = "scp -p \'$local_ftp_file\'   "."\'$scp_user_host2\'".":$tms->{COGNOS_INBOUND_DIR}";
    print "scp command: $scp_cmd2\n" if ($debug);
    my $ret2 = system($scp_cmd2);
    
    my $reslt_msg2  = '';
    if($ret2){
	$reslt_msg = "Some trouble to scp file to $scp_user_host2";
    }else{
	$reslt_msg = "SCP success for  $scp_user_host2.";	
    }
    $tms->{'log_obj'}->log_info($reslt_msg2);
}else{ 
    my $msg;
    $msg .=
	"Failed in fetching Mercurygate track file: $remote_ftp_file\n";
    $tms->{'log_obj'}->log_info("$msg");
    $tms->sendnotice( $tms->{POC}, $tms->{MAIL_FROM},
		      "$hostname: Failed to fetch track file from $tms->{REMOTE_SERVER}", $msg );
}

## Log finish time
my $end_time = strftime( '%Y-%m-%d %H:%M:%S', localtime );
$tms->{'log_obj'}->log_info("\nEnded at: $end_time\n*--------------*\n\n");
exit();

## ---End of Main----
################## subroutines ############################

## create ftp filenames in both local and remote directories:
sub get_yesterday_time_string{
    my $wrkday = DateTime->now(
	time_zone => 'local',
	formatter => DateTime::Format::Strptime->new( pattern => '%y%m%d' ),
##	)->subtract( days => 5 );
	)->subtract( days => 1 );
    my $ymd    = $wrkday->ymd;
    $ymd =~ s/\-//g;
    my $hms = $wrkday->hms;
    return ($ymd, $hms);
}

__END__



=pod

=head1 NAME

fetch_mercurygate_track_files.pl

=head1 VERSION

This documentation refers version 1.


=head1 REQUIRED ARGUMENTS

None.

=head1 OPTIONS

=over 6

=item -d or --debug

Debug mode to print out more dumped data for details at some major steps in the code

=item -h or --help

Displays a brief help message and exit.

=item -n or --noftp

If you do not want the file sent to Landair, use this option.
This option will make the program do NOT do ftp, just created files in data dir. 

=back

=head1 USAGE

Examples:

  ./fetch_mercurygate_track_files.pl --debug --noftp 


  ./fetch_mercurygate_track_files.pl 


=head1 DESCRIPTION


=head1 REQUIREMENTS

None.

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION

/usr/local/mccs/etc/edi_tms/poshipment_to_landair.config
                                                                                        
=head1 DEPENDENCIES

=over 4

=item * IBIS::EDI 

=item * IBIS::TMS

=item * Getopt::Std for options

=item * POSIX qw(strftime WNOHANG) for getting time string

=item * IBIS::Log::File for using log
 
=item * MCCS::DBI  for connecting to database server

=back

=head1 SEE ALSO 

=head1 INCOMPATIBILITIES

Unknown.

=head1 BUGS AND LIMITATIONS

No known bugs found yet

=head1 BUSINESS PROCESS OWNERs

=head1 AUTHOR

Chunhui Yu<yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2013. All rights reserved.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITE
D TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SH
ALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTI
ON OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTH
ER DEALINGS IN THE SOFTWARE.

=cut


