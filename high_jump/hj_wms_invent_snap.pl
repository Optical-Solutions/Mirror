#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Soumya K Bhowmic
##Date      : 12/08/2023
##
##Brief Desc: This program extracts the UPC codes and the associated
##            description for FraudWatch (TE-Triversity)
##
##            The program must run a rdiusr.
## --------------------------------------------------------------------------  
use strict;
use IBIS::DBI;
use Data::Dumper;
use IBIS::EDI;
use IBIS::E856;
use IBIS::EWMS;
use Net::SFTP::Foreign;
use POSIX qw(strftime WNOHANG);
use IBIS::Log::File;
use IBIS::DBI;

use Fcntl qw(:DEFAULT :flock);
my ($debug, $ftp_result, @errors);
## one process at a time.
open( SELF, "<", $0 ) or die "Cannot open $0 - $!";
flock( SELF, LOCK_EX | LOCK_NB ) or die "Already running.";

my $disable_dir = '/usr/local/mccs/data/wms/data/disable';
my $disable_f = $disable_dir . '/' . $0;
if (-e $disable_f) { exit 0; }

## Options
my  $config = "/usr/local/mccs/etc/edi_wms/edi_wms.conf";
## Object
my $wms = IBIS::EWMS->new( conf_file => $config );

## config file, object, db connections
##  Log start time
my $now               = strftime( '%Y-%m-%d %H:%M:%S', localtime );
my $d_suffix          = strftime( '%Y%m%d', localtime );
my $t_suffix          = strftime( '%Y%m%d_%H%M%S', localtime );
my $log_filename      = $wms->{LOG_DIR} . "/" . "hj_invent_log_" . $t_suffix;

   $debug             = $wms->{DEBUG_WMS_INV};
my $site_list;

## log obj
$wms->{'log_obj'}     = IBIS::Log::File->new(
    {
        file   => $log_filename,
        append => 1
    }
);
$wms->{'log_obj'}->log_info("\n\n*************\n\tStarted at:$now\n");
$wms->sget_dbh_obj();
$wms->{'log_obj'}->info("Start program: $0");
if ($debug) {
    $wms->{'debug'} = 1;
    my $obj_buffer = Dumper($wms);
    print $obj_buffer;
    $wms->{'log_obj'}->info("$obj_buffer");
}


&run_invent_stream;
sleep(2);
&run_report_on_variance_tab;

sleep(30);

&run_invent_stream;
sleep(2);
&run_report_on_variance_tab;

## all the log info, and error messages email out...
my $end      = strftime( '%Y-%m-%d %H:%M:%S', localtime );
$wms->{'log_obj'}->info("Ending time: $end\n");
$wms->destructor();


## exec rdiusr.MRI_HJ_IMPORT_INVENT_SNP.STREAM_LINE_INVENTORY_SNAPSHOT;
sub run_invent_stream {
    eval{
	my $sth1 = $wms->{'dbh_obj'}->prepare('BEGIN rdiusr.MRI_HJ_IMPORT_INVENT_SNP.STREAM_LINE_INVENTORY_SNAPSHOT; END;');
	$sth1->execute || die $wms->{'log_obj'}->info($wms->{'dbh_obj'}->errstr);
    };
    if($@){
	my $msg = "Database errors when exec claim db procedures:.  rdiusr.MRI_HJ_IMPORT_INVENT_SNP.STREAM_LINE_INVENTORY_SNAPSHOT... aborted, aborted...";
	$wms->{'log_obj'}->info($msg);
	die;
    }
}


sub run_report_on_variance_tab {

    my $query = "select (sysdate - (select max (date_processed) from  HJ_WMS_TOT_HJI_RMS_VARIANCE) ) as date_diff from dual";

    my $ret_val = $wms->{'dbh_obj'}->selectall_arrayref($query);

    if ( $ret_val->[0][0] < 0.15 ) { ## if the last job is less than 15 mins old, do report.
	
        my $cmd = '/usr/local/mccs/perl/bin/perl /usr/local/mccs/bin/high_jump/hj_rms_daily_inv_var_rep';
        
        eval{
            system($cmd);
        };

        if($@){
            my $msg = "Error happened in running: /usr/local/mccs/perl/bin/perl /usr/local/mccs/bin/high_jump/hj_rms_daily_inv_var_rep";
            $wms->{'log_obj'}->info($msg);
            die;
        }
    }
}


=head
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
=cut
