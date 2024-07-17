#!/usr/local/mccs/perl/bin/perl

use strict;
use warnings;
use IBIS::EDI;
use IBIS::E856;
use Data::Dumper;
use POSIX qw(strftime);
use IBIS::Log::File;
use Sys::Hostname;
use Getopt::Long;
##require '/usr/local/mccs/lib/perl5/IBIS/EmailHtml.pm';

my ($config, $debug, $e856, $help, $hostname);

## preparations:
## options
my $opt_result = GetOptions ( 
    "debug|d"    => \$debug,      # flag
    "help|h"     => \$help,
    );  

## config file, object
$hostname = hostname;
$config =  '/usr/local/mccs/etc/edi_856/edi_856_error_report.config';

if ($help) {
    my $use_msg = qq(
     "\n General Usage:\n\t  perldoc $0 for more information.\n\n");
    print $use_msg;
    exit();
}

$e856 = IBIS::E856->new( conf_file => $config );

## set debug mode
$e856->set_debug($debug);

## log file
my $now      = strftime( '%Y_%m_%d_%H_%M', localtime );
my $log_filename   = $e856->{LOG_DIR} . 
    "/" . "edi_856_error_reporting_log_" . $now;
$e856->{'log_obj'} = IBIS::Log::File->new(
    {
        file   => $log_filename,
        append => 1
    }
    );

$e856->{'log_obj'}->log_info( 
    "\n\n*************\n\tStarted at:$now\n" );
if($e856->is_debug()){
    print "Object Info:\n";
    print Dumper($e856);
    $e856->{'log_obj'}->info(
	"Running in debug mode for more infor in log.");
}

## start real works

## Fetching loading logs:
my $ret = $e856->scp_856_loading_logs();

## If fetching log success:
unless($ret){
    ## get ASN file name list from the log
    my $asn_list = $e856->get_filenames_from_logs();
    if($asn_list){
	
	## if ANS list, then process each 
	$e856->parse_asns();
	print Dumper($e856) if($e856->is_debug);
	
	## If report info extracted from parsed asns, sending report
	my $ary_ref = $e856->collect_reporting_items();

	if($ary_ref){
	    my $th_str ='Asn_name|Vendor_id|PO|Error_msg|Log_name|';
	    my $subject =  '856 Invalid Vendors (part1)';
	    my $content = $e856->html_table_via_sth_or_aryref($ary_ref,$th_str,$subject,35);
	    $e856->send_html_by_email($subject, $e856->{MAIL_CC}, $content);
	    ##$e856->send_report();
	}
	print Dumper($e856) if($e856->is_debug);
	
    }else{
	my $msg ="Uploading Log do not record any ASN loading Errors";
	$e856->{'log_obj'}->info($msg);
    }
    
    ## Files fetched, and may be processed, here we need to clean up them
    my $c_ret = $e856->clean_up();	
    if($c_ret){
	$e856->{'log_obj'}->info("Data clean up error, permission etc...");
	$e856->{'log_obj'}->info(Dumper $c_ret);
    }else{
	my $msg = "No errors in cleaning up files";
	$e856->{'log_obj'}->info($msg);
    }

}else{
    ## No new files, or fetching file failure
    my $msg = "File fetching failure or no new files at remote machine";
    $e856->{'log_obj'}->info($msg);
}    

my $end_time     = strftime( '%Y_%m_%d_%H_%M', localtime );
$e856->{'log_obj'}->info(
	"Program finished at: $end_time");


=pod

=head1 NAME

edi_856_error_reporting.pl

=head1 VERSION

This documentation refers to the initial version, version 1.

=head1 REQUIRED ARGUMENTS

None.

=head1 OPTIONS

=over 6

=item -d

Debug mode to print out more dumped data for details at some major steps in the code

=item -h

Displays a brief help message and exit.

=back

=head1 USAGE

 only run on drlin057 as 'rdiusr' for production with mc2p

 only run on hqlin056 as 'rdiusr' for testing with mc2q

Example:

 ./edi_856_error_report.pl -d (for debug)

=head1 DESCRIPTION
 
This program parses asn_uploading log 

generated during EDI856 file loading process 

in RMS database server,

then it extracts ASN file names with loading errors, 

in turn, extracts key information from ASN files, 

and writing report from the extracted information.


=head1 REQUIREMENTS

None.

=head1 CONFIGURATION

=over 4 

/usr/local/mccs/etc/edi_856/edi_856_error_report.config

Here is the list of configuration values:

MAIL_CC=...

MAIL_FROM =rdiusr@usmc-mccs.org

LOG_SERVER=draix03.usmc-mccs.org

SFTP_USER=rdiusr

PASSWORD=...

RMT_LOG_DIR=/rmdata/edi_856_backup2/

RMT_LOG_DIR_BKUP=/rmdata/edi_856_backup2/past_asn_upload_log/

LOG_FILE_PREFIX =asn_upload_

LOCAL_LOG_STG_DIR=/usr/local/mccs/data/edi/ftp/856_error_log/

LOCAL_LOG_ARC_DIR=/usr/local/mccs/data/edi/ftp/856_error_log_bkup/

LOG_DIR=/usr/local/mccs/log/edi/856_error_report/

ASN_BKUP_DIR=/usr/local/mccs/data/edi/ftp/856_inbound_backup/

 
=back
                                                                                        
=head1 DEPENDENCIES

=over 4

IBIS::EDI;
IBIS::E856;

=back

=head1 SEE ALSO 

RMS 856 Interface Requirements, by Mike Gonzalaz

=head1 INCOMPATIBILITIES

Unknown.

=head1 BUGS AND LIMITATIONS

Limitations
Unknown


=head1 BUSINESS PROCESS OWNERs

Mike Ganzalez ganzalezm<@usmc-mccs.org>

=head1 AUTHOR

Chunhui Yu<yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>


=head1 LICENSE AND COPYRIGHT

Copyright (c) 2008 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITE
D TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SH
ALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTI
ON OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTH
ER DEALINGS IN THE SOFTWARE.

=cut

