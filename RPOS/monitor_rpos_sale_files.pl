#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Soumya K Bhowmic
##Date      : 12/07/2023
##
##Brief Desc: The program monitors Sites for RPOS sale files and reports Sites
#             for which there are no sales.
##
##            The program must run a rdiusr.
## --------------------------------------------------------------------------  
use strict;
use Data::Dumper;
use IBIS::Email;
use IBIS::MonitorRpos;

my ($config, $debug, $monitor);
$debug = $ARGV[0];
## The Object

$config = '/usr/local/mccs/etc/rpos_monitor/monitor_rpos.conf';
$monitor = IBIS::MonitorRpos->new( conf_file => $config );

if ($debug) {
    $monitor->{'debug'} = 1;
    print Dumper($monitor);
}

my $site_ref = $monitor->get_all_rms_site_open_info();
my $dir_list = $monitor->get_dir_list('rms_dir', 33); 
print Dumper($dir_list) if ($debug);

my $file_path = $monitor->get_attribute('rms_dir');
my $file_ref = $monitor->get_file_site_ref($file_path,$dir_list);
print Dumper($file_ref) if ($debug);


my $site_date_count;
#my $site_date_count = "\n\nSite Date Count\n============================================\n";
##$site_date_count .= $monitor->print_site_date_count($file_ref);


my $date_in_order_ref = $monitor->get_dir_date_in_dsc_order();
my $threshold = $monitor->get_attribute('threshold_day');
my ($ref1, $ref2) = $monitor->exam_list_by_open_info($site_ref, $file_ref, $threshold, $date_in_order_ref);   
my ($missing_ctr, $compare_result) =  $monitor->compare_site_stats($ref2, $ref1);

$compare_result .= $site_date_count; ## if $site_date_count;

my $to         = $monitor->get_attribute('to_email');
my $from       = $monitor->get_attribute('from_email'); 
my $subject    = 'Rpos File Monitor Report';
my @list_addrs = split(/\|/, $to);

foreach my $to_add(@list_addrs){
    sendmail($to_add, $from, $subject, $compare_result);
}
## destructor...
$monitor->destructor();

=head

NAME
      monitor_rpos_sale_files.pl

VERSION
      1.0

USAGE
      perl monitor_rpos_sale_files.pl

REQUIRED ARGUMENTS
       None.

OPTIONS
       <debug> 0 or 1

DESCRIPTION
     The program monitors site rpos rpos sale files and reports sites that with no sales longer than they should.
 
REQUIREMENTS
       None.

DIAGNOSTICS
       None.

CONFIGURATION
       None

DEPENDENCIES
       Data::Dumper;
       IBIS::Email;
       IBIS::MonitorRpos;
SEE ALSO
       None

INCOMPATIBILITIES
       Not known

BUGS AND LIMITATIONS
       No bug has been found yet.

BUSINESS PROCESS OWNER
        Chunhui Yu

AUTHOR
       Chunhui Yu<yuc@usmc-mccs.org│chunhui_at_tigr@yahoo.com>

ACKNOWLEDGEMENTS
       

LICENSE AND COPYRIGHT
       Copyright (c) 2008 MCCS. All rights reserved.
       This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

DISCLAIMER OF WARRANTY
       THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
