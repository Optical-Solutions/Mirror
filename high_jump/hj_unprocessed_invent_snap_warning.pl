#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Soumya K Bhowmic
##Date      : 12/13/2023
##
##Brief Desc: This program generates a report for High Jump (HJ) for any 
##            inventory snap short host_group_id that has not been processed 
##            yet in T_AL_HOST_SQL_EXPORT_QUEUE_ARC table. 
##            Need to delete or update the host group id after examing the case.
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
use IBIS::Email;
use Fcntl qw(:DEFAULT :flock);
my ($debug);

## Options

## Only one process running at a time.
open( SELF, "<", $0 ) or die "Cannot open $0 - $!";
flock( SELF, LOCK_EX | LOCK_NB ) or die "Already running.";

## config file, object, db connections
my $config = "/usr/local/mccs/etc/edi_wms/edi_wms.conf";
my $wms = IBIS::EWMS->new( conf_file => $config );

## variables
my $now               = strftime( '%Y-%m-%d %H:%M:%S', localtime );
my $d_suffix          = strftime( '%Y%m%d', localtime );
my $t_suffix          = strftime( '%Y%m%d_%H%M%S', localtime );
my $log_filename      = $wms->{LOG_DIR} . "/" . "hj_unprocessed_invent_snap_warning" 
                        . $t_suffix;
   $debug             = $wms->{DEBUG_WMS_PO};
my $hostname          = `hostname`;
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

my $str_host_date = "Time: ".$t_suffix  . "  Server:" . $hostname; 

my $query = qq(
     select host_group_id
      from T_AL_HOST_SQL_EXPORT_QUEUE_ARC 
      where export_type ='INVENTORY' 
      -- #TODO remove next line
      and ronum < 10
    -- #TODO uncomment next line.
 --       AND STATUS ='P' 
);

my $value_ref;
eval{
    $value_ref = $wms->{'dbh_obj'}->selectall_arrayref($query);   
};
if ($@){	
    my $msg = "DB selection query errored: $@";
    $wms->{'log_obj'}->info($msg);
}

my $subject = 'UNPROCESSED HJ INVENTORY SNAP SHOT!';
my $contents ='';
#TODO unocmment next line and delete line after that
#my $email_address ='rdistaff@usmc-mccs.org';
my $email_address ='kaveh.sari@usmc-mccs.org';
my $from = 'rdistaff@usmc-mccs.org';

for ( my $i = 0 ; $i < @$value_ref ; $i++ ) {
    $contents .= "\n select * from T_AL_HOST_SQL_EXPORT_QUEUE_ARC where host_group_id ="
	."\'"."$value_ref->[$i]->[0]" ."\'";
    $contents .= "\n update T_AL_HOST_SQL_EXPORT_QUEUE_ARC set status ='C' where host_group_id ="
	."\'"."$value_ref->[$i]->[0]" ."\'";
 }
$contents  .= "\n\n";
$contents .= $str_host_date; 

if(@$value_ref > 0){
    $wms->{'log_obj'}->info($contents);
    &sendmail( $email_address, $from, $subject, $contents ); 
}else{
    $wms->{'log_obj'}->info("No unprocessed host group id for today.");
}

## log end time
my $end      = strftime( '%Y-%m-%d %H:%M:%S', localtime );
$wms->{'log_obj'}->info("Ending time: $end\n");
$wms->destructor();

=head
This is a quick report for any hj inventory snap short host_group_id that has not been processed yet in 
 T_AL_HOST_SQL_EXPORT_QUEUE_ARC table. Need to delete or update the host group id after examing the case. 

=cut

##################
