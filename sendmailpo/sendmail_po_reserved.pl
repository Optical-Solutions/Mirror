#!/usr/local/mccs/perl/bin/perl -w
#---------------------------------------------------
# Ported by: Hanny Januarius
# Date: Fri Dec  8 11:03:57 EST 2023
# Desc: 
# This program is used to monitor the table edi_850_po_reserved. 
# The table is supposed to be empty after the 850 process is done for the day, 
# usually twice per day. If not empty, you need to check the 850 log, and truncate the table. 
#
#---------------------------------------------------
use strict;
use IBIS::DBI;
use Data::Dumper;
use IBIS::EDI;
use IBIS::E856;
use IBIS::EWMS;
use POSIX qw(strftime WNOHANG);
use IBIS::Log::File;
use IBIS::Email;
my ($debug, @errors);

## config file, object, db connections
my $config = "/usr/local/mccs/etc/edi_wms/edi_wms.conf";
my $wms = IBIS::EWMS->new( conf_file => $config );

## variables
my $now               = strftime( '%Y-%m-%d %H:%M:%S', localtime );
my $d_suffix          = strftime( '%Y%m%d', localtime );
my $t_suffix          = strftime( '%Y%m%d_%H%M%S', localtime );
my $log_filename      = "/usr/local/mccs/log/850/" . "sendmail_po_reserved.log";
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
my $dbh = $wms->sget_dbh_obj();
$wms->{'log_obj'}->info("Start program: $0");
if ($debug) {
    my $obj_buffer = Dumper($wms);
    print $obj_buffer;
    $wms->{'log_obj'}->info("$obj_buffer");
}

## do the count for how much remained in edi_850_po_reserved table
my $str_host_time = "Email from host: ".$hostname .' at time '.$t_suffix.'.';
&check_edi_850_po_reserved();


## log end time
my $end      = strftime( '%Y-%m-%d %H:%M:%S', localtime );
$wms->{'log_obj'}->info("Ending time: $end\n");
$wms->destructor();

## subroutine
sub check_edi_850_po_reserved{
    my $query = "select count(*) from rdiusr.edi_850_po_reserved";
    my $ret_ref = $dbh->selectall_arrayref($query);
    
    if($ret_ref->[0][0] > 0){
	## SEND EMAIL:
	my ($from, $to, $subject, $content);
	$from ='rdistaff@usmc-mccs.org';
	$to    ='yuc@usmc-mccs.org';
	$subject ="Warning: edi_850_po_reserved not empty!";
	$content ="\n\n\nNeed to check on 850 log. \nTotal rows:".$ret_ref->[0][0].
                  "\ndelete from rdiusr.edi_850_po_reserved\n";
	$wms->{'log_obj'}->info($subject . $content);
	sendmail($from,$to,$subject, $content);  
    }

    if($ret_ref->[0][0] > 0){
        ## manage data in edi_850_po_reserved:      
       ## DROP TABLE
	my $drop_table = "drop table temp_edi_850_po_reserved";
	## COPY DATA TO THE NEW TABLE
	my $copy_table = "create table temp_edi_850_po_reserved as (select * from edi_850_po_reserved) ";
	## DELETE THE OLD DATA
	my $truncate = "truncate table edi_850_po_reserved";

	eval{
	    ## DROP TABLE
	    $dbh->do($drop_table);
	};
	if ($@){
	    $wms->{'log_obj'}->info("WARNING: error in running $drop_table !!!".$@ );
	}else{
	    $wms->{'log_obj'}->info("drop temp_edi_po_reserved successfully.");
	}
	
	eval{
	    ## COPY DATA TO THE NEW TABLE
	    $dbh->do($copy_table);
	};
	if ($@){
	    $wms->{'log_obj'}->info("WARNING: error in running: $copy_table !!!".$@ );
	}else{
	    $wms->{'log_obj'}->info("copy data successfully.");
	}
	
	eval{
	    ## DELETE THE OLD DATA
	    $dbh->do($truncate);
	};
	if ($@){
	    $wms->{'log_obj'}->info("WARNING: error in running $truncate ".$@ );
	}else{
	    $wms->{'log_obj'}->info("truncate table successfully.");
	}
    }

}

=head
This program is used to monitor the table edi_850_po_reserved. 
The table is supposed to be empty after the 850 process is done for the day, 
usually twice per day. If not empty, you need to check the 850 log, and truncate the table. 
=cut
####################### 

#######################
