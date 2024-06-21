#!/usr/local/mccs/perl/bin/perl --
#---------------------------------------------------------------------
# Program:  rima_session_list.pl
# Author:   Armando Someillan
# Created:  Mon June 26th, 2017
# Description: GeneratE AN eXCel file for a completed RIMA session id
#
# Ported by: Hanny Januarius
# Date:  Fri Dec  8 11:12:33 EST 2023
#TODO Add Comments Kaveh Sari
#---------------------------------------------------------------------
use strict;
use IBIS::DBI;
use IBIS::Log::File;
use File::Basename;
use File::Path;
use Readonly;
use MCCS::Config;
use Data::Dumper;
use MCCS::WMS::Sendmail;
use Getopt::Long;
use IBIS::ExcelWriter::File;

# Flush output
$| = 1;


#handle command line arguments
my $session_id;
my $rf_user;
my $session_time_range;
my $sites;

#- Configuration files -----------------------------------------------
my $g_cfg     = new MCCS::Config;
my $g_dbname = $g_cfg->mri_rima_rf->{dbname};
my $g_emails = $g_cfg->mri_rima_rf->{email};
my $g_data_dir = $g_cfg->mri_rima_rf->{data_dir};

#print Dumper $g_emails;

my $g_db_rms = IBIS::DBI->connect(dbname=> $g_dbname);
# Prepare sql to determine if current sesssion contains multiple sites
my $g_usr_bcs_sth = $g_db_rms->prepare("select MRI_RIMA_RF.get_curr_session_site_val(?) site_val from dual")
or fatal_error("Failed to prepare query to determine if current sesssion contains multiple sites !"); 

# Prepare sql to current session time range to pass to script that produces session activity Excel file
my $g_curr_session_time_range_sth = $g_db_rms->prepare("select MRI_RIMA_RF.get_curr_session_time_range(?) session_time_range from dual")
or fatal_error("Failed to prepare session time range query !"); 

#- Global variables --------------------------------------------------
my $g_verbose = 1; #TODO set back to 0
my $g_logfile = '/usr/local/mccs/log/' . basename(__FILE__) .  '.log';
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
    my @body     = ( $msg_bod1, $msg_bod2 );

    return if $g_verbose;    # Dont want to send email if on verbose mode

    $go_mail->logObj($g_log);
    $go_mail->subject($msg_sub);
    $go_mail->sendTo($g_emails);
    $go_mail->msg(@body);
    $go_mail->hostName($g_host);
    $go_mail->send_mail();
}



sub get_error_msg {
    my $sth   = shift;
    my $rejected_id = shift;
    my $msg     = '';
    

    $sth->execute($rejected_id);

    while ( my $a = $sth->fetchrow ) {
        $msg .= $a . "<br>";
    }

    return $msg;

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
    $g_log->info("Database = $g_dbname");
    
    
    my $sql = "select session_id,rf_user from MRI_RF_RIMA_LOG 
    WHERE rpt_printed_date is null and rf_oper = 'XLSCRT'";
    
    my $sth = $g_db_rms->prepare($sql);
    
    $sth->execute();
    my @email_body = ();

    my $from = 'rdistaff@usmc-mccs.org';
    
    while ( my $row = $sth->fetchrow_hashref ) {
        $g_log->info($row->{session_id});
        create_excel($row->{session_id},$row->{rf_user});
    
    };

   
    my $timestamp = `date`;
   
    $g_db_rms->disconnect();
       
    $g_log->info("-- End ------------------------------------------");

}

sub create_excel {
	
	my $session_id = shift;
	my $rf_user = shift;
	my @csv   = ();
	my $rima_cmd = "BEGIN mri_rima_rf.rima_session_printed($session_id); END;"; 
	my $session_time_range;
	my $sites;
	
	
    my $sql = "select POG, upc,description,on_hand,in_transit,in_picking,on_order,last_receipt_date,replen_flag,
    action, site_id, receipt_type, qty_to_stock from (
select mri_rima_rf.get_pog_desc(BAR_CODE_ID,site_id) POG, BAR_CODE_ID upc,STYLE_DESC description,on_hand,in_transit,in_picking,on_order,last_receipt_date,replen_flag,
    ACTION_MESSAGE action, site_id,action_rpt_sort, receipt_type, qty_to_stock from MRI_RF_RIMA_LOG 
    WHERE SESSION_ID = ? and (action_taken <> 'i' or action_taken is null) and rf_oper = 'BCSC'
    order by action_rpt_sort)
    order by POG,action_rpt_sort";
    
    my $sth = $g_db_rms->prepare($sql);
    
    $sth->execute($session_id);
	
	# Set up constants for Excel attributes		
	my $boldnoborder     = 'string,bold,align:center';
    my $boldcenter       = 'string,bold,align:center,border';
    my $boldcenterblue   = 'string,bold,align:center,bgcolor:blue,color:white,border';
    my $boldcentergreen  = 'string,bold,align:center,bgcolor:green,color:white,border';
    my $boldcenteryellow = 'string,bold,align:center,bgcolor:yellow,color:black,border';
    my $strbluebg        = 'string,align:right,bgcolor:blue,color:white';
    my $strgreenbg       = 'string,align:right,bgcolor:green,color:white';
    my $stryellowbg      = 'string,align:right,bgcolor:yellow,color:black';
    my $strredbg         = 'string,align:center,italic,color:red';
    my $strcenter        = 'string,align:center';
    my $numcenter        = 'number,align:center';
    my $str              = 'string,align:left';
    my $money            = 'number,accuracy:2,align:right';
	
	# Get time range for current session so it can be included in the Excel file
		  $g_curr_session_time_range_sth->execute($session_id);
		  ($session_time_range) =  $g_curr_session_time_range_sth->fetchrow_array();
		  
		  # Determine if session contained multiple sites
		  $g_usr_bcs_sth->execute($session_id);
		  ($sites) =  $g_usr_bcs_sth->fetchrow_array();
	
	# Set up title for Excel file
	push(@csv,
	      [ { "RF_USER" => $boldcenter }, { "TIME_RANGE" => $boldcenter }, { "SITE:" => $boldcenter} ]);
	     
	push(@csv,
	      [ { "$rf_user" => $strcenter }, { "$session_time_range" => $strcenter } , {"$sites" => $strcenter}]);
	      
	# Insert blank row      
	push(@csv,
	      [ { "" => $strcenter } ]); 
	      
	            
	# Set up headers for Excel file
	if ( $sites ne 'MULTI' ) { 
	push(@csv,
            [
                { 'POG'            => $boldcenter },
                { 'UPC'            => $boldcenter },
                { 'DESCRIPTION'    => $boldcenter },
                { 'ACTION'         => $boldcenter },
                { 'QTY_TO_PULL'    => $boldcenter },
                { 'ON_HAND'         => $boldcenter },
                { 'IN_TRANSIT'         => $boldcenter },
                { 'IN_PICKING'         => $boldcenter },
                { 'ON_ORDER'         => $boldcenter },
                { 'LAST_RECEIPT_DATE'         => $boldcenter },
                { 'AR'         => $boldcenter },
                { 'REC_TYPE'         => $boldcenter },
                { 'NOTES'          => $boldcenter }
            ]
        );
     }   
     else
     {
     push(@csv,
            [
                { 'POG'            => $boldcenter },
                { 'SITE_ID'        => $boldcenter },
                { 'UPC'            => $boldcenter },
                { 'DESCRIPTION'    => $boldcenter },
                { 'ACTION'         => $boldcenter },
                { 'QTY_TO_PULL'    => $boldcenter },
                { 'ON_HAND'         => $boldcenter },
                { 'IN_TRANSIT'         => $boldcenter },
                { 'IN_PICKING'         => $boldcenter },
                { 'ON_ORDER'         => $boldcenter },
                { 'LAST_RECEIPT_DATE'         => $boldcenter },
                { 'AR'         => $boldcenter },
                { 'REC_TYPE'         => $boldcenter },
                { 'NOTES'          => $boldcenter }
            ]
        ); 	
     } 	
       
		
		   # Populate detail data into array 
			while ( my $row = $sth->fetchrow_hashref ) {
			
			
			  if ( $sites ne 'MULTI' ) {
				push(@csv, 
				  [ { $row->{pog}         => $strcenter },
                    { $row->{upc}         => $strcenter },
				    { $row->{description} => $strcenter },
				    { $row->{action} => $strcenter },
				    { $row->{qty_to_stock} => $strcenter },
				    { $row->{on_hand} => $strcenter },
				    { $row->{in_transit} => $strcenter },
				    { $row->{in_picking} => $strcenter },
				    { $row->{on_order}      => $strcenter },
				    { $row->{last_receipt_date} => $strcenter },
				    { $row->{replen_flag} => $strcenter },
				    { $row->{receipt_type} => $strcenter },
				    { '            '       => $strcenter } ]);
			  }	    
		      else
		      {
		      	push(@csv, 
				  [ { $row->{pog}         => $strcenter },
				    { $row->{site_id}     => $strcenter },
				    { $row->{upc}         => $strcenter },
				    { $row->{description} => $strcenter },
				    { $row->{action} => $strcenter },
				    { $row->{qty_to_stock} => $strcenter },
				    { $row->{on_hand} => $strcenter },
				    { $row->{in_transit} => $strcenter },
				    { $row->{in_picking} => $strcenter },
				    { $row->{on_order}      => $strcenter },
				    { $row->{last_receipt_date} => $strcenter },
				    { $row->{replen_flag} => $strcenter },
				    { $row->{receipt_type} => $strcenter },
				    { '            '       => $strcenter } ]);
		      }			    
                    
          }
         
         
         my $filename = 'session_' . $session_id . '.xlsx';
         my $o        = IBIS::ExcelWriter::File->write(
            outdir   => $g_data_dir,
            file     => $filename,
            csv_data => \@csv
        );
        
               
        $sth->finish();
        $g_curr_session_time_range_sth->finish();
        $g_usr_bcs_sth->finish();
        
        
        # Mark session as printed
        $g_db_rms->do($rima_cmd);
        
	
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
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, "Untrapped Error:\n\n", " $@" );
    $g_log->info($@);
}

#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------
