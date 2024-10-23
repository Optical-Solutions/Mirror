package MCCS::SAS::Pre_Reclass_Utils;

use Moose;

use strict;
use Time::localtime;
use File::Basename;
use IBIS::Log::File;
use MCCS::Config;
use File::Copy;
use File::Path;
use Getopt::Std;
use Fcntl qw(:flock);
use IBIS::DBI;
use Net::SSH2;
use IBIS::Crypt;
use IO::Socket::INET;
use MCCS::WMS::Sendmail;
use File::Copy;
use File::Path;
use Net::SFTP::Foreign;

has pre_reclass_log => ( isa => 'IBIS::Log::File', is => 'rw');	
has email           => ( isa => 'Str', is => 'ro');
has ibis_user       => ( isa => 'Str', is => 'ro');
has m2_7_msg        => ( isa => 'Str', is => 'ro', init_arg => undef,
 default => "All files with names starting with MERCH_2 thru MERCH_7 in IBISPROD\nin directory 
  /usr/local/mccs/data/sas/pre_reclass were created by this option!");
has m10_msg        => ( isa => 'Str', is => 'ro', init_arg => undef,
 default => "File with name starting with MERCH_10 in IBISPROD\nin directory 
  /usr/local/mccs/data/sas/pre_reclass has been created by this option!"); 
has mri_missing_msg        => ( isa => 'Str', is => 'ro', init_arg => undef,
 default => "Revised file with orphan rms skus has not been returned\nThis option 
 can only be completed once the parentage information for these items is assigned!"); 
has sftp_sas_msg        => ( isa => 'Str', is => 'ro', init_arg => undef,
 default => "All flat files needed for Reclass have been sent to SAS");  
   

# Fetch application MCCS config parameters.
sub _mccs_config_fetch {
	my $self = shift;
	
	my $mccs_cfg = new MCCS::Config;
	
	return 	$mccs_cfg->pre_reclass;	
}

	


sub Disable_SAS_SFTP {
	
	my $self = shift;
	
	# Record Start of Step 1
	$self->_log_pre_rcl_step(1, 'S');
	
	open(my $fh, '>', '/usr/local/mccs/data/sas/04_BatchScripts/skip_sftp.txt') or $self->_log_n_die("Could not create '/usr/local/mccs/data/sas/04_BatchScripts/skip_sftp.txt : $!");
	
	# Record End of Step 1
	$self->_log_pre_rcl_step(1, 'E');
}


sub Enable_SAS_SFTP {
	
	my $self = shift;	
	
	# Record Start of Step 7
	$self->_log_pre_rcl_step(7, 'S');
	
	if (-e '/usr/local/mccs/data/sas/04_BatchScripts/skip_sftp.txt') {
		eval {unlink '/usr/local/mccs/data/sas/04_BatchScripts/skip_sftp.txt'};
        if ($@) { $self->_log_n_die("removal step failed\n$@") };
  }
  
    # Record Start of Step 7
	$self->_log_pre_rcl_step(7, 'E');
	}	
	
	
sub is_Task_Enabled {	
	
	my $self = shift;
	my $task_number = shift;
	my $rtn_code;
	
	# Log in to RMS Database server and populate intermediate tables needed to create required flat files for Reclass 
	my $db_rms = IBIS::DBI->connect( dbname => 'rms_r' )
	or $self->_log_n_die("Unable to connect to RMS ");
	
	# Prepare sql needed to validate LPN (aka Transfer id)
	my $val_task_sth = $db_rms->prepare("select MRI_PRE_RECLASS.IS_PRE_RCL_STEP_ENABLED(?) from dual")
 	or $self->_log_n_die("Unable to prepare val_task_sth!");
 	
 	$val_task_sth->execute($task_number) or $self->_log_n_die("Unable to execute val_task_sth!");
 	
 	($rtn_code) = $val_task_sth->fetchrow_array();
 	
 	$val_task_sth->finish();
 	
 	$db_rms->disconnect();
 	
 	# If evaluating Task 2 and $rtn_code is 1, check if SAS socket is active.
 	# If socket is not active, return appropriate message
 	if ( $task_number eq '2' &&  $rtn_code eq '1'  &&  ! $self->is_sas_socket_active() ) {
 		$rtn_code = "Task can not be run unless SAS socket program is started, contact RDI!";
 	}
 
   # If evaluating Task 5 and $rtn_code is MRI_MISSING_HIER NEEDED, submit task to populate
   # MRI_MISSING_HIER table from contents of special email attachment sent to MCCS-REQ    
 	if ( $task_number eq '5' &&  $rtn_code eq 'MRI_MISSING_HIER NEEDED') {
 		system('/usr/local/mccs/bin/sas/pre_reclass/get_missing_hier.pl &');
 		$self->{pre_reclass_log}->log_info("/usr/local/mccs/bin/sas/pre_reclass/get_missing_hier.pl has been submitted to populate MRI_MISSING_HIER");
 		$rtn_code = $self->mri_missing_msg;
 	}	 
 	
 	return $rtn_code;
	
}	

sub _get_sas_socket_ip {
	my $self = shift;
	# Get configuration parameters for pre_reclass application
	my $pre_rcl_cfg = $self->_mccs_config_fetch();
	my $socket_ip = $pre_rcl_cfg->{SAS_SERVER_IP};
	return $socket_ip;
	}

sub _send_mail_with_attachment {
	
	my $self = shift;
	my $subject = shift;
	my $body = shift;
    my $file = shift;
    
    my $host = `hostname`;
    
    my %email_h;
    
    $email_h{'web_user'} = $self->{email};
    
    my $email_ref = \%email_h;       
    
	my $email_obj = MCCS::WMS::Sendmail->new();
	
	
    $email_obj->subject($subject);
    $email_obj->sendTo( $email_ref );
    $email_obj->attachments($file) if $file;
    $email_obj->msg($body);
    $email_obj->hostName($host);
    
    $self->{pre_reclass_log}->log_info("Email sent to : " . $self->{email});
    
    if ($file) {
        eval {$email_obj->send_mail_attachment()};
        if ($@) { $self->_log_n_die("Email failed: $@") } 
    }
    else {
        eval {$email_obj->send_mail()};
        if ($@) { $self->_log_n_die("Email failed: $@") } 
    }
}

sub Submit_SAS_Merch10_Purge {
	my $self = shift;
    
    my $pid = fork();
    
    if ($pid == 0) {
    	$self->_Exec_SAS_Merch10_Purge();
    	exit 0;
    }
    
	
}

sub Submit_Manual_Parentage_List{
	my $self = shift;
    
    my $pid = fork();
    
    if ($pid == 0) {
    	$self->_Exec_Manual_Parentage_List();
    	exit 0;
    }
    
	
}

sub Submit_Merch_2_7_files {
	my $self = shift;
    
    my $pid = fork();
    
    if ($pid == 0) {
    	$self->_Exec_Merch_2_7_files();
    	exit 0;
    }
	
}

sub Submit_Merch_10_file {
	my $self = shift;
    
    my $pid = fork();
    
    if ($pid == 0) {
    	$self->_Exec_Merch_10_file();
    	exit 0;
    }
}

sub Submit_Sftp_to_SAS  {
	my $self = shift;
    
    my $pid = fork();
    
    if ($pid == 0) {
    	$self->_Sftp_to_SAS();
    	exit 0;
    }
	
}

sub _Archive_merch_files {
	my $self = shift;
	my $file_pattern = shift;
	
	# Get configuration parameters for pre_reclass application
	my $pre_rcl_cfg = $self->_mccs_config_fetch();
	
	
	my $src_dir = $pre_rcl_cfg->{IBIS_MERCH_DIR};
	my $arc_dir = $src_dir . 'archive';
	my $arc_file;
	my $file_list;
		
	# Create archive directory if needed
	unless (-d $arc_dir) {
	mkpath($arc_dir)
	 or $self->_log_n_die("Unable to create $arc_dir: $!");
	}
	 
	# Get list of files currently in $src_dir
	$file_list = $self->_get_file_list($src_dir,$file_pattern);
	
	foreach my $fn (@$file_list) {
		$arc_file = $src_dir . $fn;
		move($arc_file,$arc_dir) or $self->_log_n_die("Unable to archive $arc_file: $!");	
	}
 
}

sub _Sftp_to_SAS  {
	my $self = shift;
	
	# Define argument list needed by NET:SFTP:Foreign
	my %arglist;
	
	# Define email subject variable
	my $email_subject;
	
	# Define email body variable
	my $email_body;
	
	# Get configuration parameters for pre_reclass application
	my $pre_rcl_cfg = $self->_mccs_config_fetch();
	
	# Get directory containing MERCH files to be sent to SAS server
	my $src_dir = $pre_rcl_cfg->{IBIS_MERCH_DIR};
	
	# Get name of SAS servers to receive MERCH files
	my $sas_server = $pre_rcl_cfg->{SAS_SERVER};
	
	# Get name of SAS user to login to SAS server
	$arglist{user} = $pre_rcl_cfg->{SAS_USER};
		
	# Get password to log in to SAS server
	$arglist{password} = $pre_rcl_cfg->{SAS_SERVER_PSWD};
	
	# Get SAS destination directory to contain the MERCH files
	my $sas_merch_dir = $pre_rcl_cfg->{SAS_MERCH_DIR};
	
	# Get list of files currently in $src_dir
	my $merch_file_list = $self->_get_file_list($src_dir,'MERCH');
	
	# Record Start of Step 6
	$self->_log_pre_rcl_step(6, 'S');
	
	# Initiate attempt to establish an SFTP session with SAS Servr
	my $sftp;
	my $num_retry      = 10;
	my $successful_ftp = 'N';
	my $attempt;
	while ( $num_retry-- ) {
		eval { $sftp = Net::SFTP::Foreign->new( $sas_server, %arglist,more => [-o => 'StrictHostKeyChecking no'] ) };
		if ( !$@ ) { $successful_ftp = 'Y'; last }
		$attempt = 10 - $num_retry;
		$self->{pre_reclass_log}->log_info("Attempt $attempt to connect to $sas_server failed!\n");
		sleep(10);
	}

	if ( $successful_ftp eq 'N' ) {
			$self->_log_n_die("SFTP connection to SAS server ($sas_server) failed!");
		}
	
	# PUT each MERCH file in SAS Staging directory	
	foreach my $fn (@$merch_file_list) {
		$sftp->put(
			$src_dir . $fn, $sas_merch_dir . $fn,
			copy_perms => 0,
			copy_time  => 0,
			atomic     => 1
		);	
		
		$self->_log_n_die("File $fn could not be sent!") if ( $sftp->error );
	}
	
	# Record End of Step 6
		$self->_log_pre_rcl_step(6, 'E');
		
	# Send completion email to user
	$email_subject = 'Step 6: Transmission of MERCH files to SAS has completed';
	
	# Build body of email
	$email_body = "The following MERCH files have been sent to server: $sas_server and placed in directory: $sas_merch_dir:\n";
	 
	 foreach my $fn (@$merch_file_list) {
	 	$email_body .= $fn . "\n";
	 }	
	
	# Send the confirmation email
	$self->_send_mail_with_attachment($email_subject,$email_body);
			
}			
	


sub _get_file_list {
	my $self = shift;
    my $dir = shift;
    my $file_pattern = shift;
    my $dir_handle;
    my @files = ();

    
    opendir( $dir_handle, $dir )
        or $self->_log_n_die("Could not open dir $dir");

    while ( my $item = readdir($dir_handle) ) {
        next if ( $item eq '.' or $item eq '..' );    # Skip the dots
        next if ( -d "$dir/$item" );                  # skip the dir
        # Populate array only with those file names that match the pattern
        push( @files, $item ) if $item =~ /$file_pattern/;
    }
    return \@files;
}

sub _Merch_10_Purge_Prep {
	
	my $self = shift;
	
	my $sas_purge_prep = 'Begin MRI_PREP_FOR_PURGE10(); End;';
    	
	# Log in to SAS Database server and prepare LV10MAST for MERCH10 level purge
	my $db_sas = IBIS::DBI->connect( dbname => 'sasprd_rcl' )
	or $self->_log_n_die("Unable to connect to SAS ");
	
	my $sth_purge_prep;
    eval { $sth_purge_prep = $db_sas->prepare($sas_purge_prep) };
    if ($@) { $self->_log_n_die("Failed to prepare proc below:\n$sas_purge_prep\n$@") } 

    
    eval { $sth_purge_prep->execute() };
    if ($@) { $self->_log_n_die("Failed to execute proc below:\n$sas_purge_prep\n$@") }
    
    $sth_purge_prep->finish();
    
    $db_sas->disconnect();
	
}

sub _mri_pre_reclass_setup {
	
	my $self = shift;
	
	my @dataarray;
	
	my $mnl_parentage_list = <<MNLPRLIST;
     select rms.style_id,rms.color_id,rms.size_id,rms.dimension_id,
     (select lv6cmast_userid from lv6cmast
      where lv6cmast_id = (select lv6cmast_id from lv6ctree
      where lv6ctree_id = lv10.lv6ctree_id)) SAS_Dept_Class_Subclass
      from MRI_NO_RMS_STYLES rms
      join lv10mast lv10 on lv10.order_code = rms.order_code
MNLPRLIST

    my $output_file = '/usr/local/mccs/data/mri_missing_hier.csv';
    
    my $outputline;
    
    # Record Start of Step 3
	$self->_log_pre_rcl_step(3, 'S');
	
	my $sas_pre_reclass_setup_prep = 'Begin MRI_PRE_RECLASS.pre_reclass_setup(); End;';
    	
	# Log in to RMS Database server and populate intermediate tables needed to create required flat files for Reclass 
	my $db_rms = IBIS::DBI->connect( dbname => 'rms_r' )
	or $self->_log_n_die("Unable to connect to RMS ");
	
	my $sth_pre_reclass_setup;
    eval { $sth_pre_reclass_setup = $db_rms->prepare($sas_pre_reclass_setup_prep) };
    if ($@) { $self->_log_n_die("Failed to prepare proc below:\n$sas_pre_reclass_setup_prep\n") } 

    $self->{pre_reclass_log}->log_info("Intermediate tables needed to create required flat files for Reclass will now be populated!");
    
    eval { $sth_pre_reclass_setup->execute() };
    if ($@) { $self->_log_n_die("Failed to execute proc below:\n$sas_pre_reclass_setup_prep\n") }
    
    $sth_pre_reclass_setup->finish();
    
    $self->{pre_reclass_log}->log_info("Intermediate tables needed to create required flat files for Reclass have been populated!");
    
    # At this point, all required intermediate data tables have been populated
    
    # Execute query to populate recordset with styles that will need manual parentage assigned.
    my $sth_pre_mnl_parent;
    eval { $sth_pre_mnl_parent = $db_rms->prepare($mnl_parentage_list) };
    if ($@) { $self->_log_n_die("Failed to prepare proc below:\n$mnl_parentage_list\n") }
    
    eval { $sth_pre_mnl_parent->execute() };
    if ($@) { $self->_log_n_die("Failed to execute proc below:\n$mnl_parentage_list\n") }
    
    # Create flat file styles that require manual parentage assignment
    open my $fh, '>', $output_file or $self->_log_n_die("Failed to create $output_file!\n");
    
    # Write row containing headers
    $outputline = 'STYLE_ID,COLOR_ID,SIZE_ID,DPT_CL_SCL';
    print $fh $outputline . "\n";
        
    while ( my $row = $sth_pre_mnl_parent->fetchrow_hashref() ) {
    	
    	    @dataarray = ();
    		push @dataarray, $row->{style_id};
    		push @dataarray, $row->{color_id};
    		push @dataarray, $row->{size_id};
    		push @dataarray, $row->{sas_dept_class_subclass};
    		my $outputline = join ',', @dataarray;
    		print $fh $outputline . "\n"; 
    }
    
    close $fh;
    
    $sth_pre_mnl_parent->finish();
    
    $db_rms->disconnect();
    
    
    # Send email with attached file
    $self->_send_mail_with_attachment('PRE-RECLASS STYLES THAT NEED PARENTAGE','The attached csv file contains a list of styles that need to have parentage assigned manually.',$output_file );
    
    # Record Start of Step 3
	$self->_log_pre_rcl_step(3, 'E');
    
}

sub populate_mri_order_codes {
	
	my $self = shift;
	my $file = shift;
	my @row;
	
		
	  my $sql = qq( 
                insert into MRI_ORDER_CODES\@MC2R 
                (order_code, class, style_id)
                values (?,?,?)
    );
	
	my $db_rms = IBIS::DBI->connect( dbname => 'rms_r' )
	or $self->_log_n_die("Unable to connect to RMS ");	
	
	my $insert_sth = $db_rms->prepare($sql);
	
    # Read MERCH10 flat file created for Reclass purposes
    # to populate table MRI_ORDER_CODES
    open my $fh, '<', $file or $self->_log_n_die("Failed to open $file!\n");
    
       
    while (my $line = <$fh>) {
    	
    	 chomp $line;
    	 
    	 @row = split( /\|/, $line );
    	 
    	 $insert_sth->execute($row[1],$row[78],$row[79]) or
    	 $self->_log_n_die("Failed to insert row : @row\n");   	
    }
    
        
    close $fh;
    
    $db_rms->commit();    
        
    $db_rms->disconnect();
    
      
}


sub _set_Purge_Approved_Flags{
	
	my $self = shift;
	
	my $sas_purge_apprv = "Begin update maxdata.mdpu_purge_product set approved = 'Y'; commit; End;";
	
	$self->{pre_reclass_log}->log_info("Approved flags are about to be set !");
    	
	# Log in to SAS Database server and prepare LV10MAST for MERCH10 level purge
	my $db_sas = IBIS::DBI->connect( dbname => 'sasprd_rcl' )
	or $self->_log_n_die("Unable to connect to SAS ");
	
	$self->{pre_reclass_log}->log_info("Connection to db successful !");
	
	my $sth_purge_apprv;
    eval { $sth_purge_apprv = $db_sas->prepare($sas_purge_apprv) };
    if ($@) { $self->_log_n_die("Failed to prepare proc below:\n$sas_purge_apprv\n") } 
    
    $self->{pre_reclass_log}->log_info("Approved flags sql prepared !");

    eval { $sth_purge_apprv->execute() };
    if ($@) { $self->_log_n_die("Failed to execute proc below:\n$sas_purge_apprv\n") }
    
    $self->{pre_reclass_log}->log_info("Approved flags sql set !");
    
    $sth_purge_apprv->finish();
    
    $db_sas->disconnect();
	
}

sub is_sas_socket_active {

	my $self = shift;
	
	# Get socket ip address for SAS
	my $socket_ip = $self->_get_sas_socket_ip();
	
	# create a connecting socket
    my $socket = new IO::Socket::INET (
    PeerHost => $socket_ip,
    PeerPort => '7777',
    Proto => 'tcp',
    );	
    
    if ( $socket ) {
    	$socket->close();
    	return 1;
    }
   else
    {
     	return 0;		
    }	
        
}	

sub _purge_copy_approved_to_txt {
	
	my $self = shift;
	
	my $req;
	
	my $log;
	
	my $size;
	
	# auto-flush on socket
    $| = 1;
	
	# create a connecting socket
	
	# Get socket ip address for SAS
	my $socket_ip = $self->_get_sas_socket_ip();
	
    my $socket = new IO::Socket::INET (
    PeerHost => $socket_ip,
    PeerPort => '7777',
    Proto => 'tcp',
    );
    $self->_log_n_die("cannot connect to the server $!") unless $socket;
	
	$req = '/app/mdi/SAS/retail/plan_61/MDI/code/scripts/start_mdi.sh mdi_purge_copy_approved_to_txt.sas';
	
	# Log details of sent request
    $self->{pre_reclass_log}->log_info("$req sent to Reclass server");
	
    $size = $socket->send($req) or $self->_log_n_die("Unable to connect to Reclass socket server $@");
    
    # notify server that request has been sent
    shutdown($socket, 1);
    
    
    # receive a response of up to 1024 characters from server
    $log = "";
    $socket->recv($log, 1024);
    
    # Log response received from Reclass server
    $self->{pre_reclass_log}->log_info("$log");
	
	$socket->close();
	
	return $log;
}


sub _purge_delete_step {
	
	my $self = shift;
	
	my $req;
	
	my $log;
	
	my $size;
	
	# auto-flush on socket
    $| = 1;
	
	# Get socket ip address for SAS
	my $socket_ip = $self->_get_sas_socket_ip();
	
	# create a connecting socket
    my $socket = new IO::Socket::INET (
    PeerHost => $socket_ip,
    PeerPort => '7777',
    Proto => 'tcp',
    );
    $self->_log_n_die("cannot connect to the server $!") unless $socket;
	
	$req = '/app/mdi/SAS/retail/plan_61/MDI/code/scripts/start_mdi.sh mdi_purge_delete_merch.sas';
	
	# Log details of sent request
    $self->{pre_reclass_log}->log_info("$req sent to Reclass server");
	
    $size = $socket->send($req) or $self->_log_n_die("Unable to connect to Reclass socket server $@");
    
    # notify server that request has been sent
    shutdown($socket, 1);
      
	# receive a response of up to 1024 characters from server
    $log = "";
    $socket->recv($log, 1024);
    
    # Log response received from Reclass server
    $self->{pre_reclass_log}->log_info("$log");
	
	$socket->close();
	
	return $log;
	
}

sub _Exec_SAS_Merch10_Purge {
	
	my $self = shift;
	
	# Record Start of Step 2
	$self->_log_pre_rcl_step(2, 'S');
	
	# Execute MRI_PREP_FOR_PURGE10 sproc prior to purge_analyze step
	$self->_Merch_10_Purge_Prep();
		
	   
    # Submit request to tcp server on SAS
    
    # auto-flush on socket
    $| = 1;
    
    # Get socket ip address for SAS
	my $socket_ip = $self->_get_sas_socket_ip();
	
    # create a connecting socket
    my $socket = new IO::Socket::INET (
    PeerHost => $socket_ip,
    PeerPort => '7777',
    Proto => 'tcp',
    );
    $self->_log_n_die("cannot connect to the server $!") unless $socket;
    
    
    # Log successful connection to Reclass server
    $self->{pre_reclass_log}->log_info("connected to the server");
    
    # Send request to SAS to carry out the MERCH 10 Purge analyze process
    
    # data to send to a server
    my $req = '/app/mdi/SAS/retail/plan_61/MDI/code/scripts/start_mdi.sh mdi_purge_analyze_merch.sas';
    my $size = $socket->send($req);
    
    # Log details of sent request
    $self->{pre_reclass_log}->log_info("$req sent to Reclass server");
    
    # notify server that request has been sent
    shutdown($socket, 1);
    
      
    # receive a response of up to 1024 characters from server
    my $response = "";
    $socket->recv($response, 1024);
    
    # Close the socket
    
    $socket->close();
    
    # Log response received from Reclass server
    $self->{pre_reclass_log}->log_info("$response");
    
    # If last step came back with a return code of 0, proceed with next steps
    if ($response =~ m/mdi_purge_analyze_merch\.sas return code 0/) {
    	
    	    # Send email confirming that the purge analyze step has completed
    	    $self->_send_mail_with_attachment('Purge Analyze step has completed normally',$response);
    	
    	    # Set all approved flags to Y  
    	    $self->_set_Purge_Approved_Flags();
    	    
    	    # Launch the purge_copy_approved_to_txt.sas process
    	    $response = $self->_purge_copy_approved_to_txt();
    	    
    	    # Check response from purge_copy_approved_to_txt process and submit actual purge if return code was 0
    	    if ($response =~ m/mdi_purge_copy_approved_to_txt\.sas return code 0/) {
    	    	
    	    	# Send email confirming that the purge copy approved to txt step has completed
    	        $self->_send_mail_with_attachment('Purge approve to txt step has completed normally',$response);
    	    	
    	    	# Temporarily suspend call to this process
    	    	$response = $self->_purge_delete_step();
    	    	
    	    	# Check response from purge_delete process
    	    	if ($response =~ m/mdi_purge_delete_merch\.sas return code 0/) {
    	    		
    	    		# Send email to inform that the purge delete process has completed
    	            $self->_send_mail_with_attachment('Purge delete step has completed normally',$response);
    	            # Record Start of Step 2
	                $self->_log_pre_rcl_step(2, 'E');
    	    	}
    	    	else
    	    	{
    	    		# Send email to inform that the purge delete step has completed with errors
    	            $self->_send_mail_with_attachment('Purge delete step has completed with errors',$response);
    	    	}
    	    }
    	    	else
    	    	{
    	    		# Send email to inform that the purge copy approved to txt step has completed with errors
    	            $self->_send_mail_with_attachment('Purge approve to txt step has completed with errors',$response);
    	    	}	
    	    
    	   
    } 
    	else
    	{
    	    # Send email to inform that the purge analyze step has completed with errors
    	    $self->_send_mail_with_attachment('Purge Analyze step has completed with errors',$response);	
    	}   
    
    
    
    
    
}

sub _Exec_Manual_Parentage_List {
	
	my $self = shift;
	
	
	# Populate intermediate tables needed to produce list of styles that require parentage to be assigned manually and generate flat file with list
	$self->_mri_pre_reclass_setup();
	
	
}


sub _Exec_Merch_2_7_files {
	my $self = shift;
	
	# Record Start of Step 4
	$self->_log_pre_rcl_step(4, 'S');
	
	# Archive any MERCH_2 thru MERCH_7 files in current data directory
	$self->_Archive_merch_files('MERCH_[2-7]');
	
	# Execute shell script to produce MERCH 2 thru 7 files
	system('/usr/local/mccs/data/sas/04_BatchScripts/sas_pre_rcl_2_7.sh') == 0
	 or $self->_log_n_die("Call to sas_pre_rcl_2_7.sh failed with error: $@");
	 
	$self->_send_mail_with_attachment("Pre_Reclass MERCH 2 Thru 7 files have been produced",$self->m2_7_msg);	
	
	# Record End of Step 4
	$self->_log_pre_rcl_step(4, 'E'); 
	
}
sub _Exec_Merch_10_file {
	my $self = shift;
	
	# Record Start of Step 5
	$self->_log_pre_rcl_step(5, 'S');
	
	# Archive any MERCH_10 files in current data directory
	$self->_Archive_merch_files('MERCH_10');
	
	# Execute shell script to produce MERCH 10 file
	system('perl  /usr/local/mccs/bin/sas_data.pl --type FULL_MERCH10_LOAD_RECLASS --database rms_r  --pre_rcls') == 0
	 or $self->_log_n_die("Call to FULL_MERCH10_LOAD_RECLASS failed with error: $@");
	 
	# Perform validations and update metrics in table MRI_MERCH10_RCL_STATUS in MC2R
	 $self->_Merch10_validation();
	 
	$self->_send_mail_with_attachment("Pre_Reclass MERCH 10 file has been produced",$self->m10_msg);
	
	# Record End of Step 5
	$self->_log_pre_rcl_step(5, 'E');
}

sub _Merch10_validation {
	my $self = shift;
	
	my $m10_validation_prep = 'Begin MRI_PRE_RECLASS.upd_merch10_reclass_status(); End;';
	
	# Log in to RMS Database server to execute procedure that carries out the MERCH10 validations 
	my $db_rms = IBIS::DBI->connect( dbname => 'rms_r' )
	or $self->_log_n_die("Unable to connect to RMS ");
	
	my $sth_Merch10_validation;
    eval { $sth_Merch10_validation = $db_rms->prepare($m10_validation_prep) };
    if ($@) { $self->_log_n_die("Failed to prepare proc below:\n$m10_validation_prep\n") } 
    
      
    eval { $sth_Merch10_validation->execute() };
    if ($@) { $self->_log_n_die("Failed to execute proc below:\n$m10_validation_prep\n") }
    
    $sth_Merch10_validation->finish();
    
     $db_rms->disconnect();
    
    $self->{pre_reclass_log}->log_info("Merch 10 flat file validations have been performed!");
    
    
}

#  Log Start/End of selected Pre_Reclass step
sub _log_pre_rcl_step {
	my $self = shift;
	my $pre_rcl_step = shift;
	my $pre_rcl_code = shift;
	
	# Log in to RMS Database server to execute procedure that carries out the MERCH10 validations 
	my $db_rms = IBIS::DBI->connect( dbname => 'rms_r' )
	or $self->_log_n_die("Unable to connect to RMS ");
	
	$db_rms->do("BEGIN  MRI_PRE_RECLASS.log_pre_rcl_step($pre_rcl_step,'$pre_rcl_code','$self->{ibis_user}'); END;") 
	or $self->_log_n_die("MRI_PRE_RECLASS.log_pre_rcl_step has failed! ");
	
	$db_rms->disconnect();
	
}

sub _log_n_die {
	
	my $self = shift;
	$self->{pre_reclass_log}->error(@_);
	$self->_send_mail_with_attachment("Pre_Reclass job terminated abnormally",@_);
    die(@_);
  
}

1;