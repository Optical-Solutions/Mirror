#!/usr/local/mccs/perl/bin/perl --
#-----------------------------------------------------------------
# Ported by: Hanny Januarius
# Date: Mon Dec 11 06:39:03 EST 2023
# Desc:
#      generate the Department List recordset
#      generate the Cat List recordset
#      generate the Item Library List recordset
#      populate FlatFiles
#      SFTP FlatFiles
#      Archive Sent Files
#      Remove Old Logs
#-----------------------------------------------------------------
# Ported by:  Kaveh Sari
# Date: Thu May  9 14:37:29 EDT 2024
# Desc:
# Added a no send option to this code to allow skipping of sftp process.
# Created archive directory
#-----------------------------------------------------------------
# Updated by Kaveh Sari 
# Porting Complete  October 11, 2024 1:33:23 PM
# Restored to required functionality.  
#---------------------------------------------------------------------
use strict;
use IBIS::DBI;
use DateTime;
use IO::Compress::Zip qw(:all);
use Net::SFTP::Foreign;
use MCCS::Config;
use Net::SMTP;
use Carp;
use Getopt::Long;
use IBIS::Log::File;
use Readonly;
use File::Basename;
use File::Spec;
use File::Copy;
use File::Path;

#
#Define date variable to be used when creating log file names
chomp( my $g_log_date = `date +%F` );

# Define constant to contain fully qualified path name to this script's log file
Readonly my $g_logfile => '/usr/local/mccs/log/acv/'
  . basename(__FILE__) . '_'
  . $g_log_date . '.log';

use constant DEFAULT_OUTPUT_DIR => '/usr/local/mccs/data/acv/';

# Default verbosity mode
my $g_verbose = 0;

# Define variable for DB connection
my $g_dbh;

my $g_cfg = new MCCS::Config;
my $g_emails = $g_cfg->acv_DATA->{emails};

# Extract file name for Departments List flat file from acv configuration
my $g_DptList = $g_cfg->acv_DATA->{DEPARTMENTS_FILENAME};

# Extract file name for Categories List flat file from acv configuration
my $g_CatList = $g_cfg->acv_DATA->{CATEGORIES_FILENAME};

# Extract file name for Item Library flat file from acv configuration
my $g_ItemLibList = $g_cfg->acv_DATA->{ITEM_LIB_LIST_FILENAME};

# Retrieve name of ACV Archive directory
my $g_arc_dir = $g_cfg->acv_DATA->{ARC_DIR};

# Initialize variable with short date for Archiving purposes
my $g_date = `date +%Y%m%d`;
chomp($g_date);

# Dynamically generate names for all required acv flat files
my $g_dptlistfile = DEFAULT_OUTPUT_DIR . $g_DptList . $g_date . ".txt";
my $g_catlistfile = DEFAULT_OUTPUT_DIR . $g_CatList . $g_date . ".txt";
my $g_itemlibfile = DEFAULT_OUTPUT_DIR . $g_ItemLibList . $g_date . ".txt";
my $g_itemlibfileZip = DEFAULT_OUTPUT_DIR . $g_ItemLibList . $g_date . ".zip";

# This variable contains the name of the
my $g_ItemLibList_to_ftp =
  File::Spec->catfile( DEFAULT_OUTPUT_DIR, $g_ItemLibList . $g_date . ".zip" );

my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);

my $g_log =
  IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );

eval { $g_dbh = IBIS::DBI->connect( dbname => 'rms_r' ) };
if ($@) { fatal_error('Unable to connect to RMS database') }

$g_log->info("All pre-processing steps have completed\n");

my $suppress_send = 0;
my $options = ( GetOptions('nosend' =>  \$suppress_send));
#############################   MAIN LINE   #########################################

$g_log->info("Prepare and execute sql to generate the Department List recordset");
my $Dept_sth = crtDeptRecordSet();

$g_log->info("Prepare and execute sql to generate the Cat List recordset");
my $Cat_sth = crtCatRecordSet();

$g_log->info("Prepare and execute sql to generate the Item Library List recordset");
my $Item_sth = crtItemLibRecordSet();

$g_log->info("populate FlatFiles");
populateFlatFiles( $Dept_sth, $Cat_sth, $Item_sth );

if (!$suppress_send) {
	$g_log->info("SFTP FlatFiles");
	ftpsslFlatFiles();
}

$g_log->info("Archive Sent Files");
archiveSentFiles();

$g_log->info("Remove Old Logs");
remove_old_logs();


#---------------------------
# Create Department List                                                                                                      #
#---------------------------
sub crtDeptRecordSet {
	my $deptListSql = <<DEPTLISTSQL;
SELECT DISTINCT d.mlvl_level_id DEPARTMENT_ID, m.description
FROM styles s
JOIN bar_codes b
ON s.style_id = b.style_id
JOIN sections t
ON s.section_id = t.section_id
JOIN departments d
ON d.department_id = t.department_id
JOIN merchandising_levels m
ON m.level_id = d.mlvl_level_id
AND m.mlvl_level_id IS NOT NULL
ORDER BY d.mlvl_level_id
DEPTLISTSQL

	my $sth = $g_dbh->prepare($deptListSql)
	  or fatal_error(
		"SQL to build department list has failed preparation phase!\n");
	$sth->execute()
	  or
	  fatal_error("SQL to build department list has failed execution phase!\n");
	return $sth;
}
#-------------------------------
# Create Category List recordset                                                                                              #
#-------------------------------
sub crtCatRecordSet {
	my $catListSql = <<CATLISTSQL;
SELECT DISTINCT d.department_id CATEGORY_ID, d.mlvl_level_id DEPARTMENT_ID, d.description
FROM styles s
JOIN bar_codes b
ON s.style_id = b.style_id
JOIN sections t
ON s.section_id = t.section_id
JOIN departments d
ON d.department_id = t.department_id
JOIN merchandising_levels m
ON m.level_id        = d.mlvl_level_id
and m.mlvl_level_id is not null
ORDER BY d.department_id, d.mlvl_level_id
CATLISTSQL

	my $sth = $g_dbh->prepare($catListSql)
	  or
	  fatal_error("SQL to build category list has failed preparation phase!\n");
	$sth->execute()
	  or
	  fatal_error("SQL to build category list has failed execution phase!\n");
	return $sth;
}
##########################################################################################################################################
# Create Item library recordset                                                                                                  #
##########################################################################################################################################
sub crtItemLibRecordSet {
	my $libListSql = <<LIBLISTSQL;
SELECT S.Style_Id ITEMID,
  B.Bar_Code_Id UPC,
  V.Department_Id CATEGORYID,
  V.Dept_Name CATEGORYDESCRIPTION ,
  D.Mlvl_Level_Id DEPARTMENTID,
  M.Description DEPTDESCRIPTION,
  S.Description,
  (select (select description from Characteristic_Values where Characteristic_Type_Id = 'BRAND' and 
Characteristic_Value_Id = (select Characteristic_Value_Id from Style_Characteristics
where style_id = S.Style_Id and Characteristic_Type_Id = 'BRAND'
AND CHAR_TY_SUB_TYPE = 'STYL')) BRAND from dual) BRAND,
 (select (select description from Characteristic_Values where Characteristic_Type_Id = 'MNFCT' and 
Characteristic_Value_Id = (select Characteristic_Value_Id from Style_Characteristics
where style_id = S.Style_Id and Characteristic_Type_Id = 'MNFCT' 
AND CHAR_TY_SUB_TYPE = 'STYL')) BRAND from dual) MNFCT,
  S.Vendor_Style_No MODEL,
  get_permanent_retail_price(30,NULL,s.style_id,NULL,NULL,NULL,NULL,NULL) REG_PRICE,
  B.Color_Id COLOR,
  B.Size_Id "SIZE",
  B.Dimension_Id DIM
FROM Styles S
JOIN Bar_Codes B  ON S.Style_Id = B.Style_Id
JOIN V_Dept_Class_Subclass V  ON S.Section_Id = V.Section_Id
JOIN Departments D  ON D.Department_Id = V.Department_Id
JOIN Merchandising_Levels M  ON M.Level_Id   = D.Mlvl_Level_Id
AND M.Mlvl_Level_Id  IS NOT NULL
ORDER BY upc, S.Style_Id
LIBLISTSQL

	my $sth = $g_dbh->prepare($libListSql)
	  or fatal_error("SQL to build item list has failed preparation phase!\n");
	$sth->execute()
	  or fatal_error("SQL to build item list has failed execution phase!\n");
	return $sth;
}

###########################################################################################################################################
# This subroutine receives 3 file handles providing access to the 3 recordset needed to create the required flat files                    #
###########################################################################################################################################
sub populateFlatFiles {

	my $Dept_sth = shift;
	my $Cat_sth  = shift;
	my $Item_sth = shift;

	# Open handles for the 3 required flat files
	open my $dh, '>', $g_dptlistfile
	  or fatal_error("Could not open $g_dptlistfile file: $!");
	open my $ch, '>', $g_catlistfile
	  or fatal_error("Could not open $g_catlistfile file: $!");
	open my $lh, '>', $g_itemlibfile
	  or fatal_error("Could not open $g_itemlibfile file: $!");

	# Commence production of flat files
	my $row;

	#Write Dept list flat file data headers
	eval {
		print $dh "DEPARTMENT_ID|DESCRIPTION\n";

		# Write Dept list flat file data
		while ( $row = $Dept_sth->fetchrow_hashref() ) {
			print $dh $row->{department_id} . '|';
			print $dh $row->{description} . "\n";
		}
	};

	fatal_error("Creation of $g_dptlistfile has failed!\n") if $@;
	close $dh;

	#Write Cat list flat file data headers
	eval {
		print $ch "CATEGORY_ID|DEPARTMENT_ID|DESCRIPTION\n";

		# Write Dept list flat file data
		while ( $row = $Cat_sth->fetchrow_hashref() ) {
			print $ch $row->{category_id} . '|';
			print $ch $row->{department_id} . '|';
			print $ch $row->{description} . "\n";
		}
	};

	fatal_error("Creation of $g_catlistfile has failed!\n") if $@;
	close $ch;

	#Write Item list flat file data headers
	eval {
		print $lh
"ITEMID|UPC|CATEGORYID|CATEGORYDESCRIPTION|DEPARTMENTID|DEPTDESCRIPTION|SDESCRIPTION|BRAND|MNFCT|MODEL|REG_PRICE|COLOR|SIZE|DIM\n";

		# Write Dept list flat file data
		while ( $row = $Item_sth->fetchrow_hashref() ) {
			chomp $row->{itemid};
			chomp $row->{upc};
			chomp $row->{categoryid};
			chomp $row->{categorydescription};
			chomp $row->{departmentid};
			chomp $row->{deptdescription};
			chomp $row->{description};
			chomp $row->{brand};
			chomp $row->{mnfct};
			chomp $row->{model};
			chomp $row->{reg_price};
			chomp $row->{color};
			chomp $row->{size};
			print $lh $row->{itemid} . '|';
			print $lh $row->{upc} . '|';
			print $lh $row->{categoryid} . '|';
			print $lh $row->{categorydescription} . '|';
			print $lh $row->{departmentid} . '|';
			print $lh $row->{deptdescription} . '|';
			print $lh $row->{description} . '|';
			print $lh $row->{brand} . '|';
			print $lh $row->{mnfct} . '|';
			print $lh $row->{model} . '|';
			print $lh $row->{reg_price} . '|';
			print $lh $row->{color} . '|';
			print $lh $row->{size} . '|';
			print $lh $row->{dim} . "\n";
		}
	};

	fatal_error("Creation of $g_itemlibfile has failed!\n") if $@;
	close $lh;

}

##################################################################################
#      Routine to send/email errors and croak
##################################################################################
sub fatal_error {
	my $msg = shift;
	send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
	$g_log->info($msg);
	croak($msg);
}

########################################################################################
# This subroutine ftps (with SSL) all 3 RCX Interface flat files to target server      #
########################################################################################
sub ftpsslFlatFiles {

	# Retrieve destination server, directory & credentials
	# Define argument list needed by NET:SFTP
	my %arglist;
	my $servers  = $g_cfg->acv_DATA->{FTP_SERVERS};
	my $remote_d = $g_cfg->acv_DATA->{REMOTE_DIRS};
	$arglist{user} = $g_cfg->acv_DATA->{USER};
        $arglist{password} = $g_cfg->acv_DATA->{PSWD};
        $arglist{more}     = '-v';
	
	
	foreach my $server ( keys %{$servers} ) {
		my $num_retry              = 10;
		my $successful_SSL_connect = 'N';
		my $ftps;
		my $attempt;
		my $dest = $servers->{$server};
		while ( $num_retry-- ) {

			# Establish FTP SSL connection with Accessvia Server
			eval { $ftps = Net::SFTP::Foreign->new( $dest, %arglist ) };
			if ( $ftps->error == 0 ) { $successful_SSL_connect = 'Y'; last }
			$attempt = 10 - $num_retry;
			$g_log->info("Attempt $attempt to connect to $dest failed!\n");
			sleep(10);
		}

		fatal_error("Can't open $dest\n$Net::SFTP::ERRSTR")
		  if $successful_SSL_connect eq 'N';

		$g_log->info("FTP SSL process has started!\n");

		# Put all required files in Accessvia Server
		if ( -e $g_dptlistfile ) {
			$ftps->put( $g_dptlistfile,$g_DptList . $g_date . ".txt",copy_perms => 0,
               copy_time  => 0,
               atomic     => 1 )
				 
			  or fatal_error(
				"Can't transfer $g_dptlistfile coz " . $ftps->error );
		}

		if ( -e $g_catlistfile ) {
			$ftps->put( $g_catlistfile,$g_CatList . $g_date . ".txt",copy_perms => 0,
               copy_time  => 0,
               atomic     => 1 )

			  or fatal_error(
				"Can't transfer $g_catlistfile coz " . $ftps->error  );
		}

		if ( -e $g_itemlibfile ) {
             
			# Zip file up before sftp'ing to ACV
			#zip $g_itemlibfile => "$g_ItemLibList_to_ftp"
			
			#Zip -mj  "m" - will remove orginal unzip file just leaving zipped file
			#         "j" - Will make sure there is no path info within zip
		   $g_log->info("zip -mjq $g_itemlibfileZip $g_itemlibfile");
		   system("zip -mjq $g_itemlibfileZip $g_itemlibfile" ); 
		   $g_log->info("Sleep 60");
                   sleep(60);
           if ($? & 127) {

              fatal_error( 'Zipping step of ACV file '
                  . $g_itemlibfile
                  . ' has failed. Alet RDI personnel!' );

           } else {
			$ftps->put( $g_ItemLibList_to_ftp,$g_ItemLibList . $g_date . ".zip",copy_perms => 0,
               copy_time  => 0,
               atomic     => 1 )

			  or fatal_error( "Can't transfer $g_ItemLibList_to_ftp coz " .$ftps->error);
           }
		}

	}

}

########################################################################################
# This subroutine archives the 3 flat files created for the ACV interface    #
########################################################################################
sub archiveSentFiles {

	# Create archive directory
	if ( !-d "$g_arc_dir/$g_date" ) {
		mkpath("$g_arc_dir/$g_date")
		  or
		  fatal_error("Creation of directory $g_arc_dir/$g_date has failed!");
	}

	# Archive ACV interface files
	foreach my $dailyFile ( $g_dptlistfile, $g_catlistfile, $g_itemlibfile,
		$g_ItemLibList_to_ftp )
	{
		if ( -e $dailyFile ) {
			move( $dailyFile, "$g_arc_dir/$g_date/" . basename($dailyFile) )
			  or fatal_error("Archiving of $dailyFile failed!");
		}
	}

	# Send email confirming that process ended normally
	send_mail( 'ACCESSVIA EXTRACT',
		' FILES REQUIRED FOR ACCESSVIA HAVE BEEN SENT TO ACCESSVIA SERVER ' );
}

##################################################################################
#      Routine to send notification emails
##################################################################################
sub send_mail {
	my $msg_sub  = shift;
	my $msg_bod1 = shift;
	my $msg_bod2 = shift || '';
	return
	  if $g_verbose;    # Dont want to send email if on verbose mode

	foreach my $name ( sort keys %{$g_emails} ) {
		$g_log->info( "Sent email to $name (" . $g_emails->{$name} . ")" );
		$g_log->info("  Sbj: $msg_sub ");
		$g_log->debug("  $msg_bod1 ");
		$g_log->debug("  $msg_bod2 ");
		open( MAIL, "|/usr/sbin/sendmail -t" );
		print MAIL "To: " . $g_emails->{$name} . " \n";
		print MAIL "From: rdistaff\@usmc-mccs.org\n";
		print MAIL "Subject: $msg_sub \n";
		print MAIL "\n";
		print MAIL $msg_bod1;
		print MAIL $msg_bod2;
		print MAIL "\n\nServer: " . `hostname` . "\n";
		print MAIL "\n";
		print MAIL "\n";
		close(MAIL);
	}
}

##################################################################################
#      Routine to remove old logs
##################################################################################
sub remove_old_logs {
	my $dir_spec = '/usr/local/mccs/log/acv/';
	my @files    = glob "$dir_spec$0*";
	foreach my $file (@files) {
		if ( -M "$file" > 7 ) {
			eval { unlink("$file") };
			$g_log->info("Can't delete $file: $!\n") if ($@);
		}
	}
}
