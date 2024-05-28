#!/usr/bin/perl
# Compliance Management Solution
use strict;
use MCCS::Nielsen::Util;
use MCCS::Nielsen::Loads::Base;
use MCCS::Config;
use MCCS::WMS::Sendmail;

use IBIS::DBI;
use IBIS::Log::File;
use IBIS::SFTP;

use Getopt::Long;
use DateTime;
use Data::Dumper;
use Class::Inspector;
use Pod::Usage;

use File::Path   qw(make_path);
use File::Copy   qw(move); 
use File::Remove qw(remove);
use File::Spec;
use File::Basename;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );

use Readonly;
use Fcntl qw(:flock);

use Net::SFTP::Foreign;


use constant DBNAME             => 'rms_r';
use constant DEFAULT_OUTPUT_DIR => '/usr/local/mccs/data/nielsen/';

my $cfg = new MCCS::Config;
my $sftpHash = $cfg->nielsen_ftp->{sftp};


#- One process at a time ---------------------------------------------
my $lock_file = "/usr/local/mccs/tmp/" . basename($0) . ".lck";
open SELF, "> $lock_file" or die "Could not create lock file $lock_file";
flock SELF, LOCK_EX | LOCK_NB or die "Another $0 process already running";
#----------------------------------------------------------------------
# Some Date Stuff - 
#----------------------------------------------------------------------
my $dt          = DateTime->now();
my $yearMonth   = $dt->strftime('%Y%m');
my $date        = $dt->strftime('%Y%m%d');
my $ts          = $dt->strftime('%H%M');
my $timestamp   = $dt->strftime('%Y%m%d%H%M%S');

#----------------------------------------------------------------------
# Options - everyone likes options
#----------------------------------------------------------------------
my $dir;
my @types = ();
my $date;
my $today;
my $debug;
my $database;
my $log = "/usr/local/mccs/log/Nielsen/".$timestamp."_nielsen_data_.log";
my $help;
my $suppress_send = 0;
my $archive_sw    = 0;
my $processDate;
my $totaldays;
my $resend = 0;
my $error = 0;

#handle command line arguments
my $options = (
	GetOptions(
		'dir=s'        => \$dir,
		"type=s"       => \@types,
		'debug'        => \$debug,
		'database=s'   => \$database,
		'log=s'        => \$log,
		'nosend'       => \$suppress_send,
		'archive'      => \$archive_sw,
		'help'         => \$help,
		'date=s'       => \$processDate,
		'totaldays=i'  => \$totaldays,
		'resend'       => \$resend, 
		  
	)
);

#------------DEFINE NECESSARY VARIABLES-----------------------------#
my $log_obj = IBIS::Log::File->new( { file => $log, append => 1 } );
my $fhdir = ( !$dir ) ? DEFAULT_OUTPUT_DIR : $dir;my %opts = ();
my @fileNames;
my $sm          = MCCS::WMS::Sendmail->new();

my $error_lvl   = 0;
my $host        = `hostname`;
my $g_long_date = `date +"%D %r"`;

#---------------------------------------------------------------------
#Function/Sub-Routines
#   Lets do some actual work functions are as ismy %opts = ();
#---------------------------------------------------------------------
sub mainLine {
    log_debug("Started");
    log_debug( "Logfile: ",      $log );

    validateOptions(); 
    
    log_debug( "Database: ",    $database );
    
    foreach my $type (@types) {
    	
       my $type_class = 'MCCS::Nielsen::Loads::' . $type;
       eval("use $type_class;");
       die $@ if $@;   #<-- If no class then get out
      
#       $database = $database || $type_class->database() || DBNAME;
       $database = $database || DBNAME;
       
       log_debug( "Databasae  : ", $database );
       log_debug( "Output type: ", $type );
       my $gfile;
      if ($type eq 'SALES') {
       	my $dt_x = $dt;
       	if ($totaldays) {
       		for (my $x = 0; $x < $totaldays; $x++) {
       		  create_file($type_class, 
       		              $type_class->get_filename($dt_x->strftime('%Y%m%d'), 
       		                                        $resend),
       		              $dt_x->strftime('%Y%m%d')
       		              );
       		  $dt_x->subtract(days => 1,);         
       		  $resend = 1;     
       		}
       	} else { 
       		create_file($type_class, 
       		            $type_class->get_filename($dt->strftime('%Y%m%d'),
       		                                      $resend), 
       		            $dt->strftime('%Y%m%d')
       		           );  
       	}
       }
       
       if ($type eq 'SALES_HIST') {
       	create_file($type_class, 
       	            $type_class->get_filename($processDate,$resend),
       	            $processDate );
       	
       }my %opts = ();
       
       if ($type eq 'SALES_DATED') {
       	create_file($type_class, 
                    $type_class->get_filename($processDate, 1),
                    $processDate );
        
       }	
    }
  my @zipFileNames;
  foreach my $file (@fileNames) {
  	push(@zipFileNames, zip_files($file) );
  }    

  log_debug( "Zipped file: ",     @zipFileNames );
  
  sftpFiles(@zipFileNames);
  archiveFiles(@zipFileNames);
  
  
  send_mail('Error during process please review attached logs') if ($error);

}

sub create_file {
	my $type_class = shift;
	my $gfile      = shift;
	my $date       = shift;
	my $count = 0;
    
	my $file =
      ( !$dir )
      ? File::Spec->catfile( DEFAULT_OUTPUT_DIR, $gfile )
      : qq($dir/$gfile);
   
     push( @fileNames, $gfile );
    log_debug( "Database: ",    $database );   
    log_debug( "Output file: ", $gfile );

    my $nielsen_util =  MCCS::Nielsen::Util->new( $database, $file );    
    my $db       = $nielsen_util->get_database();
    my $type_obj = $type_class->new($nielsen_util);

    #--Get SQL from Load::Package --#
    my $sql = $type_obj->get_sql();

    debug( 'Type SQL: ', $sql );
    log_debug( "Pulling data from ONE day **PREVIOUS** of this date => $date"); 
    my $sth = $db->prepare($sql);
       $sth->execute($date);
   
    while ( my $myrow = $sth->fetchrow_arrayref() ) {
        my $make_ret = $type_obj->make_record( @{$myrow} );
        $count++ if defined $make_ret;        
    }

    $type_obj->finish();
    $db->commit();

    log_debug("Completed $count records");
#    END {
#        $type_obj = undef;
#        if ($sth) { $sth->finish(); }
#        if ($db)  { $db->disconnect(); }
#
#    }
    
}

sub validateOptions {
   my $badmsg;
   my $plugin_dir =
      dirname( 
        Class::Inspector->loaded_filename('MCCS::Nielsen::Loads::Base') 
      );
   my $dirFH;

   opendir( $dirFH, $plugin_dir ) or die "Can not open Plugin Directory";
      my @valid_types =   map { s/\.pm//; $_ }
                          grep { /(\.pm)$/ && $_ ne 'Base.pm' } 
                          readdir($dirFH);
                          
   closedir($dirFH);
    
    print "@valid_types\n";

   #-- Handle Command Line Args --#
   if($badmsg || $help || ! $options ) {
     my $msg = $badmsg;
     if(! $options){
         $msg = 'Bad arguments';
     }
#      elsif (! grep{ @types eq $_ } @valid_types){
#        $msg = "Bad type argument @types";
#     }
    log_debug("Validate command argument => $msg");
    pod2usage(-noperldoc=>1, -verbose => 2, -msg => $msg);
   }
}

#-----------------------------------------------------------------------------
#  SFTP some things oh wait we can Archive somethings as well
#-----------------------------------------------------------------------------

sub sftpFiles {
    my @zipFiles = @_;
    
    if ( !$suppress_send ) {
        my %arglist;        
        my $dest = $sftpHash->{host};
        my $inputDir = $sftpHash->{input_dir};

        # Retrieve MCL user name and password
        $arglist{user}     = $sftpHash->{user};
        $arglist{password} = $sftpHash->{password};
        $arglist{port}     = $sftpHash->{port};
        $arglist{more}     = '-v';

        # Log server name and directory
        log_debug('SFTP transfer started');
        log_debug("FTP_SERVER: $dest");
    
        # Establish SFTP connection to MCL server
        my $sftp;
        my $num_retry      = 10;
        my $successful_ftp = 'N';
        my $attempt;
        while ( $num_retry-- ) {
            eval { $sftp = Net::SFTP::Foreign->new( $dest, %arglist ) };
            if ( !$@ ) { $successful_ftp = 'Y'; last }
            $attempt = 10 - $num_retry;
            log_debug("Attempt $attempt to connect to $dest failed!\n");
            sleep(10);
        }

        if ( $successful_ftp eq 'N' ) {
            fatal_error("SFTP connection to NFI server ($dest) failed!");
            $error = 1
        }
        
        foreach my $zipFile (@zipFiles) {
          $sftp->put(
               DEFAULT_OUTPUT_DIR . $zipFile , $inputDir.'/' . $zipFile,
               copy_perms => 0,
               copy_time  => 0,
               atomic     => 1
              );
          if ( $sftp->error ) {
           log_debug("$zipFile could not be sent");
           $error = 1
          }
        }
        log_debug('SFTP transfer completed');

    }
    else {
        log_debug("Skipping file send per command line switch --nosend\nFiles to be sent @zipFiles"  );
    }
}

sub archiveFiles {
	my @archiveFiles = @_;
    if ( $archive_sw ) {
    	
      my $baseDir = "/usr/local/mccs/data/nielsen/";
      my $archive_path = "/usr/local/mccs/data/nielsen/archive/".
                         $yearMonth."/".$date;
      
      log_debug("Archiving Files to $archive_path => @archiveFiles" );  
      
      make_path($archive_path); 
      
      foreach my $file (@archiveFiles){
      	move($baseDir.$file ,$archive_path.$file );
      }
      
      #--extra clean up from zipping of files
      foreach my $file (@fileNames){
        remove($baseDir.$file)
      }
      
    }

}

sub zip_files {
  chdir('/usr/local/mccs/data/nielsen/');

  my $unzippedFile = shift;
  
  my $zip = Archive::Zip->new();
  
  $zip->addFile($unzippedFile);
  (my $zipFile = $unzippedFile) =~ s/\.dat/\.zip/ ;
  $zipFile =~ s/MCXStoresAll_/MCX_/ ;
  
  unless ( $zip->writeToFileNamed( $zipFile ) == AZ_OK ) {
      die 'write error';
  }
  
  return $zipFile;

}
	
	


#-----------------------------------------------------------------------------
#  Email and logging Functions  ------ Boring Stuff ---------
#-----------------------------------------------------------------------------
sub send_mail {
    my @body = @_; #Just going to put every thing in the body good or bad

    my $emails;
    $emails->{larry} ='larry.d.lewis@usmc-mccs.org'; 
    #$emails = $cfg->wms_vendor->{emails};

    $sm->subject('SPS DLA Outbound XML '.$g_long_date);
    $sm->sendTo($emails);
    $sm->msg(@body);
    $sm->logObj($log_obj);
    $sm->verboseLevel($debug);
    $sm->hostName($host);
    $sm->attachments($log );
    
    
    $sm->send_mail_attachment();
} 

sub log_debug {
    my $log_entry = join( '', "(PID $$) ", @_ );
    if ($log_obj) { $log_obj->info($log_entry); }
    debug($log_entry);
}

sub log_warn {
    my $log_entry = join( '', "(PID $$) ", @_ );
    $sm->errorLevel('warning');
    send_mail( "WARNING on " . __FILE__ . ' ' . $g_long_date, $log_entry );
    if ($log_obj) { $log_obj->warn($log_entry); }
    debug($log_entry);
}

sub fatal_error {
    $sm->errorLevel('error');
    my $log_entry = join( '', "(PID $$) ", @_ );
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $log_entry );
    if ($log_obj) { $log_obj->error($log_entry); }
    die $log_entry;
}

sub debug {
    if ($debug) {
        print "DEBUG: ", @_, "\n";
    }
}

#---------------------------------------------------------------------
# MAIN PROGRAM - Do not modify main below.
# Use the main above aka my_main
#---------------------------------------------------------------------

# Want to catch warning
$SIG{__WARN__} = sub { log_warn("@_") };

# Execute the main
eval { mainLine() };
if ($@) {
    fatal_error($@);
}

#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------


__END__

=pod

=head1 NAME


=head1 SYNOPSIS


=head1 DETAILS

=head1 DESCRIPTION


=head1 DEPENDENCIES

=over 4

=back

=head1 AUTHOR


=cut
