#!/usr/local/mccs/perl/bin/perl
#  AUTHOR:  Chunhui YU, <yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.org>
#  CREATED: Feb 19, 2009
#  Modified to use Net::SFTP::Foreign package, June 2015  Chunhui Yu

use strict;
use warnings;
use version; our $VERSION = qv('0.0.1');
use IBIS::EDI;
use Data::Dumper;
use FileHandle;
use Getopt::Std;
use POSIX qw(strftime WNOHANG);
use IBIS::Log::File;
use IBIS::DBI;
use MCCS::POS::TE;
use Sys::Hostname;
use Net::SFTP;
use Fcntl qw(:flock);
use IBIS::EDI_XML;

use constant MAX_CONNECTION_RETRY => 10;

getopts('tdhl:r:');

our ( $opt_d, $opt_h, $opt_l, $opt_r );

## Ensure only a single process is running
open( SELF, "<", $0 ) or die "Cannot open $0 - $!";
flock( SELF, LOCK_EX | LOCK_NB ) or die "Already running.";

## Global variables
my ($edi,            $debug,           $config,
    $local_ftp_file, $remote_ftp_file, $remote_retrive_dir,
    $staging_dir,    $archive_dir,     $err_msg
);

$config = '/usr/local/mccs/etc/edi_850/edi_850_to_spscommerce.config';

if ($opt_d) {
    $debug = 1;
}
else {
    $debug = 0;
}

if ($opt_h) {
    my $use_msg = qq(
         "\n\t ./this_script  for production (on panhead) 
          \n\t  Or\n\t ./this_script -t(test) for test (on knucklehead). 
          \n\t -l local_ftp_file  -r remote_ftp_file"
                  );
    print $use_msg;
    exit();
}

## Object
$edi = IBIS::EDI->new( conf_file => $config );

## Log:
my $t_suffix = strftime( '%Y_%m_%d_%H_%M', localtime );
my $log_filename = $edi->{INVO_LOG_DIR} . "/" . "invoice_fetch_log_" . $t_suffix;
my $log_obj = IBIS::Log::File->new(
    {
        file   => $log_filename,
        append => 1
    }
);
$log_obj->info("start time: $t_suffix\n");

######################## Main  ##############################

## Get files by ftp,
$remote_retrive_dir = $edi->{PROD_INVO_XRETRIVE_DIR};
$staging_dir = $edi->{INVO_XSTAGING_DIR};
$archive_dir = $edi->{INVO_XARCHIVE_DIR};

## 1: FTP retrive files into the staging directory: NOTE, set NON-deleting mode after retriving files
if ( ($remote_retrive_dir) && ($staging_dir) ) {
    &get_ftp_files_in_dir( $remote_retrive_dir, $staging_dir, 0 );
}
else {
    $err_msg .= "Remote Retrive directory or Staging directory value are missing";
    $log_obj->info($err_msg);
}

if ($debug) {
    print $staging_dir;
    print $archive_dir;
    print Dumper($edi);
}

## 2: Process and Archive xml files
## a, Processing xml file into irdb database
## b, Move files as archives if processing sucessful

opendir( DIR, $staging_dir ) or die "can  not opedir $staging_dir: $!";
my $file;
while ( defined( $file = readdir(DIR) ) ) {
    if ( ( $file !~ /^\./ ) && ( $file =~ /^IN/g ) ) {
        print "file:$file\n" if ($debug);
        my $good_flag        = 0;
        my $filename         = $staging_dir . "/" . $file;
        my $reposit_filename = $archive_dir . "/" . $file;
        my $result;
        #TODO Added -d to call to XML below for debugging.
        my $cmd =
            "perl /usr/local/mccs/lib/perl5/IBIS/EDI_TWM/bin/load_one_invoice_xml.pl -d -f $filename";
         eval { system($cmd); };
    }
}
close DIR;

my $end_time = strftime( '%Y_%m_%d_%H_%M', localtime );
$log_obj->info($end_time);

################SUBROUTINES###########################

sub get_ftp_files_in_dir {
    my ( $remote_dir, $local_dir, $delete_true ) = @_;

    my $msg = '';
    my $connection_retry = 0;
    my $ftp = ftp_connection($remote_dir);
    print "ftp connection established\n" if ($debug);
    my $ary_ref = $ftp->ls;
    print "starting loop over ftpd files\n" if ($debug);
    foreach my $file_hash (@$ary_ref) {
	my $file = $file_hash->{filename};
        if ( $file =~ /^IN/g ) {
            my $local_file = $local_dir . "/" . $file;
            my $g_flag     = 0;

            ## retrieve the file:
        GETBLOCK: {
                my $ret = $ftp->get( $file, $local_file );
                print "Getting file: $file to $local_file\n" if ($debug);
                if ( !$ret ) {

# ERS 8/6/2009
# Failed to get the file... we probably got disconnected. Let's retry the connection and retry the get. For a maximum of MAX_CONNECTION_RETRY times.

                    if ( ++$connection_retry <= MAX_CONNECTION_RETRY )
                    { #don't keep retrying forever (endless loop)... up to max times per execution of this program
                        $ftp = ftp_connection($remote_dir);
                        redo GETBLOCK;
                    }

                    my $error =
                        "Failed to retreive file: $file Error: " . $ftp->error(). "\n";
                    $msg .= $error;
                    warn $error;
                }
                else {
                    $msg .= "File fetched: $file     $ret\n";
                    $g_flag = 1;
                }
            }    #END GETBLOCK

            ## delete the file from remote server, if ftp has worked:
            if ( $g_flag == 1 ) {
                if ($delete_true) {
                    ## my $ret = $ftp->delete($file);
		    my $ret = $ftp->remove($file);
                    if ( !$ret ) {
                        $msg .= "Failed to delete remote file: $file.\n";
                        warn "Failed to delete remote file: $file.\n";
                    }
                    else {
                        $msg .= "Remote file: $file  has been deleted.\n";
                    }
                }
            }
	    $log_obj->info($msg);
            print "ftp result: $msg\n" if ($debug);
        }
    }
    ## $ftp->close(); ## this function means different things in the Foreign package yuc
    undef $ftp; ## the Foreign package ends its object yuc
}

# Perform the FTP connection
=head
sub ftp_connection {
    my $remote_dir = shift;

    # HJ disable FTP in DEV box

    if ( $edi->{FTP_DISABLE} ) {
       return;
    }

    my $c          = IBIS::Crypt->new();
    my $ftp        = Net::FTP->new( $edi->{REMOTE_SERVER} )
        or die "Cannot connect to $edi->{REMOTE_SERVER}: $@";
    $ftp->login( $edi->{SFTP_USER}, $c->decrypt( $edi->{PASSWORD} ) )
        or die "Cannot login ", $ftp->message;
    $ftp->binary;
    $ftp->cwd($remote_dir);
    $ftp;
}
=cut


# SFTP connection
sub ftp_connection {
    my $remote_dir = shift;
    print "ftp connection\n" if ($debug);
    ## get SFTP object, named ftp but actually SFTP from existing package
    my $sftp = $edi->get_sps_ftp_object();
    $sftp->setcwd($remote_dir) or die "unable to change cwd: " . $sftp->error;
    return $sftp;
}


=pod

=head1 NAME

retrive_invo_xml_from_sps_and_load_irdb.pl
 

=head1 VERSION

This documentation refers version 1.


=head1 REQUIRED ARGUMENTS

None.

=head1 OPTIONS

=over 6

=item -t

Testing mode using a test configuration file, containing a set of parameters including test database tables, views,ftp sever and log directories etc.

=item -d

Debug mode to print out more dumped data for details at some major steps in the code

=item -h

Displays a brief help message and exit.

=back

=head1 USAGE

./retrive_invo_xml_from_sps_and_load_irdb.pl 
or 
./retrive_invo_xml_from_sps_and_load_irdb.pl  -t 


=head1 DESCRIPTION


The script will fetch invoice xml file from sps, and load the irdb database tables. 
This program must be run as rdiusr.

Log messages goes to /usr/local/mccs/log/ftp/edi_810/ with a time stamp on file name. 

=head1 REQUIREMENTS

None.

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION

=over 4

=back
                                                                                        
=head1 DEPENDENCIES

=over 4

=item * EDI_TWM::Invoice for transaction data extraction and evaluation

=back

=head1 SEE ALSO 

MCCS wiki page about  AIMS project

=head1 INCOMPATIBILITIES

Unknown.

=head1 BUGS AND LIMITATIONS

no known bugs

=head1 AUTHOR

Chunhui Yu<yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>

=head1 ACKNOWLEDGEMENTS


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

