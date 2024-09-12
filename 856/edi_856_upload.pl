#!/usr/local/mccs/perl/bin/perl

use strict;
use warnings;
use IBIS::EDI;
use Data::Dumper;
use Net::SSH::Perl;
use Getopt::Std;
use POSIX qw(strftime WNOHANG);
use IBIS::Log::File;
use IBIS::DBI;
use IBIS::SFTP;
use MCCS::POS::TE;
use Sys::Hostname;
use Fcntl qw(:flock);
use Getopt::Long;
use IBIS::Crypt;

my ( $config, $debug, $edi, $help, $hostname, $ret_fetched, $only_download );

## preparations:
## Options

## Options
my $opt_result = GetOptions(
    "debug"         => \$debug,          # flag
    "help"          => \$help,
    "only_download" => \$only_download
);                                       # flag

$hostname = hostname;
$config   = '/usr/local/mccs/etc/edi_856/edi_856_to_spscommerce.config';

if ($help) {
    my $use_msg = qq(
     "\n General Usage:\n\t  perldoc $0 for more information.\n\n");
    print $use_msg;
    exit();
}

## config file, object, db connections
$edi = IBIS::EDI->new( conf_file => $config );

## log file
my $now = strftime( '%Y_%m_%d_%H_%M', localtime );
my $log_filename = $edi->{LOG_DIR} . "/" . "edi_856_log_" . $now;
$edi->{'log_obj'} = IBIS::Log::File->new(
    {
        file   => $log_filename,
        append => 1
    }
);

$edi->{'log_obj'}->log_info("\n\n*************\n\tStarted at:$now\n");
if ($debug) {
    print "Object Info:\n";
    print Dumper($edi);
}

## Fetch files from remote (sps) to local machine
my $rf = [];
my $sps_ftp_obj;
my $fail_connect = '';

eval { $sps_ftp_obj = $edi->get_sps_ftp_object(); };

if ($@) {
    $fail_connect = $@;
    $edi->{'log_obj'}->log_warn("Error on FTP: $fail_connect");
    exit();
}
else {
    $rf = &get_remote_file_list($sps_ftp_obj);
    my $num = @$rf;
    $edi->{'log_obj'}->log_info("FTP connected. Total files in remote:$num");
}

if ($debug) {
    print "Remote Dir: $edi->{'REMOTE_DIR'} \nRemote Files:\n";
    print Dumper($rf);
    print "list size: @$rf\n";
}

if ( @$rf > 0 ) {
    chdir( $edi->{'FTP_STAGING_DIR'} );
    ## fetch each file in the list, delete one that has been sucessfully retrieved...
    $ret_fetched = &fetchNdelete_files_in_list( $sps_ftp_obj, $rf );  # List of downloaded files.

    if ( ($ret_fetched) && ( @$ret_fetched > 0 ) ) { 

        my $f_sz = @$ret_fetched;
        $edi->{'log_obj'}
          ->log_info("Total retrieved files to local machine:$f_sz\n");

    }
    else {
        my $msg = "Troubles in fetching new 856 files from SPS.\n";
        if ($debug) {
            print "$msg\n";
        }
        $edi->{'log_obj'}->log_info($msg);
        &email_to_ccstr_pip_dlmt( $edi->{MAIL_CC},
            'EDI 856 File Transfer Result', $msg );
    }

}
elsif ( @$rf == 0 ) {
    my $msg = "No New file available at SPS to process.\n";
    $edi->{'log_obj'}->log_info($msg);
    if ($debug) {
        print "$msg\n";
    }
    my $subject =
      $fail_connect ? 'FAILED to connect to SPS' : 'No New 856 Files at SPS';
    &email_to_ccstr_pip_dlmt( $edi->{MAIL_CC}, $subject, $msg );

}
exit(); #TODO remove this line
## upload to RMS data server from local machine staging directory, no matter when the file fetchedbakup_local_files

if ( !$only_download ) {

    my $staging_files = &get_local_file_list();
    print Dumper($staging_files) if ($debug);

    my $file_loaded = &put_files_to_rms_serverNbackup2($staging_files) || [];

    if ($debug) {
        print "Following files have been uploaded:\n";
        print Dumper($file_loaded);
    }
    my $u_sz = 0;
    if ($file_loaded) {
        $u_sz = @{$file_loaded};
    }
    $edi->{'log_obj'}->log_info("Total files uploaded to RMS:$u_sz");
    my $msg = '';
    my $content;

    if ( ($file_loaded) && ( @$file_loaded > 0 ) ) {
 # LDL 20151104        
        my $cp_error = &normalize_stg_files($staging_files);
        if ( ($cp_error) && ( @$cp_error > 0 ) ) {
            my $msg = Dumper($cp_error);
            $edi->{'log_obj'}->log_info($msg);
        }
        else {
            my $msg = "Copy to normalize file success\n";
            $edi->{'log_obj'}->log_info($msg);
        }
# LDL END        
    	
        my $bk_error = &bakup_local_files($staging_files);
        if ( ($bk_error) && ( @$bk_error > 0 ) ) {
            my $msg = Dumper($bk_error);
            $edi->{'log_obj'}->log_info($msg);
        }
        else {
            my $msg = "back up local file success\n";
            $edi->{'log_obj'}->log_info($msg);
        }


        my $c_time = strftime( '%Y/%m/%d %H:%M', localtime );
        $content .= $msg
          . "\nEDI-856 Files Transferred to RMS ($c_time) \n=============================================\n";

        for ( my $i = 0 ; $i < @$file_loaded ; $i++ ) {
            if ( ( $i + 1 ) % 2 == 1 ) {
                $content .= $file_loaded->[$i] . "\t";
            }
            else {
                $content .= $file_loaded->[$i] . "\n";
            }
        }

        $content .=
"\nTotal files uploaded to RMS:$u_sz\n=============================================\n";
## email poc from the list of file_loaded
        &email_to_ccstr_pip_dlmt( $edi->{MAIL_CC},
            'EDI 856 File Transfer Result', $content );

    }
    else {    ## if nothing loaded to RMS...
        if ( ($ret_fetched) && ( @$ret_fetched > 0 ) ) {
            $msg = "Troubles to upload files to RMS from local server.\n";
            $edi->{'log_obj'}->log_info($msg);
            $content .= $msg;
            &email_to_ccstr_pip_dlmt( $edi->{MAIL_CC},
                'EDI 856 File Transfer Error', $content );
        }
    }
}

############### subroutines ##############
sub get_local_file_list {
    ##my ($edi) = @_;
    my @files = ();
    my $dirfh;
    opendir( $dirfh, $edi->{'FTP_STAGING_DIR'} );
    @files = readdir($dirfh);
    @files = grep /^SH(.*)\.7R3$/, @files;
    unless (@files) {
        my $msg = "no files in $edi->{'FTP_STAGING_DIR'}";
        $edi->{'log_obj'}->log_info($msg);
    }
    closedir($dirfh);
    return \@files;
}

sub get_remote_file_list {
    my ($ftp_obj) = @_;
    $edi->{'log_obj'}->log_info("SPS Remote dir: ". $edi->{REMOTE_DIR} ) ;
    my $o_ref = $ftp_obj->ls( $edi->{REMOTE_DIR} );
    my @list = map { $_->{filename} } @{$o_ref};

    #my @files = $ftp_obj->ls( $edi->{REMOTE_DIR} );
    my @files = grep( /^SH(.*)\.7R3/, @list);
    return \@files;
}

sub fetchNdelete_files_in_list {
    my ( $sps_ftp_obj, $new_file_list ) = @_;
    my $downloaded_file_list;
    my $new_file_count = @$new_file_list;
    my $max_recon_ctr =
      5;    ## max number of tries if one file can not be transferred..
    my $rtf_ctr      = 0;
    my $reconftp_ctr = 0;
    if (@$new_file_list) {
        $edi->{'log_obj'}->log_info("Downloading Files ......");
        foreach my $fn (@$new_file_list) {
            my $rmt_fn = $edi->{REMOTE_DIR} . '/' . $fn;

          TRANSMIT_BLOCK: {
                $edi->{'log_obj'}->log_info("Getting $rmt_fn" ) ;
                my $ret_fn = $sps_ftp_obj->get($rmt_fn) 
                             or die ("Could not sftp get $rmt_fn because " . $sps_ftp_obj->error() );
                if ($ret_fn) {
                    push( @$downloaded_file_list, $ret_fn );
                    $rtf_ctr++;
                    $edi->{'log_obj'}->log_info("Deleting $rmt_fn" ) ;
                    my $dlt = $sps_ftp_obj->remove($rmt_fn)
                             or die ("Could not sftp delete $rmt_fn because " . $sps_ftp_obj->error() );
                    if ( !$dlt ) {
                        $edi->{'log_obj'}->log_info(
"Failed to delete shipment file: $rmt_fn  Downloaded number of files:$rtf_ctr "
                        );
                    }
                }
                else {
                    $edi->{'log_obj'}->log_info(
"failed in getting file: $rmt_fn. will reconnect for ftp..."
                    );
                    if ( $reconftp_ctr < $max_recon_ctr ) {
                        $sps_ftp_obj->close()
                          ;  ## this may be useful to avoid multiple ftp objects

                        ## The change is for avoiding receiveing FTP connection failure message from email.

                        eval {
                            $sps_ftp_obj = $edi->get_sps_ftp_object();
                            $reconftp_ctr++;
                        };

                        unless ($@) {
                            $edi->{'log_obj'}->log_info(
"ftp object re-connection happened. Reconnection ctr: $reconftp_ctr  Downloaded number of files:$rtf_ctr  \n"
                            ) if ($sps_ftp_obj);

                            redo TRANSMIT_BLOCK;
                        }
                        else {
                            $edi->{'log_obj'}->log_info(
"FTP re-connection failed. Reconnection ctr: $reconftp_ctr Downloaded number of files:$rtf_ctr \n"
                            ) if ($sps_ftp_obj);
                            ##exit();
                        }

                    }
                }
            } ## end of block
        }
    }

    my $msg;
    if ( $rtf_ctr == $new_file_count ) {
        $msg =
"SUCCESS in fetching all new files from SPS in new file list. Downloaded number of files:$rtf_ctr ";
        $edi->{'log_obj'}->log_info($msg);
    }
    else {
        $msg =
"INCOMPLETE in fetching new files from SPS. Total downloaded files: $rtf_ctr";
        $edi->{'log_obj'}->log_info($msg);
    }

    return $downloaded_file_list;
}

## 856 function,
## from local 856, upload to fms 3 files. two are the orignal, one is a blank with a special name by 'touch' command

sub put_files_to_rms_serverNbackup {
    my ($file_list) = @_;
    my @files;
    foreach my $lf (@$file_list) {
        my $local_file  = $edi->{FTP_STAGING_DIR} . '/' . $lf;
        my $local_touch = $edi->{FTP_STAGING_DIR} . '/' . $lf . ".TRG";

        if ( -e $local_file ) {
            my $touch_cmd = "touch $local_touch";
            system($touch_cmd);
            my $chmd = "chmod 775 $local_touch";
            system($chmd);
        }

        push( @files, $local_file );
        push( @files, $local_touch );
    }
    ##print Dumper(@files);

    my @ftp_result = IBIS::SFTP->new(
        {
            destination => 'test_sftp_package',
            files       => \@files,
            remote_dir  => '/rmdata/ftp/sps_edi_856'
        }
    )->put();
    return \@ftp_result;
}

sub put_files_to_rms_serverNbackup2 {
    my ($file_list) = @_;
    my @files;
    foreach my $lf (@$file_list) {
        my $local_file  = $edi->{FTP_STAGING_DIR} . '/' . $lf;
        my $local_touch = $edi->{FTP_STAGING_DIR} . '/' . $lf . ".TRG";
        unless ( $local_file =~ /\.TRG$/g ) {
            my $touch_cmd = "touch $local_touch";
            system($touch_cmd);
            my $chmd = "chmod 775 $local_touch";
            system($chmd);
        }

        push( @files, $local_file );
        push( @files, $local_touch );
    }
    ##print Dumper(@files);
    my $rms_server      = $edi->{RMS_SERVER};
    my $rms_staging_dir = $edi->{RMS_STAGING_DIR};
    my $rms_ftp_user    = $edi->{RMS_SFTP_USER};

    my $scp =
        "scp -r $edi->{FTP_STAGING_DIR}" . '/' . '   '
      . $rms_ftp_user . '@'
      . $rms_server . ':'
      . $rms_staging_dir . '/';
    my $s_ret = system($scp);
    if ( $s_ret == 0 ) {
	$edi->{'log_obj'}->log_info("scp system return:".$s_ret);
        return \@files;
    }
    else {
        return undef;
    }

}

sub bakup_local_files {
    my ($file_list) = @_;
    my @files;
    my $errors;
    foreach my $lf (@$file_list) {
        my $local_file  = $edi->{FTP_STAGING_DIR} . '/' . $lf;
        my $local_touch = $edi->{FTP_STAGING_DIR} . '/' . $lf . ".TRG";
        if ( ( -e $local_file ) && ( -e $local_touch ) ) {
            my $bk_local_file = $edi->{FTP_STAGING_DIR_BKUP} . '/' . $lf;
            my $cmd1          = "mv $local_file $bk_local_file";
            my $cmd2          = "rm $local_touch";
            my $ret1          = system($cmd1);
            my $ret2          = system($cmd2);
            unless ( $ret1 == 0 ) {
                push( @$errors, $local_file );
            }
        }
    }
    return $errors;
}
# LDL 20151104
sub normalize_stg_files {
    my ($file_list) = @_;
    my @files;
    my $errors;
    foreach my $lf (@$file_list) {
        my $local_file  = $edi->{FTP_STAGING_DIR} . '/' . $lf;
        my $local_touch = $edi->{FTP_STAGING_DIR} . '/' . $lf . ".TRG";
        if ( ( -e $local_file ) && ( -e $local_touch ) ) {
            my $cp_dir = $edi->{FTP_NORMALIZE_DIR} . '/' ;
            my $cmd1          = "cp $local_file $cp_dir";
            my $cmd2          = "cp $local_touch $cp_dir";
            my $ret1          = system($cmd1);
            my $ret2          = system($cmd2);
            unless ( $ret1 == 0 ) {
                push( @$errors, $cmd1 );
            }
	    unless ( $ret2 == 0 ) {
                push( @$errors, $cmd2 );
            }
        }
    }
    return $errors;
}
# LDL END

sub email_to_ccstr_pip_dlmt {
    my ( $cc_str, $subject, $body ) = @_;
    $cc_str = 'kaveh.sari@usmc-mccs.org|';  #TODO remove this line.
    my @cc_list = split( /\|/, $cc_str );
    foreach my $to_str (@cc_list) {
        if ($to_str) {
            my $hdrstr = '';
            $hdrstr .= "To: $to_str\n";
            $hdrstr .= "From: rdistaff\@usmc-mccs.org\n";
            $hdrstr .= "Subject: $subject\n";
            my $hostname = hostname;
            $body .= "\n\temail from host: $hostname\n";
            open MAIL, "|/usr/sbin/sendmail -t"
              or die "Can not open sendmail\n";
            print MAIL $hdrstr . "\n";
            print MAIL $body . "\n";
            close MAIL;
            $edi->{'log_obj'}->log_info("email to: $to_str\n");
        }
    }
    return 1;
}

sub put_single_file {
    my ( $ftp, $local_file, $remote_file ) = @_;
    my $success_flag = 0;
    my $reput_ctr    = 0;
    my $max_try      = 5;
  PUT_BLOCK: {
        my $put_success = $ftp->put( $local_file, $remote_file )
          or die "put failed ", $ftp->message;
        if ( !$put_success ) {
            $edi->{'log_obj'}->log_info(
"ftp-put failed once. retry times: $reput_ctr, file: $local_file\n"
            );
            if ( $reput_ctr < $max_try ) {
                $reput_ctr++;
                redo PUT_BLOCK;
            }
        }
        else {
            $success_flag = 1;
        }
    }
    return $success_flag;
}

=pod

=head1 NAME

edi_856_upload.pl
EDI 856, also called Advance Shipping Notice, is an electronic version of a printed package slip that tells a buyer how a supplier has packed their items for shipment. The ASN also tells the buyer that the goods have been shipped so they can be expecting the shipment. 
The program will transfer edi 856 files from SPS, to MCCS database server for uploading. 


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

./edi_856_upload.pl 

=head1 DESCRIPTION

This program will check the sps data server, list new edi 856 files.
fetch the new files, and delete files after successful fetching them from the sps data server. 
The program will then transfer the files to RMS databaser server. From where, the data will be picked up and load into the database by another process.

Log messages goes to /usr/local/mccs/log/ftp/edi_856/ with a time stamp on file name. 

=head1 REQUIREMENTS

None.

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION

=over 

/usr/local/mccs/etc/edi_856/
 
=back
                                                                                        
=head1 DEPENDENCIES

=over 4

=item * IBIS::EDI for transaction data extraction and evaluation

=item * FileHandle for passing fh as parameters.

=item * Getopt::Std for options

=item * POSIX qw(strftime WNOHANG) for getting time string

=item * IBIS::Log::File for using log
 
=item * Net::SFTP for ftp flat files to SPS

=item * Fcntl qw(:flock) for setting only single process

=back

=head1 SEE ALSO 

RMS 856 Interface Requirements, by Mike Gonzalaz

=head1 INCOMPATIBILITIES

Unknown.

=head1 BUGS AND LIMITATIONS

Limitations:

No number count, or PO record, to show what PO has been sent. 

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

