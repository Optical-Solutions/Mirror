#!/usr/local/mccs/perl/bin/perl
#---------------------------------------------------------------------
# Program:  sendprcif
# Author:   Hanny Januarius
# Created:  Tue Mar 13 09:46:23 EDT 2012
# Description:
#   A complete rewrite the old one which called sendprcif.pl
#   This program is set to run 3 times a day
#   1. Get file from HRMSPROD.usmc-mccs.org
#   2. If the file is newer, get it
#   3. Encrypt it using PGP (No longer needed, we are using SFTP)
#   4. Send to Boa.
#   5. Send Email Notification. (maybe not)
#---------------------------------------------------------------------
use strict;
use warnings;
use IBIS::DBI;
use IBIS::Log::File;
use File::Basename;
use File::Path;
use File::Copy;
use Readonly;
use MCCS::Config;
use Data::Dumper;
use Fcntl qw(:flock);
use Getopt::Std;
use IBIS::SSH2CONNECT;
use Net::SFTP::Foreign;
use IO::File;
#use MCCS::Banking_SFTP;
use MCCS::Db_ibisora;
use MCCS::Utils qw(is_rdiusr);
use MCCS::File::Util qw(move_to_archive);
use Sys::Hostname;
use Carp;
use POSIX qw(strftime);

# Flush output
local $| = 1;

# Switches
$Getopt::Std::STANDARD_HELP_VERSION = 1;
# JOSE REVIEW our vs my IN this context
my %opts = ( d => 0 );
getopts( "d", \%opts );

#- Make sure you are rdiusr ------------------------------------------
unless ( is_rdiusr() ) {
    print "Must run $0 as rdiusr\n";
    exit 1; # Exit for error
}

#- One process at a time ---------------------------------------------
#TODO Fix this next 4 lines.
# my $lock_file = "/usr/local/mccs/tmp/" . basename($0) . ".lck";
# open my $self_fh, ">", $lock_file or croak "Could not create lock file $lock_file";
# flock $self_fh, LOCK_EX | LOCK_NB or croak "Another $0 process already running";
# close $self_fh;


#- Configuration files -----------------------------------------------
my $g_cfg = MCCS::Config->new;
my $g_emails       = $g_cfg->prcif->{notify_sftp}; #FIXME
#my $g_emails       = $g_cfg->prcif->{notify};
my $g_mtime_file   = $g_cfg->prcif->{mtime_file};
my $g_verbose      = $opts{d};
my $g_download_dir = $g_cfg->prcif->{download_dir};
my $g_remote_host  = $g_cfg->prcif->{datasrc};  #TODO convert to using secrets
my $g_remote_dir   = $g_cfg->prcif->{srcdir};
my $g_encrypt_dir  = $g_cfg->prcif->{enc_dir};
my $g_archive_dir  = '/usr/local/mccs/data/banking_sftp/PRcif/archive';
my $g_log_dir      = '/usr/local/mccs/log/';
my $g_txfile = $g_cfg->prcif->{txfile};


#Access Secrets

my $name= 'MVMS-Middleware-PRCIF-SFTP';
my $g_secret = $util->get_secret($name);  
my $g_remote_host  = $g_secret->{host};   

my $ts = POSIX::strftime("%y%m%d%H%M%S", localtime());
chomp($ts);
$g_txfile = $g_txfile . "_$ts";

my $g_max_mtime;
my $g_remote_file_mtime;
my $g_cif;
sub create_missing_directotry {
    my $dirname = shift;
    unless ( -d $dirname ) {
        mkpath($dirname);
    }   
}

create_missing_directotry($g_archive_dir );
create_missing_directotry($g_download_dir );
create_missing_directotry($g_encrypt_dir);
create_missing_directotry($log_dir);

if ($g_verbose) {
    print Dumper $g_emails;
    print Dumper $g_mtime_file;
    print Dumper $g_download_dir;
}

#- Global variables --------------------------------------------------
#TODO remove next line.
# Readonly my $g_logfile => '/usr/local/mccs/log/' . basename(__FILE__) . '.log';
  Readonly my $g_logfile => $g_log_dir . basename(__FILE__) . '.log';
my $g_long_date = do {
    my @t = localtime;
    sprintf("%02d/%02d/%02d %02d:%02d:%02d %s", 
        $t[4] + 1, $t[3], $t[5] % 100,
        $t[2], $t[1], $t[0],
        $t[2] >= 12 ? "PM" : "AM");
};
my $g_log = IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );
my $g_host = hostname;

#---------------------------------------------------------------------
# SUBS
#---------------------------------------------------------------------
# REVIEW send_mail
# sub send_mail {
#     my ($msg_sub, $msg_bod1, $msg_bod2) = @_;
#     $msg_bod2 //= '';
    
#     return if $g_verbose;    # Don't want to send email if on verbose mode

#     foreach my $name (sort keys %{$g_emails}) {
#         $g_log->info("Sent email to $name (" . $g_emails->{$name} . ")");
#         $g_log->info("  Sbj: $msg_sub ");
#         $g_log->debug("  $msg_bod1 ");
#         $g_log->debug("  $msg_bod2 ");

#         # Prepare email content first
#         my $email_content = "To: " . $g_emails->{$name} . "\n" .
#                            "From: rdistaff\@usmc-mccs.org\n" .
#                            "Subject: $msg_sub\n" .
#                            "\n" .
#                            $msg_bod1 .
#                            $msg_bod2 .
#                            "\n" .
#                            "Server: $g_host\n" .
#                            "\n\n";

#         # Brief open-write-close sequence
#         open(my $mail_fh, '|-', '/usr/sbin/sendmail -t') 
#             or do {
#                 $g_log->error("Failed to open sendmail pipe: $!");
#                 next;
#             };
#         print $mail_fh $email_content
#             or $g_log->warn("Failed writing to mail pipe: $!");
#         close($mail_fh) 
#             or $g_log->warn("Failed to close mail pipe: $!");
#     }
    
#     return 1;
# }

#---------------------------------------------------------------------
sub fatal_error {
    my $msg = shift;
    # send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
    $g_log->error($msg);
    croak $msg; 
}

#---------------------------------------------------------------------
# JOSE REVIEW
sub get_remote_file_mtime {
    
    my $sftp = Net::SFTP::Foreign->new( $g_remote_host, user => 'rdiusr', password => '2UC9ze'); 
    $sftp->die_on_error("Unable to establish SFTP connection to $g_remote_host user rdiusr ");

    my $list_ref = $sftp->ls($g_remote_dir);

    # print Dumper $list_ref;
    my $h;
    foreach my $e ( @{$list_ref} ) {
        if ( $e->{filename} =~ m/MCCSPRCIF$/x ) {
            $g_log->info( "Found file " . $e->{filename} . " mtime = " . $e->{a}->{mtime} . "\n" );
            $g_remote_file_mtime->{ $e->{filename} } = $e->{a}->{mtime};
            $h->{MCCSPRCIF} = $e->{a}->{mtime};
        }
    }
    $sftp->disconnect;

    if ($g_verbose) {
        print Dumper $g_remote_file_mtime;
    }
    
    return $h;  # Added explicit return statement
}
#---------------------------------------------------------------------
sub get_local_max_mtime {
    # Gonna put it in DB later !!!
    my $f = $g_mtime_file;
    if ( -e $f ) {
        open(my $fh, '<', $f) or fatal_error("Could not read $f, $!");
        my $n = <$fh>;
        chomp($n);
        close $fh;
        $g_log->info("Max local  mtime: $n epoch seconds");
        return $n;
    }
    else {
        $g_log->info("File $f init, so");
        $g_log->info("Max local  mtime: 0 epoch seconds");
        return 0;
    }
}

#---------------------------------------------------------------------
sub get_remote_max_mtime {
    my $max = 1;
    foreach my $e ( keys %{$g_remote_file_mtime} ) {
        if ( $g_remote_file_mtime->{$e} > $max ) {
            $max = $g_remote_file_mtime->{$e};
        }
    }
    $g_log->info("Max remote mtime: $max epoch seconds");
    return $max;
}

#---------------------------------------------------------------------
sub download_file {
    my $f = "";
    #my $object = IBIS::SSH2CONNECT->new( IBISlog_object => $g_log );
    #my $ssh2 = $object->connect( user => 'rdiusr', host => $g_remote_host );


    chdir($g_download_dir) or fatal_error("Could not cd to $g_download_dir, $!");

    #TODO REMOVE NEXT LINE AND ADD 6 lines after that.
    #my $sftp = Net::SFTP::Foreign->new( $g_remote_host, user => 'rdiusr', password => '2UC9ze');
    my $user = $g_secret->{'username'} || $g_secret->{'user'};
    my $sftp = Net::SFTP::Foreign->new(
                $g_remote_host,
                user     => $user,
                password => $g_secret->{'password'} || $g_secret->{'pw'},
                port     => $g_secret->{'port'} || '22'
                );   
    $sftp->die_on_error("Unable to establish SFTP connection to $g_remote_host for user $user");

    $g_log->info("Download New/update files:");
    $sftp->get("$g_remote_dir/MCCSPRCIF", $g_txfile ) 
      or fatal_error("Could not sftp get $$g_remote_dir/MCCSPRCIF from $g_remote_host because " . $sftp->error());

    $g_log->info("    MCCSPRCIF OK");

    if ( -e "$g_download_dir/$g_txfile" ) {
        $f = "$g_download_dir/$g_txfile";
        $sftp->disconnect;
        return $f;
    }
    else {
        fatal_error("Downloading MCCSPRCIF file from HR server to $g_download_dir/$g_txfile  has FAILED");
        return; 
    }
}

#---------------------------------------------------------------------
sub set_local_max_mtime {
    my $f = $g_mtime_file;
    open(my $out_fh, '>', $f) or fatal_error("Could not write to $f: $!");
    print $out_fh $g_max_mtime;
    close $out_fh;
    return 1;
}

#---------------------------------------------------------------------
# Get File
#---------------------------------------------------------------------
sub get_file {
    my $file = "";
    get_remote_file_mtime();
    my $local_mtime  = get_local_max_mtime();
    my $remote_mtime = get_remote_max_mtime();


    if ( $local_mtime < $remote_mtime ) {
     
        $g_log->info("\tMCCSPRCIF is newer");

        $file        = download_file();
        $g_max_mtime = $remote_mtime;
        return $file;

    }
    else {

        # File is not new/updated, so returns blank.
        return $file;
    }

}

#---------------------------------------------------------------------
# THIS BIT IS NOT USED
#---------------------------------------------------------------------
# sub encrypt_file {
#     my $file = shift;
#     my $enc_file;
#     $g_log->info("Encrypt it using PGP");

#     if ( -e $file ) {
#         my $workname = $file;
#         my $basename = basename($workname);
#         $enc_file = "$g_encrypt_dir/$basename.pgp";

#         my $pgp = Crypt::OpenPGP->new(
#             Compat  => "PGP5",
#             PubRing => '/usr/local/mccs/etc/openpgp/pubring.gpg',
#             SecRing => '/usr/local/mccs/etc/openpgp/secring.gpg'
#         ) or fatal_error("Could not obtain new OpenPGP object");

#         my $pgpresults = $pgp->encrypt(
#             Armour         => 1,
#             Filename       => $workname,
#             Recipients     => $g_cfg->banking->{BOAKEYID},
#             SignKeyID      => $g_cfg->prcif->{MCCSKEY},
#             SignPassphrase => $g_cfg->prcif->{MCCSPPHRASE}
#         ) or fatal_error( "Could not encrypt: " . $pgp->errstr );

#         my $outfh = IO::File->new();
#         $g_log->info("  Printing pgp output ");
#         $g_log->info("  $enc_file");

#         $outfh->open("> $enc_file") or fatal_error("Can not open pgp output $enc_file: $!");
#         print $outfh $pgpresults;
#         $outfh->close;

#     }
#     else {
#         fatal_error("$file is missing, could not encrypt it");
#     }

#     return $enc_file;
# }

#---------------------------------------------------------------------

sub send_file {
    my $file = shift;

    $g_log->info("Sending $file to BOA");

    my $session = BankSession->new();
    $session->{bksess_operation} = 'sendprcif';
    $session->write_bank_sessions();

    my $bft = PRCIF_Transfer->new($session);
    $bft->bkft_filename($g_txfile);

    $g_log->info( "File transfer id = " . $bft->bkft_id );

    my $cfgapp    = $bft->_cfg_block;
    my $flat_file = $g_download_dir . "/" . $bft->bkft_filename;
    $bft->{workname} = $flat_file;
    
    # Open file handle and ensure it gets closed
    my @lines;
    {
        open(my $wf, "<", $flat_file) or fatal_error("Could not read $flat_file: " . $!);
        @lines = <$wf>;
        close($wf) or fatal_error("Could not close $flat_file: " . $!);
    }
    
    my $trec = $lines[ scalar(@lines) - 1 ];
    $bft->bkft_transcnt( 0 + substr( $trec, 13, 10 ) );

    if ( $g_cfg->banking->{NOBOATX} ) {
        $g_log->info("No BOA FTP");
        $bft->bkft_result("Bypassed");
        $bft->bkft_method("NIL");

        # send_mail("Banking send Payroll CIF file", 
        #           "Bypassed\n\n" .
        #           $g_cfg->banking->{SFTPUSER_BOA} . " " .
        #           $g_cfg->banking->{SFTPPW_BOA} . " " .
        #           $g_cfg->$cfgapp->{TXBATCHID} );
    }
    else {
        $g_log->info("SEND BOA FTP");
        $bft->send_boa_SFTP( $g_cfg->$cfgapp->{TXBATCHID}, "/incoming/arp" );
        $bft->bkft_result("Transmitted");
        # send_mail("Banking send Payroll CIF file", "Transmitted");
    }
    
    return $bft->write_bank_filetransfers;
}

#---------------------------------------------------------------------
sub write_bank_ciftracking {
    my $val = shift;

    my $rowcnt = 0;
    # my $dbh = MCCS::Db_ibisora::connect( dbname => "ibisora" );
    my $dbh = IBIS::DBI->connect( dbname =>'MVMS-Middleware-RdiUser');
    my $name= 'MVMS-Middleware-EmpowerIT-SFTP';
    my $secret = $util->get_secret($name);  #my $secret = $util->get_secret($name);
    my $sql = <<"ESQL";
select count(*) from bank_ciftracking
where
    bcif_filename = ?
ESQL

    my $sql_ins = <<"ESQLINS";
insert into bank_ciftracking( bcif_filename, bcif_batched, bcif_type)
values(?,?,?)
ESQLINS

    my $sql_upd = <<"ESQLUPD";
update bank_ciftracking
    set bcif_batched = ?,
    bcif_type = ?
where
    bcif_filename = ?    
ESQLUPD
    my $sth = $dbh->prepare($sql);
    $sth->execute( $val->{bcif_filename} );
    my $n = ( $sth->fetchrow_array )[0];

    my $result = eval {
        if ($n)
        {
            $g_log->info("UPDATE BANK_CIFTRACKING table");
            $rowcnt =
              $dbh->do( $sql_upd, undef, $val->{bcif_batched}, $val->{bcif_type}, $val->{bcif_filename} );
        }
        else {
            $g_log->info("INSERT INTO BANK_CIFTRACKING table");
            $rowcnt =
              $dbh->do( $sql_ins, undef, $val->{bcif_filename}, $val->{bcif_batched}, $val->{bcif_type} );
        }
        1;  # Indicate successful completion
    };
    if (!$result || $@) {
        fatal_error( "Could not write BANK_CIFTRACKING table \n" . "$@ " );
    }
    $g_log->info("rowcnt = $rowcnt");

    return 1;
}

#---------------------------------------------------------------------
# JOSE REVIEW
sub cif_status {
    my $status = shift;
    $g_cif = {
        bcif_filename => 'MCCSPRCIF',
        bcif_batched  => $status,
        bcif_time     => undef,
        bcif_type     => ""
    };

    return write_bank_ciftracking($g_cif);
}

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

    $g_log->info("host $g_remote_host on dir $g_remote_dir");

    my $g_pr_file = get_file(); # COMMENT THIS IN

    #-------------------------------------------------
    # debug
    # short cut WIP
    #my $g_pr_file = $g_download_dir . "/" . $g_txfile;

    #-------------------------------------------------
    if (-e $g_pr_file) {
        $g_log->info("PR file is NEW");
        cif_status("");

        #-----------------------------------------------------
        # my $g_pr_file_encrypted = encrypt_file($g_pr_file);
        # No NEED TO ENCRYPT, we are on SFTP 
        #-----------------------------------------------------

        cif_status("Pending");

        #send_file($g_pr_file_encrypted);

        #FIXME       send_file($g_pr_file);  
        #FIXME cif_status("Batched");
        #FIXME set_local_max_mtime();

        # Archive it ---------------------------------------------------
        #TODO remove next line?
        # my $g_archive_dir   = '/usr/local/mccs/data/banking_sftp/PRcif/archive';
        #FIXME move_to_archive({file=>$g_pr_file, dir=>$g_archive_dir});
        #---------------------------------------------------------------
    }
    else {
        $g_log->info("PR file is not NEW");
        $g_log->info("Nothing to do");
        #send_mail("Banking send Payroll CIF file (void)", "No new/upd file detected");
    }

    $g_log->info("-- End ------------------------------------------");
    
    return 1;
}

#---------------------------------------------------------------------
# MAIN PROGRAM - Do not modify main below.
# Use the main above aka my_main
#---------------------------------------------------------------------

# Want to catch warning
local $SIG{__WARN__} = sub { $g_log->warn("@_") };

# Execute the main
my $eval_result = eval { my_main() };
if ($@) {
    # send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, "Untrapped Error:\n\n", " $@" );
    $g_log->info($@);
}
elsif (!defined $eval_result) {
    $g_log->error("my_main() returned undefined value");
}
#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------
