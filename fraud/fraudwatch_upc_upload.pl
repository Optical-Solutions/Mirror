#!/usr/local/mccs/perl/bin/perl
##--------------------------------------------------------------------------
##Ported by : Soumya K Bhowmic
##Date      : 12/08/2023
##
##Brief Desc: This program extracts the UPC codes and the associated
##            description for FraudWatch (TE-Triversity)
##
##            The program must run a rdiusr.
## --------------------------------------------------------------------------  
##Ported by Kaveh Sari 5/15/2024
#Chnged the program to accept a -n option which will prevent SCP.

use strict;
use warnings;

use IBIS::Log::File;
use IBIS::DBI;
use POSIX qw(strftime);
use MCCS::Utils;
use File::Basename;
##use Net::SCP;
use IBIS::SSH2CONNECT;

use Readonly;
use Getopt::Std;
use IBIS::Mail;
use English qw(-no_match_vars);
use version; our $VERSION = qv('1.0.0');

#TODO uncomment next line and delete line after.
#Readonly::Scalar my $MAIL_TO => 'rdistaff@usmc-mccs.org';
Readonly::Scalar my $MAIL_TO => 'kaveh.sari@usmc-mccs.org';

Readonly::Scalar my $MAIL_FROM => 'IBIS FraudWatch Processor';

##ERS 11/30/2009 New server and transfer for fraudwatch
#TODO uncomment next line and delete after that.
#Readonly my $FW_HOST  => 'hqw2k3svr17.windows.usmc-mccs.org';
Readonly my $FW_HOST  => 'hqlin056';
Readonly my $FW_DIR   => '.';
Readonly my $FW_USER  => 'rdiusr';
Readonly my $WORKDIR  => '/usr/local/mccs/data/fraudwatch/upload';
Readonly my $LOGFILE  => '/usr/local/mccs/log/fraudwatch/fraudwatch_upload.log';
Readonly my $DATEFILE => '/usr/local/mccs/data/fraudwatch/upload/last_upload.dat';

my $DEBUG = 0;
my $NOSEND = 0; #Added NOSEND option used to suppress SCP.
my %options;
my $dbh      = undef;
my $msg      = undef;
my $sth      = undef;
my $upc_file = undef;
my $lu_file  = undef;


#Added No SCP option.
getopts( 'dan', \%options );

if ( $options{d} ) {
    $DEBUG = 1;
}
#Added No SCP option.
if ( $options{n}) {
    $NOSEND = 1;
}
my $log = new IBIS::Log::File( { file => $LOGFILE, append => 1 } );
my $last_upload = '197001010';    #The epoch



if ( !$options{'a'} ) {    #options a means get all barcodes
    if ( open my $fh, '<', $DATEFILE ) {
        my $line = <$fh>;
        ($last_upload) = $line =~ /(\d{9})/msx;
        close $fh;
    } else {
        send_notify("Could not open date checkpoint file: $!");
    }
}
if ($DEBUG) {
    $log->debug('Fetching UPC data from Essentus system.');
    $log->debug("Using $last_upload as the last upload date.");
}
$dbh = IBIS::DBI->connect( dbname => 'rms_p' );

my $sql = q{ 
    select 
        distinct(b.bar_code_id),
        s.description, 
        b.date_created
    from 
        styles s,
        bar_codes b
    where 
        s.style_id = b.style_id 
        and b.business_unit_id = '30'
        and b.date_created > to_date(?, 'YYYYMMDDSSSSS')
};

eval { $sth = $dbh->prepare($sql); };
if ( $@ || !$sth ) {
    send_notify("SQL prepare failed: $@.");
}

if ($DEBUG) {
    $log->debug('Fetching UPC data from Essentus system.');
    $log->debug("Using $last_upload as the last upload date.");
}

my $ret;
eval { $ret = $sth->execute($last_upload); };
if ( $@ || !$ret ) {
    send_notify("SQL execute failed: $@.");
}

my $today = strftime '%Y%m%d', localtime;

if ($DEBUG) {
    $log->debug('Creating tempory file for upload.');
}

my $skufile = $WORKDIR . "/$today.sku";

if ( !open $upc_file, '>', $skufile ) {
    send_notify("Open UPC file ($upc_file) failed: $@.");
}

if ($DEBUG) {
    $log->debug('Examining UPCs for errors.');
}
my $cnt = 0;

while ( my ( $upc, $desc, $date ) = $sth->fetchrow_array() ) {
    next if ( $upc =~ /\D/msx );

    my $size = length $upc;
    next if ( $size > 14 || $size < 8 );

    my $check = substr $upc, $size - 1, 1;

    # UPC-E needs to be expanded to UPC-A
    # if the first digit of the UPC is not 0 or 1
    # it is either EAN-8 or invalid
    if ( $size == 8 && substr( $upc, 0, 1 ) < 2 ) {
        $log->debug("UPC-E convert to UPC-A: $upc\t$desc\t$date\n");

        my $last_num = substr $upc, 6, 1;

        if ( $last_num < 3 ) {
            $upc = substr $upc, 0, 3 . $last_num . '0000' . substr $upc, 3,
              3 . $check;
        } elsif ( $last_num == 3 ) {
            $upc = substr $upc, 0, 4 . '00000' . substr $upc, 4, 2 . $check;
        } elsif ( $last_num == 4 ) {
            $upc = substr $upc, 0, 5 . '00000' . substr $upc, 5, 1 . $check;
        } elsif ( $last_num > 4 ) {
            $upc = substr $upc, 0, 6 . '0000' . $last_num . $check;
        }
    }

    # This zero padding should not effect the check digit for
    # UPC-A, EAN-13, or EAN-8
    $upc = sprintf '%014s', $upc;

    my ( $esum, $osum ) = ( 0, 0 );

    for ( 0 .. 12 ) {
        my $val = substr $upc, $_, 1;

        if ( $_ % 2 ) {
            $osum += $val;
        } else {
            $esum += $val;
        }
    }

    my $res = ( 10 - ( ( ( $esum * 3 ) + $osum ) % 10 ) ) % 10;
    next if ( $res != $check );

    if ( !$desc ) {
        $desc = 'No Description';
    }

    my $blank_space = q{ };
    $desc =~ s/[\t\r\n]/$blank_space/gmsx;

    print {$upc_file} "$upc\t$desc\r\n";

    $cnt++;
}

close $upc_file;

$log->info("File contains $cnt records");

if ($DEBUG) {
    $log->debug('Compressing temporary file');
}

MCCS::Utils::zipit( archive => "$skufile.zip", files => [$skufile] );

#Added No Send option for scp.
if ( !$NOSEND) {

    my $sshobj = IBIS::SSH2CONNECT->new( IBISlog_object => $log );
    my $scp = $sshobj->connect( user => $FW_USER, host => $FW_HOST );

    if ( $@ || !$scp ) {
        my $mess = $scp ? join( ' ', $scp->error ) : $!;
        send_notify("SCP login failed:: $@. Login error:$mess.");
    } else {
        $log->info("Login OK to $FW_HOST");
        $log->info("Starting file transfer to $FW_HOST");
    }

    my $out_file = basename("$skufile.zip");

    $log->info("SCP $skufile.zip to $FW_HOST :: $out_file");

    if ( !$scp->scp_put( "$skufile.zip", $out_file ) ) {
        send_notify( "SKU file copy failed: " . join( ' ', $scp->error ) );
    } else {
        $log->info("SCP OK to $FW_HOST");
    }

    my $donefile = join '/', $WORKDIR, "$today.donsku";

    MCCS::Utils::touch($donefile);

    if ( !$scp->scp_put( $donefile, basename($donefile) ) ) {
        send_notify( "File copy failed: $@. SCP error: " . join( ' ', $scp->error ) );
    }

    $log->info('Files transfered.');
}
if ( !$options{'a'} ) {    #if doing bulk loading... don't update last run file

    if ($DEBUG) {
        $log->debug('Writing checkpoint file.');
    }

    # update the last_upload.dat file
    if ( !open $lu_file, '>', $DATEFILE ) {
        send_notify("$DATEFILE write failed:$@. Sys error: $!");
    }

    # Add 0 seconds after midnight to the date, some overlap okay.
    print {$lu_file} $today, "0\n";

    close $lu_file;
}

#######################################################################################
# Subs
#######################################################################################

sub send_notify {
    my ($message) = @_;
    error_mail( $message . "\n" );
    $log->die($message);
}

sub error_mail {
    my $error_msg = shift;

    my $now = strftime( '%Y-%m-%d %H:%M:%S', localtime );

    my $msg = <<"END";
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE HTML PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www/w3c.org/TR/xhtml1/xhtml1-strict.dtd">
    <html xmlns="http://www/w3c.org/1999/xhtml" xml:lan="en" lang="en">
    <head>
        <title>Fraud Watch Upload Notification</title>
       <style type="text/css">
       body{ font-family: arial, helvetica, sans; }
       .bold{ font-weight: bold; }
       </style>
    </head>
        <body>
            <div style="text-align: left;">
                <h3>$error_msg</h3>
            </div>
            <div>
                Error Generated at: <span class="bold">$now</span>
            </div>
            <br>
            <div>
                Please contact <a href="mailto:rdistaff\@usmc-mccs.org?subject=Fraud Watch Upload Error">the RDI Group</a>  if you have any questions.<br />
            </div>
        </body>
    </html>
END

    my $mail = IBIS::Mail->new(
                                to      => [$MAIL_TO],
                                cc      => [],
                                from    => 'IBIS<rdistaff@usmc-mccs.org>',
                                type    => 'text/html',
                                subject => 'Fraud Watch UPC Upload',
                                body    => $msg,
    );

    $mail->send;

    return;
}

__END__

=head1 NAME

fraudwatch_upc_upload.pl - UPC and associated description uploader for FraudWatch (TE-Triversity)

=head1 VERSION

This documentation refers to test.pl version 0.0.1.

=head1 USAGE

fraudwatch_upc_upload.pl

=head1 REQUIRED ARGUMENTS

Start Date supplied by '/usr/local/mccs/data/fraudwatch/upload/last_upload.dat
    
=head1 OPTIONS

=over 1

=item -d

Invoke DEBUG

=item -a

Override the last run date to include all barcodes. Usable for bulk loading.

=back

=head1 DESCRIPTION
   
Uploads UPC, price (if defined), and item description (if defined) to Fraudwatch servers. 
Runs weekly on Saturdays at 0232 hours fraudwatch_upc_upload.pl located in /usr/local/mccs/bin. 
Last ran date stored in /usr/local/mccs/data/fraudwatch/upload/last_upload.data. 
Post data to current directory on hqw2k3svr17.

=head1 REQUIREMENTS

<Document all application requirements here>

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION

None.

=head1 EXIT STATUS

None.

=head1 DEPENDENCIES

    use IBIS::Log::File;
    use IBIS::DBI;
    use POSIX qw(strftime);
    use MCCS::Utils;
    use File::Basename;
    use Net::SCP;
    use Readonly;
    use Getopt::Std;
    use IBIS::Mail;
    use English qw(-no_match_vars); 
    use version; 

=head1 INCOMPATIBILITIES

None.

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to the L<MCCS Help Desk|mailto:help.desk@usmc-mccs.org>.
Patches are welcome.

=head1 BUSINESS PROCESS OWNER

<Process Owner Name|mailto:rdistaff@usmc-mccs.org>

=head1 AUTHOR

Richard L. Roberts L<rdistaff@usmc-mccs.org|mailto:rdistaff@usmc-mccs.org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2007 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
