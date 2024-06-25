#!/usr/local/mccs/perl/bin/perl --
#----------------------------------------------------
# Ported by: Hanny Januarius
# Date: Wed Dec  6 14:14:51 EST 2023
# Desc:
#       dbms pipe written by Armando
#----------------------------------------------------
use strict;
use IBIS::DBI;
use Net::SMTP;
use MCCS::Config;
use IBIS::Log::File;
use Readonly;
use Carp;
use File::Basename;
use warnings;
use Fcntl qw(:flock);
use File::Path;

#- One process at a time --------------------------
my $lock_file = "/usr/local/mccs/tmp/" . basename($0) . ".lck";
open SELF, "> $lock_file" or die "Could not create lock file $lock_file";
flock SELF, LOCK_EX | LOCK_NB or die "Another $0 process already running";

my $g_cfg = new MCCS::Config;
my $g_log_dir = $g_cfg->dbms_pipes->{log_d};
Readonly my $g_logfile => $g_log_dir . basename(__FILE__) . '.log';
my $g_verbose = 0;
unless ( -e $g_log_dir ) {
	mkpath($g_log_dir);
}

my $g_log = IBIS::Log::File->new( { file => $g_logfile, append => 1, level => 4 } );

my $g_dbname = $g_cfg->wms_global->{DBNAME};
#TODO uncomment next line, delete the next two after.
#my $g_emails = $g_cfg->dbms_pipes->{emails};
my $g_emails;
$g_emails->{kav}='kaveh.sari@usmc-mccs.org';
my $g_dbh = IBIS::DBI->connect( dbname => $g_dbname );
my $g_cmd = '';

sub my_main {

	while ( $g_cmd ne '## END ##' ) {

		$g_cmd = "";
		my $g_sql_f = $g_dbh->prepare( q{ BEGIN :g_cmd := RDIUSR.MRI_GET_PIPE_COMMAND; END; });
		$g_sql_f->bind_param_inout( ":g_cmd", \$g_cmd, 1024 );
		$g_sql_f->execute;
		$g_log->info('Command string from pipe: ' . $g_cmd);
		last if $g_cmd eq '## END ##' or $g_cmd eq '## ERROR-IN-PIPE ##';

                system($g_cmd) == 0 or fatal_error( 'Unable to execute ' . $g_cmd );
	}

        # Send out alert email regarding normal requested termination of dbms_pipes deamon
	# --------------------------------------------------------------------------------
	send_mail( 'dbms_pipes terminated', "Received request: $g_cmd" );
	$g_dbh->disconnect;
}


#--------------------------------------------
#      Routine to send/email errors and croak
#--------------------------------------------
sub fatal_error {
	my $msg = shift;
	send_mail( 'dbms_pipes deamon failure', $msg );
	$g_log->info($msg);
	croak($msg);
}

#--------------------------------------------
#      Routine to send notification emails
#--------------------------------------------
sub send_mail {
	my $msg_sub  = shift;
	my $msg_bod1 = shift;
	my $msg_bod2 = shift || '';
	return if $g_verbose;    # Dont want to send email if on verbose mode

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

#  Execute main and trap any errors
eval { my_main() };
if ($@) {
	fatal_error("Untrapped error: $@");
}
