#!/usr/local/mccs/perl/bin/perl -w
#---------------------------------------------------------------------
# Ported by: Hanny Januarius
# Date: Thu Dec  7 08:20:12 EST 2023
# Desc: Connect to DISA site, just sake of connect to we dont get 
#       disabled.
#
#---------------------------------------------------------------------
use Net::SFTP::Foreign;
use MCCS::WMS::Sendmail;
use Data::Dumper;
use IBIS::Log::File;
use File::Basename;
use IBIS::DBI;
use MCCS::Config;
my $prog_name = basename($0);
$prog_name =~ s/\.\w+$//;
my $g_logfile = "/usr/local/mccs/log/DISA/$prog_name.log";
my $g_log = new IBIS::Log::File( { file => $g_logfile, append => 1, level => 4 }
 );
my $g_cfg = new MCCS::Config;
my $g_host = `hostname`;
chomp($g_host);
my $g_long_date = `date +"%D %r"`;
chomp($g_long_date);
my $go_mail = MCCS::WMS::Sendmail->new();
my $g_emails = $g_cfg->DISA->{emails};  #TODO
my $g_dbname = $g_cfg->DISA->{dbname};  #TODO

#---------------------------------------------------------------------
sub send_mail {
    my $msg_sub  = shift;
    my $msg_bod1 = shift;
    my $msg_bod2 = shift || '';
    my @body = ($msg_bod1, $msg_bod2);

    return if $g_verbose;    # Dont want to send email if on verbose mode

    $go_mail->logObj($g_log);
    $go_mail->subject($msg_sub);
    $go_mail->sendTo($g_emails);
    $go_mail->msg(@body);
    $go_mail->hostName($g_host);
    $go_mail->send_mail();
}
#---------------------------------------------------------------------
sub fatal_error {
    my $msg = shift;
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
    $g_log->error($msg);
    die $msg;
}
#---------------------------------------------------------------------
sub get_disa_args {
    my $dbh = IBIS::DBI->connect(dbname=>$g_dbname);

    my $sql = <<ENDSQL;
select rdi_encrypt_pkg.decrypt_pwd(passwd) as passwd, 
       remote_svr,
       user_id 
from rdi_sftp_parms  
where intf_id = 'DFAS_PICKUP'
ENDSQL

    my $sth = $dbh->prepare($sql);
    $sth->execute;
    my $e = $sth->fetchrow_hashref;

    $arg->{host} = $e->{remote_svr};
    $arg->{user} = $e->{user_id};
    $arg->{password} = $e->{passwd};

    $sth->finish;
    $dbh->disconnect();

    return $arg;
}
#---------------------------------------------------------------------
sub my_main {
    my $sub = __FILE__ . " the_main";
    if ($g_verbose) {

        $g_log->level(5);
    }
    else {

        $g_log->level(4);
    }

    $g_log->info("-- Start ----------------------------------------");
    my %arglist = %{get_disa_args()};
    #$g_log->info(Dumper \%arglist); 
    my $host = $arglist{host};

    # DEBUG mode
    #$arglist{more} = qq(-v);
    #$arglist{password} = 'xsxaveRW+$09oylhmn';
    
    # Suppress welcome screen
    open my $ssherr, '>', '/dev/null' or fatal_error("unable to open /dev/null");
    $arglist{stderr_fh} = $ssherr;

    my $sftps;
    $ftps = Net::SFTP::Foreign->new( $host,%arglist );
    
    
    if ( $ftps->error == 0 ) { 
            $g_log->info("SFTP CONNECTED $host");
            my $target_dir = 'share/mctfs@mccs:sfg-adm.csd.disa.mil';
            $g_log->info("listing file on dir $target_dir");
            #my $ls = $ftps->ls("/export/home/mclc006");
            my $ls = $ftps->ls($target_dir);
            for my $e ( sort { $a->{filename} cmp $b->{filename} } @{$ls}) {
		$g_log->info("\t" . $e->{filename});
            }
            #$g_log->info(Dumper $ls);
            $ftps = undef;
    } else { 
            fatal_error("Could not connect to $host\n");
    }

    $g_log->info("-- End ------------------------------------------");
}		
$SIG{__WARN__} = sub { $g_log->warn("@_") };

eval { my_main() };
if ($@) {
    send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date,
        "Untrapped Error:\n\n", " $@" );
    $g_log->info($@);
}
