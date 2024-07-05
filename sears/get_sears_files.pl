#!/usr/local/mccs/perl/bin/perl
#--------------------------------------------------------------------------
#Ported by : Hung Nguyen
#Date      : 12/19/2023
#
#Brief Desc:  The process will be triggered to run after each period closing.
#             It will extract sears commission data from RMS.
#             Then, convert the data to a flat file which FMS can be used to genex directly.
#             The file then will be sent to FMS server for genex.
#            
#			 
#
# --------------------------------------------------------------------------   

use strict;
use MCCS::Config;
use IBIS::Log::File;
use File::Basename;
use POSIX qw(strftime);
use Data::Dumper;
use IBIS::DBI;
use Readonly;
use DateTime::Format::Strptime;
use IBIS::DateTime::Retail;

my $debug = 0;
my $g_cfg = new MCCS::Config;
print Dumper($g_cfg->sears) if $debug;
Readonly  my $LOG_FILE                => $g_cfg->sears->{log_file};
Readonly  my $TARGET_SERVER           => $g_cfg->sears->{target_server};
Readonly  my $REMOTE_DIR              => $g_cfg->sears->{remote_dir};
Readonly  my $LOCAL_DIR               => $g_cfg->sears->{local_dir};
Readonly  my $LOCAL_ARCHIVE_DIR       => $g_cfg->sears->{local_archive_dir};
Readonly  my $REMOTE_SCP_USER         => $g_cfg->sears->{remote_scp_user};
Readonly  my $DB_NAME                 => 'rms_p';
my $month4sears = {
1 => 'FEBURARY',
2 => 'MARCH',
3 => 'APRIL',
4 => 'MAY',
5 => 'JUNE',
6 => 'JULY',
7 => 'AUGUST',
8 => 'SEPTEMBER',
9 => 'OCTOBER',
10 => 'NOVEMBER',
11 => 'DECEMBER',
12 => 'JANUARY'
};

my ($pd_start, $pd_end1, $month, $cur_pd_month, $log_obj );
#-------------------------------------------------------------------------------
# Initialization steps:
my $cur_date = POSIX::strftime( '%Y%m%d%H%M%S', localtime() );
## get log obj
$log_obj = IBIS::Log::File->new( {file => $LOG_FILE, append => 1} );
$log_obj->info("Start running: $cur_date\n");

## dbh:
my $dbh = IBIS::DBI->connect( dbname => $DB_NAME, attribs => { AutoCommit => 0 }) 
    or die "Can not connect to $DB_NAME";
print Dumper($dbh) if $debug;

unless($dbh){
    my $msg = "Failed to connect to database with $DB_NAME\n";
    $log_obj->info($msg);
    die;
}

## get last closed year, period not processed yet.
my ($rms_lst_closed_year, $rms_lst_closed_period)  = &get_last_closed_yearperiod();
##my ($rms_lst_closed_year, $rms_lst_closed_period) = ('2013','8');
print "$rms_lst_closed_year, $rms_lst_closed_period \n"  if $debug;
unless ($rms_lst_closed_year && $rms_lst_closed_period){
    my $msg = "No unprocessed closing year, closing period. Program will exit\n";
    $log_obj->info($msg);
    exit(); ## this will be most of the cases
}

## if 8, hard code end_date, and month; else get from database ##
if($rms_lst_closed_period eq '8'){ ## hardcoded for consistent with FMS 
    ($pd_end1, $month) = ($rms_lst_closed_year.'0930', 'SEPTEMBER');
}else{
    ($pd_end1, $month) = &get_period_end_date($rms_lst_closed_year, $rms_lst_closed_period);   
    $month =~ s/\s+//g;
}

print "$pd_end1, $month \n" if $debug;
unless ($pd_end1 && $month){
    print "Failed to get period end date: $pd_end1, $month\n";
    die; ## is this necessary?
}
## if past all checks so far, then, go ahead to generate a new file...
my $outfile =  $LOCAL_DIR.'/'.'Sears_'.$rms_lst_closed_year.'_'.$rms_lst_closed_period.".txt";
my $ret_ref = &get_sears_data($rms_lst_closed_year, $rms_lst_closed_period);

if($ret_ref){    
    my $stpd_month  = $month4sears->{$rms_lst_closed_period};
    &convert_2_genex_flat($ret_ref, $pd_end1, $rms_lst_closed_period, $stpd_month, $outfile);
}else{
    my $msg = "Failed to get any data from subroutine:get_sears_data\n";
    $log_obj->info($msg);
    die;
}

if (-s $outfile){ ## if file generated, do some database update
    my $msg = "Sears files generated. This job_id will be saved into sears_jobs\n";
    $log_obj->info($msg);
    my $ymd = substr($cur_date, 0, 8);
    my $result =  &save_sears_jobs($ymd, $rms_lst_closed_year, $rms_lst_closed_period);
    unless($result){
	my $msg = "Failed to update sears_jobs table\n";
	$log_obj->info($msg);
	exit;
    }
    &copy_file_to_remote_server($outfile);
    &archive_file($outfile);
}

######populate data into sears_jobs
## insert into sears_jobs (
## select job_id, date_closed, merchandising_year, merchandising_period
## from period_closings);

### subroutines ###
sub copy_file_to_remote_server{
    my ($fp_file) = @_;
     my $TARGET_SERVER2 = 'hqm04ibisvr0010';
     my $REMOTE_DIR2='/tmp';
    my $cmd = "scp -q ".$fp_file."  ". $REMOTE_SCP_USER.'@'.$TARGET_SERVER2.':'.$REMOTE_DIR2;
     #my $cmd = "scp -q ".$fp_file."  ". $REMOTE_SCP_USER.'@'.'hqm04ibisvr0010'.'./tmp';
    my $ret;
    if (-s $fp_file){
	$ret = system($cmd);
	unless($ret){
	    my $msg = "$fp_file copy sucess";
	    $log_obj->info($msg);
	}else{
	    my $msg = "$fp_file copy filed. cmd: $cmd\n";
	    $log_obj->info($msg);
	}
    }
}

sub archive_file{
    my ($fp_file) = @_;
    my $cmd = "cp $fp_file  ".$LOCAL_ARCHIVE_DIR.'/';
    my $ret = system($cmd);
    if($ret){
	my $msg = "Copy to local archive dir failed.";
	$log_obj->info($msg);
    }
}

sub get_last_closed_yearperiod{
    my $query = "select merchandising_year, merchandising_period from v_sears_unprocessed_yp";
    my $ret = $dbh->selectall_arrayref($query);
    if($ret){
	return ($ret->[0][0], $ret->[0][1]);
    }else{
	return undef;
    }
}

## compare what is run, to the closed yearperiod
sub get_period_end_date{
    my ($m_y, $m_p) = @_;
    my $query = "
       select 
          to_char(PERIOD_ENDING_DATE, 'yyyymmdd'),
          to_char(PERIOD_ENDING_DATE, 'MONTH')
       from 
          fiscal_calendars 
       where 
          fiscal_year = \'$m_y\' 
          and fiscal_period = \'$m_p\'
    ";
    my $ret = $dbh->selectall_arrayref($query);
    my ($pd_end1, $month);
    if($ret){
	($pd_end1, $month) = ($ret->[0][0], $ret->[0][1]);
    }else{
	($pd_end1, $month) = (undef, undef);
    }
    return ($pd_end1, $month);
}

## compare what is run, to the closed yearperiod
sub compare_with_sears_last_run{
    my ($input_yp) = @_;
    my $query = "
     select max(merchandise_year||merchandising_period) 
     from   sears_jobs 
     where date_processed is not null 
     and job_id is not null
    ";
    my $ret = $dbh->selectall_arrayref($query);
    my $last_sears_yp = '';
    if($ret){
	$last_sears_yp = $ret->[0][0];
    }else{
	die "failed to get data from sears_jobs";
    }

    if($input_yp > $last_sears_yp){
	return 1;
    }else{
	return 0;
    }
}

sub get_sears_data{
    my ($year, $period) = @_;
    my $query = qq(
   select  
      command,
      class,
      ServiceDesc,
      UnitSales, 
      MarkupIncome8322,
      SearsLiability2108,
      Commission8207,
      Account9989,
      ? as merchandise_year,
      ? as merchandise_period
   from 
     v_sears_commission_data
   where 
     merchandising_year = ?
     and merchandising_period = ?
    );

    my @bind_values;    
    push(@bind_values, $year);
    push(@bind_values, $period);
    push(@bind_values, $year);
    push(@bind_values, $period);    
    my $sth = $dbh->prepare($query);
    my $rv = $sth->execute(@bind_values);    
    my $ret  = $sth->fetchall_arrayref;
    print Dumper($ret) if ($debug);
    if($ret){
	return $ret;
    }else{
	return undef;
    }
}

## translate data into GLs
sub convert_2_genex_flat{
    my ($ret, $in_period_endate,$in_period, $in_month, $outfile) = @_;

    print Dumper($ret);
    
    open (OUT, ">$outfile") or "failed to open file to write: $outfile\n";

    my $front = 'GEN|'.$in_period_endate.'|';
    my $end   = 'SEARS PD '.$in_period.' '.$in_month.'|';
    for(my $i=0; $i<@$ret;$i++){
	##print Dumper($ret->[$i]);
	##for(my $j=0; $j<@{$ret->[$i]}; $j++){
	##    print "i:$i j:$j $ret->[$i][$j]\n";
	##}
	## parse out each lines at this point:
	if($ret->[$i]){
	    my ($cmd, $service_type, $mkup, $liability, $commission, $act9, $year, $period);
	    $cmd            = $ret->[$i][0]; 
	    $cmd =~ /(^\w+)\-/g;
	    my $fst3  = $1;
	    unless($fst3){
		print "Failed to find command: $cmd\n";
		die;
	    }
	    $service_type   = $ret->[$i][2]; 
	    $mkup           = $ret->[$i][4]; 
	    $liability      = $ret->[$i][5]; 
	    $commission     = $ret->[$i][6]; 
	    $act9           = $ret->[$i][7]; 
	    $year           = $ret->[$i][8]; 
	    $period	    = $ret->[$i][9]; 
	    print "$fst3|$service_type|$mkup|$liability|$commission|$act9|$year|$period\n" if $debug;
	    ## print gl lines:

	    ## get gl accounts:
	    ## 1, determine last 3 letter 
	    my $lst3 = '';
	    if($service_type     =~ /DELIV/g){
		$lst3 ='858';
	    }elsif($service_type =~ /HAUL/g){
		$lst3 ='859';
	    }elsif($service_type =~ /INSTA/g){
		$lst3 ='860';
	    }else{
		print "UNKNOWN service type: $service_type\n";
		die;
	    }

	    my $gl_act = '';
            ## print markup line:
	    if($mkup){
		$gl_act = $fst3.'-7771-01-8322-000-'.$lst3;	
		print OUT "$front".$gl_act.'||'.$mkup.'|'.$end."\n"; 
	    }
            ## print liability line:
	    if($liability ){
		$gl_act = $fst3.'-0000-01-2108-000-'.$lst3;	
		print OUT "$front".$gl_act.'||'.$liability.'|'.$end."\n"; 
	    }
            ## print commision
	    if($commission){
		$gl_act = $fst3.'-7771-01-8207-000-'.$lst3;
		print OUT "$front".$gl_act.'||'.$commission.'|'.$end."\n"; 
	    }
            ## print act9989
	    if($act9){
		$gl_act = $fst3.'-7771-01-9989-000-'.$lst3;	
		print OUT "$front".$gl_act.'|'.$act9.'||'.$end."\n"; 
	    }
	}
    }  
    close OUT;
}

sub get_454_closing_date_by_period{
    my ($year, $period) = @_;
    return "DUMMY_FMS_PERIOD_CLOSING_DATE";
}

sub get_454_closing_month_by_period{
    my ($year, $period) = @_;
    return "DUMMY_FMS_PERIOD_CLOSING_MONTH";
}

## print out formated file
sub print_sears_file{

}

## save sears data to sears_commission table
sub save_sears_jobs{
    my ($job_id, $m_y, $m_p) = @_;
    my $query = "
    insert into rdiusr.sears_jobs(
    job_id, 
    date_processed, 
    merchandising_year, 
    merchandising_period
    )values(
    ?,
    sysdate,
    ?,
    ?)";
    my @bind_values;
    push(@bind_values, $job_id);
    push(@bind_values, $m_y);
    push(@bind_values, $m_p);
  
    my $sth = $dbh->prepare($query);

    eval{
	$sth->execute(@bind_values);
    };
    if($@){
	my $msg = "DB error: $@";
	$log_obj->info($@);
	$dbh->rollback;
	return undef;
    }else{
	$dbh->commit;
	return 1;
    }
}

=pod
=head
=head1 NAME
get_sears_files.pl
The process will be triggered to run after each period closing.
It will extract sears commission data from RMS.
Then, convert the data to a flat file which FMS can be used to genex directly.
The file then will be sent to FMS server for genex. 

=head1 VERSION

This documentation refers to the initial version, released in Summer of 2014.

=head1 REQUIRED ARGUMENTS

None.

=head1 OPTIONS

=over 6

=head1 DESCRIPTION

=head1 REQUIREMENTS

None.

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION

                                                                                        
=head1 DEPENDENCIES

=head1 SEE ALSO 

=head1 INCOMPATIBILITIES

Unknown.

=head1 BUGS AND LIMITATIONS

Limitations:

No number count, or PO record, to show what PO has been sent. 

=head1 BUSINESS PROCESS OWNERs

=head1 AUTHOR

Chunhui Yu<yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2008 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
