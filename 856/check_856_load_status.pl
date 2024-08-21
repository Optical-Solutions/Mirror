#!/usr/local/mccs/perl/bin/perl
use strict;
use Data::Dumper;
use Time::localtime;
use File::Basename;
use IBIS::Log::File;
use File::Copy;
use File::Path;
use Getopt::Std;
use Fcntl qw(:flock);
use Data::Dumper;
use IBIS::DBI;
##use IBIS::Email;
my $debug = 0;
my $ld_status_ref;

my $dbh = IBIS::DBI->connect( dbname => 'rms_p', attribs => {AutoCommit => 0} );
my $query = qq(
    select 
         count(*) 
    from 
         edi_archive_transactions 
    where partnership_id = ?
    and key_data = ?
    and application_id = 'RAMS'
    and business_unit_id ='30'
    and transaction_set ='856'
    and transaction_type = 'IN'
    );
my $sth_confirm = $dbh->prepare($query);

my $data_dir = '/usr/local/mccs/data/edi/ftp/856_inbound_backup/';
my $repro_dir = '/usr/local/mccs/data/edi/ftp/856_inbound/';
my $archive_dir = '/usr/local/mccs/data/edi/ftp/856_inbound_archive/';
my $log_file ='/usr/local/mccs/log/edi/edi_856/check_loading_status.log';
my $log_obj     = IBIS::Log::File->new(
    {
        file   => $log_file,
        append => 1
    }
);

$log_obj->info("Start of Process");
my $list_ref = &get_yesterday_files($data_dir);

$ld_status_ref = &process_list_file($list_ref, $ld_status_ref);
if ($debug){print Dumper($ld_status_ref);}

$ld_status_ref = &check_edi_archive_status($ld_status_ref);
if ($debug){print Dumper($ld_status_ref);}
&manage_files($ld_status_ref);
$log_obj->info("End of Process");

##########  SUBROUTINES  ################
##1, get list of files yesterday find ./ -ctime 1
##input: nothing
##output: list reference of files
sub get_yesterday_files{
    my ($data_dir) = @_;
    
    ## my $buffer = `find /usr/local/mccs/scratch/yuc_temp/856_snty_check/test_data/ -ctime 0`;
    my $buffer = `find $data_dir -ctime 1`;
    if($debug) {print Dumper($buffer);}
    my @ary = split(/\n/, $buffer);
     
    if($debug) {print Dumper(\@ary);}
    return \@ary;
}

## return the hash_ref
sub process_list_file {
    my ($list_ref, $result_ref) = @_;
    foreach my $file(@$list_ref){
	unless($file =~ /\.7R3$/){
	    next;
	}
	my ($p_id, $k_data) = &parse_856_line_10($file);
	$k_data =~ s/\s+$//g;
	if(($p_id) && ($k_data)){
	    $result_ref->{$file}->{'partnership_id'} = $p_id;
	    $result_ref->{$file}->{'key_data'}       = $k_data;
	}else{
	    my $msg =  "failed to parse out data";
	    $result_ref->{$file}->{'desc'} = $msg;  
	}
    }
    return $result_ref;
}

#2, foreach file, 
#parse the file, to get partnership_id, and key_data 
#the result will be in a hash tree->filename->{partnership_id}
##                      a hash tree->filename->{key_data}
## out : (partnership_id, key_data)
sub parse_856_line_10{
    my ($infile) = @_;
    open(IN, "<$infile") or die "failed to open file:".$infile;
    my @ary = <IN>;
    close IN;
    
    my ($partnership_id, $key_data);
    foreach my $line(@ary){
	chomp($line);
	if($line =~ /\w+/){
	    my $line_type = substr($line, 16, 2);
	    if ($line_type eq '10'){
		##print "Found line 10";
		$partnership_id = substr($line, 0, 14);
		$key_data       = substr($line, 20, 30);
		$key_data =~ s/^\s+//g;
		$key_data =~ s/\s+$//g;
		return ($partnership_id, $key_data);
	    }else{
		next;
	    }
	}
    }
}


##3, formation of query for each file
##  and do a db search on each file, save the result in the tree
##                          a hash tree->filename->{load_status} = 1;
sub check_edi_archive_status{
    my ($status_ref) = @_;
       
    foreach my $key (keys %{$status_ref}){
	my @bind_vals;
	unless($status_ref->{$key}->{'desc'}){
	    my $p_id    = $status_ref->{$key}->{'partnership_id'};
	    my $k_data  = $status_ref->{$key}->{'key_data'};
	    push (@bind_vals, $p_id);
	    push (@bind_vals, $k_data);	    
	    my $result = &select_check(\@bind_vals);
	    if($result){
		$status_ref->{$key}->{'load_status'} = 'Y';
	    }else{
		$status_ref->{$key}->{'load_status'} = 'N';
		## print "File did not validate:". $key;
	    }   
	}
    }
    return $status_ref;
}



sub select_check{
    my $binding_ref = shift;
    $sth_confirm->execute(@$binding_ref);
    my $boolean = ($sth_confirm->fetchrow_array)[0];
    return $boolean;
}


sub manage_files{
    my ($result_ref) = @_;
    for my $file (keys %{$result_ref}){
    if($result_ref->{$file}->{'load_status'} eq 'Y'){
     ### copy the file to archive directory
        &move_file($file, $archive_dir);
    }

    if($result_ref->{$file}->{'load_status'} eq 'N'){
     ### copy file to reprocess 
        &copy_file($file, $repro_dir);
	$log_obj->info("Reprocessing:".$file);
    }
    }
}

sub copy_file{
   my ($from, $to) = @_;
   my $cmd = "cp  $from  $to";
   if($debug){print "cmd: $cmd\n";}
   system($cmd);
}

sub move_file{
   my ($from, $to) = @_;
   my $cmd = "mv $from $to";
   if($debug){
      print "$cmd";
   }
    system($cmd);	
}

####################

=head
4, if load status is 1, do nothing. confirmed loading in log
   if load status is 0, then, log it, and copy the file to the staging directory to process them again. 

=cut



