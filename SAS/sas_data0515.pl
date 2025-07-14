#!/usr/local/mccs/perl/bin/perl
# SAS Planning and Assortment data
use strict;
use warnings;
use MCCS::SAS::Util; 
use IBIS::DBI;
use IBIS::Log::File;
use Getopt::Long;
use Date::Manip;
use Data::Dumper;
use Pod::Usage;
use File::Spec;
use File::Basename qw(basename dirname);
use Class::Inspector;
use MCCS::SAS::Loads::Base;
#use MCCS::SAS::Pre_Reclass_Utils; PER LL not needed.
use File::Path   qw(make_path);
use MCCS::MCE::Util;
use Net::SFTP::Foreign;
use Carp;
# Flush output
local $| = 1;  
my $util = MCCS::MCE::Util->new();

my $DBNAME = 'MVMS-Middleware-RdiUser';

my $DEFAULT_OUTPUT_DIR = '/usr/local/mccs/data/sas/';
#my $DEFAULT_OUTPUT_DIR_PRCLS = '/usr/local/mccs/data/sas/pre_reclass/';  #TODO REMOVED PER LL 5/5/2025
my $FIND_THIS_MERCH_WEEK = "SELECT get_merch_year, get_merch_week FROM dual ";

my @sites = ();
my $file;
my $type;
my $date;
my $today;
my $debug;
my $database;
my $log = "/usr/local/mccs/log/sas_data_$$.log";
my $merchyear;
my $merchweek;
my $merchweek2;
my $help;
my $g_count = 0;
my $limit_records = 0;
my $pre_rcls;

#handle command line arguments
my $options = (GetOptions(
	'site=s' => \@sites,
	'file=s' => \$file,
	'type=s' =>  \$type,
	'debug' => \$debug,
	'database=s' => \$database,
	'merchyear=i' => \$merchyear,
	'merchweek=i' => \$merchweek,
	'limit_records=i' => \$limit_records,
	'log=s' => \$log,
	'help' => \$help
	)
);
unless (-d "/usr/local/mccs/data/sas") {             
    make_path("/usr/local/mccs/data/sas");
}
unless (-d "/usr/local/mccs/log" ) {  
    make_path("/usr/local/mccs/log");
}
my $badmsg;

if( ($merchyear && ! $merchweek) || ($merchweek && ! $merchyear) ){
	$badmsg = 'Both Merchant Year and Merchant Week need to be defined';
}
my $g_remote_path = "/app/mdi/mccs/";  #TODO Verify remote path
my $plugin_dir = dirname( Class::Inspector->loaded_filename('MCCS::SAS::Loads::Base') );
my $dirfh;
opendir($dirfh, $plugin_dir) or die "Can not open Plugin Directory";
my @valid_types = map{ s/\.pm//; $_ } grep{ /(\.pm)$/ && $_ ne 'Base.pm' } readdir($dirfh); ## no critic qw(ControlStructures::ProhibitMutatingListFunctions)
closedir($dirfh);

if($badmsg || $help || ! $options || ! grep{ $type eq $_ } @valid_types
){
	my $msg = $badmsg;
	if(! $options){
		$msg = 'Bad arguments';
	} elsif(! $help){
		$msg = 'Bad type argument'
	}
	pod2usage(-noperldoc=>1, -verbose => 2, -msg => $msg);
}
# END handle command line arguments

#logging
my $log_obj = IBIS::Log::File->new( {file=>$log,append=>1} );
mylog("Started");
mylog("Sites: ", join(',', @sites));
mylog("Output type: ", $type);
mylog("Logfile: ", $log);
mylog("Merch Year: ", $merchyear);
mylog("Merch Week: ", $merchweek);
mylog("Record limit: ", $limit_records) if $limit_records;  

my $type_class = 'MCCS::SAS::Loads::' . $type;
eval("use $type_class;"); die $@ if $@; ## no critic qw(BuiltinFunctions::ProhibitStringyEval)

if(! $file){  #TODO CHECK what this is
	##my $type_obj_tmp = $type_class->new( );	#this instance of type_obj will only be used to get the filename of the class type
	my $gfile = $type_class->get_filename($merchyear,$merchweek);
	$file = File::Spec->catfile($DEFAULT_OUTPUT_DIR, $gfile);
	print "gfilename is $gfile \n";
}
#Insert here
print "filename is $file  \n";
$database = $database || $type_class->database() || $DBNAME;
mylog("Database: ", $$database);
mylog("Output file: ", $$file);

my $sas_util = MCCS::SAS::Util->new($database, $file );	#automatically write to file path  #TODO had to comment this out.
my $db = $sas_util->get_database();
my $type_obj = $type_class->new( $sas_util );

#The data needs filtered to a merchandising year/week
if($type_obj->week_limiting()){
	if(! $merchyear){	#merch dates not passed in, use current merch year/week
		my $date_sth = $db->prepare( $FIND_THIS_MERCH_WEEK );
		$date_sth->execute();
		($merchyear,$merchweek) = $date_sth->fetchrow_array();
	}
}

# Form SQL
my $sql = sprintf($type_obj->get_sql(), make_site_in(\@sites, $type_obj->site_field()));
print STDERR "Type SQL: $sql\n";

# Prepare SQL
my $sth = $db->prepare($sql) or die "Prepare failed: " . $db->errstr;
print STDERR "Prepare succeeded\n";

# Execute SQL
if ($type_obj->week_limiting()) {
    print STDERR "Executing SQL with merchyear/week: $merchyear, $merchweek\n";
    $sth->execute($merchyear, $merchweek) or die "Execute failed: " . $sth->errstr;
    print STDERR "Execute succeeded with week limits\n";
} else {
    print STDERR "Executing SQL without week limits\n";
    $sth->execute() or die "Execute failed: " . $sth->errstr;
    print STDERR "Execute succeeded without week limits\n";
}

# Process rows
my $row_count = 0;
while (my $myrow = $sth->fetchrow_arrayref()) {
    print STDERR (join (',',@{$myrow}) . "\n");  #TODO delete this line.
    #my $make_ret = $type_obj->make_record(@{$myrow});  #TODO uncomment this line.
    $g_count++ if defined $make_ret;
    $row_count++;
    last if ($limit_records && $g_count >= $limit_records);
}
$type_obj->finish();
print STDERR "Rows fetched: $row_count\n";
exit;  #TODO delete this line.
push_to_sas();  #TODO sure hope the files have been written.
# Check DB connection before committing
# unless ($db->ping) {
#     die "Lost connection to Oracle before commit.";
# }
#$db->commit();


# If pm has been called for Pre_reclass processing (pre_rcls flag on ) and MERCH10, populate MRI_ORDER_CODES
# if ($pre_rcls && $type eq 'FULL_MERCH10_LOAD_RECLASS') {  #TODO CHECK WHAT THIS DOES
# 	my $pre_reclass =  MCCS::SAS::Pre_Reclass_Utils->new(pre_reclass_log=>$log_obj, email => 'rdistaff@usmc-mccs.org');
# 	$pre_reclass->populate_mri_order_codes($file);
# }  #TODO comented out per LL.

sub make_site_in {
	my $sites = shift;
	my $field_name = shift || 'site_id';
	
	#form optional sites in clause
	my $site_in='';
	if(ref($sites) && @$sites){
		my @sites = @{ $sites };
		#if the sites paramter happens to be a file path
		if(@sites == 1 && $sites[0] =~ /\D/){
			my $site_fh;
			if(! open($site_fh, '<', $sites[0])){
				my $msg = "Can not open $sites[0]";
				mylog($msg);
				die $msg;
			}
			@sites = <$site_fh>;	#get my list of sites (one per line)
			close($site_fh);
			map{ chomp; } @sites;	#strip off ending new line
			mylog("Sites read from file: ", join(',',@sites));
		}
		#assemble SQL IN clause
		$site_in = " AND $field_name IN (%s)";
		my $site_str = join("', '", @sites);	$site_str = "'" . $site_str . "'";
		$site_in = sprintf($site_in, $site_str);
	}
	return $site_in;

}

sub mylog{
	my $str = shift;
	my $log_entry = join('',"(PID $$) ",$str);
	if($log_obj){ $log_obj->info( $log_entry ); }
	return debug($log_entry);
}

sub debug{
	my $str = shift;
	
	if($debug){
		return print "DEBUG: ", $str, "\n";
	}

}
sub push_to_sas {
   my $dir = $DEFAULT_OUTPUT_DIR;
   my @files = glob("$dir/*");
   my $name        = 'MVMS-Middleware-SAS-DEV';  #TODO Using Value provided by Jose.
   my $secret      = $util->get_secret($name);
   my $sftp_server = $secret->{'host'} || $secret->{'hostname'}
    or die "Missing SFTP host in secret config (expected 'ip' or 'ip')";
   my $sftp = Net::SFTP::Foreign->new(
		$sftp_server,
		user     => $secret->{'username'} || $secret->{'user'},
		password => $secret->{'password'} || $secret->{'pw'},
		port     => $secret->{'port'}     || '22',
		more     => [qw(-o PreferredAuthentications=password -o PubkeyAuthentication=no)]
		);
   $sftp->die_on_error("Unable to establish SFTP connection to $sftp_server\n");

	foreach my $file (@files) {
  		my $basename = basename($file);  
  		my $test_basename = "TEST" . $basename;  # TEMP for testing; remove "TEST" after
		my $local_path  = $file;
		my $outfile = $g_remote_path . "/" . $test_basename;  #TODO remove the word TEST_  AFTER TESTING
		#Upload the local file
		$sftp->put($local_path, $outfile) or croak("Could not put: " . $sftp->error);
	}
    zprint('SFTP transfer completed');
    $sftp->disconnect;
    return;
} ## end sub push_to_sas
END{
	$type_obj = undef;
	if($sth){ $sth->finish(); }
	if($db){ $db->disconnect(); }
	mylog("Completed $g_count records");
}
__END__

=pod

=head1 NAME

sas_data.pl

=head1 SYNOPSIS

sas_data.pl
--file [output file path]
--type [DIVISION | LOB | DEPARTMENT | CLASS | SUBCLASS | STYLE | PRODUCT | LOCATION | SALE | INVENTORY | ONORDER]
--site [optional site(s)]
--database [optional database]
--merchyear [optional merchandising year]
--merchweek [optional merchandising week]
--help
--debug

=head1 DETAILS

=head1 DESCRIPTION

SAS data extract. There are different types of data to extract from RMS for the SAS Planning and Assortment application.
The extract types use a plugin architecture where the module for extracting lives in the MCCS::SAS::Loads package and inherits from
MCCS::SAS::Loads::Base.

Some extracts take a merchandising year and week (merchyear, merchweek).

=head1 DEPENDENCIES

=over 4

=item MCCS::SAS::Util

Is used to create the various types of SAS records necessary to accurately output the correct files.

=back

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
