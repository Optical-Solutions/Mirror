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
my $nosend = 0;
my $g_count = 0;
my $limit_records = 0;
my $pre_rcls;
my $type_string;
#handle command line arguments
my $options = (GetOptions(
	'site=s' => \@sites,
	'file=s' => \$file,
	'type=s' => \$type_string  
	'debug' => \$debug,
	'database=s' => \$database,
	'merchyear=i' => \$merchyear,
	'merchweek=i' => \$merchweek,
	'limit_records=i' => \$limit_records,
	'log=s' => \$log,
	'help' => \$help,
	"nosend" => \$nosend
	)
);
my @types = split /,/, $type_string if defined $type_string;  #multiple values seperated by commas
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

if ($badmsg || $help || ! $options || ! @types || grep { my $t = $_; ! grep { $t eq $_ } @valid_types } @types) {
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
mylog("Sites: " . join(',', @sites));
mylog("Output type: ", $type);
mylog("Logfile: ", $log);
mylog("Merch Year: ", $merchyear);
mylog("Merch Week: ", $merchweek);
mylog("Record limit: ", $limit_records) if $limit_records;  


foreach my $type (@types) {
    my $type_class = 'MCCS::SAS::Loads::' . $type;
    eval("use $type_class;"); die $@ if $@;  ## no critic qw(BuiltinFunctions::ProhibitStringyEval)

    # Determine file for this type, if not passed
    my $file_for_type = $file;
    if (! $file_for_type) {
        my $gfile = $type_class->get_filename($merchyear, $merchweek);
        $file_for_type = File::Spec->catfile($DEFAULT_OUTPUT_DIR, $gfile);
        print "gfilename is $gfile \n";
    }
    print "filename is $file_for_type  \n";

    # Prefer existing $database or fallback to the type's database or default $DBNAME
    my $db_for_type = $database || $type_class->database() || $DBNAME;
    mylog("Database:  $db_for_type");
    mylog("Output file:  $file_for_type");

    my $sas_util = MCCS::SAS::Util->new($db_for_type, $file_for_type);
    my $db = $sas_util->get_database();
    my $type_obj = $type_class->new($sas_util);

    # Set merchyear/week if week_limiting
    if ($type_obj->week_limiting()) {
        if (! $merchyear) {
            my $date_sth = $db->prepare($FIND_THIS_MERCH_WEEK);
            $date_sth->execute();
            ($merchyear, $merchweek) = $date_sth->fetchrow_array();
        }
    }

    my $sql = sprintf($type_obj->get_sql(), make_site_in(\@sites, $type_obj->site_field()));


    # Prepare and execute SQL

    print "Type SQL: $sql\n";
    my $sth = $db->prepare($sql) or die "Prepare failed: " . $db->errstr;
    print "Prepare succeeded\n";

    if ($type_obj->week_limiting()) {
        print "Executing SQL with merchyear/week: $merchyear, $merchweek\n";
        $sth->execute($merchyear, $merchweek) or die "Execute failed: " . $sth->errstr;
        print "Execute succeeded with week limits\n";
    } else {
        print "Executing SQL without week limits\n";
        $sth->execute() or die "Execute failed: " . $sth->errstr;
        print "Execute succeeded without week limits\n";
    }

    # Fetch rows
    my $row_count = 0;
    while (my $myrow = $sth->fetchrow_arrayref()) {
        my $make_ret = $type_obj->make_record(@{$myrow});
        $g_count++;
        $row_count++;
        last if ($limit_records && $g_count >= $limit_records);
    }
    $type_obj->finish();
    print "Rows fetched: $row_count\n";
    print "Going to push to sas\n";

    if (!$nosend) {
        # Pass the filename(s) associated with this type to push_to_sas
        push_to_sas($file_for_type);
    } else {
        print "Skipping send to SAS\n";
    }
}

  #TODO sure hope the files have been written.  #TODO UNCOMMENT THIS
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
    my @files_to_upload = @_;

    unless (@files_to_upload) {
        @files_to_upload = glob("$DEFAULT_OUTPUT_DIR/*");
    }

    print ">>> push_to_sas: starting\n";
    print ">>> push_to_sas: files to upload: ", scalar(@files_to_upload), "\n";

    my $util = MCCS::MCE::Util->new();
    my $name = 'MVMS-Middleware-SAS-DEV';
    print ">>> push_to_sas: getting secret for $name\n";
    my $secret = $util->get_secret($name);

    unless (defined $secret) {
        die ">>> push_to_sas: Secret $name not found\n";
    }
    print ">>> push_to_sas: got secret\n";

    my $sftp_server = $secret->{'host'};
    $util->print_secret($name);
    my $user = $secret->{'user'};
    my $pw;

    if (defined $secret->{'pw'}) {
        $pw = $secret->{'pw'};
        print "on pw key\n";
    }
    if (defined $secret->{'password'}) {
        $pw = $secret->{'password'};
        print "on password key\n";
    }

    if (defined $pw) {
        $pw =~ s/\\//g;  # Remove backslashes from password
    } else {
        die ">>> push_to_sas: Password not found in secret\n";
    }

    my $port = $secret->{'port'} || '22';
    print("port = $port\n");

    my @ssh_options = (
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'PreferredAuthentications=password',
        '-o', 'PubkeyAuthentication=no',
        '-o', 'ServerAliveInterval=15',
        '-o', 'ServerAliveCountMax=3',
        '-o', 'ConnectTimeout=60',
    );

    my %sftp_options = (
        timeout  => 60,
        ssh_cmd  => '/usr/bin/ssh',
        port     => $port,
        more     => \@ssh_options,
    );

    foreach my $file (@files_to_upload) {
        my $basename = basename($file);
        my $remote_path = "$g_remote_path/$basename";

        my $size = -s $file;
        if (!defined $size || $size == 0) {
            print ">>> push_to_sas: skipping zero-length file $file\n";
            next;
        }

        print ">>> push_to_sas: connecting to $sftp_server for file $basename\n";

        my $sftp;
        my $connected = 0;
        for my $attempt (1..3) {
            $sftp = Net::SFTP::Foreign->new(
                $sftp_server,
                user     => $user,
                password => $pw,
                %sftp_options,
            );

            if ($sftp->error) {
                warn "SFTP connection failed on attempt $attempt for file $basename: " . $sftp->error . "\n";
                sleep 1;
            } else {
                print ">>> push_to_sas: connection successful for file $basename\n";
                $connected = 1;
                last;
            }
        }

        unless ($connected) {
            warn ">>> push_to_sas: giving up after 3 failed attempts for file $basename\n";
            next;
        }

        my $remote_stat = $sftp->stat($remote_path);
        if (defined $remote_stat) {
            print ">>> push_to_sas: remote file already exists, skipping upload: $remote_path\n";
            $sftp->disconnect() if defined $sftp && ref($sftp);
            next;
        } elsif ($sftp->error) {
            warn ">>> push_to_sas: stat failed for $remote_path: " . $sftp->error . "\n";
            # Decide to continue or not; here continue
        }

        print ">>> push_to_sas: uploading $file to $remote_path\n";
        unless ($sftp->put($file, $remote_path)) {
            warn ">>> push_to_sas: failed to upload $file: " . $sftp->error . "\n";
        } else {
            print ">>> push_to_sas: upload successful for $basename\n";
        }

        $sftp->disconnect() if defined $sftp && ref($sftp);
        print ">>> push_to_sas: disconnected from SFTP for file $basename\n";
    }

    print ">>> push_to_sas: all file transfers completed\n";
    return;
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
