#!/usr/bin/perl
use strict;
use warnings;
use File::Copy;

# $ENV{'ORACLE_HOME'} = '/usr/local/mccs/instantclient_12_1';
# $ENV{'LD_LIBRARY_PATH'} = '/usr/local/mccs/instantclient_12_1';
# my $run_dir = '/usr/local/mccs/bin/SAS'; #TODO 
my $run_dir = '/home/perl/bin/SAS/'; 
my $io_dir = '/usr/local/mccs/data/sas/04_Runtime';
my $weekly_file = "$io_dir/merch_weeks";
my $sas_file = "$io_dir/sas_weekly_sw.txt";
die "Usage: $0 [prev|curr]\n" unless @ARGV == 1;
my $arg = $ARGV[0];

print "\n";
print "SAS Weekly Input Files Start\n";
# Get current date and time
my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
$year += 1900;
$mon  += 1;
printf "Date: %04d-%02d-%02d\n", $year, $mon, $mday;
printf "Script Start Time: %02d:%02d:%02d\n",$hour,$min,$sec;

print "\n\n";

#TODO insert echo statements
# Print initial messages

# Check command-line arguments
print "Checking arguments passed from cron/commandline to see what weeks to run\n";

if (defined $arg) {
    if ($arg eq 'prev') {
        print "We will be processing previous 5 weeks\n";
    } elsif ($arg eq 'curr') {
        print "We will be processing the last closed week. Meaning week end date of Sunday\n";
    } else {
        print "There was an error \"curr\" or \"prev\" was not provided!!!!!\n";
        print "How am I to know what to process?\n";
        exit 2;
    }
} else {
    print "No argument provided. Please specify \"curr\" or \"prev\".\n";
    exit 2;
}

print "Running SqlPlus to start SAS Data Batch Process\n";
#
my $sqlplus_cmd;
$sqlplus_cmd = "sqlplus -s eric/ericdata\@draix22.usmc-mccs.org/sastst \@$io_dir/sas_weekly_get_${arg}_weeks.sql > $io_dir/merch_weeks 2>/tmp/error1";
system($sqlplus_cmd) == 0 or die "Failed to execute SQL*Plus command: $!";

$sqlplus_cmd = "sqlplus -s eric/ericdata\@draix22.usmc-mccs.org/sastst \@$run_dir/sas_weekly_${arg}_start.sql  2>/tmp/error2";
system($sqlplus_cmd) == 0 or die "Failed to execute SQL*Plus command: $!";
print "\n\n";

print "Creating Merch/Product\n";
my @types = qw(DIVISION LOB DEPARTMENT CLASS SUBCLASS STYLE_DAILY PRODUCT_DAILY);
my @pids;

foreach my $type (@types) {
    my $pid = fork();
    if ($pid == 0) {
        exec("perl /home/perl/bin/SAS/sas_data.pl --type $type --database MVMS-Middleware-RdiUsr");  #TODO fix db name and verify PL dir
        exit;
    } else {
        push @pids, $pid;
    }
}

# Wait for all child processes to complete
foreach my $pid (@pids) {
    waitpid($pid, 0);
}

($sec,$min,$hour) = localtime();
print "\nCreating SALES\n";
printf "Lets start creating SALES files: %02d:%02d:%02d\n",$hour,$min,$sec;
open my $fh, '<', $weekly_file or die "Cannot open $weekly_file: $!";

while (my $line = <$fh>) {
    chomp $line;
    my ($rec, $year, $week) = split /,\s*/, $line;
    if ($rec =~ /^rec/) {
        my $pid = fork();
        if ($pid == 0) {
            exec("/usr/local/mccs/bin/SAS/sas_data.pl --type SALE_SAS_PROD --database MVMS-Middleware-RdiUsr --merchyear $year --merchweek $week");
            exit;
        } else {
            push @pids, $pid;
        }
    } else {
        print "No more weeks to process.\n";
    }
}
close $fh;

my $flag = 0;
my $cnt = 0;

while (!$flag) {
    sleep 1200; # Sleep for 20 minutes
    system("sqlplus -s eric/ericdata\@draix22.usmc-mccs.org/sastst \@$run_dir/sas_weekly_complete.sql > $io_dir/sas_weekly_sw.txt 2>/tmp/error3") == 0
        or die "Failed to execute SQL*Plus command: $!";

    open my $fh, '<', $sas_file or die "Cannot open $sas_file: $!";
    while (my $line = <$fh>) {
        chomp $line;
        my (undef, $status) = split /,\s*/, $line;
        if ($status eq 'true') {
            $flag = 1;
            last;
        }
    }
    close $fh;

    $cnt++;
    if ($cnt == 3) {
        $cnt = 0;
        print "Still waiting - Time is now: ", scalar localtime, "\n";
    }
}
# CREATE INVENTORY
print "\nCreating Inventory\n";
open my $fh2, '<', $weekly_file or die "Cannot open $weekly_file: $!";
while (my $line = <$fh2>) {
    chomp $line;
    my ($rec, $year, $week) = map { s/\s+//gr } split /,/, $line;
    next unless $rec =~ /^rec/i;

    my $pid = fork();
    if ($pid == 0) {
        exec("perl", "$run_dir/sas_data.pl", "--type", "INVENTORY_SAS_PROD", "--database", "sasprd", "--merchyear", $year, "--merchweek", $week);
        exit 0;
    }
    push @pids, $pid;
}
close $fh2;

# ONORDER
print "\nCreating OnOrder\n";
my $pid = fork();
if ($pid == 0) {
    exec("perl", "$run_dir/sas_data.pl", "--type", "ONORDER", "--database", "rms_p_force");
    exit 0;
}
push @pids, $pid;

# WAIT FOR ALL CHILD PROCESSES
foreach my $p (@pids) {
    waitpid($p, 0);
}
print "WooHoo files have been created: " . strftime("%H:%M:%S", localtime) . "\n";

# SCRUB DUPLICATES
print "\nScrubbing MFINC for duplicates...\n";
my $data_dir = "/usr/local/mccs/data/sas/";
chdir $data_dir or die "Can't cd to $data_dir: $!";
my @files = glob("MFINC*");

foreach my $file (@files) {
    my $dup_file = $file =~ s/001/002/r;
    my $clean_file = $file =~ s/001/003/r;

    system("sort $file | uniq -d > $dup_file");
    my $lines = `wc -l < $dup_file`;
    chomp $lines;

    if ($lines == 0) {
        unlink $dup_file;
    } else {
        print "Duplicate found in $file\n";
        system("sort $file | uniq -u > $clean_file");
        unlink $file;
    }
}

# ARCHIVE FILES
print "\nArchiving files...\n";
my $archive_dir = "/usr/local/mccs/data/sas/03_Weekly/" . strftime("%Y%m%d", localtime) . "_weekly";
mkdir $archive_dir unless -d $archive_dir;

system("mv /usr/local/mccs/data/sas/MERCH* $archive_dir/");
system("mv /usr/local/mccs/data/sas/MINV* $archive_dir/");
system("mv /usr/local/mccs/data/sas/MFINC* $archive_dir/");
system("mv /usr/local/mccs/data/sas/MON* $archive_dir/");

print "SAS Weekly Input Complete\n";
print "Completion Time: " . strftime("%H:%M:%S", localtime) . "\n";

my $directory = "/usr/local/mccs/data/sas/03_Weekly/" . strftime('%Y%m%d', localtime) . "_weekly";
mkdir $directory unless -d $directory;


foreach my $pattern (qw(MERCH* MINV* MFINC* MON*)) {
    foreach my $file (glob "/usr/local/mccs/data/sas/$pattern") {
        move($file, $directory) or warn "Could not move $file to $directory: $!";
    }
}




