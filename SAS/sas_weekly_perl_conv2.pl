#!/usr/bin/perl
use strict;
use warnings;
use POSIX qw(strftime);
use Cwd;

# ENVIRONMENT SETUP
$ENV{ORACLE_HOME} = "/usr/local/mccs/instantclient_12_1";
$ENV{LD_LIBRARY_PATH} = "/usr/local/mccs/instantclient_12_1";

# DIRECTORIES
my $run_dir     = "/usr/local/mccs/bin/SAS";
my $io_dir      = "/usr/local/mccs/data/sas/04_Runtime";
my $weekly_file = "$io_dir/merch_weeks";
my $sas_file    = "$io_dir/sas_weekly_sw.txt";

# TIME LOGGING
print "\nSAS Weekly Input Files Start\n";
print "Date: " . strftime("%Y-%m-%d", localtime) . "\n";
print "Script Start Time: " . strftime("%H:%M:%S", localtime) . "\n\n";

# ARGUMENT CHECK
my $arg = shift @ARGV || '';
if ($arg eq 'prev') {
    print "We will be processing previous 5 weeks\n";
} elsif ($arg eq 'curr') {
    print "We will be processing the last closed week. Meaning week end date of Sunday\n";
} else {
    die "Error: 'curr' or 'prev' argument not provided! Cannot proceed.\n";
}

# RUN SQL SCRIPTS
print "Running SqlPlus to start SAS Data Batch Process\n";
system("sqlplus -s eric/ericdata\@draix22.usmc-mccs.org/sastst \@$io_dir/sas_weekly_get_${arg}_weeks.sql > $weekly_file 2>/tmp/error1");
system("sqlplus -s eric/ericdata\@draix22.usmc-mccs.org/sastst \@$run_dir/sas_weekly_${arg}_start.sql 2>/tmp/erro2");

# PROCESS TYPES
my @types = qw(DIVISION LOB DEPARTMENT CLASS SUBCLASS STYLE_DAILY PRODUCT_DAILY);
my @pids;

foreach my $type (@types) {
    my $pid = fork();
    if ($pid == 0) {
        exec("perl", "$run_dir/sas_data.pl", "--type", $type, "--database", "rms_p_force");
        exit 0;
    }
    push @pids, $pid;
}

# CREATE SALES
print "\nCreating SALES\n";
print "Start Time: " . strftime("%H:%M:%S", localtime) . "\n";

open my $fh, '<', $weekly_file or die "Cannot open $weekly_file: $!";
while (my $line = <$fh>) {
    chomp $line;
    my ($rec, $year, $week) = map { s/\s+//gr } split /,/, $line;
    next unless $rec =~ /^rec/i;

    my $pid = fork();
    if ($pid == 0) {
        exec("perl", "$run_dir/sas_data.pl", "--type", "SALE_SAS_PROD", "--database", "rms_p_force", "--merchyear", $year, "--merchweek", $week);
        exit 0;
    }
    push @pids, $pid;
}
close $fh;

# WAIT LOOP
print "\nWaiting For creation of Sas_Prod_Complete to Finish\n";
my $done = 0;
my $check_count = 0;

while (!$done) {
    sleep 1200; # 20 minutes
    system("sqlplus -s eric/ericdata\@draix22.usmc-mccs.org/sastst \@$run_dir/sas_weekly_complete.sql > $sas_file 2>/tmp/error3");
    sleep 10;

    open my $sfh, '<', $sas_file or die "Cannot open $sas_file: $!";
    while (my $line = <$sfh>) {
        my $flag = (split /,/, $line)[1] // '';
        $flag =~ s/\s+//g;
        if ($flag eq "true") {
            $done = 1;
            last;
        }
    }
    close $sfh;

    $check_count++;
    if ($check_count == 3) {
        $check_count = 0;
        print "Still Waiting - Time is now -> " . strftime("%H:%M:%S", localtime) . "\n";
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
