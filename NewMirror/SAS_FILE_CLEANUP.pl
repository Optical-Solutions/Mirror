#!/usr/local/bin/perl --
#-----------------------------------------------------------------------------
# Ported by: Hanny Januarius
# Date: Mon Dec 11 09:00:15 EST 2023
# Desc:
#
# Purpose of this script is to allow for periodic cleaning of the following files produced by SAS RDI processes:
#  Daily Merch & misc files
#  Weekly FACT & other misc files
#  Daily Process Log files
#-----------------------------------------------------------------------------
#
# Initialize parameters
use strict;
use warnings;
use Carp;
use IO::File;

my ( $display_mode, $sas_arch_days_to_retain, $cleanupdaily, $cleanupweekly,
    $cleanuplogs ) = @ARGV;

# Clean up daily directories older than specified # of days
cleanupdirs( "/usr/local/mccs/data/sas/02_Daily/", $display_mode ) if $cleanupdaily eq 'Y';

# Clean up weekly directories older than specified # of days
cleanupdirs( "/usr/local/mccs/data/sas/03_Weekly/", $display_mode ) if $cleanupweekly eq 'Y';

# Clean up daily log files older than specified # of days
cleanuplogs( "/usr/local/mccs/data/sas/05_Logs", $display_mode ) if $cleanuplogs eq 'Y';

sub cleanupdirs {
    my $dir_sas_archive_files = shift;
    my $display_mode          = shift;
    my $DH;
    opendir( $DH, $dir_sas_archive_files )
      or die "Can not open $dir_sas_archive_files: $!";
    foreach ( readdir $DH ) {
        if (   -d "$dir_sas_archive_files$_"
            && -M "$dir_sas_archive_files$_" > $sas_arch_days_to_retain
            && $_ !~ /^\./ )
        {
            if ( $display_mode =~ /Y|y/ ) {
                system "ls -dl $dir_sas_archive_files$_";
            }
            else {
                system "rm -r $dir_sas_archive_files$_";
            }
        }
    }
    return closedir DH;
}

sub cleanuplogs {
    my $log_dir      = shift;
    my $display_mode = shift;

    # Clear out SAS log data older than specified # of days
    if ( $display_mode =~ /Y|y/ ) {
        return system "cd $log_dir;  find . -mtime +$sas_arch_days_to_retain -type f";
    }
    else {
        return system "cd $log_dir; find . -mtime +$sas_arch_days_to_retain -type f -exec rm {} \\;";
    }

}
