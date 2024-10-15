#!/usr/local/mccs/perl/bin/perl
#--------------------------------------------------------------------------
#Ported by : Hung Nguyen
#Date      : 12/07/2023
#
#Brief Desc: This script is meant to check on the health and status of the 
#            system and send out notifications if there are any problems.  

# Updated by Kaveh Sari 
# Porting Complete  October 15, 2024 1:23:04 PM
# Restored to required original email targets from config files.            
#            
# --------------------------------------------------------------------------  

use strict;
use warnings;

use Sys::Hostname;
use MCCS::Utils qw( is_rdiusr is_running );
use IBIS::DBI;
use IBIS::Mail;
use IBIS::Log::File;
use LWP::Simple;

use version; our $VERSION = qv('0.0.1');

use constant LOGFILE => "/usr/local/mccs/log/system_monitor.log";

my $log = IBIS::Log::File->new( {file => LOGFILE, append => 1} );
my $host = hostname( );

#my $text_msg_eric = '7033077065@vtext.com';
my $cc_list = [];

#if ( 'softtail' ne $host && 'knucklehead' ne $host ) { $cc_list = [ $text_msg_eric ]; }

sub notify {
    my ( $arg_ref ) = @_;

    $log->info( $arg_ref->{subject} );
    my $m = IBIS::Mail->new(
        to      => [ 'rdistaff@usmc-mccs.org' ],
        cc      => $cc_list,
        from    => "$host <ibis\@usmc-mccs.org>",
        type    => 'text/plain',
        subject => $arg_ref->{subject},
        body    => $arg_ref->{body},
    );

    $m->send( );


}

sub check_webserver {
    my $url = 'http://localhost' ;
    my $content = get $url;
    unless ( defined $content ) {
        notify( { subject => "Webserver is down on $host",
                  body    => "Webserver is down on $host\nSo maybe you should do something about that\n\n/usr/local/mccs/bin/system_monitor.pl\n\n\n\nserver: $host",
                } );
    }
}

sub check_database {
    my $ibis_dbh;
    eval {
       $ibis_dbh = IBIS::DBI->connect( dbname => 'ibisora' );
    };

    if ( $@ ) {
        notify( { subject => "Database is down on $host",
                  body    => "Database is down on $host\nSo maybe you should do something about that\n\n/usr/local/mccs/bin/system_monitor.pl\n\n\n\nserver: $host",
                } );
    }
}

check_webserver();
check_database();


