#!/usr/local/mccs/perl/bin/perl
# Compliance Management Solution
use strict;
use MCCS::Config;
use MCCS::WMS::Sendmail;

my $sm  = MCCS::WMS::Sendmail->new();





#------------------------------------------------------------------------------
#  Application Processes    - Meat and Potatoes
#* For each type in an array we will fork da process and give each type their
#  own log.
#* The Parent will wait till all their children come home to complete its' task
#  and clean up   
#------------------------------------------------------------------------------
sub mainLine {
     send_mail('This is to check if send_mail is working');
}



#----------------------------------------------------------------#
# Routines for Random all Other needed Stuff-Logs,Emails,Markers
#----------------------------------------------------------------#
sub send_mail {
    #my @body = @_; #Just going to put every thing in the body good or bad
    my %args = @_;
    my @body = ('bod 1', 'bod 2');
    my $emails;
    $emails->{kav} ='kaveh.sari@usmc-mccs.org'; 

	$sm->subject('XXYYZZ Sending from  '.`hostmane`);
	$sm->sendTo($emails);
    $sm->msg(@body);
	#$sm->msg( 'THERE is no MSG '  );
	#$sm->logObj($log_obj);
	#$sm->verboseLevel($debug);
#$sm->msg( $args{body} );

	$sm->hostName(`hostname`);
	
	
    $sm->send_mail();
} 





#---------------------------------------------------------------------
# MAIN PROGRAM - Do not modify main below.
# Use the main above aka my_main
#---------------------------------------------------------------------

# Want to catch warning
$SIG{__WARN__} = sub { log_warn("@_") };

# Execute the main
eval { mainLine() };
if ($@) {
	fatal_error($@);
}

#---------------------------------------------------------------------
# End program
#---------------------------------------------------------------------

