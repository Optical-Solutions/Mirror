##################################################################################
#      Routine to send notification emails
##################################################################################
sub send_mail {
		open( MAIL, "|/usr/sbin/sendmail -t" ); ## no critic qw(InputOutput::ProhibitBarewordFileHandles InputOutput::ProhibitTwoArgOpen InputOutput::RequireBriefOpen)
		print MAIL "To: kaveh.sari\@usmc-mccs.org \n";
		print MAIL "From: rdistaff\@usmc-mccs.org\n";
		print MAIL "Subject: testing email \n";
		print MAIL "\n";
		print MAIL "hello";
		print MAIL "kaveh";
		print MAIL "\n\nServer: " . `hostname` . "\n";
		print MAIL "\n";
		print MAIL "\n";
		close(MAIL);
}
send_mail();