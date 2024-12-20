my $lock_file = "/tmp/lock" . ".lck";
open(my $fh, ">", $lock_file) or die "Could not create lock file $lock_file";
flock $fh, LOCK_EX | LOCK_NB or die "Another $0 process already running";
sleep 60;
#close $fh;  #TODO Needs verification.