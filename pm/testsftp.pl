use Net::SFTP::Foreign;

my $sftp = Net::SFTP::Foreign->new(
	'landair-mft.mercurygate.net',
	user     => 'landair',
	password => 'pG7n!mR9d')
	|| die " Can not connect to remote server: landair-mft.mercurygate.net \n";
print $sftp;

# ==========================================================================================
#  # Create an SFTP object
#  my $sftp = Net::SFTP::Foreign->new($hostname, user => $username, password => $password);


#  # Check for errors in creating the SFTP object
#  die "Could not establish SFTP connection: " . $sftp->error unless defined $sftp;

#  # Upload the local file
#  $sftp->put($local_path, $remote_path) or die "Failed to upload file: " . $sftp->error;
#  ========================================================================================