use Net::SFTP::Foreign;

my $sftp = Net::SFTP::Foreign->new(
	'landair-mft.mercurygate.net',
	user     =>'landair',
	password => 'pG7n!mR9d')
	|| die " Can not connect to remote server: landair-mft.mercurygate.net \n";
