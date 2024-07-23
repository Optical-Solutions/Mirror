use Net::SFTP::Foreign;
my $hosname =	'landair-mft.mercurygate.net';
my $username = 'landair';
my $password = 'pG7n!mR9d';
my $sftp = Net::SFTP::Foreign->new($hostname, user => $username, password => $password);
print $sftp;
