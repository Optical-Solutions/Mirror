#!/bin/env perl
use strict;
use warnings;
use MCCS::MCE::Util;
use Net::SFTP::Foreign;
use Data::Dumper;
 

eval {
        my $name = shift();
        print "Secret name = $name\n";
        my $util = MCCS::MCE::Util->new();
        my $secret = $util->get_secret($name);
        
        if ( defined($secret) ) {
            my $sftp_server = $secret->{'host'};
            $util->print_secret($name);
            my $sftp = Net::SFTP::Foreign->new(
                                    $sftp_server,
                                    user     => $secret->{'user'},
                                    password => $secret->{'password'} || $secret->{'pw'},
                                    port     => $secret->{'port'} || '22',
                                    more     => '-v'
                                  );
            $sftp->die_on_error("Unable to establish SFTP connection to $sftp_server\n");

            my @output = $sftp->ls("/app/mdi/mccs/");
            print Dumper \@output;
        }

} or do {
        print "ERROR!\n$@\n";
};