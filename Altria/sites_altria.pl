#!/usr/local/mccs/perl/bin/perl
#---------------------------------------------------------------------
# Program    : sites_altria.pl
# Author     : Kaveh Sari
# Created    : Wed Aug  9 09:19:32 EDT 2023
#
# Description:
#
# Requestor  :
# Ported by  : Hanny Januarius
# Date       : Thu Dec  7 08:11:38 EST 2023
#---------------------------------------------------------------------
use strict;
use warnings;
use IBIS::DBI;


# Flush output
local $| = 1;

my $super_dbh = IBIS::DBI->connect( dbname =>'MVMS-Middleware-RdiUser');

    my $sql = 'select * from site_altria where rownum < 10';
    my $sth = $super_dbh->prepare($sql) or fatal_error("Cannot prepare $sql");
     $sth->execute;   
    my @array = ();
    while (my $row = $sth->fetchrow_array) {
        push(@array, $row);
        print $row." \n"  if ($DEBUG); 
    }
  