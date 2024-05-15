#!/usr/local/mccs/perl/bin/perl
#-------------------------------------------------------------------------
# Ported by: Hanny Januarius
# Date: Thu Dec  7 10:34:44 EST 2023
# Desc:
#       The goal of the code is to confirm if all invoice files 
#       downloaded from SPS has been loaded to irdbp edi_invoice table
## 1, get a list of the files downloaded today process the buffer into 
#     a list of files
## 2, get connection to irdb database check each of the file name 
#     in edi_invoices table.
## 3, send emails if not all files are found loaded. 
#
#-------------------------------------------------------------------------

use strict;
use Data::Dumper;
use IBIS::DBI;
use IBIS::Email;
use IBIS::Log::File;

my $log_filename      ="/usr/local/mccs/log/850/VALIDATe_invoice_loading.log";
my $log  = IBIS::Log::File->new({
                                 file   => $log_filename,
                                  append => 1
                                 }
			        );
$log->log_info("\n**********");
$log->log_info("Program started");
## The goal of the code is to confirm if all invoice files downloaded from SPS 
## has been loaded to irdbp edi_invoice table

## 1, get a list of the files downloaded today 
## process the buffer into a list of files

my $buffer =  `find /usr/local/mccs/data/e850/archive/invoice/xml/  -mtime -1`;
my @list = split(/\n/, $buffer);
my (@downloaded, @indb, @missed);

foreach my $file(@list){
    $file =~ /\/(IN.*\.7R3)/g;
    push(@downloaded, $1);
}
## print Dumper(\@downloaded);

## 2, get connection to irdb database 
my $dbh = IBIS::DBI->connect(dbname => 'irdb')
    or die("Cannot connect to irdb database\n");
my $query = "select count(*) from edi_invoices where source_file = ? "; 
my $sth = $dbh->prepare($query);

foreach my $file(@downloaded){
    if($file =~ /\d+/g){
	$sth->execute($file);
	my $ret_ref = $sth->fetchall_arrayref();
	my $count = $ret_ref->[0][0];
	if($count > 0){
	    ## do nothing
	}else{
	    push(@missed, $file);
	}
    }
}

## check each of the file name in edi_invoices table.
## put files not in the table into a list
## 3, send emails if not all files are found loaded. 

my $size = @missed;

if ($size >0){
    my ($from, $to, $subject, $content);
    $from ='rdistaff@usmc-mccs.org';
    $to    ='rdistaff@usmc-mccs.org';
    $subject ="Warning: Some invoice files did not load!";
    $content = Dumper(\@missed);
    $log->info($subject . $content);
    sendmail($from,$to,$subject, $content);  

}else{
    $log->info("All invoice files fetched in the last 24 hours are validated.");
}

# log end time
$log->info("Program Ended");

