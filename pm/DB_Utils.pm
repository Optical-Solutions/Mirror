package IBIS::DB_Utils;

use strict;
use Carp;
use base qw(Exporter);
use IBIS::DBI;

our (@EXPORT_OK, %EXPORT_TAGS);

@EXPORT_OK = qw(   
irdb_connect
rms_p_connect
ibisora_connect
ibisora_connect_nc
);

%EXPORT_TAGS = (
    ALL => [ @EXPORT_OK ],
    );

sub rms_p_connect   { IBIS::DBI->connect(dbname => 'rms_p');   }
sub irdb_connect    { IBIS::DBI->connect(dbname => 'irdb');    }
sub ibisora_connect { IBIS::DBI->connect(dbname => 'ibisora'); }
sub ibisora_connect_nc { 
  IBIS::DBI->connect(dbname => 'ibisora', attribs => {AutoCommit => 0}); 
}

1;
