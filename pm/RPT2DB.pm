package IBIS::RPT2DB;
use strict;
use base ('Class::Accessor');
use IBIS::Config::Auto;
use Data::Dumper;
use File::Basename;
use POSIX qw(strftime WNOHANG);
use vars '$AUTOLOAD';
use IBIS::DBI;
use IBIS::Log::File; 


## 1, constructor
sub new {
    my ( $class, %args ) = @_;
    my $self = {};
    bless $self, $class;    
    $self->_make_accessors(\%args );
    return $self;
}
## 2, set debug mode
sub set_debug{
    my ($self, $debug) = @_;
    $self->{'DEBUG'} = $debug;
}
## 3, private function to read input config values
sub _make_accessors {
    my ( $self, $args ) = @_;

    my $config = Config::Auto::parse( $args->{conf_file}, format => $args->{conf_format} || 'equal' );

    Class::Accessor->mk_accessors( keys %{$config} );
    foreach ( keys %{$config} ) {
        $self->$_( $config->{$_} );
    }
}
## 4, 
sub AUTOLOAD {
	my ($self) = @_;
	if ( $AUTOLOAD =~ /.*::_get_(\w+)/ ) {
		if ( exists $self->{$1} ) {
		    return $self->{$1};
		}
		else {
		     print "No such attribute: $1\n";
		}
	}
	else {
		die "No such method: $AUTOLOAD\n";
	}
}

#1, function to return table names by type from config information
## one type at a time. 
sub get_report_table_names_by_type{
    my ($self, $type) = @_;
    my $tables;
    my $tab_str;
    ## assume the table list is defined like PO_REPORT_TABLES, OR ASN_REPORT_TABLES etc..
    if($type){
	my $msg ="report table type:".$type;
	$self->{'log_obj'}->info($msg);
	my $function = '_get_'.uc($type).'_REPORT_TABLES()';
	$tab_str = $self->$function;
	unless($tab_str){
	    my $msg ="Failed to retrieve table type:".$type;
	    $self->{'log_obj'}->info($msg);
	    die();
	}
    }else{
	my $msg ="Missing report table. program exiting...";
	$self->{'log_obj'}->info($msg);	
	die();
    }    
    
    foreach my $tab(split(/[\||\,]/, $tab_str)){
	push(@$tables, $tab);
    }
    return $tables;
}

## input a TABLENAME,
## output a hash ref for all the files in the report table so far

sub _cache_all_report_files{
    my ($self, $tablename) = @_;
    unless($tablename){
	my $msg = 'Missing required table name value to get cache'.__LINE__;
	$self->{'log_obj'}->info($msg);
	die;
    }
 
    my $hash_ref;
    $tablename = uc($tablename); ## set to upper case
    my $sth = $self->{'dbh_obj'}->prepare(
        qq{
         SELECT 
            DISTINCT SOURCE_FILE
         FROM 
            $tablename
         }
	);
    
    $sth->execute();
    my $col_list = $sth->fetchall_arrayref(
        {   
	    source_file           => 1 
        }
	);
    foreach my $r ( @{$col_list} ) {
        if( $r->{source_file} ){
	    $hash_ref->{$r->{source_file}} = 1;
	}
    }
    return $hash_ref;
}

sub _get_column_list_by_tabname{
    my ($self, $tabname) = @_;
    
    my $column_ref;
    $tabname = uc($tabname); ## set to upper case
    my $sth = $self->{'dbh_obj'}->prepare(
        qq{
         SELECT 
            COLUMN_ID, 
            COLUMN_NAME 
         FROM 
            ALL_TAB_COLUMNS 
         WHERE 
            TABLE_NAME =\'$tabname\' 
         }
	);
    
    $sth->execute();
    my $ret_ref;
    
    my $col_list = $sth->fetchall_arrayref(
        {   column_id           => 1,
	    column_name         => 1
        }
	);
    
    foreach my $r ( @{$col_list} ) {
        if( $r->{column_id} ){
	    $column_ref->{$r->{column_id}} = $r->{column_name};
	}
    }
    $ret_ref->{$tabname} = $column_ref;
    return $ret_ref;
}

## return template from table name, and column reference
sub create_insert_report_template{
    my ($self, $tablename, $column_ref, $date_format) = @_;
    
    my $ret_query = "";
    my $temp_param ="";
    unless($date_format){
	$date_format ='MM/DD/YYYY'; 
    }
    
    $ret_query .= "INSERT INTO ".uc($tablename)."  ("; 
    foreach my $key (sort {$a<=>$b} keys %{$column_ref->{$tablename}}) {
	if($key =~ /\w+/ig){
	    $ret_query .= uc($column_ref->{$tablename}->{$key}).",";
	    if  ((uc($column_ref->{$tablename}->{$key}) =~ /^DATE|DATE$/g)
		 ||(uc($column_ref->{$tablename}->{$key}) =~ /^TIME|TIME$/g)
		 ||(uc($column_ref->{$tablename}->{$key}) =~ /\_DATE\_/g)) ## silly
	    {
		if(uc($column_ref->{$tablename}->{$key}) eq 'REPORT_DATE'){ ## ALWAYS THIS FORMAT
		     $temp_param .= ' to_date('.' ? '.', \'MM/DD/YYYY\'),';
		}else{
		    $temp_param .= ' to_date('.' ? '.', \''.$date_format.'\'),';
		}

	    }else{
	        $temp_param .= " ?,";
	    }
	}
    }
    
    $temp_param =~ s/\,$//g;
    $ret_query  =~ s/\,$//g;
    $ret_query .= ") values ( ".$temp_param." )";
    return $ret_query;
}

## input: file name;
## output: hash_ref arrays, header, detail or uniform

sub parse_binding_values_from_file{
    my ($self, $filename, $delimiter, $lf_file) = @_;
    my $hdr_ref;
    my $dtl_ref;
    my $data_ref;
    my $hd_flag = 0; ## a flag indicating file has header and detail sections
    my $uniform_ref;

    if(!$delimiter){
	$delimiter ="|";
    }

    open(IN, $filename)|| die "can not open infile: $filename\n";
    while (my $line = <IN>){
	chomp($line); ## 
	if($line =~ /\w+/g){
	    ## chomp($line); ## 
	    my $date   = strftime( '%m/%d/%Y', localtime );
	    my @ary;
	    if($delimiter eq '|'){
		$line .= '|'.$date; 
		$line .= '|'.$lf_file;
		@ary = split(/\|/, $line);
	    }elsif ( $delimiter eq ','){
		$line .= ','.$date; 
		$line .= ','.$lf_file; 
		@ary = split(/\,/, $line);
	    }else{
		my $msg = "can not handle this type of delimiter yet!!!. Need to confirm if this works...\n";
		$self->{'log_obj'}->info($msg);
		@ary = split(/$delimiter/, $line);	
	    }

	    if (uc($ary[0]) eq 'HEADER'){
		unless($hd_flag){
		    $hd_flag = 1;
		}
		shift (@ary); 
		
		##print "AFTER shift:\n\n";
		##print Dumper(\@ary);
		my $size = @ary;
		##if ($lf_file =~ /^PO/g){
		## print "lllllllllllllllllf_file: $lf_file";
		if ((substr($lf_file, 0, 2) eq 'PO')&&($size == 16)){
	        ## if (($lf_file =~ /PO/g)&&($size == 16)){###???????????? NOT WORKING!!!! WHY????????????????????
		    my $msg = "CCCCCCCCCCCCCCCCase found\n\n";
		    $self->{'log_obj'}->info($msg);
		    splice @ary, 14, 0, ''; 
		}else{
		    my $msg ="SSSSSSSSSSSSSSsize of ary". $size;
		    my $buffer = Dumper(\@ary);
		    $self->{'log_obj'}->info($msg);
		    $self->{'log_obj'}->info($buffer);
		}

		if ($self->{'DEBUG'}){ 
		    print "AFTER shifting:\n";
		    print Dumper(\@ary);
		}
		if($ary[0]&&$ary[1]){
		   push(@$hdr_ref, \@ary);
	        }
	    }elsif (uc($ary[0]) eq 'DETAIL'){
              
		unless($hd_flag){
		    $hd_flag = 1;
		}
		
		shift (@ary);
		if ($self->{'DEBUG'}){ 
		    print "AFTER shifting:\n";
		    print Dumper(\@ary);
		}
		if($ary[0]){ 
		   push(@$dtl_ref, \@ary);
	        }
	    }else{ 	
		if($ary[0]){ 
		    ## custom code for stripping T in time string:
		    for (my $i=0; $i<@ary; $i++){

			if($ary[$i] =~ /\d{4}\-\d{2}\-\d{2}T\d{2}\:/g){
			    $ary[$i] = $self->replace_T($ary[$i]);   
			}

			if($ary[$i] =~ /\d{8}T\d{2}\:\d{2}/g){
			    $ary[$i] = $self->reformat_and_replace_T($ary[$i]);   
			}

		    }
		    push(@$uniform_ref, \@ary);
	        }
	    }
	    
	}
    }
    close IN;
    if($hd_flag){
	$data_ref->{'HEADER'} = $hdr_ref;
	$data_ref->{'DETAIL'} = $dtl_ref;
    }else{
	$data_ref->{'UNIFORM'} = $uniform_ref;
    }
    
    return $data_ref;
}


sub  reformat_and_replace_T{
 my ($self, $time) = @_;
    if($time =~ /\d{8}T\d{2}\:\d{2}/g){
	$time =~ s/T/ /g;
	my $yyyy = substr($time, 0, 4);
	my $mm   = substr($time, 4, 2);
	my $dd   = substr($time, 6, 2);
	my $rest = substr($time, 8, );
	$time = $yyyy.'-'.$mm.'-'.$dd.$rest;
    }else {
	## print "not matching";
    }
    return $time;  
}


sub replace_T{
    my ($self, $time) = @_;
    if($time =~ /\d{2}\-\d{2}T\d{2}\:/g){
	$time =~ s/T/ /g;
    }else {
	## print "not matching";
    }
    return $time;   
}


## input: a single template, and a single type of binding values ref
## output:  load errors.

sub load_table_by_template_and_bindingvals{
    my ($self, $template, $data_ref) = @_;
    my $errors;
    ##my $buffer = Dumper($data_ref);
    ##$self->{'log_obj'}->info($buffer);
    my $sth = $self->{'dbh_obj'}->prepare($template);
   
    eval{ 
	##if($data_ref->[0]) {
	    #my $msg ="data_ref0:".$data_ref->[0];
	    #$msg .= "data_ref1:".$data_ref->[1];
	    #$self->{'log_obj'}->info($msg);
	    $sth->execute(@{$data_ref}); 
	##}
    };
    
    if( $@ ) {
	my $msg = "DB error:".$@;
	$self->{'log_obj'}->info($msg);
	push(@$errors, $@);	
	$self->{'dbh_obj'}->rollback;
    }else{
	$self->{'dbh_obj'}->commit;
    }
    
    return $errors;
}

## this is NOT a generic function. It is an application for PO and ASN only...
## input: type, date(opt), tablename, files_dir_path
## output: loading files into the tables from the files in the directory path
sub wms_load_report_table{
    my ($self, $type, $date, $tablename, $input_dir, $delimiter) = @_;
    $tablename = uc($tablename);
    unless($type){
	my $msg ='Missing requried value to run, need a type such as PO, or ASN';
	$self->{'log_obj'}->info($msg);
	die();
    }
    unless($date){
	$date   = strftime( '%Y%m%d', localtime ); 
    }

    unless($self->{'dbh_obj'}){
	my $connection_id = $self->_get_CONNECT_INSTANCE();
	unless($connection_id){
	    print "You need a CONNECT_INSTANCE value like mc_p, or mc_s in config file.\n";
	    die;
	}
	$self->sget_dbh_obj($connection_id);
    }

    unless($delimiter){
	$delimiter ='|';
    }
    
    my $data_dir; ## where the files are
    my @files;    ## files need to load
    my $loaded_already; 

    ## get all the files need for loading, also a hash reference for what has been loaded already
    if ($input_dir){
	my $msg ='This will be the input directory where the data files are located:'.$input_dir;
	$self->{'log_obj'}->info($msg);
        $data_dir = $input_dir;
       ## this need to tie the table name to the input directory;
	unless($tablename){
	    my $msg ='You have to have table name when using input directory ';
	    $self->{'log_obj'}->info($msg);
	    die;
	}
	$loaded_already = $self->_cache_all_report_files($tablename);
	
    }else{
	if (uc($type) eq 'PO'){
	    $input_dir = $self->_get_PO_ARCHIVE_DIR();
	    my $po_hdr_table = $self->_get_PO_REPORT_HDR_TABLE();
	    $loaded_already = $self->_cache_all_report_files($po_hdr_table);
	}elsif(uc($type) eq 'ASN'){
	    $input_dir = $self->_get_ASN_ARCHIVE_DIR();	 
	    my $asn_hdr_table = $self->_get_ASN_REPORT_HDR_TABLE();
	    $loaded_already = $self->_cache_all_report_files($asn_hdr_table);
	}	
	$data_dir = $input_dir.'/'.$date;
    }		
    
    if (uc($type) eq 'PO'){
	@files = glob "$data_dir"."/PO*";
    }elsif(uc($type) eq 'ASN'){
	@files = glob "$data_dir"."/ASN*";
    }else{
	@files = glob "$data_dir"."/".$type."*"; ## get anything in the input directory
    }

    ## get all templates for the file
    my $template_h; 
    my $template_d; 
    my $template_u; 
    my $report_tables; 
    my $template_ref;  

    $report_tables = $self->get_report_table_names_by_type($type);
    
    foreach my $table(@$report_tables){
	$template_ref->{uc($table)} = $self->_get_column_list_by_tabname($table);
    }
    
    ## get file binding values:
    ## here we assume po, asn report files will have file names contains words like PO, or ASN 

    foreach my $file(@files){
	my $lf_file = basename($file);

	if($loaded_already->{$lf_file}){
	    my $msg ="\nSKIPPING file:".$lf_file;
	    $self->{'log_obj'}->info($msg);
	    next;
	}

	my $data_ref = $self->parse_binding_values_from_file($file, $delimiter,  $lf_file);
	my $errors;
	unless ( -s $file ){
	    next; ## file needs to be non zero size
	}
	unless ($lf_file){
	    next;
	}
	if($lf_file =~ /po/ig){
	    my $po_hdr_table = $self->_get_PO_REPORT_HDR_TABLE();
	    my $po_dtl_table = $self->_get_PO_REPORT_DTL_TABLE();
	    $template_h =  
		$self->create_insert_report_template($po_hdr_table, $template_ref->{$po_hdr_table});
	    $template_d =  
		$self->create_insert_report_template($po_dtl_table, $template_ref->{$po_dtl_table});
	}elsif($lf_file =~ /asn/ig){
	    my $asn_hdr_table = $self->_get_ASN_REPORT_HDR_TABLE();
	    my $asn_dtl_table = $self->_get_ASN_REPORT_DTL_TABLE();
	    $template_h =   
		$self->create_insert_report_template($asn_hdr_table,  $template_ref->{$asn_hdr_table});
	    $template_d =  
		$self->create_insert_report_template($asn_dtl_table,  $template_ref->{$asn_dtl_table});
	}else{
	    $template_u = 
		 $self->create_insert_report_template($tablename,  $template_ref->{uc($tablename)});
	}
	
	if ((substr($lf_file, 0, 2) eq 'PO') || (substr($lf_file, 0, 3) eq 'ASN')){ 
	## if ($lf_file =~ /po|asn/ig){ ?????????????????
	## if (($lf_file =~ /po/ig) or ($lf_file =~ /asn/ig)) { ?????????????? WTF???
	    foreach my $val_ref (@{$data_ref->{'HEADER'}}){
		$self->load_table_by_template_and_bindingvals($template_h, $val_ref);
	    }
	    
	    foreach my $val_ref (@{$data_ref->{'DETAIL'}}){
		$self->load_table_by_template_and_bindingvals($template_d, $val_ref);
	    }	

	}else{
	    foreach my $val_ref (@{$data_ref->{'UNIFORM'}}){
		$self->load_table_by_template_and_bindingvals($template_u, $val_ref);
	    }
	}
    }
}


sub sget_dbh_obj{
    my ($self, $connection_id) = @_;
    unless($connection_id){
	$connection_id = $self->_get_CONNECT_INSTANCE();
    }
    if($self->{'dbh_obj'}){
	return $self->{'dbh_obj'};
    }else{
	$self->{'dbh_obj'} = IBIS::DBI->connect(
	    dbname  => $connection_id,
	    attribs => { AutoCommit => 0 }
	    )
	    or $self->{'log_obj'}->log_die(
		"Cannot connect database using: 
            $connection_id\n"
	    );
	return $self->{'dbh_obj'};
    }
}

sub sget_log_obj{
    my ($self, $log_file) = @_;
    if($self->{'log_obj'}){
	return $self->{'dbh_obj'};
    }else{
	unless($log_file){
	    $log_file = $self->_get_LOG_FILE(); ## required in config
	    unless($log_file){
		my $msg = "YOU are missing a log file with a written permission";
		print "$msg\n";
		die;
	    }
	}
	$self->{'log_obj'}     = IBIS::Log::File->new(
	    {
		file   => $log_file,
		append => 1
	    }
	    );
	return $self->{'log_obj'};
    }
}

## this is an application  to use the above functions
## input: your file type, tablename you want to load into, files_dir_path where your files are
## output: loading files into the tables from the files in the directory path
sub load_a_report_table{
    my ($self, $input_dir, $type, $delimiter, $date_format) = @_;
   

    unless($input_dir){
	my $msg ='You need both table name, and directory path for input files to run the program.\n';
	$self->{'log_obj'}->info($msg);
	print $msg;
	die;
    }

    my $tablename = $self->_get_DATA_REPORT_TABLE();
    unless ($tablename){
	my $msg ="You missed a config value in your config file key: DATA_REPORT_TABLE";
	$self->{'log_obj'}->info($msg);
	die;
    }

    unless($self->{'dbh_obj'}){
	my $connection_id = $self->_get_CONNECT_INSTANCE();
	unless($connection_id){
	    print "You need a CONNECT_INSTANCE value like mc_p, or mc_s in config file.\n";
	    die;
	}
	$self->sget_dbh_obj($connection_id);
    }
  
    unless($self->{'log_obj'}){
	$self->{'log_obj'} = $self->sget_log_obj();
    }

    unless($delimiter){
	$delimiter ='|';
    }

    $tablename = uc($tablename); 
    my @files;    
    if($type){
	@files = glob "$input_dir"."/".$type."*"; 
    }else{
	@files = glob "$input_dir"."/"."*"; 
    }

    my $loaded_already = $self->_cache_all_report_files($tablename);


    my $template_u;  
    my $template_ref; 
    
    $template_ref->{$tablename} = $self->_get_column_list_by_tabname($tablename);
    
    foreach my $file(@files){
	my $lf_file = basename($file);
	
	if($loaded_already->{$lf_file}){
	    my $msg ="\nSKIPPING file:".$lf_file;
	    $self->{'log_obj'}->info($msg);
	    next;
	}
	
	my $data_ref = $self->parse_binding_values_from_file($file, $delimiter,  $lf_file);
	my $errors;
	
	unless ( -s $file ){
	    next; ## file needs to be non zero size
	}
	unless ($lf_file){
	    next;
	}
	$template_u = 
	    $self->create_insert_report_template($tablename,  $template_ref->{$tablename}, $date_format);
	
	foreach my $val_ref (@{$data_ref->{'UNIFORM'}}){
	    $self->load_table_by_template_and_bindingvals($template_u, $val_ref);
	}
    }   
}

sub destructor{
    my ($self) = @_;
    if ($self->{'dbh_obj'}){
	$self->{'dbh_obj'}->disconnect;
    }
    $self = undef;
}

sub DESTROY{
    my ($self) = @_;
    if ($self->{'dbh_obj'}){
	$self->{'dbh_obj'}->disconnect;
    }
    $self = undef;
}

1;


=head1   NAME

RPT2DB.pm

This is a package for loading regular tabulated data into a DB table. 

=head1  SYNOPSIS

use IBIS::RPT2DB;

my $wms = IBIS::RPT2DB->new( conf_file => '/dir_path_to_a_config_file/test_run.conf' );

$wms->load_a_report_table('/dir_path_to_your_data_file/', 'filename_pattern', '|_delimiter','Date_format_in_your_file');

see DESCRIPTION for details.

=head1    DESCRIPTION

You need the following 3 things done before you can use the package:

## 1, you need to create a table for all the fields in the file, plus two more fields, report_date, and source_file (for tracking time, and data source for each row).

Example:

create table test_report_t(

PO_ID               NUMBER(10)     NOT NULL,

VERSION_NO          NUMBER(5)      NOT NULL,

BAR_CODE_ID         VARCHAR2(30)   NULL,

QTY_ORDERED         NUMBER(11,3)   NULL,

...

...

(need to add extra two fields for tracking time and filename)...

REPORT_DATE         DATE           NOT NULL,

SOURCE_FILE         VARCHAR2(100)  NULL

);


## 2, you need to have a config file containing at least 3 items including:

CONNECT_INSTANCE=rms_p (db connection id you want to use)

LOG_FILE=/home/rdiusr/mytest.log(a log file you want to use)

DATA_REPORT_TABLE=TEST_REPORT_T(name of the table you created in the first step)



## 3, you need to have the following information to call load_a_table function:
 
a, Directory path where your data files are: '/full_path_to_your_dirctory/', (case sensitive)

b, Data file name starting with letters such as 'PO' (case sensitive, optional)

c, The delimiter in your data files (pipe or comma only) (optional, if use pipe)

d, If you have date in your file, the DATE format such as, 'mm/dd/yyyy' or 'DD-Mon-YY' in your file. (optional)


Here is the list of all the public functions in the package:

sub new {

need a configuration file. see description

sub set_debug{

sub get_report_table_names_by_type{

sub get_column_list_by_tabname{

sub create_insert_report_template{

sub parse_binding_values_from_file{

sub load_table_by_template_and_bindingvals{

sub sget_dbh_obj{

sub sget_log_obj{

sub load_a_report_table{

    1, you need input directory path for your file

    2, file name pattern, 

    3, delimiter of your file ('|' or ',') 

    4, DATE format

sub destructor

=head1    BUGS/CAVEATS/etc

a, when you create the table, make sure the order of the fields is the same as what are in the file. 

b, The delimiters allowed in the files are '|' (pipe) or ',' (comma) only.  

c, If you have Date in you file, your date format is other than  'MM/DD/YYYY', you need to call  the the function, load_a_report_table with the date format you need such as 'DD-Mon-YY' that matches your data. 


=head1    AUTHOR

Chunhui Yu<yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>

=head1    DEPENDENCIES

IBIS::DBI;

IBIS::LOG::File;

=head1    COPYRIGHT and LICENSE

Copyright (c) 2015 MCCS. All rights reserved.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITE
D TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SH
ALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTI
ON OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTH
ER DEALINGS IN THE SOFTWARE.

=cut

##################
