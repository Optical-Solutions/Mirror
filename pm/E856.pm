package IBIS::E856;
@ISA = qw(IBIS::EDI);
use strict;
use Data::Dumper;
use IBIS::Crypt;
use Net::SFTP;
use IBIS::Email;
use IBIS::DBI;
use MIME::Lite;

##
## yuc added a test for type line 40. Aug 28, 14
##
## constructor
sub new {
    my ( $class, %args ) = @_;
    my $self = {};
    bless $self, $class;    
    $self->_make_accessors(\%args );
    return $self;
}

## utility functions
sub _sget_loading_log_list{
    my ($self, $asn_list_ref) = @_;
    unless($self->{'LOADING_LOG_REF'}){
	$self->{'LOADING_LOG_REF'}= $asn_list_ref;
    }
    return $self->{'LOADING_LOG_REF'};
}

sub _sget_parsed_data_ref{
    my ($self, $data_ref) = @_;
    unless($self->{'PARSED_DATA_REF'}){
	$self->{'PARSED_DATA_REF'}= $data_ref;
    }
    return $self->{'PARSED_DATA_REF'};
}


sub is_debug{
    my ($self)= @_;
    return $self->{'DEBUG'};
}

sub set_debug{
    my ($self, $debug) = @_;
    $self->{'DEBUG'} = $debug;
}

sub _sget_report_buffer{
    my ($self, $buffer) = @_;
    if($buffer){
	$self->{'REPORT_BUFFER'}= $buffer
    }
    return $self->{'REPORT_BUFFER'};
}

## main functions
## fetch loading logs from draix machines
sub scp_856_loading_logs{
    my ($self) = @_;
    my $scp_cmd =
	"scp  ".$self->{SFTP_USER}.'@'.$self->{LOG_SERVER}.
	':'.$self->{RMT_LOG_DIR}.$self->{LOG_FILE_PREFIX}.
	'*'."    $self->{LOCAL_LOG_STG_DIR}";   
	self->{'log_obj'}->info('password is ' . $c->decrypt( $self->{PASSWORD} ) . "\n"); 
    print "$scp_cmd\n" if $self->is_debug();
    my $ret = system($scp_cmd);
    unless($ret){
	$self->{'log_obj'}->info(
	    "Log file fetching is successful.");
    }else{
	$self->{'log_obj'}->info(
	    "Log file fetching failed.");
    }
    return $ret;
}

## A fle, log processing function
## get filename list from logs
 
sub get_filenames_from_logs{
    my ($self, $log_dir) = @_;

    my $data_ref;
    my $log_list;

    ## LOCAL_LOG_STG_DIR=/usr/local/mccs/data/edi/ftp/856_error_log/
    ## LOCAL_LOG_ARC_DIR=/usr/local/mccs/data/edi/ftp/856_error_log_bkup/
    unless($log_dir){
	$log_dir = $self->{LOCAL_LOG_STG_DIR};
    }
    ##  ASN_BKUP_DIR=/usr/local/mccs/data/edi/ftp/856_inbound_backup/
    unless($self->{'ASN_BKUP_DIR'}){
	$self->{'ASN_BKUP_DIR'} = 
	    '/usr/local/mccs/data/edi/ftp/856_inbound_backup/';
    }
    
    opendir(DIR, "$log_dir"); 
    my @files = readdir(DIR); 
    closedir(DIR);
    
    foreach my $file(@files){
	if($file =~ /\d+/g){
	    push(@$log_list, $file);
	    my $fp_name = $log_dir.'/'.$file;
	    open(IN, $fp_name);
	    my @ary = <IN>;
	    close IN;
	    foreach my $line(@ary){
		if(($line =~ /Error/g)&&($line =~ /(SH\d+\.7R3)/g)){
		    $data_ref->{$1}->{'load_log'} 
		    = $fp_name; ## this is the log file name
		    $data_ref->{$1}->{'asn_fp_name'} 
		    = $self->{'ASN_BKUP_DIR'}.$1;
		    $data_ref->{$1}->{'total_message'} 
		    .= $line; ## record the whole line if errors
		}
	    }    
	}
    }
    ## save list result into object as hash
    $self->_sget_parsed_data_ref($data_ref);
    $self->_sget_loading_log_list($log_list);
    return $data_ref; 
}


## loop through all the asn files, parse filename to parse_single_asn_file function
## save the returned result back into the data tree.
sub parse_asns{
    my ($self) = @_;
    my $data_ref = $self->_sget_parsed_data_ref();
    foreach my $asn(sort keys %$data_ref){
	my $single_file_data = 
	    $self->_parse_single_asn_file($data_ref->{$asn}->{'asn_fp_name'});
	## build up the parsed_data for this file:
	if($single_file_data){
	    $self->_sget_single_file_data_ref($asn, $single_file_data);
	}
    }    
}

## set or get parsed data_ref from filename, and single_file_data_ref
## return $self->{'PARSED_DATA_REF'}->{$asn_filename}->{'parsed_data'}

sub _sget_single_file_data_ref{
    my ($self, $asn_filename, $single_file_data_ref) = @_;
    if($single_file_data_ref){
	$self->{'PARSED_DATA_REF'}->{$asn_filename}->{'parsed_data'} 
	= $single_file_data_ref;
	return $self->{'PARSED_DATA_REF'}->{$asn_filename}->{'parsed_data'};
    }else{
	return $self->{'PARSED_DATA_REF'}->{$asn_filename}->{'parsed_data'};
    }
}

## take a full path of an asn filename
## output a date tree: 
## data_ref->{filename}->{from_logfile}
## data_ref->{filename}->{line10}->[];
## data_ref->{filename}->{line20}->[]; 
## data_ref->{filename}->{line30}->[]; 
## ....
## ....

sub _parse_single_asn_file{
    my ($self, $fp_filename) = @_;
    my @lines;
    eval{    
	open(IN2, "<$fp_filename") || die "can not open file to read: $fp_filename";
	@lines = <IN2>;
	print Dumper(\@lines);
	close IN2;
    };    

    if($@){
	my $msg = "file opening failed: $fp_filename";
	$self->{'log_obj'}->info("$msg");
	return undef;
    }

    my $one_file_data;
    foreach my $line(@lines){
	if($line =~ /\d+/g){## skip blank lines
	    chomp($line);
	    $line =~ s/^\s+//g; ## just in case...
	    my $line_type = substr($line, 16, 2);
	    print "line_type: $line_type\n" if ($self->is_debug());
	    if($line_type){
		push(@{$one_file_data->{$line_type}}, $line);
	    }else{
		my $msg ="missing line_type for line: $line file: $fp_filename\n";
		$self->{'log_obj'}->info("$msg");
	    }
	}
    }
    return $one_file_data;
}


## from the data tree
## get all the required items into collected_info reference

sub collect_reporting_items{
    my ($self) = @_;
    my $report_info_buff = "Asn_name\tVendor_id\t POs\tError_msg\t Log_name\n=======================================================================================\n";##1, get asn_load log filename
## get it from $data_ref->{$1}->{'load_log'}
##2, get vendor_id i.e. partinership_id
## from: line10, 1-16
##3, get PO_id 
## from: line 40, 44-54
##4, get asn file name id
## from $data_ref->{$sh3_filename}
##5, get error message from log
## from total message: $data_ref->{$1}->{'total_message'}

    my @tot_ary = ();
    my $data_ref = $self->_sget_parsed_data_ref(); 
    my $ctr = 0;
    foreach my $sh3 (sort keys %$data_ref){
	my ($log_name, $vendor_id, $po_id, $asn_name, $error_msg);
	my @row;
	$log_name  = $data_ref->{$sh3}->{'load_log'};
	$log_name =~ /(asn\_upload.*)/g;
	$log_name = $1;
	$vendor_id = 
	    substr(${$data_ref->{$sh3}->{'parsed_data'}->{'10'}}[0], 0, 16);

	my $po_string;
        ## add a test on the type of the reference for 40, if array, do process, else next
	

	if ( ref($data_ref->{$sh3}->{'parsed_data'}->{'40'}) eq 'ARRAY'){
	##if($data_ref->{$sh3}->{'parsed_data'}->{'40'}){
	    for(my $i=0; $i<@{$data_ref->{$sh3}->{'parsed_data'}->{'40'}}; $i++){
		my $po = 
		    substr(${$data_ref->{$sh3}->{'parsed_data'}->{'40'}}[$i], 43, 10);
		$po =~ s/\s+//g;
		unless($po_string =~ /$po/g){
		    $po_string .= $po.'-';
		}
	    }
	}else{
	    $po_string .= "No 40 lines, no PO info";
	    ## next; ## if empty 40 lines, go to next file
	}
        ## remove the last '-'
	$po_string =~ s/\-$//g;
	

	$asn_name  = $sh3;
	$error_msg = $self->_extract_key_words($data_ref->{$sh3}->{'total_message'});
	eval{
	    $report_info_buff .= 
		"$asn_name,$vendor_id,$po_string,$error_msg,$log_name\n";

	    push(@{$tot_ary[$ctr]}, $asn_name);
	    push(@{$tot_ary[$ctr]}, $vendor_id);
	    push(@{$tot_ary[$ctr]}, $po_string);
	    push(@{$tot_ary[$ctr]}, $error_msg);
	    push(@{$tot_ary[$ctr]}, $log_name);
	    ##push(@row, $asn_name);
	    ##push(@row, $vendor_id);
	    ##push(@row, $po_string);
	    ##push(@row, $error_msg);
	    ##push(@row, $log_name);
	    ##push(@{$tot_ary[$ctr]}, @row);
	    $ctr++;
	}
    }
    $report_info_buff .=  
"=======================================================================================\n";
    $self->_sget_report_buffer($report_info_buff);
    $self->{'log_obj'}->info("Report in Email\n");
    $self->{'log_obj'}->info("\n".$report_info_buff);## record info in the log
    return \@tot_ary;
}


sub _extract_key_words{
    my ($self, $error_string) = @_;
    if($error_string =~ /parent key not found/ig){
	return 'Invalid Vendor ID';
    }else{
	$error_string =~ s/\,/\-/g;
	## replace , with - for csv file format
	return $error_string;
    }
}


sub _get_sftp_object {
    my ($self) = @_;
    my $c = IBIS::Crypt->new();
    my $sftp = Net::SFTP->new(
                               $self->{LOG_SERVER},
                               user     => $self->{SFTP_USER},
                               password => $c->decrypt( $self->{PASSWORD} )
    ) or die " Can not connect to remote server: $self->{LOG_SERVER} !\n";
    return $sftp;
}


##my ($to_str, $from_str, $subject, $body) = @_;
sub send_report{
    my ($self) = @_;
    my $subject = 'ASN uploading error report';
    my $from_str = $self->{'SFTP_USER'};
    my $body = $self->_sget_report_buffer();
    eval{    
	if($body){
	    foreach my $address(split(/\|/, $self->{MAIL_CC})){
		sendmail($address, $from_str, $subject, $body);	
	    }
	}
    };
    
    if($@){
	$self->{'log_obj'}->info("Email failure at some point");
    }else{
	$self->{'log_obj'}->info("Email sending success");
    }
    return $@;
}


## move log to processed directory on remote server:
## move log from staging directory to archive dir
## clean up reporting directory
## return ref to list of errors in the operation

sub clean_up{
    my ($self) = @_;
    my $error =[]; 

## move remote files:
    my $log_list = $self->_sget_loading_log_list();
    if($log_list){
	my $sftp = $self->_get_sftp_object();
	my $rlog_dir = $self->{RMT_LOG_DIR};
	my $rlog_dir_bkup = $self->{RMT_LOG_DIR_BKUP};
	if($rlog_dir&&$rlog_dir_bkup){
	    eval{
		foreach my $file(@$log_list){
		    my $old = $rlog_dir.'/'.$file;
		    my $new = $rlog_dir_bkup.'/'.$file;
		    $sftp->do_rename($old, $new);
		}
	    };
	    if($@){
		push(@$error, $@);
		$self->{'log_obj'}->info($@);
	    }
	}else{
	    my $msg ='missing info in config:
                RMT_LOG_DIR or RMT_LOG_DIR_BKUP';
	    $self->{'log_obj'}->info($msg);
	    push(@$error, $msg);
	}
    }else{
	my $msg = "No error loading log files";
	$self->{'log_obj'}->info($msg);
	push(@$error, $msg);
    }
## move staging files
    my $llog_dir = $self->{LOCAL_LOG_STG_DIR};
    my $llog_dir_bkup = $self->{LOCAL_LOG_ARC_DIR};
    my $cmd = "mv $llog_dir".$self->{LOG_FILE_PREFIX}.'*'."   $llog_dir_bkup";
    my $ret = system($cmd);
    if($ret){
	push(@$error, $ret);
    }
## cleaning up reporting directory
    return $error;
}

## get rms dbh connection
sub sget_dbh_obj{
    my ($self) = @_;
    if($self->{'dbh_obj'}){
	return $self->{'dbh_obj'};
    }else{
## Set DB handle autocommit as 0 for transaction control
	$self->{'dbh_obj'} = IBIS::DBI->connect(
	    dbname  => $self->{CONNECT_INSTANCE},
	    attribs => { AutoCommit => 0 }
	    )
	    or $self->{'log_obj'}->log_die(
		"Cannot connect database using: 
            $self->{CONNECT_INSTANCE}\n"
	    );
	return $self->{'dbh_obj'};
    }
}


## fetch rms_data for part2 of 856 error report

sub fetch_edi_susp_errors{
    my ($self) = @_;

    my $query = "
SELECT distinct e.vendor_id, name, substr(a.transaction_data,44,6) PO_ID,
		(select distinct substr(t.transaction_data,173,10)
		from edi_archive_transactions t
		where t.business_unit_id = '30'
		and t.transaction_set = '856'
		and substr(t.transaction_data,17,2) = '30'
		and substr(t.partnership_id,1,11) = e.vendor_id
		and t.key_data = a.key_data and rownum < 2) SITE_ID, e.asn_id,

		case
		when shipment_level is not null
		then shipment_level
		when order_level is not null
		then order_level
		when prepack_level is not null
		then prepack_level
		when carton_level is not null
		then carton_level
		when item_level is not null
		then item_level
		end REASON

FROM edi_susp_asn_shipments e, vendors v, edi_archive_transactions a,

		(select distinct asn_id, description SHIPMENT_LEVEL
		from edi_susp_asn_ship_errors s, message_texts m
		where business_unit_id = '30'
		and language_id = 'AMERICAN'
		and s.message_id = m.message_id)s,
		
		(select distinct asn_id, description ORDER_LEVEL
		from edi_susp_asn_order_errors o, message_texts m
		where business_unit_id = '30'
		and language_id = 'AMERICAN'
		and o.message_id = m.message_id)o,
		
		(select distinct asn_id, description PREPACK_LEVEL
		from edi_susp_asn_prepack_errors p, message_texts m
		where business_unit_id = '30'
		and language_id = 'AMERICAN'
		and p.message_id = m.message_id)p,
		
		(select distinct asn_id, description CARTON_LEVEL
		from edi_susp_asn_carton_errors c, message_texts m
		where business_unit_id = '30'
		and language_id = 'AMERICAN'
		and c.message_id = m.message_id)c,
		
		(select distinct asn_id, description ITEM_LEVEL
		from edi_susp_asn_item_errors i, message_texts m
		where business_unit_id = '30'
		and language_id = 'AMERICAN'
		and i.message_id ! = '2925'
		and i.message_id = m.message_id)i

WHERE e.business_unit_id = '30'
and e.business_unit_id = v.business_unit_id
and e.vendor_id = v.vendor_id
and e.asn_id = s.asn_id(+)
and e.asn_id = o.asn_id(+)
and e.asn_id = p.asn_id(+)
and e.asn_id = c.asn_id(+)
and e.asn_id = i.asn_id(+)
and a.transaction_set = '856'
and substr(a.transaction_data,17,2) = '40'
and e.asn_id = a.key_data
and trunc(a.date_created) > trunc(sysdate-2)
and e.vendor_id in
	(select distinct vendor_id
	from pom_defaults
	where business_unit_id = '30'
	and edi_856_test_mode_ind = 'N'
	and carton_trck_asn_ind = 'Y')
order by name, e.vendor_id, substr(a.transaction_data,44,6), SITE_ID, e.asn_id
";
    my $sth = $self->{'dbh_obj'}->prepare($query);
    $sth->execute();
    return $sth;
}

## write message data into an 
sub write_asn_error_msg{
    my ($self, $sth) = @_;
    my $buffer ='';

    $buffer .= "PO        Site     ASN                      Vendor_id        Vendor_name                   Reason\n";
    $buffer .= "--------  -----    -----------------------  ---------------  ----------------------------  ------------------------------------\n";
    while ( my $row = $sth->fetchrow_hashref() ) {
	$row->{site_id} =~ s/\s+//g;## remove empty space in site
	$row->{name} =~ s/^\s+//g;
	my $name = substr($row->{name}, 0, 30);
	my $reason = substr($row->{reason}, 0, 35);
	$buffer .= sprintf('%-10s%-9s%-25s%-17s%-30s%-30s',$row->{po_id},$row->{'site_id'},$row->{'asn_id'},$row->{vendor_id},$name,$reason)."\n";
	
	## $buffer .= "$row->{po_id},$row->{'site_id'},$row->{'asn_id'},$row->{vendor_id},$row->{name},$row->{'reason'}\n";
    }   
    $buffer .= "--------  -----    -----------------------  ---------------  ----------------------------  ------------------------------------\n";
    $self->{'dbh_obj'}->disconnect();
    return $buffer;
}

 
sub write_html_from_dbhandle{
    my ($self, $sth, $th_str, $str_size, $subject) = @_;
    my $tab_data ='';    
    ## prepare the header part of the data
    if($th_str){
	$tab_data .='<tr style="color:blue;background-color:olive;">';
	foreach my $th(split(/\|/,$th_str)){ 
	    $tab_data .= "<th>$th</th>";
	}
	$tab_data .= '</tr>'; 
    }
    unless($sth){
	print "MISSING db handle, will exit";
	die;
    }
    my $ref = $sth->fetchall_arrayref();

    ## process to generate the table buffer
    for(my $i=0; $i<@$ref; $i++) { 
	$tab_data .="<tr>";
	for(my $j=0; $j<@{$ref->[$i]};$j++){
	    if(length($ref->[$i][$j]) > $str_size){
		$ref->[$i][$j] = substr($ref->[$i][$j], 0, $str_size);
	    }
	    $tab_data .= "<td>$ref->[$i][$j]</td>";
	}
	$tab_data .= "</tr>";
    } 
    ## assemble the html body
    my $total_buff .= qq(<body>
			     <h3 align="center">$subject</h3>
			     <table cellspacing="0" cellpadding="4"
			     style='font-family: "Lucida Sans Unicode", "Lucida Grande", Sans-Serif;font-size: 16px;text-align: left;border-collapse: collapse;border: 1px solid #69c;'>
			     $tab_data
			     </table>
			     </body>);
    return $total_buff;   
}


# write message data into an 
sub write_asn_error_html{
    my ($self, $sth) = @_;

    my $subject = "E856 error report part2 (Sample)";
    my $tab_data .='<tr style="color:blue;background-color:olive;"><th>PO</th><th>SITE</th><th>ASN</th><th>VENDOR_ID</th><th>NAME</th><th>REASONS</th></tr>';    
    while ( my $row = $sth->fetchrow_hashref() ) {
	$row->{site_id} =~ s/\s+//g;## remove empty space in site
	$row->{name} =~ s/^\s+//g;
	my $name = substr($row->{name}, 0, 30);
	my $reason = substr($row->{reason}, 0, 35);
	$tab_data .= qq{<tr><td>$row->{po_id}</td><td>$row->{'site_id'}</td><td>$row->{'asn_id'}</td><td>$row->{vendor_id}</td><td>$name</td><td>$reason</td></tr>};
    }   
    my @mlist = split(/\|/, $self->{MAIL_CC});
    foreach my $to(@mlist){    
	my  $msg = MIME::Lite->new(
				   To      => $to,
				   Subject => $subject,
				   Type    =>'multipart/related'
				   );
	$msg->attach(
		     Type => 'text/html',
		     Data => qq{
			 <body>
			     <h3 align="center">$subject</h3>
			     <table cellspacing="0" cellpadding="4"
			     style='font-family: "Lucida Sans Unicode", "Lucida Grande", Sans-Serif;font-size: 16px;text-align: left;border-collapse: collapse;border: 1px solid #69c;'>
			     $tab_data
			     </table>
			     </body>
			 },
		     );
	
	$msg->send();
    }
}




##my ($to_str, $from_str, $subject, $body) = @_;
sub send_part2_report{
    my ($self, $subject, $body) = @_;
    unless($subject){
	$subject = 'ASN inbound visibility';
    }
    my $from_str = $self->{'MAIL_FROM'};
    eval{    
	if($body){
	    foreach my $address(split(/\|/, $self->{MAIL_CC})){
		sendmail($address, $from_str, $subject, $body);	
	    }
	}
    };
    
    if($@){
	$self->{'log_obj'}->info("Email failure at some point");
    }else{
	$self->{'log_obj'}->info("Email sending success");
    }
    return $@;
}

sub create_output_by_aryref{
    my ($self, $ary_ref, $tstamp) = @_;
    my $llog_dir_bkup = $self->{LOCAL_LOG_ARC_DIR};
    my $filename = $llog_dir_bkup."/".'invalid_vendor_'.$tstamp;
    ##print Dumper($ary_ref);
    my $tab_data .="Asn_name|Vendor_id|PO|Error_msg|Log_name|";
    for(my $i=0; $i<@$ary_ref; $i++) { 
	$tab_data .= "\n";
	for(my $j=0; $j<@{$ary_ref->[$i]};$j++){
	    $tab_data .= "$ary_ref->[$i][$j]"."|";
	}
    } 
    open(OUT, ">$filename") || die "can  not open file: $filename";
    print OUT $tab_data;
    close OUT;
    return $filename;
}


sub html_table_via_sth_or_aryref{
    my ($self, $sth_or_aref,$th_str,$subject,$str_size) = @_;
    my $tab_data ='';    
    
    my $ref; ## reference for array or data from db handle...
    unless($sth_or_aref){
	print "miss db statmenthandle or 2D array reference..";
	die;
    }
    
    ## assuming statement handle, or 2D array reference:
    if (ref($sth_or_aref) eq 'ARRAY') {
	$ref = $sth_or_aref;
    }else{
	$ref = $sth_or_aref->fetchall_arrayref();
    }
    
    unless(@$ref>0){
	return undef;
    }

    ## prepare the header part of the buffer
    if($th_str){
	$tab_data .='<tr style="color:blue;background-color:olive;">';
	foreach my $th(split(/\|/,$th_str)){ 
	    $tab_data .= "<th style='align:left'>$th</th>";
	}
	$tab_data .= '</tr>'; 
    }
    
    ## process to generate the table buffer
    my $s_flag = 0;
    for(my $i=0; $i<@$ref; $i++) {
	if(ref($ref->[$i]) eq 'ARRAY'){
	    $tab_data .="<tr>";
	    for(my $j=0; $j<@{$ref->[$i]};$j++){
		if ($ref->[$i][$j]){
		    if($str_size){	
			if(length($ref->[$i][$j]) > $str_size){
			    $ref->[$i][$j] = substr($ref->[$i][$j], 0, $str_size);
			}
		    }
		    $tab_data .= "<td>".$ref->[$i][$j]."</td>";
		}else{
		    $tab_data .= "<td> </td>";
		}
	    }
	    $tab_data .= "</tr>";
	}else{ ## this is the scalar case..
	    ##$s_flag = 1;
	    ##$tab_data .= "<td>$ref->[$i]</td>";
	}
    } 
    if($s_flag){
	$tab_data .= "<tr>$tab_data</tr>";
    }

    ## assemble the html body
    unless($subject){
	$subject = ' ';
    }
    my $total_buff .= qq(<body>
			     <h3 style='font-family:Sans-Serif;font-size: 14px;text-align:center;'>$subject</h3>
			     <table cellspacing="0" cellpadding="4" align="center" 
			     style='font-family: "Lucida Sans Unicode", "Lucida Grande", "Sans-Serif";font-size: 12px;text-align: left;border-collapse: collapse;border: 1px solid #69c;'>
			     $tab_data
			     </table>
			     </body>);
    return $total_buff;   
}


sub send_html_by_email{
    my ($self, $subject, $to_list, $content_buff) = @_;
    my @mlist = split(/\|/, $to_list);
    eval{
	foreach my $to(@mlist){    
	    my  $msg = MIME::Lite->new(
		To      => $to,
		Subject => $subject,
		Type    =>'multipart/related'
		);
	    $msg->attach(
		Type => 'text/html',
		Data => qq{
			$content_buff
			 },
		);	
	    $msg->send();
	}    
    };
    return $@;
}

sub destructor{
    my ($self) = @_;
    $self = undef;
}

1;

=pod

=head1 NAME

E856.pm

=head1 VERSION

This documentation refers to the initial version, version 1.

=head1 REQUIRED ARGUMENTS

N/A

=head1 OPTIONS

N/A

=head1 DESCRIPTION

When EDI856 files  from SPS are loaded into RMS database, loading errors occur sometimes. In the past, these errors, recorded in asn loading log, have been handled manually for extracting useful information such as vendor id, POs for reporting purposes. Since the process is tedious, an automated process was proposed. 

This package is for automating the edi_856 uploading error reporting process. The scope of functions here mainly include parsing/fetching asn_upload logs, parsing ASN files for information required in the report. The package also include functions for collecting required data items, email reporting of the parsed data, and cleaning up files after processing. 

=head1 REQUIREMENTS

N/A

=head1 DIAGNOSTICS

use IBIS::EDI;

use IBIS::E856;

my $e856 = new IBIS::E856();


=head1 CONFIGURATION

=over 

/usr/local/mccs/etc/edi_856/edi_856_error_report.config
 
=back
                                                                                        
=head1 DEPENDENCIES

=over 4

@ISA = qw (IBIS::EDI);

use strict;

use Data::Dumper;

use IBIS::Crypt;

use Net::SFTP;

use IBIS::Email;

=back

=head1 SEE ALSO 

EDI ASN File Upload Request Enhancement by Mike Gonzalez

EDI 856 Inbound File Definition

=head1 INCOMPATIBILITIES

N/A

=head1 BUGS AND LIMITATIONS

Limitations:


=head1 BUSINESS PROCESS OWNERs

Mike Gonzalez

=head1 AUTHOR

Chunhui Yu<yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2008 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut

