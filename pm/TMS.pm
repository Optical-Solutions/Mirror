package IBIS::TMS;
@ISA = qw(IBIS::EDI);
use strict;
use Data::Dumper;
use File::Basename;

sub new {
    my ( $class, %args ) = @_;
    my $self = {};
    bless $self, $class;    
    $self->_make_accessors(\%args );
    return $self;
}

## get all data after a ride_in_date from the config
## return a list reference of selectall_arrayref
sub pool_tms_details{
    my ($self, $povn_list_ref, $cdate) = @_;

    my $query = "select 
vendor_id,
vendor_name,
po_id,
site_id,
order_date,
cancellation_date,
fob_point,
version_no,
remark,
cancelled_ind,
address_to_site_id,
trans_num,
buyer_employee_id,
reason_id,
event_desc,
event_start_date,
origin
 from $self->{VIEW_EDI_TMS} ";


    ##my $query = "select * from $self->{VIEW_EDI_TMS} ";    
    my $povn_list='';

    if($povn_list_ref){
	foreach my $po_id(@$povn_list_ref){
	    $povn_list .= "$po_id".",";
	}
	$povn_list =~ s/\,$//g;
	$query .= "where  ponvsn in ($povn_list) ";
    }
    
    $query .= "order by po_id, site_id";
    print "query: $query\n" if $self->{debug};
    my $value_ref =
	$self->{'dbh'}->selectall_arrayref( $query)
	;

    if(@$value_ref == 0){
	$self->{'log_obj'}->log_info("No new TMS data, or query error: $query");
    }

    $self->{'tms_ary_ref'} = $value_ref; 
    return $value_ref;
}

sub get_tms_array_ref{
    my ($self) = @_;
    return $self->{'tms_ary_ref'};
}

## generate hash reference from array data, 
## remove duplicate site_id in the site_id array
sub get_tms_hash_ref{
    my ($self) = @_;
    my $ret_href;
    if($self->{'tms_ary_ref'}){
	for(my $i=0; $i<@{$self->{'tms_ary_ref'}};$i++){
	    my $ponvsn    = $self->{'tms_ary_ref'}->[$i][2].$self->{'tms_ary_ref'}->[$i][7];
	    $ret_href->{$ponvsn}->{'vendor_id'}        =  $self->{'tms_ary_ref'}->[$i][0];
	    $ret_href->{$ponvsn}->{'vendor_name'}      =  $self->{'tms_ary_ref'}->[$i][1];
	    $ret_href->{$ponvsn}->{'po_id'}            =  $self->{'tms_ary_ref'}->[$i][2];
	    unless ( $self->{'tms_ary_ref'}->[$i][3]){
		$self->{'log_obj'}->log_info("!!Warning: missing site_id for $ponvsn");
	    }
            my $exist_ind 
		= $self->check_presence($self->{'tms_ary_ref'}->[$i][3], $ret_href->{$ponvsn}->{'site_id'});
            if (!$exist_ind){
		push(@{$ret_href->{$ponvsn}->{'site_id'}},    $self->{'tms_ary_ref'}->[$i][3]);
	    }
            
	    $ret_href->{$ponvsn}->{'order_date'}       =  $self->{'tms_ary_ref'}->[$i][4];
	    $ret_href->{$ponvsn}->{'cancellation_date'}=  $self->{'tms_ary_ref'}->[$i][5];
	    $ret_href->{$ponvsn}->{'fob_point'}        =  $self->{'tms_ary_ref'}->[$i][6];
	    unless ( $self->{'tms_ary_ref'}->[$i][6]){
		$self->{'log_obj'}->log_info("!!Warning: missing Terms info for $ponvsn");
	    }
	    $ret_href->{$ponvsn}->{'version_no'}         =  $self->{'tms_ary_ref'}->[$i][7];
	    $ret_href->{$ponvsn}->{'remark'}             =  $self->{'tms_ary_ref'}->[$i][8];
	    $ret_href->{$ponvsn}->{'cancelled_ind'}      =  $self->{'tms_ary_ref'}->[$i][9];
            $ret_href->{$ponvsn}->{'address_to_site_id'} =  $self->{'tms_ary_ref'}->[$i][10];
	    $ret_href->{$ponvsn}->{'trans_num'}          =  $self->{'tms_ary_ref'}->[$i][11];
	    $ret_href->{$ponvsn}->{'buyer_employee_id'}  =  $self->{'tms_ary_ref'}->[$i][12];
	    $ret_href->{$ponvsn}->{'reason_id'}          =  $self->{'tms_ary_ref'}->[$i][13];
	    $ret_href->{$ponvsn}->{'event_desc'}         =  $self->{'tms_ary_ref'}->[$i][14];
	    $ret_href->{$ponvsn}->{'event_start_date'}   =  $self->{'tms_ary_ref'}->[$i][15];
	    $ret_href->{$ponvsn}->{'origin'}             =  $self->{'tms_ary_ref'}->[$i][16];
	}
    }
    $self->{'tms_hash_ref'} = $ret_href;
    return $ret_href;
}


## generate file from hash reference
## !!!! remove comma in some fields!!!!

sub generate_tms_from_hash_ref{
    my ($self, $filename, $ret_href, $prefix_zero_flag)=@_;
    unless($ret_href){
	$ret_href = $self->{'tms_hash_ref'};
    }
    my $ret_buff= '';   
    my $header_line = "Vendor No.,Vendor Name, PO No.,Destination,PO Start Date,PO Stop Date,PO Freight Term,PO Version No.,Remarks,Cancelled,Override ship-to Address,Employee id,Reason,Event,Event Start Date, Origin\n";
    $ret_buff .=  $header_line;
    foreach my $ponvsn(sort {$a<=>$b} keys %{$ret_href}){
	foreach my $site_id(sort {$a<=>$b} @{$ret_href->{$ponvsn}->{'site_id'}}){
	    $ret_href->{$ponvsn}->{'vendor_name'}   =~ s/\,/ /g;
	    $ret_href->{$ponvsn}->{'fob_point'}     =~ s/\,/ /g;
	    $ret_href->{$ponvsn}->{'remark'}        =~ s/\,/ /g; 
	    $ret_href->{$ponvsn}->{'reason_id'}     =~ s/\,/ /g;
	    $ret_href->{$ponvsn}->{'event_desc'}    =~ s/\,/ /g;
	    ## Jan 2013, required by Lyndal at Landair, Kim and Terri at MCCS
	    my $address_to_site_id;
            if($prefix_zero_flag){ 
		$address_to_site_id = 
		    $self->zero_prefix_string($ret_href->{$ponvsn}->{'address_to_site_id'});
	    }else{
		$address_to_site_id = 
		    $ret_href->{$ponvsn}->{'address_to_site_id'};
	    }
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'vendor_id'});
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'vendor_name'});
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'po_id'});
	    $ret_buff .=  $self->process_tms_value_str($site_id);
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'order_date'});
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'cancellation_date'});
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'fob_point'});
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'version_no'});
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'remark'});
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'cancelled_ind'});
	    $ret_buff .=  $self->process_tms_value_str($address_to_site_id); 
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'buyer_employee_id'});
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'reason_id'});
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'event_desc'});
	    $ret_buff .=  $self->process_tms_value_str($ret_href->{$ponvsn}->{'event_start_date'});
	    $ret_buff .=  $self->process_tms_value_str("$ret_href->{$ponvsn}->{'origin'}\n"); ## note \n at end 
	}
    }
    open(OUT, ">$filename")|| die "can not open file to write: $filename\n";
    print OUT $ret_buff;
    close OUT; 
    return ($filename, \$ret_buff);   
}

sub process_tms_value_str{
    my ($self, $in_str) = @_;
    my $ret_str = '';
    if($in_str){
	if($in_str =~ /\n$/){
	    $ret_str = $in_str; ## last value with new line char
	}else{
	    $ret_str = $in_str.","; ## middle value
	}
    }else{
        $ret_str = ","; #null value
    }
    return $ret_str;
}


##only append zeros if there is number for this field. else return original value:
sub zero_prefix_string{
    my ($self, $value) = @_;
    my $ret = '';
    if($value =~ /\d+/g){
	$ret = sprintf("%05d", $value);	 
    }else{
	$ret = $value;
    }
    return $ret;
}


sub check_po_version_presence{
    my ($self, $po, $version_no) = @_;
    my $statement = "select count(*) from $self->{'TABLE_EDI_TMS_PO_TRACKING'} 
                  where po_id =\'$po\' and version_no = \'$version_no\'";
    my $ret = $self->{'dbh'}->selectall_arrayref($statement);
    return $ret->[0]->[0];
}


## insert new POs into tms po tracking table
sub insert_tms_po_track{
    my ($self) = @_;
    my @errors;
    my $msg = "PO\tVsn\tStart date  Stop date\tVendor name\n===========================================================\n";
    my $ctr = 0;
    my $statement = "insert into $self->{'TABLE_EDI_TMS_PO_TRACKING'} 
                  (PO_ID, VERSION_NO, PONVSN, TRANS_NUM, DATE_CREATED) 
                   values 
                  (?,?,?,?,sysdate)";
    my $sth = $self->{'dbh'}->prepare($statement);    
    if($self->{'tms_hash_ref'}){
	foreach my $ponvsn(sort {$a<=>$b} keys %{$self->{'tms_hash_ref'}}){

	    my $check = $self->check_po_version_presence($self->{'tms_hash_ref'}->{$ponvsn}->{'po_id'}, $self->{'tms_hash_ref'}->{$ponvsn}->{'version_no'});
	    if ($check == 0){
		my @bdvs;
		push(@bdvs, $self->{'tms_hash_ref'}->{$ponvsn}->{'po_id'});
		push(@bdvs, $self->{'tms_hash_ref'}->{$ponvsn}->{'version_no'});
		push(@bdvs, $ponvsn);
		push(@bdvs, $self->{'tms_hash_ref'}->{$ponvsn}->{'trans_num'});
		print "inserting po: $self->{'tms_hash_ref'}->{$ponvsn}->{'po_id'}\n" if $self->{'debug'};
		eval { $sth->execute(@bdvs); };
		if ($@) {
		    push( @errors, $@ );
		    $self->{'log_obj'}->log_info("$@");
		} else {
		    $msg  .=
			"$self->{'tms_hash_ref'}->{$ponvsn}->{'po_id'}\t$self->{'tms_hash_ref'}->{$ponvsn}->{'version_no'}\t$self->{'tms_hash_ref'}->{$ponvsn}->{'order_date'}\t   $self->{'tms_hash_ref'}->{$ponvsn}->{'cancellation_date'}\t$self->{'tms_hash_ref'}->{$ponvsn}->{'vendor_name'}\n";
		    $ctr++;
		}
	    }else{
		
		$self->{'log_obj'}->log_info("po:$self->{'tms_hash_ref'}->{$ponvsn}->{'po_id'}, version:$self->{'tms_hash_ref'}->{$ponvsn}->{'version_no'} exist already.\n");
		
	    }
	}	   
	
    }else{
	push(@errors, "Missing object data\'tms_hash_ref\'");
	
    }
    
    if(@errors == 0){
	$msg  .= "===========================================================\n";
    }

    return (\@errors, $ctr, $msg);
}

## check by po_id to see if po has been sent before

sub confirm_sent_tms_PONVSNs{
    my ($self, $resend_povn_list) = @_;
    my $result_ref;
    my $flag = 0;
    my $msg = '';
    my $list='';

    foreach my $povn (@{$resend_povn_list}){
	
	if($povn) {
	    $list .= "$povn".",";
            $result_ref->{$povn}->{'tms_po_tracking'}='';  ## put a blank value before confirmed
	}
    }
    $list =~ s/\,$//g; #remove the last ','
    
    ## check tracking table:
    my $query1 = "
select ponvsn  
from $self->{'TABLE_EDI_TMS_PO_TRACKING'}
where ponvsn in ($list) ";
    
        
    my $ret1 = $self->{'dbh'}->selectall_arrayref($query1);
    
    for(my $i=0; $i<@$ret1; $i++){
	$result_ref->{$ret1->[$i]->[0]}->{'tms_po_tracking'} = 'Y';
    }
    
## analysis on the result:
    foreach my $ponvsn(keys %$result_ref){
	## 2 cases     
	if($result_ref->{$ponvsn}->{'tms_po_tracking'} eq 'Y'){
	    $msg .= "$ponvsn in tms tracking table, passed confirmation\n";
	}elsif($result_ref->{$ponvsn}->{'tms_po_tracking'} eq ''){
	    $msg .= "$ponvsn is in Not in tracking table\n";
	    $flag = 1;
	}
    }
    return ($flag, $msg);   
}

sub refresh_sent_tms_PONVSNs{
    my ($self, $resend_povn_list) = @_;
    my @errors; 
    my $list = '';
    foreach my $povn (@{$resend_povn_list}){	
	if($povn) {
	    $list .= "$povn".",";
	}
    }
    $list =~ s/\,$//g; 
    
    if($list) {
	my $statement = " delete from $self->{'TABLE_EDI_TMS_PO_TRACKING'} where ponvsn in ($list)";
	my $sth = $self->{'dbh'}->prepare($statement);    
	
	eval { $sth->execute();};
	if ($@) {
	    push( @errors, $@ );
	    $self->{'log_obj'}->log_info("$@");
	} 
    }
    return \@errors;
}

sub check_presence{
    my ($self, $item, $list_ref) = @_;
    my $flag =0;
    foreach my $ele(@$list_ref){
        $ele  =~ s/\s+//g;
        $item =~ s/\s+//g; ## remove possible spaces
	if(($ele eq $item)&&($flag ==0)){
	    $flag = 1;
	}
    }
    return $flag;
}

sub sftp_file_from_remote_server {
    my ( $self, $remote_file, $local_file) = @_;
    my $c = IBIS::Crypt->new();
    my $sftp = Net::SFTP->new(
	$self->{REMOTE_SERVER},
	user     => $self->{SFTP_USER},
	password => $c->decrypt( $self->{PASSWORD} )
	) || die " Can not connect to remote server: $self->{REMOTE_SERVER} !\n";

    my $ftp_result = $sftp->get( $remote_file, $local_file );
    if ($ftp_result) {
        $self->{'log_obj'}->log_info("\nSFTP successful!\n");
    } else {
        $self->{'log_obj'}->log_info("\nSFTP failed for some reason: $!\n");
    }
    return $ftp_result;
}


# Added debugging code with 5 log statements.
sub sftp_list_and_get {
    my ( $self, $remote_file, $local_file, $remote_dir) = @_;
    $self->{'log_obj'}->log_info('remote file is' . $remote_file . "\n");
        $self->{'log_obj'}->log_info('local file is' . $local_file . "\n");
        $self->{'log_obj'}->log_info('remote dir is' . $remote_dir . "\n");

    my $c = IBIS::Crypt->new();
    my $sftp = Net::SFTP->new(
	$self->{REMOTE_SERVER},
	user     => $self->{SFTP_USER},
	password => $c->decrypt( $self->{PASSWORD} )
	) || die " Can not connect to remote server: $self->{REMOTE_SERVER} !\n";
        $self->{'log_obj'}->log_info('sftp user is' . $self->{SFTP_USER} . "\n");
        $self->{'log_obj'}->log_info('password is' . $c->decrypt( $self->{PASSWORD} ) . "\n");

  ## take care of possible spaces in the STUPD MercuryGate names  
    my @flist;
    my @flist_ref = $sftp->ls($remote_dir);
    
    #TODO Printing each file name from a list array.
    foreach my $key(@flist_ref){
	push(@flist, $key->{'filename'});
    $self->{'log_obj'}->log_info('file ' . $key->{'filename'} . "\n");
    }
    
    
    my $r_bn = File::Basename::basename($remote_file);
    
    my $tmp_msg = "\nbase name: $r_bn\n";
    $self->{'log_obj'}->log_info($tmp_msg);
            $self->{'log_obj'}->log_info("line 384\n");
=head
    my @found = grep {/$r_bn/g} @flist; 
    print "found:\n";
    print Dumper(\@found);   
## try to see if there are files without space in the file name:
    if(@found == 0){
	$r_bn =~ s/\s+//g; ## take away space
	@found = grep {/$r_bn/g} @flist; 
	if(@found > 0){ ## file without space was found there:
	    $remote_file =~ s/\s+//g;
	    $local_file  =~ s/\s+//g;
	}
    }
=cut


## check if file has space, with the filename with space to test first:
    my @found;
    my $space_flag = 0;
    foreach my $file(@flist){
	if($r_bn eq $file){
	    push(@found, $r_bn);
	    if(!$space_flag){
		$space_flag = 1;
	    } 
	}
    }
    $self->{'log_obj'}->log_info("line 411\n");
    if(@found ==0){
    $self->{'log_obj'}->log_info("in  411 loop\n");
	$r_bn =~ s/\s+//g; ## remove space:
	foreach my $file(@flist){
	    if($r_bn eq $file){
		push(@found, $r_bn);
     
	    }    
	}
	    $self->{'log_obj'}->log_info("line 420\n");
	if(@found){ ## name without space was found:
	    $remote_file =~ s/\s+//g;
	    $local_file  =~ s/\s+//g;
	}else{
	    $self->{'log_obj'}->log_info("\nNO FILE in remote with name: $r_bn\n");

	}
    }
    	    $self->{'log_obj'}->log_info("line 430\n");
## with name or without a name, anyways take the front space in names away:
    $local_file =~ s/\s+//g;
    $self->{'log_obj'}->log_info("local $local_file remote $remote_file right before sftp\n");
    my $ftp_result = $sftp->get( $remote_file, $local_file );
    $self->{'log_obj'}->log_info($ftp_result . "line 436\n");
    if ($ftp_result) {
        $self->{'log_obj'}->log_info("\nSFTP successful!\n");
    } else {
        $self->{'log_obj'}->log_info("\nSFTP failed for some reason: $!\n");
    }
        $self->{'log_obj'}->log_info($ftp_result . "line 439\n");
    return $ftp_result;
}


1;

__END__

=pod

=head1 NAME

IBIS::TMS  -- An IBIS shipment file generator

=head1 VERSION

This documentation refers to IBIS::TMS version 0.0.1.

=head1 SYNOPSIS

    use IBIS::EDI;
    use IBIS::TMS;
    my $config = 'myconfig'; 
    ## a config file in the form 'key=value' for db name, directory path values.    
    $tms = IBIS::TMS->new( conf_file => $config );

    $tms->pool_tms_details();                   ## get tms data from database
    $tms->generate_tms_file($ftp_file_name);    ## generating formated file 
    my $ary_ref  =   $tms->get_tms_array_ref(); ## return array ref of the tms data
    my $hash_ref =   $tms->get_tms_hash_ref();  ## return hash ref of the data 
                                                ##($href->{ponvsn}->{column}=$value)

=head1 DESCRIPTION

These subroutines provide basic utilities to extract data from iro_po_headers, and iro_po_details for shipment info.

=head1 SUBROUTINES/METHODS

=over 4

=item pool_tms_details()

Returns an array reference of all the new shipment information has not been sent before. 
Total of 12 columns are returned in the array.

=item generate_tms_file()

Returns a filename contains all the shipment informationa as indicated in the specs

=item  get_tms_array_ref(); 

Returns an array ref of the tms data

=item  get_tms_hash_ref(); 

Returns hash ref of the tms data 

=back

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION AND ENVIRONMENT

A configurateion file is need to define the database tables, ftp user/password (encrpted), db views. 

=head1 DEPENDENCIES

=over 4

=item * IBIS::EDI;

=item * IBIS::DBI;

=item * Net::FTP;

=back

=head1 INCOMPATIBILITIES

None.

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to the L<MCCS Help Desk|mailto:help.desk@usmc-mccs.org>.
Patches are welcome.

=head1 AUTHOR

Chunhui (Chuck) YU <yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2006 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut

