package IBIS::MonitorRpos;
use strict;
use Data::Dumper;
use File::Basename;
use base ('Class::Accessor');
use IBIS::Config::Auto;
use IBIS::DBI;
use IBIS::Log::File;


## 
sub new {
    my ( $class, %args ) = @_;
    my $self = {};
    bless $self, $class;    
    $self->_make_accessors(\%args );
    return $self;
}

sub _make_accessors {
    my ( $self, $args ) = @_;
    my $config = Config::Auto::parse( $args->{conf_file},
                                      format => $args->{conf_format} || 'equal' );
    Class::Accessor->mk_accessors( keys %{$config} );
    foreach ( keys %{$config} ) {
        $self->$_( $config->{$_} );
    }
}

## untils
sub _sget_dbh{
    my ($self) = @_;
    if($self->{'dbi_obj'}){
	return $self->{'dbi_obj'};
    }else{
	$self->{'dbi_obj'} = IBIS::DBI->connect( 
	dbname => $self->{'rms_connection_id'}, 
	attribs => { AutoCommit => 0 });
	unless($self->{'dbi_obj'}){
	    die "failed to connection rms with id: $self->{'rms_connection_id'}";
	}
	return $self->{'dbi_obj'};	
    }
}


sub _sget_log{
    my ($self) = @_;
    #TODO remove next line
    print '_sget_log \n';
    if($self->{'log_obj'}){
	return $self->{'log_obj'};
    }else{
	$self->{'log_obj'} = 
	new IBIS::Log::File( { 
	    file => $self->_get_attribute('log_file') , 
	    append => 1 } );
                #TODO remove next line
            print '_sget_log finshed complete \n';
	return $self->{'log_obj'};	
    }
}

## in:name of attribute
## out: value
sub _get_attribute{
    my ($self, $name) = @_;
    my $ret_value;
    foreach my $att (keys %{$self}){
	if(lc($att) eq lc($name)){
	    $ret_value = $self->{$att};
	}
    }
    return $ret_value;
}

## public function for attribute
sub get_attribute{
    my ($self, $name) = @_;
    return $self->_get_attribute($name);
}

## application related
sub get_all_rms_site_open_info{
    my ($self) = @_;
    unless($self->{'dbi_obj'}){
	    $self->_sget_dbh();
    }
    my $site_ref;
## get open days, sites and names
    my $query ="
select 
site_id, 
days_open_per_week,
name,
case 
when name like '%MAIN STORE%' then 1 
else 0 
end
as main_store
from sites
where site_id !='00000'
and DATE_CLOSED IS NULL
order by site_id ASC
";
## classify the sites at different levels
    my $value_ref = $self->{'dbi_obj'}->selectall_hashref($query, 'site_id');
    if($value_ref){
	foreach my $site_id (keys %{$value_ref}){
	    print Dumper($value_ref->{$site_id}) if ($self->_get_attribute('debug'));
	    $site_ref->{$site_id} = $value_ref->{$site_id};
	}
	return $site_ref;
    }else{
	my $log_msg ="Failed to get sites information from RMS database, this program will exit.";
	$self->{'log_obj'}->info($log_msg);
	die;
    }
}

##input: directory path, days for cut off (33 or 63)
## output:reference of list of date for files for all sites, counts
## get the list for all the past data in the cut off days
sub get_dir_list{
    my ($self,$dir_name,$cut_off_days) = @_;
    my $data_dir = $self->_get_attribute($dir_name);
    my @date_list;
    ## list by date desc..(default)
    my $date_buffer = `ls -lar $data_dir`;
    print "$date_buffer\n" if ($self->_get_attribute('debug'));
    
    my @ary = split(/\n/, $date_buffer);
    my $size = @ary;
    #TODO remove 0 && next line.
    if(0 && $size < 10){
	my $msg ="Less than enough data for running the program. Will die.\n";
	print $msg if ($self->_get_attribute('debug'));
	$self->{'log_obj'}->info($msg) if defined $self->{'log_obj'};
	die;
    }
    
    my $ctr = 0;
    foreach my $line (@ary){
	if ($ctr < $cut_off_days){
	    chomp($line);
	    print "line:$line\n" if $self->_get_attribute('debug');
	    my @items = split(/\s+/, $line);
	    if($items[8]){
		push(@date_list, $items[8]);
		$ctr++;
	    }
	}
    }
    if($self->_get_attribute('debug')){
	print Dumper(\@date_list);
    }
    return (\@date_list);
}

## input: base dir, dir_list, sit ref
## output: site ref with count in date ordered 

sub get_file_site_ref{
    my ($self, $base_dir, $dir_list) = @_;
    my $site_ref;
    ## get list of sites and count
    foreach my $dir(@$dir_list){
	my $sub_path = $base_dir.'/'.$dir;       
	my $ret_buff = `ls -lar $sub_path`;
	foreach my $line (split(/\n/, $ret_buff)){
	    my @l_items = split(/\s+/, $line);
	    my $fname = $l_items[8];
	    if($fname =~ /\d|w/g){
		my @items = split(/\./, $fname);
		my $site = $items[0];
		if($site){
		    unless($site_ref->{$site}){
			$site_ref->{$site}->{$dir}->{'counter'} = 1;
		    }else{
			$site_ref->{$site}->{$dir}->{'counter'} 
			=  $site_ref->{$site}->{$dir}->{'counter'} +1;
		    }
		}else{
		    my $msg = "failed to parse out site: $line";
		    $self->{'log_obj'}->info($msg);
		    print "msg: $msg\n";
		}	    
	    }
	}
    }
    return $site_ref;
}

#

sub print_site_date_count{
    my ($self, $site_file_info) = @_;
    ##
    my $printout ='';
    foreach my $site (sort {$a cmp $b} keys (%{$site_file_info} )){
	if ($site eq '00000'){next;}
	foreach my $bdate (sort {$a cmp $b} keys (%{$site_file_info->{$site}}) ){
	    $printout .= $site."\t".$bdate."\t".$site_file_info->{$site}->{$bdate}->{'counter'}."\n";
	}
    }
    return $printout;
}

#

## input: open info, site_file_list
## output: a record for all missing info
## and maybe a list of stores added laterly

sub exam_list_by_open_info{
    my ($self, $site_open_info, $site_file_info, $threshold, $order_date) = @_;
    ## from the site_file info,
    ## 1, order the site_file list by date desc,
    ## 2, get open info, cut off = 7 - $daysopen + $threshold
    ## 3, loop through this list, 
    ## do two things: a, get site/day count values within the threshold list, b, get the
    ## count from above the threshold list.
    ## 4, compare the two, 
    ## if both lists exist, if count not in the short list, then write missing data msg
    ##         else if both a and b has the count, then nothing missing.    
    unless($site_open_info && $site_file_info){
	my $msg ="missing required values: site open info or site file info";
	$self->{'log_obj'}->info($msg);
	die;
    }
    
    ## set a default threshold days for alarm
    unless($threshold){
	$threshold = 3;
    }

    ## GET THE TWO LIST PER SITE:
    my $st_hash_ref; 
    my $lg_hash_ref;
    foreach my $site(sort {$a<=>$b} keys (%{$site_file_info})){
	## $site_ref->{$site}->{$dir}->{'counter'} + 1;
        ## skip two sites that are test, or with non-fixed opening schedules
	if(($site eq '00000')||($site eq '02800')){next;}
	my $cut_off = 7 - $site_open_info->{$site}->{'days_open_per_week'} + $threshold;	
	my $s_ctr = 0;
	my $result_msg = '';
        ## initialize values
        $st_hash_ref->{$site}->{'counter'} = 0;
	$lg_hash_ref->{$site}->{'counter'} = 0;	
	$st_hash_ref->{$site}->{'cut_off'} = $cut_off;
	$lg_hash_ref->{$site}->{'cut_off'} = $cut_off;
	$st_hash_ref->{$site}->{'days_open_per_week'} 
                    = $site_open_info->{$site}->{'days_open_per_week'};
	$lg_hash_ref->{$site}->{'days_open_per_week'} 
                    = $site_open_info->{$site}->{'days_open_per_week'};

	## set a default value for days open per week
	unless($st_hash_ref->{$site}->{'days_open_per_week'}){
	    $st_hash_ref->{$site}->{'days_open_per_week'} = 7;
	}
	unless($lg_hash_ref->{$site}->{'days_open_per_week'} ){
	    $lg_hash_ref->{$site}->{'days_open_per_week'} = 7;
	}
	
	## load values onto the two hashes
	foreach my $date (sort {$b cmp $a} keys (%{$site_file_info->{$site}}) ){
	    if($s_ctr <= $cut_off && ($order_date->{$date} <= $cut_off)){ ## less than Or equals to 3 days are fine for 5 day stores ##  also, the ordered date range need to be in the cut off limit 
		$st_hash_ref->{$site}->{'counter'} += $site_file_info->{$site}->{$date}->{'counter'};
	    }else{	## if four days not files for 5 days store, will send alarm.	
		$lg_hash_ref->{$site}->{'counter'} += $site_file_info->{$site}->{$date}->{'counter'};	
	    }
	    $s_ctr++;    
	}
    }
    return ($st_hash_ref, $lg_hash_ref);
}


##input: directory path
## output:reference of list of sites, counts
sub get_short_site_list{

}

## count number of files 
sub get_site_file_count{


}


## compare site in two list
## create warning messages in reference to the levels of sites, and days of openings per week 

## input: references
## output: ctr, and content

sub compare_site_stats{
    my ($self, $stand_ref, $test_ref) = @_;
    my $total_sites = 0;
    my $missing_ctr = 0;
    my $msg_content = "RPOS Missing Sales, or New Site Report";
    $msg_content .= "\n=====================================\n";
    my $msg4missing = '';
    foreach my $s_site (sort {$a<=>$b} keys %$stand_ref){
	if($test_ref->{$s_site}->{'counter'}> 0){
	    ## $msg_content .=  sprintf('%s%3s%5s', "$s_site:","30 vs past $test_ref->{$s_site}->{'cut_off'} days:", "$stand_ref->{$s_site}->{'counter'} vs $test_ref->{$s_site}->{'counter'}\n"); ## this is to print list sites, number of files of 33 days to  recent (3) days 
	}else{
	    $missing_ctr++;
	    ##$msg_content .= sprintf('%s%3s%5s', "$s_site:","30 vs past $test_ref->{$s_site}->{'cut_off'} days:","$stand_ref->{$s_site}->{'counter'} vs $test_ref->{$s_site}->{'counter'}\n");
	    $msg4missing .=  "MISSING data for $s_site\n";
	}
	$total_sites ++;
    }
    
    if($msg4missing){
	$msg_content .= $msg4missing;
    }
    
    ## adding newly found site id in the msg
    my $newsite_ctr = 0;
    my $newsite_buffer = '';
    foreach my $t_site (sort {$a<=>$b} keys %$test_ref){
	unless($stand_ref->{$t_site}->{'counter'} > 0){
	    $newsite_ctr++;
	    $newsite_buffer .=  "New site in last 3 days: $t_site\n";
	}
    }
    if($newsite_buffer){
	$msg_content .= $newsite_buffer;
    }
    
    $msg_content .= "======================================\n";

    my $remaining_ctr = $total_sites - $missing_ctr;
    if ($missing_ctr > 0){
	$msg_content .= 
 "Total sites: $total_sites,  Active sites: $remaining_ctr  
\nNumber of sites that may be missing sales: $missing_ctr
\nSites added in last 3 days: $newsite_ctr
\n";
    }else{
	$msg_content .= 
 "Total sites: $total_sites,  Active sites: $remaining_ctr
\nSites added in last 3 days: $newsite_ctr
\n";
    }
    return ($missing_ctr, $msg_content);
}

sub destructor{
    my ($self) = @_;
    if ($self->{'dbi_obj'}){
	$self->{'dbi_obj'}->disconnect();	
    }
    $self = undef;
}

## input: content of the warnings
## output: none, will just send emails when need to
sub email_error_notice{

}


## input: base directory for the list of date directory
## output: reference keyed by count, and date as value

sub get_dir_date_in_dsc_order{
    my ($self) = @_;
    my $base_dir = $self->_get_attribute('rms_dir');
    my @dirs  = glob ("$base_dir/20*");
    my $ctr = 0;
    my $dates_ref;
    foreach my $dir (sort {$b cmp $a} @dirs){
	##print "dir: $dir\n";
	my $fd = basename($dir);
	##print "base dir: $fd\n";
	if($fd){
	    if($ctr <= 34){
		$ctr++;
		$dates_ref->{$fd} = $ctr;
	    }
	}
    }
    return $dates_ref;
}





1;

__END__

=pod

=head1 NAME

IBIS::MonitorRpos A package to monitor Rpos sales, FMS files flow

=head1 VERSION

This documentation refers to IBIS::MonitorRpos.pm 1.0

=head1 SYNOPSIS

    

=head1 DESCRIPTION

The program will be some untility functions for Monitoring Rpos data flow

=head1 SUBROUTINES/METHODS

=over 4

=item 

=item 

=item  

=item  

=back

=head1 DIAGNOSTICS

None.

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over 4

=item * 

=item * 

=item * 

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

