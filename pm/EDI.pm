package IBIS::EDI;
use strict;
use base ('Class::Accessor');
use IBIS::Config::Auto;
use IBIS::FTP;
use Carp;
use Net::SFTP;
use Net::SFTP::Foreign;
use IBIS::Crypt;
use Data::Dumper;
use Sys::Hostname;
use POSIX qw(strftime WNOHANG);
use IBIS::EmailHtml;
use Devel::StackTrace;

our $debug;

sub new {
    my ( $class, %args ) = @_;
    my $self = {};
    bless $self, $class;
    if ( $args{conf_file} ) {
        $self->_make_accessors( \%args );
    }
    return $self;
}

sub _make_accessors {
    my ( $self, $args ) = @_;

    my $config = Config::Auto::parse( $args->{conf_file}, format => $args->{conf_format} || 'equal' );

    Class::Accessor->mk_accessors( keys %{$config} );
    foreach ( keys %{$config} ) {
        $self->$_( $config->{$_} );
    }
}

## Get next flatfile name of the day, in the format of RTK_TELXON_20080730120135_001, 002, 003 etc
##updated, seqnum_start was required to start with 9

sub filename_with_increment {
    my ( $self, $path, $pattern, $seqnum_start ) = @_;
    opendir DIR, "$path";
    my @files = readdir(DIR);
    closedir DIR;
    my @items = map { /($pattern\d+_\d+)/ } sort @files;

    ## get max filename index
    ## get a start sequence number:
    my $max;
    if ( $seqnum_start eq '' ) {
        $max = 0;
    }
    else {
        $max = $seqnum_start;
    }

    if ( @items > 0 ) {
        ## pick the largest number of the day, increase it by one.
        foreach my $file (@items) {
            $file =~ /$pattern\d+_(\d+)/g;
            my $temp = $1;
            $temp =~ s/^0//;
            if ( $temp > $max ) {
                $max = $temp;
            }
        }
    }
    $max = $max + 1;    ## increase by one at a time
    ## put 0s in the front of the number
    my $name_str = $self->get_prefixed_string( 3, $max, '0' );
    return $name_str;
}

sub get_flatfile_buffer {
    my ( $self, $flatfile ) = @_;
    my $buffer = '';    ## "\n\t    Record entries:\n";
    open( IN, "<$flatfile" );
    my $last_po;
    my @entries = <IN>;
    foreach my $entry (@entries) {
        my $new_line;
        my $cur_po    = substr( $entry, 64, 10 );
        my $rest_line = substr( $entry, 0,  64 );
        if ( $cur_po ne $last_po ) {
            $last_po = $cur_po;
            $buffer .= "\n\t  " . $entry;
        }
        else {
            ##$new_line .= "\t  "."$rest_line\n";
            next;
        }

        #$buffer .= $new_line;
    }
    close IN;
    if ( $buffer ne '' ) {
        $buffer = "\n\t    Record entries:\n" . $buffer;
    }
    return \$buffer;
}

sub sendnotice {
    my ( $self, $to_str, $cc_str, $subject, $body ) = @_;
    my $hdrstr = "";

    $hdrstr .= "To: $to_str\n";
    $hdrstr .= "From: rdistaff\@usmc-mccs.org\n";
    $hdrstr .= "Cc: $cc_str\n" if $cc_str;
    $hdrstr .= "Subject: $subject\n";
    my $hostname = hostname;
    $body .= "\n\temail from host: $hostname\n";

    open MAIL, "|/usr/sbin/sendmail -t" or die "Can not open sendmail\n";
    print MAIL $hdrstr . "\n";
    print MAIL $body . "\n";
    close MAIL;
    return 1;
}

## input: $db_operrors
## output: $ref_to_new_errors
sub commit_or_failure_notice {
    my ( $self, $db_op_errors, $subject ) = @_;
    my $size = 0;
    if ( defined $db_op_errors ) {
        $size = @$db_op_errors;
    }

    my $db_success = 0;
    my @second_level;
    if ( $size == 0 ) {
        eval { $self->{'dbh'}->commit(); };
        if ($@) {
            push( @second_level, $@ );
            $self->{'log_obj'}->log_info($@);
            die();
        }
        else {
            $db_success = 1;
            $self->{'log_obj'}->log_info($subject." Current db processes are OK, changes commited to db.\n");
            ## print "Current db processes are OK, changes commited to db.\n";
        }
    }
    else {
        $self->{'dbh'}->rollback();
        my $msg =
          "No new data. Or db operation error. Will rollback if later cases. error:  @$db_op_errors \n";
        $self->{'log_obj'}->log_info($msg);
        print $msg;
        $self->sendnotice( $self->{POC}, $self->{MAIL_FROM}, $subject, $msg );
    }
    return $db_success;
}

## NOT a generic function!!!! do not use elesewhere except in runing edi_850_spscommerce.pl
## By this time, the ept has been updated for the edi_seq_id by this extraction.
## date_processed has been updated as sysdate. Only the ride_session_ind in edi_850_staging table is still null.
##

sub get_total_number_of_40lines_by_PO {
    my ( $self, $po_id ) = @_;

### THIS IS JUST TEST FOR THE SVN THING
    #    my $query1 = "select count(*)
    #from $self->{'TABLE_EPT'} e,
    #$self->{'TABLE_E8S'} s
    #where e.key_data =\'$po_id\'
    #and substr(e.transaction_data, 17, 2) ='40'
    #and e.edi_sequence_id = s.edi_sequence_id
    #and s.ride_processed_fin_ind ='N'
    #and s.ride_processed_fin_date is null
    #";

## get it from 90 line in e8s table

    my $query1 = "
select substr(transaction_data, 19, 6) 
from $self->{'TABLE_E8S'} 
where po_id = \'$po_id\' 
and ride_processed_fin_ind = 'N'
and ride_processed_fin_date is null
and data_type ='90'
";
    my $ret1 = $self->{'dbh'}->selectall_arrayref($query1);
    if ( $ret1->[0][0] ) {
        return $ret1->[0][0];
    }
    else {
        warn "failed to get line 40 count for $po_id, by $query1\n";
        return $ret1->[0][0];
    }
}

## Not a generic function:

sub get_total_quantity_of_lineitems_by_PO {
    my ( $self, $po_id ) = @_;

    #    my $query1 = "
    #select substr(transaction_data, 19,10)
    #from $self->{'TABLE_EPT'}
    #where key_data =\'$po_id\'
    #and substr(transaction_data, 17, 2) ='40'
    #";

    my $query1 = "
select substr(transaction_data, 19, 10) 
from $self->{'TABLE_E8S'} 
where po_id = \'$po_id\' 
and ride_processed_fin_ind = 'N'
and ride_processed_fin_date is null
and data_type ='40'
order by edi_sequence_id asc
";

    my $ret1 = $self->{'dbh'}->selectall_arrayref($query1);
    ##print Dumper($ret1);

    my $total_qty;
    for ( my $i = 0 ; $i < @$ret1 ; $i++ ) {
        $total_qty = $total_qty + $ret1->[$i]->[0];
    }
    return $total_qty;
}

## return 1 or 0 for the list and log messages
sub confirm_sent_POs {
    my ( $self, $sent_po_list, $staging_table ) = @_;
    my $result_ref;
    my $flag = 0;
    my $msg  = '';
    my $list = '';
    foreach my $po (@$sent_po_list) {
        if ( $po ne '' ) {
            $list .= '\''."$po".'\''. ",";
            $result_ref->{$po}->{'ept'} = '';
            $result_ref->{$po}->{'e8s'} = '';
        }
    }
    $list =~ s/\,$//g;    #remove the last ','
##
## check ept
    my $query1 = "
select distinct(key_data)  
from $self->{'TABLE_EPT'}
where key_data in ($list) 
and date_processed is not null
and ride_session_id = 100";

    my $ret1 = $self->{'dbh'}->selectall_arrayref($query1);
    ##print Dumper($ret1);

    for ( my $i = 0 ; $i < @$ret1 ; $i++ ) {
        $result_ref->{ $ret1->[$i]->[0] }->{'ept'} = 'Y';
    }

## check e8s
    my $query2 = "
select distinct(po_id)  
from $staging_table
where po_id in ($list) 
and ride_processed_fin_ind = 'Y'
and ride_processed_fin_date is not null";

    my $ret2 = $self->{'dbh'}->selectall_arrayref($query2);
    ##print Dumper($ret2);

    for ( my $i = 0 ; $i < @$ret2 ; $i++ ) {
        $result_ref->{ $ret2->[$i]->[0] }->{'e8s'} = 'Y';
    }
    ##print Dumper($result_ref);

## analysis on the result:
    foreach my $po_id ( keys %$result_ref ) {
        ## 4 cases
        if (   $result_ref->{$po_id}->{'ept'} eq 'Y'
            && $result_ref->{$po_id}->{'e8s'} eq 'Y' )
        {
            $msg .= "$po_id in both table, passed confirmation\n";
        }
        elsif ($result_ref->{$po_id}->{'ept'} eq 'Y'
            && $result_ref->{$po_id}->{'e8s'} eq '' )
        {
            $msg .= "$po_id is in EPT, Not in Staging table\n";
            $flag = 1;
        }
        elsif ($result_ref->{$po_id}->{'ept'} eq ''
            && $result_ref->{$po_id}->{'e8s'} eq 'Y' )
        {
            $msg .= "$po_id is in Not in EPT, but in Staging table\n";
            $flag = 1;
        }
        else {
            $msg .= "$po_id NOT in EPT or Staging table. It may not have been transmitted before.\n";
            $flag = 1;
        }
    }
    return ( $flag, $msg );
}

## return 1 or 0 for the list and log messages
sub confirm_sent_860_POs {
    my ( $self, $sent_po_list, $c_date ) = @_;
    my $result_ref;
    my $flag = 0;
    my $msg  = '';
    my $list = '';
    foreach my $po (@$sent_po_list) {
        if ($po) {
            $list .= '\''."$po" .'\''.",";
            $result_ref->{$po}->{'ept'} = '';    ## put a blank value before confirmed
        }
    }
    $list =~ s/\,$//g;                           #remove the last ','

##
## check ept
    my $query1 = "
select distinct(key_data)  
from $self->{'TABLE_EPT'}
where key_data in ($list) 
and date_processed is not null 
and ride_session_id = 100
and transaction_set ='860'";

    if ($c_date) {
        $query1 .= "  and date_created like \'%$c_date%\' ";
    }

    my $ret1 = $self->{'dbh'}->selectall_arrayref($query1);

    for ( my $i = 0 ; $i < @$ret1 ; $i++ ) {
        $result_ref->{ $ret1->[$i]->[0] }->{'ept'} = 'Y';
    }

## analysis on the result:
    foreach my $po_id ( keys %$result_ref ) {
        ## 2 cases
        if ( $result_ref->{$po_id}->{'ept'} eq 'Y' ) {
            $msg .= "$po_id in table, passed confirmation\n";
        }
        elsif ( $result_ref->{$po_id}->{'ept'} eq '' ) {
            $msg .= "$po_id is in Not in EPT\n";
            $flag = 1;
        }
    }
    return ( $flag, $msg );
}

sub update_resending_po_in_e8s {
    my ( $self, $po_list_ref, $date ) = @_;
    my $flag = 0;
    my $list = '';
    foreach my $po_id (@$po_list_ref) {

        my $query = " update 
         $self->{'TABLE_E8S'} 
         set ride_processed_fin_ind ='N', 
         ride_processed_fin_date = null 
         where po_id = \'$po_id\'";

        if ($date) {
            ##$query .= " and ride_processed_fin_date like \'%$date%\'";
            $query .= "  and  edi_sequence_id in (
         select edi_sequence_id
         from $self->{TABLE_EPT} 
         where date_created like 
         \'%$date%\'
          and key_data = \'$po_id\' )";
        }

        print "QQQuery: $query\n" if ( $self->{'debug'} );
        eval { $self->{'dbh'}->do($query); };

        if ($@) {
            $self->{'log_obj'}->log_info($@);
            die();
        }
        else {
            $self->{'dbh'}->commit();
            $self->{'log_obj'}->log_info( "update resend po_list in e8s are OK, changes commited to db.\n" );
            $flag = 1;
        }
    }
    return $flag;
}

sub refresh_860_pos_for_resending {
    my ( $self, $po_list_ref, $date ) = @_;
    my $flag = 0;
    my $list = '';

    foreach my $po_id (@$po_list_ref) {

        my $query = " update 
         $self->{'TABLE_EPT'} 
         set date_processed = null, 
          ride_session_id = null     
         where transaction_set = '860'
         and key_data = \'$po_id\' ";

        if ($date) {
            $query .= " and  date_created like \'%$date%\'";
        }

        print "Refresh 860 Query: $query\n" if $self->{'debug'};

        eval { $self->{'dbh'}->do($query); };

        if ($@) {
            $self->{'log_obj'}->log_info($@);
            die();
        }
        else {
            $self->{'dbh'}->commit();
            $self->{'log_obj'}->log_info( "update resend po_list in e8s are OK, changes commited to db.\n" );
            $flag = 1;
        }
    }
    return $flag;
}

sub collect_edi_850_stack {
    my ( $self, $po_list_ref ) = @_;
    my $query2 =
"                                                                                                                                          
select                                                                                                                                               
edi_sequence_id,
po_id,                                                                                                                                    
transaction_data,                                                                                                                                    po_type,                                                                                                                                            
origin  
from                                                                                                                                                     $self->{TABLE_E8S}                                                                                                                                 
where                                                                                                                                                 
    RIDE_PROCESSED_FIN_IND = 'N'                                                                                                                      
    AND                                                                                                                                               
    RIDE_PROCESSED_FIN_DATE is null                                                                                                                   
";

    my $list = '';
    if ( defined $po_list_ref ) {
        foreach my $po_id (@$po_list_ref) {
            $list .= '\''."$po_id".'\''.",";
        }
        $list =~ s/\,$//g;
        $query2 .= " AND po_id in ( $list ) ";
    }

    print $query2 if ( $self->{'debug'} );
    my $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "edi_sequence_id" );
    ## CASE MATTERS HERE\!!!
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "EDI_SEQUENCE_ID" );
    }

    if ( keys(%$value_ref) == 0 ) {
        $self->{'log_obj'}->log_info("extract query not working: $query2");
    }
    return $value_ref;
}

sub collect_edi_850_stack2 {
    my ( $self, $po_list_ref ) = @_;
    my $query2 =
"                                                                                                                                          
select s.edi_sequence_id,s.po_id, e.transaction_data, s.po_type,s.origin, e.date_created  
from $self->{TABLE_E8S} s,
$self->{TABLE_EPT} e 
where s.RIDE_PROCESSED_FIN_IND = 'N' AND s.RIDE_PROCESSED_FIN_DATE is null and s.edi_sequence_id 
= e.edi_sequence_id";

    my $list = '';
    if ( defined $po_list_ref ) {
        foreach my $po_id (@$po_list_ref) {
            $list .= '\''."$po_id".'\''.",";
        }
        $list =~ s/\,$//g;
        $query2 .= " AND s.po_id in ( $list ) ";
    }

    print $query2 if ( $self->{'debug'} );
    my $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "edi_sequence_id" );
    ## CASE MATTERS HERE\!!!
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "EDI_SEQUENCE_ID" );
    }

    if ( keys(%$value_ref) == 0 ) {
        $self->{'log_obj'}->log_info("extract query not working: $query2");
    }
    return $value_ref;
}

sub empty_nps_stack {
    my ($self) = @_;
    my $query = "
select 
    PO_ID,
    EDI_SEQUENCE_ID,  
    QTY_ORDERED,
    BAR_CODES,
    NEX_RICHTER_SITE,
    NEX_WHSE,
    BUSINESS_UNIT_ID, 
    SITE_ID
from 
   $self->{TABLE_NPS}
where
    RIDE_PROCESSED_FIN_IND = 'N' 
    AND                                                                                            
    RIDE_PROCESSED_FIN_DATE is null   
";

##    $self->{'log_obj'}->log_info("extract query: $query");
##    my $query2 = "select * from $self->{TABLE_NPS}";
##    write into a hash that easy to create ftp files

    print "query: $query\n";
    my $nps_ref;
    my $value_ref = $self->{'dbh'}->selectall_arrayref($query);
    for ( my $i = 0 ; $i < @$value_ref ; $i++ ) {
        $nps_ref->{ $value_ref->[$i][0] }->{ $value_ref->[$i][1] }->{'qty_ordered'} = $value_ref->[$i][2];
        $nps_ref->{ $value_ref->[$i][0] }->{ $value_ref->[$i][1] }->{'upc'}         = $value_ref->[$i][3];
        $nps_ref->{ $value_ref->[$i][0] }->{ $value_ref->[$i][1] }->{'nex_richter_site'} =
          $value_ref->[$i][4];
        $nps_ref->{ $value_ref->[$i][0] }->{ $value_ref->[$i][1] }->{'nex_whse'} = $value_ref->[$i][5];
        $nps_ref->{ $value_ref->[$i][0] }->{ $value_ref->[$i][1] }->{'business_unit_id'} =
          $value_ref->[$i][6];
        $nps_ref->{ $value_ref->[$i][0] }->{ $value_ref->[$i][1] }->{'site_id'} = $value_ref->[$i][7];
        $nps_ref->{ $value_ref->[$i][0] }->{ $value_ref->[$i][1] }->{'po_id'}   = $value_ref->[$i][0];
    }
    return $nps_ref;
}

sub create_edi_ftp_files_from_nps_data {
##   $fh, $nex_po_staging_ref->{$po_id}, $local_ftp_file
##    my ($fh, $data_ref, $filename) = @_;
    my ( $self, $data_ref, $fh, $filename ) = @_;
    my $rctr = 0;
    foreach my $po_id ( keys %{$data_ref} ) {
        ##print "poid: $po_id\n";
        foreach my $edi_seq_id ( sort { $a <=> $b } keys %{ $data_ref->{$po_id} } ) {
            ##print "seq_id: $edi_seq_id\n";
            if ( defined( $data_ref->{$po_id}->{$edi_seq_id}->{'upc'} ) ) {
                print "bar_code: $data_ref->{$po_id}->{$edi_seq_id}->{'upc'}\n";
                ## my $po_data_str = $self->get_one_line_data( $data_ref->{$po_id}->{$edi_seq_id} );
		my $po_data_str = $self->get_one_line_data_v2( $data_ref->{$po_id}->{$edi_seq_id} );
                my $len         = length($po_data_str);
                ##print "length: $len\n"; ## this should be 74
                if ( $po_data_str =~ /\d+/g ) {    ## this is just to avoid blank lines
                    print $fh "$po_data_str\n";
                    $rctr++;
                }
            }
        }
    }
    return ( $filename, $rctr );
}

sub get_one_line_data {
    my ( $self, $data_per_sid ) = @_;
    my $time = strftime( '%Y%m%d', localtime );
    my $str_data = '';
    if (   ( $data_per_sid->{upc} ne '' )
        && ( $data_per_sid->{qty_ordered} ne '' ) )
    {
        my (
            $d1_4,   $d5_8,   $d9_11,  $d12_15, $d16_23, $d24_25, $d26,
            $d27_30, $d31_43, $d44_50, $d51_60, $d61_63, $d64,    $d65_74
        );
        $d1_4   = $self->get_fix_length_string( 4, '',                                ' ' );
        $d5_8   = $self->get_fix_length_string( 4, '',                                ' ' );
        $d9_11  = $self->get_fix_length_string( 3, '',                                ' ' );
        $d12_15 = $self->get_fix_length_string( 4, $data_per_sid->{nex_richter_site}, ' ' );    ##???
        $d16_23 = $self->get_fix_length_string( 8, $time,                             ' ' );    ##yyyymmdd
        $d24_25 = $self->get_fix_length_string( 2, '',                                ' ' );
        $d26    = $self->get_fix_length_string( 1, '',                                ' ' );
        $d27_30 = $self->get_prefixed_string( 4,  $data_per_sid->{qty_ordered}, '0' );
        $d31_43 = $self->get_prefixed_string( 13, $data_per_sid->{upc},         '0' );
        $d44_50 = $self->get_fix_length_string( 7,  '',                        ' ' );
        $d51_60 = $self->get_fix_length_string( 10, '',                        ' ' );
        $d61_63 = $self->get_fix_length_string( 3,  $data_per_sid->{nex_whse}, ' ' );
        $d64    = $self->get_fix_length_string( 1,  '',                        ' ' );
        $d65_74 = $self->get_prefixed_string( 10, $data_per_sid->{po_id}, '0' );
        $str_data =
            $d1_4 
          . $d5_8 
          . $d9_11 
          . $d12_15 
          . $d16_23 
          . $d24_25 
          . $d26 
          . $d27_30 
          . $d31_43 
          . $d44_50 
          . $d51_60
          . $d61_63
          . $d64
          . $d65_74;
    }
    return $str_data;
}

sub get_one_line_data_v2 {
    my ( $self, $data_per_sid ) = @_;
    my $time = strftime( '%Y%m%d', localtime );
    my $str_data = '';
    if (   ( $data_per_sid->{upc} ne '' )
        && ( $data_per_sid->{qty_ordered} ne '' ) )
    {
        my (
            $d1_4,   $d5_8,   $d9_11,  $d12_15, $d16_23, $d24_25, $d26,
            $d27_34, $d35_48, $d49_55, $d56_60, $d61_63, $d64,    $d65_74
        );
        $d1_4   = $self->get_fix_length_string( 4, '',                                ' ' );
        $d5_8   = $self->get_fix_length_string( 4, '',                                ' ' );
        $d9_11  = $self->get_fix_length_string( 3, '',                                ' ' );
        $d12_15 = $self->get_fix_length_string( 4, $data_per_sid->{nex_richter_site}, ' ' );    ##???
        $d16_23 = $self->get_fix_length_string( 8, $time,                             ' ' );    ##yyyymmdd
        $d24_25 = $self->get_fix_length_string( 2, '',                                ' ' );
        $d26    = $self->get_fix_length_string( 1, '',                                ' ' );
        $d27_34 = $self->get_prefixed_string( 8,  $data_per_sid->{qty_ordered}, '0' );
	## $d27_30 = $self->get_prefixed_string( 4,  $data_per_sid->{qty_ordered}, '0' ); ## increase length yuc Oct 25, 15
        ## $d35_48 = $self->get_prefixed_string( 14, $data_per_sid->{upc},         ' ' ); ## Donna aske to do suffixed blank Nov 18, 15. yuc
	## $d31_43 = $self->get_prefixed_string( 13, $data_per_sid->{upc},         '0' ); ## increase length yuc Oct 25, 15

	$d35_48 = $self->get_suffixed_string( 14, $data_per_sid->{upc},' ');
        $d49_55 = $self->get_fix_length_string( 7,  '',                        ' ' );
        $d56_60 = $self->get_fix_length_string( 5, '',                        ' ' );
        $d61_63 = $self->get_fix_length_string( 3,  $data_per_sid->{nex_whse}, ' ' );
        $d64    = $self->get_fix_length_string( 1,  '',                        ' ' );
        $d65_74 = $self->get_prefixed_string( 10, $data_per_sid->{po_id}, '0' );
        $str_data =
            $d1_4 
          . $d5_8 
          . $d9_11 
          . $d12_15 
          . $d16_23 
          . $d24_25 
          . $d26 
          . $d27_34 
          . $d35_48 
          . $d49_55 
          . $d56_60
          . $d61_63
          . $d64
          . $d65_74;
    }
    return $str_data;
}


sub get_fix_length_string {
    my ( $self, $mxlen, $field_data, $place_hd ) = @_;
    my $ret_str = '';
    my $d_len   = 0;

    if ( defined $field_data ) {
        $field_data =~ s/^\s+//;    ## remove front space
        $field_data =~
          s/\s+$//;    ## remove back space                                                                  \

        $d_len = length($field_data);
    }

    if ( $mxlen >= $d_len ) {
        $ret_str = $field_data;
        for ( my $i = 0 ; $i < ( $mxlen - $d_len ) ; $i++ ) {
            $ret_str .= $place_hd;
        }
    }
    else {
        $ret_str = $field_data;
        $ret_str = substr( $field_data, 0, $mxlen );
    }
    return $ret_str;
}

sub sftp_one_file_to_nexcom {
    my ( $self, $local_file, $remote_file ) = @_;
    my $c    = IBIS::Crypt->new();
    my $sftp = Net::SFTP->new(
        $self->{REMOTE_SERVER},
        user     => $self->{SFTP_USER},
        password => $c->decrypt( $self->{PASSWORD} )
    ) or die " Can not connect to remote server: $self->{REMOTE_SERVER} !\n";

    my $ftp_result = $sftp->put( $local_file, $remote_file );
    if ($ftp_result) {
        $self->{'log_obj'}->log_info("\nSFTP successful!\n");
    }
    else {
        $self->{'log_obj'}->log_info("\nSFTP failed for some reason: $!\n");
    }
    return $ftp_result;
}

sub sftp_one_file_to_remote_server {
    my ( $self, $local_file, $remote_file ) = @_;
    my $c    = IBIS::Crypt->new();
    my $sftp = Net::SFTP->new(
        $self->{REMOTE_SERVER},
        user     => $self->{SFTP_USER},
        password => $c->decrypt( $self->{PASSWORD} )
    ) || die " Can not connect to remote server: $self->{REMOTE_SERVER} !\n";

    my $ftp_result = $sftp->put( $local_file, $remote_file );
    if ($ftp_result) {
        $self->{'log_obj'}->log_info("\nSFTP successful!\n");
    }
    else {
        $self->{'log_obj'}->log_info("\nSFTP failed for some reason: $!\n");
    }
    return $ftp_result;
}

#-------------------------------------------------------------------------------
# ftp_one_file
#
# Thu Jun 12 09:42:37 EDT 2014
#   HJ: Convert from Net::FTP to Net::SFTP::Foreign
#
#-------------------------------------------------------------------------------
sub ftp_one_file {
    my ( $self, $local_file, $remote_file ) = @_;
    my $c = IBIS::Crypt->new();
    my $sftp;
    my $result = 0;
    my $pass = $c->decrypt( $self->{PASSWORD} );
    $sftp = Net::SFTP::Foreign->new(
        $self->{REMOTE_SERVER},
        user     => $self->{SFTP_USER},
        password => $pass,
        port     => '10022'
    );
    $sftp->die_on_error("Unable to establish SFTP connection ");
    
    if ( -e $local_file ) {

        $sftp->put( $local_file, $remote_file );

        if ( $sftp->error() ) { 
            $sftp->disconnect;
            return 0; 
        } else { 
	        $sftp->chmod($remote_file, 0777);
            $sftp->disconnect;
            return 1; 
        }
        #   or die( "Could not sftp put $local_file to $remote_file because " . $sftp->error() );
    }
    else {
        die "Could not sftp put, local file $local_file missing";
    }
}

sub update_E8S_table {
    my ( $self, $nex_po_recorded_ref ) = @_;
    my @errors;
    my $statement =
"update $self->{'TABLE_E8S'} set ride_processed_fin_ind ='Y', ride_processed_fin_date =sysdate where edi_sequence_id = ? ";
    my $sth = $self->{'dbh'}->prepare($statement);
    if ( defined $nex_po_recorded_ref ) {
        foreach my $edi_seq_id ( sort { $a <=> $b } %$nex_po_recorded_ref ) {
            if ( $edi_seq_id =~ /^\d+/ ) {
                eval { $sth->execute($edi_seq_id); };
                if ($@) {
                    push( @errors, $@ );
                    $self->{'log_obj'}->log_info("$@");
                }
                else {
                    my $msg = "update $self->{'TABLE_E8S'} with edi_seq_id: $edi_seq_id worked\n";
                    ##print $msg;
                    ##  $self->{'log_obj'}->log_info($msg);
                }
            }
        }
    }
    return \@errors;
}

sub update_E8S_by_PO {
    my ( $self, $po_id, $opt_l, $cdate ) = @_;
    my @errors;
    my $statement;
    if ( !$opt_l ) {
        $statement =
"update $self->{'TABLE_E8S'} set ride_processed_fin_ind ='Y', ride_processed_fin_date =sysdate where po_id = ? and ride_processed_fin_date is null";

        my $sth = $self->{'dbh'}->prepare($statement);
        eval { $sth->execute($po_id); };
        if ($@) {
            push( @errors, $@ );
            $self->{'log_obj'}->log_info("$@");
        }
        else {
            my $msg = "update $self->{'TABLE_E8S'} with po_id: $po_id  worked\n";

        }
    }
    else {

        $statement =
"update $self->{'TABLE_E8S'} set ride_processed_fin_ind ='Y', ride_processed_fin_date =sysdate where edi_sequence_id in (select edi_sequence_id from $self->{'TABLE_EPT'} where key_data = \'$po_id\' and date_created like \'%$cdate%\' )";
        my $sth = $self->{'dbh'}->prepare($statement);
##	my @bding_val;
##	push(@bding_val, $po_id);
##	push(@bding_val, $cdate);
##	eval { $sth->execute(@bding_val); };
        eval { $sth->execute(); };
        if ($@) {
            push( @errors, $@ );
            $self->{'log_obj'}->log_info("$@");
        }
        else {
            my $msg = "update $self->{'TABLE_E8S'} with po_id: $po_id cdate: $cdate worked\n";

        }
    }

    return \@errors;
}

sub update_NPS_table {
    my ( $self, $nex_po_recorded_ref ) = @_;
    my @errors;
    my $statement =
"update $self->{'TABLE_NPS'} set ride_processed_fin_ind ='Y', ride_processed_fin_date =sysdate where edi_sequence_id = ? ";
    my $sth = $self->{'dbh'}->prepare($statement);
    if ( defined $nex_po_recorded_ref ) {
        foreach my $edi_seq_id ( sort { $a <=> $b } %$nex_po_recorded_ref ) {
            if ( $edi_seq_id =~ /^\d+/ ) {
                eval { $sth->execute($edi_seq_id); };
                if ($@) {
                    push( @errors, $@ );
                    $self->{'log_obj'}->log_info("$@");
                }
                else {
                    my $msg = "update $self->{'TABLE_NPS'} with edi_seq_id: $edi_seq_id worked\n";
                    ##print $msg;
                    ##  $self->{'log_obj'}->log_info($msg);
                }
            }
        }
    }
    return \@errors;
}

sub update_EPT_table {
    my ( $self, $nex_po_recorded_ref ) = @_;
    my @errors;
    my $statement =
      "update $self->{TABLE_EPT} set date_processed=sysdate, ride_session_id=100 where edi_sequence_id = ? ";
    my $sth = $self->{'dbh'}->prepare($statement);

    if ( %{$nex_po_recorded_ref} ) {
        foreach my $edi_seq_id ( sort { $a <=> $b } %{$nex_po_recorded_ref} ) {
            if ( $edi_seq_id =~ /^\d+/ ) {
                eval { $sth->execute($edi_seq_id); };
                if ($@) {
                    push( @errors, $@ );
                    $self->{'log_obj'}->log_info("$@");
                }
                else {
                    my $msg = "update $self->{TABLE_EPT} with edi_sequence_id: $edi_seq_id succeeded\n";
                    ##print $msg;
                    ## $self->{'log_obj'}->log_info($msg);
                }
            }
        }
    }
    return \@errors;
}

sub insert_NPS_table {
    my ( $self, $data_per_poid ) = @_;
    my $table_name = uc( $self->{TABLE_NPS} );
    my $insert_ctr = 0;
    my @errors;
    my $query = qq{INSERT INTO $table_name VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) };
    my $sth   = $self->{'dbh'}->prepare($query);
    foreach my $edi_seq_id ( keys %{$data_per_poid} ) {
        if ( $edi_seq_id =~ /^\d+/ ) {
            my @ary = (
                $data_per_poid->{$edi_seq_id}->{business_unit_id},
                $data_per_poid->{$edi_seq_id}->{site_id},
                $data_per_poid->{$edi_seq_id}->{qty_ordered},
                $data_per_poid->{$edi_seq_id}->{upc},
                $data_per_poid->{$edi_seq_id}->{nex_richter_site},
                $data_per_poid->{$edi_seq_id}->{nex_whse},
                $data_per_poid->{$edi_seq_id}->{po_id},
                $data_per_poid->{$edi_seq_id}->{edi_sequence_id},
                $data_per_poid->{$edi_seq_id}->{ride_processed_fin_ind},
                $data_per_poid->{$edi_seq_id}->{ride_processed_fin_date}
            );
            eval { $sth->execute(@ary); };
            if ($@) {
                push( @errors, $@ );
                ##print "insert failed for edi_seq_id: $edi_seq_id\n";
                $self->{'log_obj'}->log_info("Insert failed for edi_sequence id: $edi_seq_id\n");
            }
            else {
                $insert_ctr++;
                ##print "Insert sucessful: $edi_seq_id\n";
                ## $self->{'log_obj'}->log_info("Insert successful: $edi_seq_id\n");
            }
        }
    }
    return ( \@errors, $insert_ctr );
}

sub insert_E8S_table {
    my ( $self, $data_per_poid ) = @_;
    my $table_name = uc( $self->{TABLE_E8S} );
    my $insert_ctr = 0;
    my @errors;
    my $query = qq{INSERT INTO $table_name VALUES(?, ?, ?, ?, ?, ?, ?, ?)};
    my $sth   = $self->{'dbh'}->prepare($query);
    foreach my $edi_seq_id ( keys %{$data_per_poid} ) {
        if ( $edi_seq_id =~ /^\d+/ ) {
            my @ary = (
                $data_per_poid->{$edi_seq_id}->{edi_sequence_id},
                $data_per_poid->{$edi_seq_id}->{transaction_data},
                $data_per_poid->{$edi_seq_id}->{key_data},
                $data_per_poid->{$edi_seq_id}->{data_type},
                $data_per_poid->{$edi_seq_id}->{po_type},
                $data_per_poid->{$edi_seq_id}->{origin},
                'N',
                $data_per_poid->{$edi_seq_id}->{ride_processed_fin_date}
            );

            eval { $sth->execute(@ary); };
            if ($@) {
                push( @errors, $@ );
                ##print "insert failed for edi_seq_id: $edi_seq_id\n";
                $self->{'log_obj'}->log_info("Insert failed for edi_sequence id: $edi_seq_id\n");
            }
            else {
                $insert_ctr++;
            }
        }
    }
    return ( \@errors, $insert_ctr );
}

sub insert_E8S_wo_transaction_data {
    my ( $self, $data_per_poid ) = @_;
    my $table_name = uc( $self->{TABLE_E8S} );
    my $insert_ctr = 0;
    my @errors;
    my $query = qq{INSERT INTO $table_name VALUES(?, ?, ?, ?, ?, ?, ?, ?)};
    my $sth   = $self->{'dbh'}->prepare($query);
    foreach my $edi_seq_id ( keys %{$data_per_poid} ) {
        if ( $edi_seq_id =~ /^\d+/ ) {
            my @ary = (
                $data_per_poid->{$edi_seq_id}->{edi_sequence_id},
                '',
                $data_per_poid->{$edi_seq_id}->{key_data},
                $data_per_poid->{$edi_seq_id}->{data_type},
                $data_per_poid->{$edi_seq_id}->{po_type},
                $data_per_poid->{$edi_seq_id}->{origin},
                'N',
                $data_per_poid->{$edi_seq_id}->{ride_processed_fin_date}
            );

            eval { $sth->execute(@ary); };
            if ($@) {
                push( @errors, $@ );
                ##print "insert failed for edi_seq_id: $edi_seq_id\n";
                $self->{'log_obj'}->log_info("Insert failed for edi_sequence id: $edi_seq_id\n".$@);
            }
            else {
                $insert_ctr++;
            }
        }
    }
    return ( \@errors, $insert_ctr );
}

## need to check with someone to make sure these creterias are correct.
sub check_valid_upc {
    my ( $self, $dbh, $log, $test_upc ) = @_;
    my $flag = 1;    ## if valid, return 1
## remove blank space at the end:
    $test_upc =~ s/\s+$//g;
##  length check
    if ( $test_upc !~ /\d+/g ) {
        $flag = 0;
        $log->log_info("UPC wrong, no numbers in: $test_upc <-");

    }
    elsif ( length($test_upc) < 3 ) {
        $flag = 0;
        $log->log_info("length of the upc NOT right, less than 3?: $test_upc <-");
## if letters in upc, check
    }
    elsif ( $test_upc =~ /[A-Za-z]+/g ) {
        $flag = 0;
        $log->log_info("There are letters in UPC id?? $test_upc <-");

    }
    else {
## database check? Here, we assume all the upcs existing in the bar_codes table
        my $query     = "select bar_code_id from bar_codes where bar_code_id = \'$test_upc\'";
        my $value_ref = $dbh->selectall_arrayref($query);
        if ( $value_ref->[0][0] eq undef ) {
            $flag = 0;
            $log->log_info("NO such UPC in local database: $test_upc <-\n");
        }
    }
    return $flag;
}

sub get_store_info {
    my ( $self, $dbh, $log, $site_id ) = @_;
    my $temp_id = $self->get_prefixed_string( 5, $site_id, '0' );

    my $query     = "select nex_richter_site, nex_whse from $self->{TABLE_NS} where site_id = \'$temp_id\'";
    my $value_ref = $dbh->selectall_arrayref($query);
    if ( $value_ref->[0][0] ne undef ) {
        return ( $value_ref->[0][0], $value_ref->[0][1] );
    }
    else {
        $log->log_info("Trouble! No  nex_richter_site, nex_whse info for $temp_id");
        return;
    }
}

## input: filehandle, tablename
## output: schema_reference in order
##         and requirement

#input: transaction_data string,
#output:
# BUSINESS_UNIT_ID  from business_id
# SITE_ID  from t_d  83-87
# QTY_ORDERED   from _t_d 19-28
# UPC    from t_d     49-61
# PO_ID          from key_data
# RIDE_PROCESSED_FIN_IND ->'N' before update
# RIDE_PROCESSED_FIN_DATE ->blank till update

sub parse_transaction_data_str {
    my ( $self, $valid_data_ref ) = @_;
    my $nex_po_staging_data;
    my $total = 0;
    foreach my $key_data ( keys %$valid_data_ref ) {
        foreach my $edi_seq_id ( keys %{ $valid_data_ref->{$key_data} } ) {
            my $transaction_data_str = $valid_data_ref->{$key_data}->{$edi_seq_id}->{'transaction_data'};
            my $business_unit_id     = $valid_data_ref->{$key_data}->{$edi_seq_id}->{'business_unit_id'};
            my $site_id              = $valid_data_ref->{$key_data}->{$edi_seq_id}->{'site_id'};
            my $nex_richter_site     = $valid_data_ref->{$key_data}->{$edi_seq_id}->{'nex_richter_site'};
            my $nex_whse             = $valid_data_ref->{$key_data}->{$edi_seq_id}->{'nex_whse'};
            my $td_se_id             = $valid_data_ref->{$key_data}->{$edi_seq_id}->{'td_se'};
            ##   if(($td_se_id eq '30') or ($td_se_id eq '40')){ #### ONLY 30 and 40 in column 17,18 are selected!!! OR only 40's case consider???
            if ( $td_se_id eq '40' ) {
                my ( $qty_ordered, $upc, $po_id );
                $transaction_data_str =~ s/^\s+//;
                my @td_ary = split( //, $transaction_data_str );
                for ( my $i = 0 ; $i < @td_ary ; $i++ ) {
                    if ( ( $i >= 18 ) && ( $i <= 27 ) ) {
                        if ( $td_se_id eq '40' ) {    ## only the 40 case of 19-28 considered
                            if ( $td_ary[$i] ne ' ' ) {
                                $qty_ordered .= $td_ary[$i];
                            }
                        }
                    }
                    if ( ( $i >= 48 ) && ( $i <= 60 ) ) {
                        if ( $td_ary[$i] ne ' ' ) {
                            $upc .= $td_ary[$i];
                        }
                    }
                }

                $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{td_se_id}    = $td_se_id;
                $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{qty_ordered} = $qty_ordered;
		if($upc){
		    $upc =~ s/\s+$//g; ## back space
		}
		if($upc){
		    $upc =~ s/^\s+//g; ## front space ## added Nov 17, 15 yuc
		}
                ## my $prefixed_upc = $self->get_prefixed_string( 13, $upc, '0' );## commented out Nov 17, 15 yuc
                ## prefix upc with 0s for upc
                ## $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{upc}              = $prefixed_upc;
                ## commented out Nov 17, 15 yuc
		if($upc && (length($upc) > 0)){
		    $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{upc}  = $upc;
		}else{
		    $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{upc}  = 
			$self->get_prefixed_string( 13, $upc, '0' );
		}
		## Nov 17, 15 yuc
                $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{po_id}            = $key_data;
                $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{site_id}          = $site_id;
                $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{edi_sequence_id}  = $edi_seq_id;
                $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{business_unit_id} = $business_unit_id;
                $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{nex_richter_site} = $nex_richter_site;
                $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{nex_whse}         = $nex_whse;
                $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{ride_processed_fin_ind} =
                  'N';    ## set this value as N before any update
                $nex_po_staging_data->{$key_data}->{$edi_seq_id}->{ride_processed_fin_date} =
                  '';     ## set this as '' before any update
                $total++;
            }
        }
    }
    return ( $nex_po_staging_data, $total );
}

## prefix string with a place holder like space, 0 etc,
## return a fixed length string

sub get_prefixed_string {
    my ( $self, $mxlen, $field_data, $place_hd ) = @_;
    my $ret_str = '';
    $field_data =~ s/^\s+//;    ## remove front space
    $field_data =~ s/\s+$//;    ## remove back space
    my $d_len = length($field_data);
    if ( $mxlen >= $d_len ) {
        my $prefix = '';
        for ( my $i = 0 ; $i < ( $mxlen - $d_len ) ; $i++ ) {
            $prefix .= $place_hd;
        }
        $ret_str = $prefix . $field_data;
    }
    else {
        ###??????????????????????????? Ask??????????
        $ret_str = substr( $field_data, 0, $mxlen );
    }
    return $ret_str;
}


## prefix string with a place holder like space, 0 etc,
## return a fixed length string

sub get_suffixed_string {
    my ( $self, $mxlen, $field_data, $place_hd ) = @_;
    my $ret_str = '';
    $field_data =~ s/^\s+//;    ## remove front space
    $field_data =~ s/\s+$//;    ## remove back space
    my $d_len = length($field_data);
    if ( $mxlen >= $d_len ) {
        my $suffix = '';
        for ( my $i = 0 ; $i < ( $mxlen - $d_len ) ; $i++ ) {
            $suffix .= $place_hd;
        }
        $ret_str = $field_data . $suffix;
    }
    else {
        ###??????????????????????????? Ask??????????
        $ret_str = substr( $field_data, 0, $mxlen );
    }
    return $ret_str;
}


#input: filehandle
#output: reference of data key by unique edi_sequence_id
#my $query = "select * from edi_pending_transactions";
#my $value_ref = $dbh->selectall_arrayref($query);

sub extract_edi_pending_transactions {
    my ($self) = @_;
    my $ret_ref;

## query replace in Dec 7, 2010 after extensive tests. yuc

=head
    my $query2 =
"                                                                                                                                   
SELECT                                                                                                                                              
   E.EDI_SEQUENCE_ID,                                                                                                                               
   E.TRANSACTION_DATA,                                                                                                                              
   E.KEY_DATA,                                                                                                                                      
   E.BUSINESS_UNIT_ID,                                                                                                                              
   P.PO_TYPE, 

   P.ORIGIN

FROM                                                                                                                                                
    $self->{TABLE_EPT} E,                                                                                                                            
    $self->{TABLE_PO} P,                                                                                                                             
    $self->{TABLE_NPS} N                                                                                                                            
WHERE                                                                                                                                               
   (E.BUSINESS_UNIT_ID = 30 OR E.BUSINESS_UNIT_ID = 40)                                                                                             
   AND E.PARTNERSHIP_ID  = '00001707694850'                                                                                                         
   AND E.TRANSACTION_SET = '850'                                                                                                                    
   AND E.TRANSACTION_TYPE ='OUT'                                                                                                                    
   AND E.DATE_PROCESSED is null                                                                                                                     
   AND E.RIDE_SESSION_ID is null                                                                                                                    
   AND P.PO_ID = E.KEY_DATA                                                                                                                           
   AND N.PO_ID <> E.KEY_DATA                                                                                                                        
   AND e.business_unit_id = p.business_unit_id
   
   AND e.business_unit_id <> n.business_unit_id

";
=cut

    my $query2 = "
SELECT
   /*+ PARALLEL(merch.edi_pending_transactions, 8) */ 
   E.EDI_SEQUENCE_ID,
   E.TRANSACTION_DATA,
   E.KEY_DATA ,
   E.BUSINESS_UNIT_ID,
   P.PO_TYPE,
   P.ORIGIN
FROM
   $self->{TABLE_EPT} E,
   $self->{TABLE_PO} P                                                                                                                            
WHERE
       E.BUSINESS_UNIT_ID = 30
   AND E.PARTNERSHIP_ID   = '00001707694850'
   AND E.TRANSACTION_SET  = '850'
   AND E.TRANSACTION_TYPE ='OUT'
   AND E.DATE_PROCESSED is null
   AND E.RIDE_SESSION_ID is null
   AND P.PO_ID = E.KEY_DATA
   AND e.business_unit_id = p.business_unit_id
   AND P.ORIGIN IN ('REPLENISHMENT','ENTRY')  
   AND P.PO_TYPE IN ('DROPSHIP','PREPACKED','BULK')
   AND nvl(P.REASON_ID, 0)  <> '1'
   AND NOT EXISTS (
        SELECT 1 
        FROM $self->{TABLE_NPS} 
        WHERE po_id = e.key_data 
              AND business_unit_id = e.business_unit_id
    )
";

    $self->{'log_obj'}->log_info("extract query: $query2");
    print $query2;
    my $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "edi_sequence_id" );  ## CASE MATTERS HERE\!!!

    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "EDI_SEQUENCE_ID" );
    }
    return $value_ref;
}

sub validate_list_poid {
    my ( $self, $list ) = @_;
    $list =~ s/\s+//g;
    my $problem = 0;
    if ( $list !~ /^[0-9,]+$/ ) {
        $problem = 1;
        print "PO_ID has to have numbers only.";
    }
    elsif ( $list =~ /(^,|,,|,$)/ ) {
        $problem = 2;
        print "list can not start with , or have two or more ,, together";
    }
    else {
        my @ary = split( /\,/, $list );
        foreach my $po_id (@ary) {
            ## $problem = validate_po_id($dbh_rmsp, $po_id);
            my @row;
            $po_id =~ s/\s+//g;
            if ( $po_id =~ /[A-Za-z]/ ) {
                $problem = 3;
                print "No letters allowed in the po_id field";
            }
            elsif ( length($po_id) < 6 ) {
                $problem = 4;
                print "The po_id is shorter than 5 numbers?";
            }
            else {
                my $sth = $self->{dbh}->prepare(
                    "                                                          
                  select  count(*) 
                  from    edi_pending_transactions
                  where   key_data = ?                                                            
               "
                );

                $sth->execute($po_id);
                while ( @row = $sth->fetchrow_array ) {
                    if ( $row[0] == 0 ) {
                        $problem = 5;
                        print "Invalid po_id found in your input:$po_id";
                        print "I get some values from database\n";
                    }

                }   ##else in the foreach loop                                                               \

            }    ##foreach                                                                                  \

        }    ## outside of foreach                                                                        \

    }
    return $problem;
}

sub extract_origin_EPT_2 {
    my ( $self, $PO_ID ) = @_;
    my $ret_ref;
    my $query2 =
"SELECT E.EDI_SEQUENCE_ID, E.TRANSACTION_DATA, E.KEY_DATA, E.BUSINESS_UNIT_ID, NS.NEX_RICHTER_SITE, NS.NEX_WHSE, P.PO_TYPE, P.ORIGIN FROM $self->{TABLE_EPT} E, $self->{TABLE_PO} P, $self->{TABLE_NPS} N, $self->{TABLE_NS} NS WHERE (E.BUSINESS_UNIT_ID = 30 OR E.BUSINESS_UNIT_ID = 40) AND E.PARTNERSHIP_ID = '00001707694850' AND E.TRANSACTION_SET = '850' AND E.TRANSACTION_TYPE ='OUT' AND E.DATE_PROCESSED is null AND E.RIDE_SESSION_ID is null AND NS.SITE_ID = N.SITE_ID AND P.PO_ID=E.KEY_DATA AND N.PO_ID <> E.KEY_DATA AND P.ORIGIN ='ENTRY' AND E.KEY_DATA = \'$PO_ID\'";

    my $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "edi_sequence_id" );  ## CASE MATTERS HERE\!!!
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "EDI_SEQUENCE_ID" );
    }
    return $value_ref;
}

## check if the any data returned after using selectall_hashref DBI function
## check two levels deep
## return 0 or 1 ONLY
sub TEST_check_empty_hash_ref {
    my ( $self, $value_ref ) = @_;
    my $size;
    foreach my $key ( keys %{$value_ref} ) {
        if ( ( $value_ref->{$key} =~ /\d+|\w+/g ) && ( $size != 1 ) ) {
            $size = 1;
        }
        else {
            $size = &check_empty_hash_ref( $self, $value_ref->{$key} );
        }
    }
    return $size;
}

sub check_empty_hash_ref {
    my ( $self, $value_ref ) = @_;
    my $size = 0;
    if ( defined $value_ref ) {
        foreach my $key ( keys %$value_ref ) {
            foreach my $skey ( keys %{ $value_ref->{$key} } ) {
                if (   ( $value_ref->{$key}->{$skey} =~ /\d+|\w+/g )
                    && ( $size == 0 ) )
                {
                    $size = 1;
                }
            }
        }
    }
    return $size;
}

## input: hash_ref from extract_edi_pending_transactions
## processing: check po_type field, write 'non-bulk' into file.
##             group data by key_data, check if pending transaction is valid
## output: log buff, valid transaction data
sub evaluate_edi_pending_transactions_non_dropship {
    my ( $self, $value_ref ) = @_;
    my ( $log_buf, $group_ref, $valid_transaction_ref );
    my @valid_key_data;
    my @kd_with_invalid_upcs;
    my $marker1        = '10';
    my $marker2        = '90';
    my $select_po_type = 'BULK';
    ##my $select_po_type='DROPSHIP'; ##this is just for testing. SET 12, 2008
    my $site_id_ref;    # store infor about site_id, nex_richter_site, nex_whse etc...
    my $td_se_ref;      # store infor about colume 17,18 values
    my $items_wrong_potype;
    my $items_missing_upc;
    my $items_wrong_origin;

## re-group data by key_data
    foreach my $edi_sequence_id ( sort { $a <=> $b } keys %$value_ref ) {
        my $key_data = $value_ref->{$edi_sequence_id}->{key_data};
        ##print "keydata:".$key_data;
        if ( $key_data ne "" ) {
            $group_ref->{$key_data}->{$edi_sequence_id}->{transaction_data} =
              $value_ref->{$edi_sequence_id}->{transaction_data};
            $group_ref->{$key_data}->{$edi_sequence_id}->{po_type} =
              $value_ref->{$edi_sequence_id}->{po_type};
            $group_ref->{$key_data}->{$edi_sequence_id}->{business_unit_id} =
              $value_ref->{$edi_sequence_id}->{business_unit_id};
            $group_ref->{$key_data}->{$edi_sequence_id}->{origin} = $value_ref->{$edi_sequence_id}->{origin};
            ##$group_ref->{$key_data}->{$edi_sequence_id}->{nex_richter_site} = $value_ref->{$edi_sequence_id}->{nex_richter_site};
            ##$group_ref->{$key_data}->{$edi_sequence_id}->{nex_whse}         = $value_ref->{$edi_sequence_id}->{nex_whse};
        }
    }

## processing data markers for valid transactions
    foreach my $kd ( keys %$group_ref ) {
        my ( $flag1, $flag2, $upc_missing ) = 0;    ##flag1 for 10, flag2 for 90
        foreach my $edi_seq_id ( sort { $a <=> $b } keys %{ $group_ref->{$kd} } ) {
            my $transaction_data = $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
            ## remove possible front spaces:???
            $transaction_data =~ s/^\s+//;
            my @ary = split( //, $transaction_data );
            ## 1, find out if the transaction contain 10 and 90:
            my $seq1 = "$ary[16]" . "$ary[17]";
            unless ( $seq1 =~ /[1-9]0/ ) {
                $self->{'log_obj'}->log_info("Wrong to extract 10 to 90. what is: $seq1 <-\n");
            }

            if ( $seq1 eq $marker1 ) {
                $flag1 = 1;
            }
            if ( $seq1 eq $marker2 ) {
                $flag2 = 1;
            }
            ## record site_id which only exists in 17-18 as 30 type
            if ( $seq1 eq "30" ) {
                if ( $site_id_ref->{$kd} eq '' ) {
                    $site_id_ref->{$kd}->{'site_id'} =
                        $ary[83]
                      . $ary[84]
                      . $ary[85]
                      . $ary[86]
                      . $ary[87];    ## collect site_id according to specifications
                    ( $site_id_ref->{$kd}->{'nex_richter_site'}, $site_id_ref->{$kd}->{'nex_whse'} ) =
                      $self->get_store_info( $self->{'dbh'}, $self->{'log_obj'},
                        $site_id_ref->{$kd}->{'site_id'} );
                }
            }
            $td_se_ref->{$kd}->{$edi_seq_id} = $seq1;

            ## 2, for 17-18's value 40, check if there is a valid UPC, mark invalid upc, index pos 49-61, for it
            if ( ( $seq1 eq "40" ) && ( $upc_missing == 0 ) ) {
                my $upc = '';
                for ( my $j = 48 ; $j < 61 ; $j++ ) {
                    $upc .= $ary[$j];
                }
                ##print "upc before check: $upc\n";
                ## 3, valid upc criterias: length<3, with letters inside, and no-exist in bar_codes tables
                $upc =~ s/\s+$//g;    ## remove end spaces from above operation
                my $upc_checker = $self->check_valid_upc( $self->{'dbh'}, $self->{'log_obj'}, $upc );
                if ( $upc_checker == 0 ) {
                    $group_ref->{$kd}->{$edi_seq_id}->{'upc_missing'} = 1;
                    $upc_missing = 1;
                }
                ##print "upc_checker: $upc_checker\n";
            }
        }
        ## important to book keeping valid data, and pos with invalid upcs:
        if ( ( $flag1 == 1 ) && ( $flag2 == 1 ) ) {
            if ( $upc_missing == 1 ) {
                push( @kd_with_invalid_upcs, $kd );
                $self->{'log_obj'}->log_info(
                    "\nkey_data|po_id: $kd have invalid UPCs with one or more of its edi_seq_ids.\n" );
            }
            else {
                push( @valid_key_data, $kd );
                $self->{'log_obj'}
                  ->log_info( "\nkey_data|po_id: $kd have both 10, and 90 in the transaction data list\n" );
            }

        }
        else {
            $self->{'log_obj'}
              ->log_info( "\n $kd DO NOT HAVE BOTH 10, AND 90 IN THE TRANSACTION_DATA LIST.\n" );
            if ( ( $flag1 == 1 ) && ( $flag2 == 0 ) ) {
                $self->{'log_obj'}->log_info("-- $kd miss 90.\n");
            }
            elsif ( ( $flag1 == 0 ) && ( $flag2 == 1 ) ) {
                $self->{'log_obj'}->log_info("-- $kd miss 10.\n");
            }
            elsif ( ( $flag1 == 0 ) && ( $flag2 == 0 ) ) {
                $self->{'log_obj'}->log_info("-- $kd miss both 10 and 90.\n");
            }
        }
    }

    ## Only select data with both markers, 10 and 90. Then group the data into missing_upc, and wrong_po_types, and valid_data

    foreach my $kd (@valid_key_data) {
        foreach my $edi_seq_id ( sort { $a <=> $b } keys %{ $group_ref->{$kd} } ) {
            if ( uc( $group_ref->{$kd}->{$edi_seq_id}->{po_type} ) eq uc($select_po_type) )
            {    ## right po_type cases:
                if ( uc( $group_ref->{$kd}->{$edi_seq_id}->{origin} ) eq 'ENTRY' ) {
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{'po_type'} =
                      $group_ref->{$kd}->{$edi_seq_id}->{'po_type'};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{transaction_data} =
                      $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{business_unit_id} =
                      $group_ref->{$kd}->{$edi_seq_id}->{business_unit_id};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{site_id} = $site_id_ref->{$kd}->{'site_id'};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{td_se}   = $td_se_ref->{$kd}->{$edi_seq_id};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{nex_richter_site} =
                      $site_id_ref->{$kd}->{nex_richter_site};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{nex_whse} = $site_id_ref->{$kd}->{nex_whse};
                }
                else {
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{transaction_data} =
                      $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{business_unit_id} =
                      $group_ref->{$kd}->{$edi_seq_id}->{business_unit_id};
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{site_id} =
                      $site_id_ref->{$kd}->{'site_id'};
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{td_se} = $td_se_ref->{$kd}->{$edi_seq_id};
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{nex_richter_site} =
                      $site_id_ref->{$kd}->{nex_richter_site};
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{nex_whse} =
                      $site_id_ref->{$kd}->{nex_whse};
                }

            }
            else {    ## if po_type != 'bulk', i.e., wrong po_type case:
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{'po_type'} =
                  $group_ref->{$kd}->{$edi_seq_id}->{'po_type'};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{transaction_data} =
                  $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{business_unit_id} =
                  $group_ref->{$kd}->{$edi_seq_id}->{business_unit_id};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{site_id} = $site_id_ref->{$kd}->{'site_id'};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{td_se}   = $td_se_ref->{$kd}->{$edi_seq_id};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{nex_richter_site} =
                  $site_id_ref->{$kd}->{nex_richter_site};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{nex_whse} = $site_id_ref->{$kd}->{nex_whse};
            }
        }
    }

    ##
    foreach my $kd (@kd_with_invalid_upcs) {
        foreach my $edi_seq_id ( sort { $a <=> $b } keys %{ $group_ref->{$kd} } ) {

            $items_missing_upc->{$kd}->{$edi_seq_id}->{transaction_data} =
              $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
            $items_missing_upc->{$kd}->{$edi_seq_id}->{business_unit_id} =
              $group_ref->{$kd}->{$edi_seq_id}->{business_unit_id};
            $items_missing_upc->{$kd}->{$edi_seq_id}->{site_id} = $site_id_ref->{$kd}->{'site_id'};
            $items_missing_upc->{$kd}->{$edi_seq_id}->{td_se}   = $td_se_ref->{$kd}->{$edi_seq_id};
            $items_missing_upc->{$kd}->{$edi_seq_id}->{nex_richter_site} =
              $site_id_ref->{$kd}->{nex_richter_site};
            $items_missing_upc->{$kd}->{$edi_seq_id}->{nex_whse} = $site_id_ref->{$kd}->{nex_whse};
        }
    }

    return ( $valid_transaction_ref, $items_wrong_potype, $items_missing_upc, $items_wrong_origin,
        $group_ref );
}

## the function jump over missing upc cases. Oct 03, 08

sub evaluate_edi_pending_transactions {
    my ( $self, $value_ref ) = @_;
    my ( $log_buf, $group_ref, $valid_transaction_ref );
    my @valid_key_data;
    my @kd_with_invalid_upcs;
    my $marker1        = '10';
    my $marker2        = '90';
    my $marker3        = '50';
    my $select_po_type = 'DROPSHIP';    ##this is just for testing. SET 12, 2008
    my $invalid_origin = 'ENTRY';
    my $site_id_ref;                    # store infor about site_id, nex_richter_site, nex_whse etc...
    my $td_se_ref;                      # store infor about colume 17,18 values
    my $items_wrong_potype;
    my $items_missing_upc;
    my $items_wrong_origin;

## re-group data by key_data
    foreach my $edi_sequence_id ( sort { $a <=> $b } keys %$value_ref ) {
        my $key_data = $value_ref->{$edi_sequence_id}->{key_data};
        if ( $key_data ne "" ) {
            $group_ref->{$key_data}->{$edi_sequence_id}->{transaction_data} =
              $value_ref->{$edi_sequence_id}->{transaction_data};
            $group_ref->{$key_data}->{$edi_sequence_id}->{po_type} =
              $value_ref->{$edi_sequence_id}->{po_type};
            $group_ref->{$key_data}->{$edi_sequence_id}->{business_unit_id} =
              $value_ref->{$edi_sequence_id}->{business_unit_id};
            $group_ref->{$key_data}->{$edi_sequence_id}->{origin} = $value_ref->{$edi_sequence_id}->{origin};
        }
    }

## processing data markers for valid transactions
    foreach my $kd ( keys %$group_ref ) {
        my ( $flag1, $flag2, $upc_missing ) = 0;    ##flag1 for 10, flag2 for 90
        foreach my $edi_seq_id ( sort { $a <=> $b } keys %{ $group_ref->{$kd} } ) {
            my $transaction_data = $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
            ## remove possible front spaces:???
            $transaction_data =~ s/^\s+//;
            my @ary = split( //, $transaction_data );
            ## 1, find out if the transaction contain 10 and 90:
            my $seq1 = "$ary[16]" . "$ary[17]";
            unless ( $seq1 =~ /[1-9]0/ ) {
                $self->{'log_obj'}->log_info("Wrong to extract 10 to 90. what is: $seq1 <-\n");
            }

            if ( $seq1 eq $marker1 ) {
                $flag1 = 1;
            }
            if ( $seq1 eq $marker2 ) {
                $flag2 = 1;
            }
            ## record site_id which only exists in 17-18 as 50 type
            if ( $seq1 eq $marker3 ) {
                if ( $site_id_ref->{$kd} eq '' ) {
                    $site_id_ref->{$kd}->{'site_id'} =
                        $ary[22]
                      . $ary[23]
                      . $ary[24]
                      . $ary[25]
                      . $ary[26];    ## collect site_id according to new specifications
                    $self->{'log_obj'}->log_info("site_id:   $site_id_ref->{$kd}->{'site_id'}\n");
                    ##print "site_id:   $site_id_ref->{$kd}->{'site_id'} \n";
                    ( $site_id_ref->{$kd}->{'nex_richter_site'}, $site_id_ref->{$kd}->{'nex_whse'} ) =
                      $self->get_store_info( $self->{'dbh'}, $self->{'log_obj'},
                        $site_id_ref->{$kd}->{'site_id'} );
                }
            }
            $td_se_ref->{$kd}->{$edi_seq_id} = $seq1;

            ## 2, for 17-18's value 40, check if there is a valid UPC, mark invalid upc, index pos 49-61, for it
            ## length is 13!!!!!!!!
            if ( ( $seq1 eq "40" ) && ( $upc_missing == 0 ) ) {
                my $upc = '';
                for ( my $j = 48 ; $j < 61 ; $j++ ) {
                    $upc .= $ary[$j];
                }
                ##print "upc_beforecheck: $upc\n";
                ## 3, valid upc criterias: length<3, with letters inside, and no-exist in bar_codes tables
                my $upc_checker = $self->check_valid_upc( $self->{'dbh'}, $self->{'log_obj'}, $upc );
                if ( $upc_checker == 0 ) {
                    $group_ref->{$kd}->{$edi_seq_id}->{'upc_missing'} = 1;
                    $upc_missing = 1;
                }
                ###print "upc_checker: $upc_checker\n";
            }
        }
        ## important to book-keeping valid data, and pos with invalid upcs:
        if ( ( $flag1 == 1 ) && ( $flag2 == 1 ) ) {
            if ( $upc_missing == 1 ) {
                push( @kd_with_invalid_upcs, $kd );    ### jump over missing upc???
                $self->{'log_obj'}->log_info(
                    "\nkey_data|po_id: $kd have invalid UPCs with one or more of its edi_seq_ids.\n" );
            }
            else {
                push( @valid_key_data, $kd );
                $self->{'log_obj'}
                  ->log_info( "\nkey_data|po_id: $kd have both 10, and 90 in the transaction data list\n" );
            }
        }
        else {
            $self->{'log_obj'}
              ->log_info( "\n $kd DO NOT HAVE BOTH 10, AND 90 IN THE TRANSACTION_DATA LIST.\n" );
            if ( ( $flag1 == 1 ) && ( $flag2 == 0 ) ) {
                $self->{'log_obj'}->log_info("-- $kd miss 90.\n");
            }
            elsif ( ( $flag1 == 0 ) && ( $flag2 == 1 ) ) {
                $self->{'log_obj'}->log_info("-- $kd miss 10.\n");
            }
            elsif ( ( $flag1 == 0 ) && ( $flag2 == 0 ) ) {
                $self->{'log_obj'}->log_info("-- $kd miss both 10 and 90.\n");
            }
        }
    }
    ## Only select data with both markers, 10 and 90. Then group the data into missing_upc, and wrong_po_types, and valid_data
    foreach my $kd (@valid_key_data) {
        foreach my $edi_seq_id ( sort { $a <=> $b } keys %{ $group_ref->{$kd} } ) {
            if ( uc( $group_ref->{$kd}->{$edi_seq_id}->{po_type} ) eq uc($select_po_type) )
            {    ##if origin is not 'replenishment', take it as items_wrong_origin 'entry';
                if ( uc( $group_ref->{$kd}->{$edi_seq_id}->{origin} ) eq $invalid_origin ) {
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{'po_type'} =
                      $group_ref->{$kd}->{$edi_seq_id}->{'po_type'};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{transaction_data} =
                      $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{business_unit_id} =
                      $group_ref->{$kd}->{$edi_seq_id}->{business_unit_id};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{site_id} = $site_id_ref->{$kd}->{'site_id'};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{td_se}   = $td_se_ref->{$kd}->{$edi_seq_id};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{nex_richter_site} =
                      $site_id_ref->{$kd}->{nex_richter_site};
                    $items_wrong_origin->{$kd}->{$edi_seq_id}->{nex_whse} = $site_id_ref->{$kd}->{nex_whse};
                }
                else {    ## if origin is replenishment, this is what we want
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{transaction_data} =
                      $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{business_unit_id} =
                      $group_ref->{$kd}->{$edi_seq_id}->{business_unit_id};
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{site_id} =
                      $site_id_ref->{$kd}->{'site_id'};
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{td_se} = $td_se_ref->{$kd}->{$edi_seq_id};
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{nex_richter_site} =
                      $site_id_ref->{$kd}->{nex_richter_site};
                    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{nex_whse} =
                      $site_id_ref->{$kd}->{nex_whse};
                }
            }
            else {    ## if po_type != 'bulk', i.e., wrong po_type case:
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{'po_type'} =
                  $group_ref->{$kd}->{$edi_seq_id}->{'po_type'};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{transaction_data} =
                  $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{business_unit_id} =
                  $group_ref->{$kd}->{$edi_seq_id}->{business_unit_id};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{site_id} = $site_id_ref->{$kd}->{'site_id'};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{td_se}   = $td_se_ref->{$kd}->{$edi_seq_id};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{nex_richter_site} =
                  $site_id_ref->{$kd}->{nex_richter_site};
                $items_wrong_potype->{$kd}->{$edi_seq_id}->{nex_whse} = $site_id_ref->{$kd}->{nex_whse};
            }
        }
    }
    ## screen out the items with missiong upc
    foreach my $kd ( keys %{$valid_transaction_ref} ) {
        foreach my $edi_seq_id (
            sort { $a <=> $b }
            keys %{ $valid_transaction_ref->{$kd} }
          )
        {
            if ( $group_ref->{$kd}->{$edi_seq_id}->{'upc_missing'} == 1 ) {
                $valid_transaction_ref->{$kd}->{$edi_seq_id} = undef;
                $items_missing_upc->{$kd}->{$edi_seq_id}->{'po_type'} =
                  $group_ref->{$kd}->{$edi_seq_id}->{'po_type'};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{transaction_data} =
                  $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{business_unit_id} =
                  $group_ref->{$kd}->{$edi_seq_id}->{business_unit_id};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{site_id} = $site_id_ref->{$kd}->{'site_id'};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{td_se}   = $td_se_ref->{$kd}->{$edi_seq_id};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{nex_richter_site} =
                  $site_id_ref->{$kd}->{nex_richter_site};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{nex_whse} = $site_id_ref->{$kd}->{nex_whse};
            }
        }
    }
    return ( $valid_transaction_ref, $items_wrong_potype, $items_missing_upc, $items_wrong_origin,
        $group_ref );
}

sub extract_edi_850 {
    my ( $self, $date_created ) = @_;
    my $ret_ref;
    my $query2 =
"                                                                                                              
select             
distinct 
e.edi_sequence_id,                                                                                                              
e.transaction_data,                                                                                                             
e.key_data,                                                                                                                     
e.business_unit_id,                                                                                                             
p.po_type,                                                                                                                      
p.origin                                                                                                                        
from                                                                                                                            
$self->{TABLE_EPT} e,                                                                                                           
$self->{TABLE_PO}  p

where                                                                                       
e.business_unit_id = 30
AND p.business_unit_id = e.business_unit_id
and e.partnership_id != '00001707694850' 
and e.transaction_set = '850'                                                                                                   
and e.transaction_type ='OUT'                                                                                                   
and e.date_processed is null                                                                                                    
and e.ride_session_id is null                                                                                                   
---and p.po_type in ('DROPSHIP','BULK') -- remove po type limits yuc march 15
and p.po_id=e.key_data                                                                                                          
";

    if ($date_created) {
        $query2 .= " and e.date_created like \'%$date_created%\'";

    }

## and e.partnership_id in ('00001340876850', '00066983586850','00711261490850','00001707694850')
## and e.partnership_id = '00711261490850'
## and e.partnership_id = '00001340876850'

##     $self->{'log_obj'}->log_info("extract query: $query2");
    print $query2 if ( $self->{'debug'} );
    my $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "edi_sequence_id" );
    ## CASE MATTERS HERE\!!!
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "EDI_SEQUENCE_ID" );
    }

    if ( keys(%$value_ref) == 0 ) {
        $self->{'log_obj'}->log_info("extract query not working: $query2");
    }
    return $value_ref;
}

sub re_grouping_edi_850_data {
    my ( $self, $value_ref ) = @_;
    my ($group_ref);
## re-group data by key_data
    foreach my $edi_sequence_id ( sort { $a <=> $b } keys %$value_ref ) {
        my $key_data = $value_ref->{$edi_sequence_id}->{key_data};
        if ( $key_data ne "" ) {
            $group_ref->{$key_data}->{$edi_sequence_id}->{transaction_data} =
              $value_ref->{$edi_sequence_id}->{transaction_data};
            my $transaction_data = $value_ref->{$edi_sequence_id}->{transaction_data};
            $transaction_data =~ s/^\s+//;
            my @ary = split( //, $transaction_data );
            my $seq1 = "$ary[16]" . "$ary[17]";
            $group_ref->{$key_data}->{$edi_sequence_id}->{data_type} = $seq1;
            $group_ref->{$key_data}->{$edi_sequence_id}->{po_type} =
              $value_ref->{$edi_sequence_id}->{po_type};
            $group_ref->{$key_data}->{$edi_sequence_id}->{origin} = $value_ref->{$edi_sequence_id}->{origin};
            $group_ref->{$key_data}->{$edi_sequence_id}->{key_data}        = $key_data;
            $group_ref->{$key_data}->{$edi_sequence_id}->{edi_sequence_id} = $edi_sequence_id;
            $group_ref->{$key_data}->{$edi_sequence_id}->{data_type}       = $seq1;

        }
    }
    return $group_ref;
}

#   The function does 3 things:
##   group the data so data from the same PO, goes together, ## adding DDDDDate?????!!!!!
##   as 'date_created' only has information up to 'day'.
##   check if the PO has both 10 and 90 lines,
##   check if the PO need to be skiped

sub grouping_e850_data_and_check_skip_pos {
    my ( $self, $value_ref ) = @_;
    my ($group_ref);

    my $skip_po_ref;
    my $flag_ref;

## first round: get completeness of POs by check type 10, and 90, and check if a PO should be skipped:
## also, here I checked if same PO has been submitted twice in the same day:

    foreach my $edi_sequence_id ( sort { $a <=> $b } keys %$value_ref ) {
        my $po_id = $value_ref->{$edi_sequence_id}->{key_data};
        if ( $po_id ne "" ) {
            my $transaction_data = $value_ref->{$edi_sequence_id}->{transaction_data};
            $transaction_data =~ s/^\s+//;
            my @ary = split( //, $transaction_data );
            my $seq1 = "$ary[16]" . "$ary[17]";

            if ( $seq1 eq '10' ) {

                if ( !( $flag_ref->{$po_id}->{'10'} ) ) {
                    $flag_ref->{$po_id}->{'10'} = 1;
                }
                else {
                    $flag_ref->{$po_id}->{'2nd_10'}       = 1;
                    $flag_ref->{$po_id}->{'2nd_10_seqid'} = $edi_sequence_id;
                }

                my $d_type = substr( $transaction_data, 18, 2 );

                if ( ($po_id) && ($d_type) ) {
                    my $skip = $self->check_PO_for_skip( $po_id, $d_type );
                    if ($skip) {
                        ##push(@$skip_po_list, $po_id);
                        $skip_po_ref->{$po_id} = 1;
                    }
                }
                else {
                    my $w_msg = "MISSING PO_ID, or Data_type for skipping check. PO: $po_id, type: $d_type";
                    $self->{'log_obj'}->log_info($w_msg);
                    warn($w_msg);
                }
            }
            elsif ( $seq1 eq '90' ) {

                ## $flag_ref->{$po_id}->{'90'} = 1;
                if ( !( $flag_ref->{$po_id}->{'90'} ) ) {
                    $flag_ref->{$po_id}->{'90'} = 1;
                }
                else {
                    $flag_ref->{$po_id}->{'2nd_90'}       = 1;
                    $flag_ref->{$po_id}->{'2nd_90_seqid'} = $edi_sequence_id;
                }

            }
        }
    }

    ## in the following part, redundant POs were not considered, need information!!!!
    foreach my $edi_sequence_id ( sort { $a <=> $b } keys %$value_ref ) {
        my $key_data = $value_ref->{$edi_sequence_id}->{key_data};
        $key_data =~ s/\s+//g;
        ## remove all space from the key_data. may not be necessary, but not hurt
        if (   ($key_data)
            && ( $flag_ref->{$key_data}->{'10'} )
            && ( $flag_ref->{$key_data}->{'90'} )
            && ( !( $skip_po_ref->{$key_data} ) ) )
        {
            $group_ref->{$key_data}->{$edi_sequence_id}->{transaction_data} =
              $value_ref->{$edi_sequence_id}->{transaction_data};
            my $transaction_data = $value_ref->{$edi_sequence_id}->{transaction_data};
            $transaction_data =~ s/^\s+//;
            my @ary = split( //, $transaction_data );
            my $seq1 = "$ary[16]" . "$ary[17]";
            $group_ref->{$key_data}->{$edi_sequence_id}->{data_type} = $seq1;
            $group_ref->{$key_data}->{$edi_sequence_id}->{po_type} =
              $value_ref->{$edi_sequence_id}->{po_type};
            $group_ref->{$key_data}->{$edi_sequence_id}->{origin} = $value_ref->{$edi_sequence_id}->{origin};
            $group_ref->{$key_data}->{$edi_sequence_id}->{key_data}        = $key_data;
            $group_ref->{$key_data}->{$edi_sequence_id}->{edi_sequence_id} = $edi_sequence_id;
            $group_ref->{$key_data}->{$edi_sequence_id}->{data_type}       = $seq1;

        }
    }
    return ( $group_ref, $skip_po_ref );
}

## in: edi850 query data
## out: a data structure for writing xml file

sub parse_edi_850_data {
    my ( $self, $value_ref ) = @_;
    my ( $log_buf, $group_ref );
    my $marker1 = '10';
    my $marker2 = '30';
    my $marker3 = '40';
    my $marker4 = '50';
    my $marker5 = '90';
    my $xml_data_ref;
    my $links_ref;
    my $struc2_ref;

    my $skip_po_list;
    my $skip_po_href;

## Just sort by key_data Aug 10, 09
    my $flag_ref;
    foreach my $edi_sequence_id ( sort { $a <=> $b } keys %$value_ref ) {
        my $po_id = $value_ref->{$edi_sequence_id}->{po_id};
        if ($po_id) {
            $group_ref->{$po_id}->{$edi_sequence_id}->{transaction_data} =
              $value_ref->{$edi_sequence_id}->{transaction_data};
            my $transaction_data = $value_ref->{$edi_sequence_id}->{transaction_data};
            $transaction_data =~ s/^\s+//;
            my @ary = split( //, $transaction_data );
            my $seq1 = "$ary[16]" . "$ary[17]";
            $group_ref->{$po_id}->{$edi_sequence_id}->{data_type} = $seq1;

            if ( $seq1 eq '10' ) {
                $flag_ref->{$po_id}->{'10'} = 1;
                my $d_type = substr( $transaction_data, 18, 2 );

                if ( ($po_id) && ($d_type) ) {
                    my $skip = $self->check_PO_for_skip( $po_id, $d_type );
                    if ($skip) {
                        push( @$skip_po_list, $po_id );
                    }
                }
                else {
                    my $w_msg = "MISSING PO_ID, or Data_type for skipping check. PO: $po_id, type: $d_type";
                    $self->{'log_obj'}->log_info($w_msg);
                    warn($w_msg);
                }
            }

            if ( $seq1 eq '90' ) {
                $flag_ref->{$po_id}->{'90'} = 1;
            }

            $group_ref->{$po_id}->{$edi_sequence_id}->{po_type} = $value_ref->{$edi_sequence_id}->{po_type};
            $group_ref->{$po_id}->{$edi_sequence_id}->{origin}  = $value_ref->{$edi_sequence_id}->{origin};
            $group_ref->{$po_id}->{$edi_sequence_id}->{po_id}   = $po_id;
            $group_ref->{$po_id}->{$edi_sequence_id}->{edi_sequence_id} = $edi_sequence_id;
            $group_ref->{$po_id}->{$edi_sequence_id}->{data_type}       = $seq1;
        }
        else {
            my $die_msg = "missing PO_id value in transaction_data. Will die!";
            $self->{'log_obj'}->log_info($die_msg);
            die "$die_msg";

        }
    }

    if ( $self->{'debug'} ) {
        print "skippos\n";
        print Dumper($skip_po_list);
        print "group_ref\n";
        print Dumper($group_ref);
    }
## print out
##    foreach my $tpo(keys %{$group_ref}){
## remove skip po from the tree of group_ref, put it onto skip_po_href
    if ( defined $skip_po_list ) {
        foreach my $tpo (@$skip_po_list) {
            foreach my $tseq_id ( sort keys %{ $group_ref->{$tpo} } ) {
                print "========> $tpo\t$tseq_id\n" if ( $self->{'debug'} );
                $skip_po_href->{$tpo}->{$tseq_id} = 1;
            }
            delete $group_ref->{$tpo};
            my $i_msg = "skiped PO: $tpo\n";
            $self->{'log_obj'}->log_info($i_msg);
        }
    }

## remove skip po from the tree of group_ref, put it onto skip_po_href
##    if(defined $skip_po_list) {
##	foreach my $p_id(@$skip_po_list){
##	    if($p_id =~ /\d+/g){
##                $skip_po_href->{$p_id} = $group_ref->{$p_id};
##		delete $group_ref->{$p_id};
##                my $i_msg = "skiped PO: $p_id\n";
##		$self->{'log_obj'}->log_info($i_msg)
##	    }
##	}
##    }

## Here the assumptions are: a 40 line is ahead of those 50 lines related to this 40 line in ept table!!!
## re-organize 50s to its linked 40 lines: Aug 10, 09
    foreach my $s_po ( keys %$group_ref ) {
        my $last_40_seq_id;    ## remember what is the last 40 line's edi_sequence_id
        my $last_seq_id;       ## remember last edi_sequence_id in the loop
        foreach my $s_seq_id ( sort { $a <=> $b } keys %{ $group_ref->{$s_po} } ) {
            if ( $group_ref->{$s_po}->{$s_seq_id}->{data_type} eq '40' ) {
                $last_40_seq_id = $s_seq_id;
                $self->{'log_obj'}->log_info("llllllllllast 40_line id: $last_40_seq_id\n");
            }

            if ( $group_ref->{$s_po}->{$s_seq_id}->{data_type} eq '50' ) {

## To organize 50 type by its linked its 40 type, as 50 has mutiple lines, jan 14, 09
## modification on Jan 14, 09:
## if last $edi_seq_id is line 40, this line is line 50, replace this link_to_40 by last remembered 40_edi_seq_id

                if ( $group_ref->{$s_po}->{$last_seq_id}->{data_type} eq '40' )
                {    ## !! assumming 40 line is before 50 lines!!!
                    $group_ref->{$s_po}->{$s_seq_id}->{link_to_40} = $last_40_seq_id;
                    $links_ref->{$s_po}->{$last_40_seq_id} .= $s_seq_id . "|";
                }

## if last line is 50, this line is 50 too, link_to_40 is the old value, do not reset the temp as blank
                if ( $group_ref->{$s_po}->{$last_seq_id}->{data_type} eq '50' ) {
                    $group_ref->{$s_po}->{$s_seq_id}->{link_to_40} = $last_40_seq_id;
                    $links_ref->{$s_po}->{$last_40_seq_id} .= $s_seq_id . "|";
                }

            }
            $last_seq_id = $s_seq_id;
        }
    }
    if ( $self->{'debug'} ) {
        print "gggggggggggreoup_ref\n";
        print Dumper($group_ref);
        print "links refereeeeeeeece\n";
        print Dumper($links_ref);
    }

## check flag_ref

    foreach my $po ( keys %$flag_ref ) {
        if ( $po ne '' ) {
            if (   ( $flag_ref->{$po}->{'10'} == 1 )
                && ( $flag_ref->{$po}->{'90'} == 1 ) )
            {
                $self->{'log_obj'}->log_info("data complete for po: $po\n");
            }
            else {
                $self->{'log_obj'}->log_info("Missing 10 or 90 lines for a po: $po\n");
                ##exit(); ## skip the situation
                $group_ref->{$po} = undef;    ### ==========>>> April 3, 09
                ##return undef;
            }
        }
    }

    ## classify,  parse data, add default values if data absent for xml data structures
    my $line_items_ref;
    my $valid_data;
    my $po_type_ref;
    my $zone_ref;                             ## zone_x->{ctr}->{'destination_ctr'}
    ## zone_x->{ctr}->{'qty_ctr'};
    ## zone_x->{'cost'}
    ## zone_x->{'retail_price'}
    ## zone_x->{'unit_of_measure'}

    my $comb_ref;                             ## ref->{comb_key}->{rc}, {cost}, des_qty

    foreach my $po_id ( keys %$group_ref ) {
        ## re-examine the po_type here for each po_id, or po_id:
        $po_type_ref->{$po_id} = $self->re_exam_po_type($po_id);
        foreach my $edi_seq_id ( sort { $a <=> $b } keys %{ $group_ref->{$po_id} } ) {
            my $transaction_data = $group_ref->{$po_id}->{$edi_seq_id}->{transaction_data};
            $transaction_data =~ s/^\s+//;
            my $data_type = $group_ref->{$po_id}->{$edi_seq_id}->{data_type};
            if ( $data_type eq $marker1 ) {
                $valid_data->{$po_id}->{$marker1}->{$edi_seq_id} =
                  $self->parse_type_10( $transaction_data, $po_id, $po_type_ref );
            }
            elsif ( $data_type eq $marker2 ) {
                $valid_data->{$po_id}->{$marker2}->{$edi_seq_id} =
                  $self->parse_type_30( $transaction_data, $po_id, $po_type_ref );
            }
            elsif ( $data_type eq $marker3 ) {
                $valid_data->{$po_id}->{$marker3}->{$edi_seq_id} =
                  $self->parse_type_40( $transaction_data, $po_id, $po_type_ref );
            }
            elsif ( $data_type eq $marker4 ) {
                $valid_data->{$po_id}->{$marker4}->{$edi_seq_id} =
                  $self->parse_type_50( $transaction_data,
                    $group_ref->{$po_id}->{$edi_seq_id}->{'link_to_40'},
                    $po_id, $po_type_ref );
                ## update marker3 one data item:
                $valid_data->{$po_id}->{$marker3}->{ $group_ref->{$po_id}->{$edi_seq_id}->{'link_to_40'} }
                  ->{'SITE_ID'} = $valid_data->{$po_id}->{$marker4}->{$edi_seq_id}->{'SITE_ID'};

            }
            elsif ( $data_type eq $marker5 ) {
                $valid_data->{$po_id}->{$marker5}->{$edi_seq_id} = $self->parse_type_90($transaction_data);
            }
        }
    }

    ## re-asign the 50 value to its linked 40
    foreach my $po_id ( keys %{$links_ref} ) {
        foreach my $edi_40_id ( keys %{ $links_ref->{$po_id} } ) {

            my $sorted_des;
            my $des_qty_ctr  = 0;
            my $sub_zone_ctr = 0;

            my @ary = split( /\|/, $links_ref->{$po_id}->{$edi_40_id} );

            if ( @ary == 0 ) {
                print "empty stack of 40 and 50 links\n";
                ##$line_items_ref->{$po_id}->{'line_item_ctr'} = $line_items_ref->{$po_id}->{'line_item_ctr'} + 1;
            }
            else {
                foreach my $edi_50_id ( sort @ary ) {

                    ## 1, sort all the destination and qty into order
                    ##a, put all 50 line's edi_seq_id, under the same 40 lines
                    my $per_group_ref;    ## each group of 50 can have at most 10 destinations per 50 line:
                    foreach my $key ( keys %{ $valid_data->{$po_id}->{$marker4}->{$edi_50_id} } ) {
                        if ( $key eq 'RECORD_TYPE' ) { next; }

                        if ( $key =~ /^Destination(\d+)|^Qty(\d+)/g ) {
                            if ( $key =~ /^Destination(\d+)/ ) {
                                $per_group_ref->{$1}->{$key} =
                                  $valid_data->{$po_id}->{$marker4}->{$edi_50_id}->{$key};
                            }
                            if ( $key =~ /^Qty(\d+)/ ) {
                                $per_group_ref->{$1}->{$key} =
                                  $valid_data->{$po_id}->{$marker4}->{$edi_50_id}->{$key};
                            }
                        }
                    }

                    foreach my $akey ( sort { $a <=> $b } keys %$per_group_ref ) {
                        $des_qty_ctr++;
                        if ( $des_qty_ctr > 40 ) {
                            $self->{'log_obj'}
                              ->log_info( "Maximum site per item is 40. Over limit. Program will exit" );
                            die;
                        }
                        my $d_key = "Destination" . "$akey";
                        my $q_key = "Qty" . "$akey";
                        $sorted_des->{$des_qty_ctr}->{$d_key} = $per_group_ref->{$akey}->{$d_key};
                        $sorted_des->{$des_qty_ctr}->{$q_key} = $per_group_ref->{$akey}->{$q_key};
                        my $td_key = "Destination";
                        my $tq_key = "Qty";

                        my $unit_key = &get_unit_key( $des_qty_ctr, 'UNIT_MEASURE_EA' );

                        $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}->{$td_key} =
                          $per_group_ref->{$akey}->{$d_key};
                        $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}->{$tq_key} =
                          $per_group_ref->{$akey}->{$q_key};

## this is setting a default value for it:
                        ## this was commented out on April 13
                        $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}->{$unit_key} =
                          'EA'
                          if ( !$valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{$unit_key} );
                        ## this is being tested on April 13
                        ##       if(( $per_group_ref->{$akey}->{$q_key} > 0)&&($valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}->{$unit_key} eq '')) {
                        ##	$valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}->{$unit_key} =  'EA';
                        ##	}

###==> rewrite function from here:

##a, get group key, cost, retail price from style_id, po, and site_id:

##b, group it by the key:

##===>>>>

                        my $style_id =
                          $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{'STYLE_ID'};    ## rms style_id
                        my ( $cost, $retail_price, $comb_cr_id ) =
                          $self->get_rp_cost_from_po_style_site( $po_id, $style_id,
                            $per_group_ref->{$akey}->{$d_key} );
                        if ( $self->{'debug'} ) {
                            print ">>>>>=====>>>>>: $cost, $retail_price, $comb_cr_id\n";
                        }
                        if ( ($cost) && ($retail_price) && ($comb_cr_id) ) {

                            $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}
                              ->{'UNIT_PRICE_NC'} = $cost;
                            $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}
                              ->{'UNIT_PRICE_RP'} = $retail_price;
###<<<<======

## ==get a artificial id:

                            ## $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}->{'UNIT_PRICE_NC'}."|".$valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}->{'UNIT_PRICE_RP'};
                            $comb_cr_id =~ s/\s+//g;
                            $comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id}->{'Destination_Qty_Unit'} .=
                              $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}
                              ->{$td_key} . "_"
                              . $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}
                              ->{$tq_key} . "_"
                              . $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}
                              ->{$unit_key} . "|";

                            $comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id}->{'UNIT_PRICE_NC'} =
                              $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}
                              ->{'UNIT_PRICE_NC'}
                              if ( !$comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id}->{'UNIT_PRICE_NC'} );

                            $comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id}->{'UNIT_PRICE_RP'} =
                              $valid_data->{$po_id}->{$marker3}->{$edi_40_id}->{zdq}->{$des_qty_ctr}
                              ->{'UNIT_PRICE_RP'}
                              if ( !$comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id}->{'UNIT_PRICE_RP'} );

##===>   Record relationship based on zone_id

                        }
                        else {
                            $self->{'log_obj'}->log_info(
"missing value of cost: $cost, retail price: $retail_price or combination key: $comb_cr_id\n"
                            );
                        }
                    }
                }
            }
        }
        $valid_data->{$po_id}->{$marker4} = '';
    }

    if ( $self->{'debug'} ) {
        print "ZZZZone_ref";
        print Dumper($comb_ref);
    }

## organize the zone_ref: recalculate the total number of items in that zone
    foreach my $po_id ( keys %$comb_ref ) {
        foreach my $edi_40_id ( keys %{ $comb_ref->{$po_id} } ) {
            foreach my $comb_cr_id ( sort keys %{ $comb_ref->{$po_id}->{$edi_40_id} } ) {
                my $zone_qty = 0;
                my @des_qtys =
                  split( /\|/, $comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id}->{'Destination_Qty_Unit'} );
                for ( my $i = 0 ; $i < @des_qtys ; $i++ ) {
                    my @items = split( /\_/, $des_qtys[$i] );
                    my $si    = $i + 1;
                    my $d_key = "Destination" . $si;
                    my $q_key = "Qty" . $si;
                    $comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id}->{$d_key} = $items[0];
                    $comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id}->{$q_key} = $items[1];
                    $zone_qty                                                  = $zone_qty + $items[1];
                }
                $comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id}->{'QUANTITY_ORDERED'} = $zone_qty;
            }
        }
    }

## rewrite the whole structure
## 1, copy valid_data for 10, 30, 90,
## 2, for 40 line, if SA type, copy as it is.
##                 if DS type, rewrite the structure

##    my $last_90_seqid='';

    foreach my $po_id ( sort keys %$valid_data ) {
        foreach my $marker ( sort keys %{ $valid_data->{$po_id} } ) {
            if (   ( $marker eq '10' )
                || ( $marker eq '30' )
                || ( $marker eq '90' )
                || ( $marker eq '' ) )
            {
                $struc2_ref->{$po_id}->{$marker} = $valid_data->{$po_id}->{$marker};
            }
            elsif ( $marker eq '40' ) {
                if ( $po_type_ref->{$po_id}->{'sps_po_type'} ne 'DS' )
                {    ## for SA (NONE_DS) type, with address infor, copy it down
                    $struc2_ref->{$po_id}->{$marker} = $valid_data->{$po_id}->{$marker};
                    my $size_40_a =
                      keys( %{ $valid_data->{$po_id}->{$marker} } );
                    print "SSSSSSSSSsssize 40: $size_40_a\n";
                    $line_items_ref->{$po_id}->{'line_item_ctr'} = $size_40_a;
                }
                else {
                    ## rewrite the entire structure:
                    if ( $po_type_ref->{$po_id}->{'db_po_type'} eq 'BULK' ) {
                        $struc2_ref->{$po_id}->{$marker} =
                          $valid_data->{$po_id}->{$marker};    ## single site_id, qty

                        ## added on oct 29, 09: remove the 'Destination1' key value:
                        foreach my $l_40_id ( keys %{ $struc2_ref->{$po_id}->{$marker} } ) {
                            delete $struc2_ref->{$po_id}->{$marker}->{$l_40_id}->{'Destination1'};
                            delete $struc2_ref->{$po_id}->{$marker}->{$l_40_id}
                              ->{'Qty1'};                      ## added on Dec 7, 09
                            delete $struc2_ref->{$po_id}->{$marker}->{$l_40_id}
                              ->{'QtyUOM'};                    ## added on Dec 7, 09
                        }
                        ## end of modification on oct 29, 09

                        my $size_40_b =
                          keys( %{ $valid_data->{$po_id}->{$marker} } );
                        $line_items_ref->{$po_id}->{'line_item_ctr'} = $size_40_b;
                    }
                    else {
                        foreach my $edi_40_id ( keys %{ $valid_data->{$po_id}->{40} } ) {
##  $struc2_ref->{$po_id}->{$marker}->{$edi_40_id} = [];
##  add all the key-value to zone id from edi_seq_id
##  append each zone_id to replace
                            foreach my $comb_cr_id ( keys %{ $comb_ref->{$po_id}->{$edi_40_id} } ) {
                                foreach my $key ( keys %{ $valid_data->{$po_id}->{40}->{$edi_40_id} } ) {
                                    if (   ( $key eq 'zdq' )
                                        || ( $key !~ /\d+|\w+/g )
                                        || ( $key eq 'UNIT_PRICE_NC' )
                                        || ( $key eq 'UNIT_PRICE_RP' )
                                        || ( $key eq 'QUANTITY_ORDERED' ) )
                                    {
                                        next;
                                    }
                                    else {
                                        $comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id}->{$key} =
                                          $valid_data->{$po_id}->{40}->{$edi_40_id}->{$key};
                                    }
                                }

                                if ( ( keys %{ $comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id} } ) > 0 ) {
                                    my $comb_key = $edi_40_id . "_" . $comb_cr_id;
                                    $struc2_ref->{$po_id}->{$marker}->{$comb_key} =
                                      $comb_ref->{$po_id}->{$edi_40_id}->{$comb_cr_id};
                                    $comb_ref->{$po_id}->{'zone_line_items'} =
                                      $comb_ref->{$po_id}->{'zone_line_items'} + 1;
                                }
                            }
                        }
                        $line_items_ref->{$po_id}->{'line_item_ctr'} =
                          $comb_ref->{$po_id}->{'zone_line_items'};
                    }
                }
            }
        }
        ##$line_items_ref->{$po_id}->{'line_item_ctr'} = $comb_ref->{$po_id}->{'zone_line_items'};
    }

    ## plug in the total line_item number
    my $last_90_seqid;
    my $last_po;
    my $total_line_items;

    foreach my $po_id ( keys %$struc2_ref ) {
        foreach my $edi_90_id ( keys %{ $struc2_ref->{$po_id}->{90} } ) {
            $struc2_ref->{$po_id}->{90}->{$edi_90_id}->{'SUB_PO_SEGMENTS'} =
              $line_items_ref->{$po_id}->{'line_item_ctr'};
            ### $struc2_ref->{$po_id}->{90}->{'PO_SEGMENTS'} .= $line_items_ref->{$po_id}->{'line_item_ctr'};
            ### $total_line_items = $line_items_ref->{$po_id}->{'line_item_ctr'};
            $last_90_seqid = $edi_90_id if ( $edi_90_id ne '' );
            $last_po       = $po_id     if ( $po_id     ne '' );
        }
        $total_line_items = $total_line_items + $line_items_ref->{$po_id}->{'line_item_ctr'};
    }

    ### here to chose the last po and type 90 edi_seq_id to record the total line items:
    ### This is for the sake of precedence of structure over logics:
    $struc2_ref->{$last_po}->{90}->{$last_90_seqid}->{'PO_SEGMENTS'} = $total_line_items;

    if ( $self->{'debug'} ) {
        print "line_items_ref:\n";
        print Dumper($line_items_ref);
        print "Sturec2_ref\n";
        print Dumper($struc2_ref);
    }

    if ( $self->{'debug'} ) {
        print "check ordered destinations and quanties:\n";
        print "po_type_ref:";
        print Dumper($po_type_ref);
        print "valid_data_ref:";
        print Dumper($valid_data);
    }

    return ( $struc2_ref, $group_ref, $skip_po_href );
}

sub get_rp_cost_from_po_style_site {
    my ( $self, $po_id, $style_id, $site_id ) = @_;
    $po_id    =~ s/\s+//g;
    $site_id  =~ s/\s+//g;
    $style_id =~ s/\s+//g;

    my $query = "SELECT 
estimated_landed_cost, 
retail_price, 
rawtohex(estimated_landed_cost) || rawtohex(retail_price) group_key
FROM $self->{V_PO_STCODISI_SITES}
WHERE business_unit_id = '30'
AND po_id     = \'$po_id\'
AND style_id  = \'$style_id\'
AND site_id   = \'$site_id\'
GROUP BY site_id, estimated_landed_cost, retail_price
ORDER BY estimated_landed_cost, retail_price, site_id";
    my $ret = $self->{'dbh'}->selectall_arrayref($query);
    if ( $self->{'debug'} ) {
        print "Cost, retail price query: $query\n";
        print "retuned cost, prices result:\n";
        print Dumper($ret);
    }

    if ( ( $ret->[0][0] ) && ( $ret->[0][1] ) && ( $ret->[0][2] ) ) {
        return ( $ret->[0][0], $ret->[0][1], $ret->[0][2] );
    }
    else {
        ## zero case
        ##if(($ret->[0][0] == 0)||($ret->[0][1] == 0)){
        ##    return ($ret->[0][0],$ret->[0][1],$ret->[0][2]);
        ##}else{ ## undef case, blank it for certain uses in later code
        if ( !$ret->[0][0] ) { $ret->[0][0] = ' '; }
        if ( !$ret->[0][1] ) { $ret->[0][1] = ' '; }
        if ( !$ret->[0][2] ) { $ret->[0][2] = ' '; }
        $self->{'log_obj'}->log_info("missing cost, rp data:$po_id, $style_id, $site_id\n");
        return ( $ret->[0][0], $ret->[0][1], $ret->[0][2] );
        ##}
    }
}

## all the data from extract_edi_850 is based on business_unit_id = 30, so, here use 30.

sub get_zoneid_by_site {
    my ( $self, $site_id ) = @_;
    $site_id =~ s/\s+//g;
    my $query = "select mccs_plsql_functions.get_zone_from_site(\'$site_id\') zone_id FROM dual";
    ##my $query = "select zone_id from $self->{TABLE_SITES}  where site_id =  \'$site_id\' and business_unit_id = '30'";
    my $ret = $self->{'dbh'}->selectall_arrayref($query);
    if ( $self->{debug} ) {
        print "query: $query\n";
        print Dumper($ret);
    }
    return $ret->[0][0];
}

sub get_cost_by_zoneid_styleid {
    my ( $self, $po_id, $site_id, $style_id, $color_id ) = @_;

    $po_id    =~ s/\s+//g;
    $site_id  =~ s/\s+//g;
    $style_id =~ s/\s+//g;
    $color_id =~ s/\s+//g;

##    my $query = "select mccs_plsql_functions.get_po_item_cost(\'304752\',\'02100\',\'00414572725101\',\'000\') pocost FROM dual";

## my $query = "select mccs_plsql_functions.get_po_item_cost(\'$po_id\',\'$site_id\',\'$style_id\',\'$color_id\') pocost FROM dual";

    my $query = " 
select 
c.net_cost
from 
po_style_colors c, 
po_item_sites s
where 
c.po_id = $po_id
and c.color_id = \'$color_id\'
and c.style_id = \'$style_id\'
and c.color_id = s.po_style_color_id
and s.site_id = \'$site_id\'
";

    my $ret = $self->{'dbh'}->selectall_arrayref($query);
    if ( $self->{'debug'} ) {
        print "CCCCCCCCCCCCCCCost query: $query\n";
        print Dumper($ret);
    }
    return $ret->[0][0];
}

## assuming store number is below 50, this should be true all the time
sub get_unit_key {
    my ( $number, $unit_key ) = @_;
    if ( $number < 11 ) {
## do nothing in this case
    }
    elsif ( ( $number > 10 ) && ( $number < 21 ) ) {
        $unit_key .= "1";
    }
    elsif ( ( $number > 20 ) && ( $number < 31 ) ) {
        $unit_key .= "2";
    }
    elsif ( ( $number > 30 ) && ( $number < 41 ) ) {
        $unit_key .= "3";
    }
    elsif ( ( $number > 40 ) && ( $number < 51 ) ) {
        $unit_key .= "4";
    }
    return $unit_key;
}

sub parse_type_10 {
    my ( $self, $transaction_data, $po, $po_type_ref ) = @_;
    my $data_ref;
    $data_ref->{PARTNERSHIP_CODE} = substr( $transaction_data, 0,  16 );
    $data_ref->{RECORD_TYPE}      = substr( $transaction_data, 16, 2 );
    $data_ref->{PURPOSE}          = substr( $transaction_data, 18, 2 );
    $data_ref->{PO_TYPE}          = substr( $transaction_data, 20, 2 );
    $data_ref->{PO_NUMBER}        = substr( $transaction_data, 22, 10 );
    $data_ref->{RELEASE_NUMBER}   = substr( $transaction_data, 32, 10 );
    $data_ref->{PO_DATE}          = substr( $transaction_data, 42, 8 );
    $data_ref->{QUALIFIER}        = substr( $transaction_data, 50, 2 );
    $data_ref->{BPARTY_CURRENCY}  = substr( $transaction_data, 52, 3 );
    $data_ref->{RQ_VENDOR}        = substr( $transaction_data, 55, 2 );
    $data_ref->{INTERNAL_VENDOR}  = substr( $transaction_data, 57, 11 );
    $data_ref->{RQ_CUSTOMER}      = substr( $transaction_data, 68, 2 );
    $data_ref->{IC_NUMBER}        = substr( $transaction_data, 70, 15 );
    $data_ref->{SR_CODE}          = substr( $transaction_data, 85, 2 );
    $data_ref->{BO_FLAG}          = substr( $transaction_data, 87, 1 );
    $data_ref->{FOB_POINT}        = substr( $transaction_data, 88, 20 );
    ##$data_ref->{HEADER_REMARKS}   = substr($transaction_data, 108, 30);

    ## format time:
    $data_ref->{PO_DATE} = $self->get_sps_time_str( $data_ref->{PO_DATE} );

## get vendor_id, version_no from po
    my $query2 =
"                                                                                                                                              
select 
   vendor_id,
   version_no, 
   fob_point  
from
   $self->{'TABLE_PO'}                                                                                                      
where 
   po_id = $po ";

    my $value_ref = $self->{'dbh'}->selectall_arrayref($query2);
    $data_ref->{INTERNAL_VENDOR} = $value_ref->[0][0];
    $data_ref->{RELEASE_NUMBER}  = $value_ref->[0][1];
    $data_ref->{FOB_POINT}       = $value_ref->[0][2];
    print "internal_vendor:  $data_ref->{INTERNAL_VENDOR} \n"
      if $self->{'debug'};

    $po_type_ref->{$po}->{'internal_vendor'} = $data_ref->{INTERNAL_VENDOR};

    ## get header_remarks
    $po =~ s/\s+//g;
    my $fob_remarks = " 
select 
   sr.remark
from
    $self->{'TABLE_PO'}  p,
    $self->{'TABLE_SPECIFIC_REMARKS'}  sr
where
       p.remark_key = sr.remark_key
    and sr.level_id = 'HEADER'
    and sr.structure_id = 'PO'
    and p.po_id = $po
    order by p.creation_date desc
";
    my $value_ref2 = $self->{'dbh'}->selectall_arrayref($fob_remarks);
    print Dumper($value_ref2) if $self->{'debug'};
#### $data_ref->{HEADER_REMARKS}   =  $value_ref->[0][0];
    $po_type_ref->{$po}->{'header_remarks'} = $value_ref2->[0][0];

## REFORMAT FOB_POINT
    my $tmp_fob_point;
    if ( $data_ref->{FOB_POINT} eq 'ORIGIN' ) {
        $tmp_fob_point = 'OR';
    }
    elsif ( $data_ref->{FOB_POINT} eq 'DESTINATION' ) {
        $tmp_fob_point = 'DE';
    }
    elsif ( $data_ref->{FOB_POINT} =~ /city/gi ) {
        $tmp_fob_point = 'AC';
    }
    else {
        $tmp_fob_point = uc( substr( $data_ref->{FOB_POINT}, 0, 2 ) );
    }
    $data_ref->{FOB_POINT} = $tmp_fob_point;    ## so, it only can be: OR, DE or AC

##  update the po type, check SA type to see if different ship address needed
## this is questionable??? Jan 21,09

    if ( $data_ref->{'PO_TYPE'} eq 'SA' ) {
## if parsed value is SA, check with dabase result:
## if database is DS, change it to 'DS' for the data_ref and for the po_type record
        if ( $po_type_ref->{$po}->{'sps_po_type'} eq 'DS' ) {
            $data_ref->{'PO_TYPE'} = 'DS';      ## re-asign
        }
    }

    if ( $self->{'debug'} ) {
        print "after parse_type_10: \n";
        print Dumper($data_ref);
    }
    return $data_ref;
}

sub parse_type_30 {
    my ( $self, $transaction_data, $po, $po_type_ref ) = @_;
    my $data_ref;
    $data_ref->{PARTNERSHIP_CODE}       = substr( $transaction_data, 0,  16 );
    $data_ref->{RECORD_TYPE}            = substr( $transaction_data, 16, 2 );
    $data_ref->{TERMS_TYPE}             = substr( $transaction_data, 18, 2 );
    $data_ref->{TERMS_BASIS_DATE_CODE}  = substr( $transaction_data, 20, 2 );
    $data_ref->{TERMS_DISCOUNT_PERCENT} = substr( $transaction_data, 22, 6 );
    $data_ref->{TERMS_DISCOUNT_DAYS}    = substr( $transaction_data, 28, 3 );
    $data_ref->{TERMS_NET_DAYS}         = substr( $transaction_data, 31, 3 );
    $data_ref->{DATE_QUALIFIER_SBD}     = substr( $transaction_data, 34, 3 );
    $data_ref->{SHIP_NOT_BEFORE_DATE}   = substr( $transaction_data, 37, 8 );
    $data_ref->{DATE_QUALIFIER_DD}      = substr( $transaction_data, 45, 3 );
    $data_ref->{DELIVERY_DATE}          = substr( $transaction_data, 48, 8 );
    $data_ref->{DATE_QUALIFIER_CD}      = substr( $transaction_data, 56, 3 );
    $data_ref->{CANCEL_DATE}            = substr( $transaction_data, 59, 8 );
    $data_ref->{ASSIGNED_BY}            = substr( $transaction_data, 67, 2 );
    ##$data_ref->{SCAC_CODE}             = substr($transaction_data, 69, 10);
    $data_ref->{QUALIFIER}      = substr( $transaction_data, 79, 2 );
    $data_ref->{ASSIGNED_BY_PO} = substr( $transaction_data, 81, 2 );
    $data_ref->{STORE_CODE}     = substr( $transaction_data, 83, 5 );

## The following information suppose to be missing from the data, anyway:
##    $data_ref->{ADDRESS_1}            = substr($transaction_data, 86, 60);
##    $data_ref->{ADDRESS_2}            = substr($transaction_data, 146, 60);
##    $data_ref->{ADDRESS_3}            = substr($transaction_data, 206, 60);
##    $data_ref->{CITY}                 = substr($transaction_data, 266, 60);
##    $data_ref->{STATE_ID}             = substr($transaction_data, 326, 10);
##    $data_ref->{ZIP_CODE_ID}          = substr($transaction_data, 336, 30);
##    $data_ref->{COUNTRY_ID}           = substr($transaction_data, 366, 10);
##    $data_ref->{TERMS_DESCRIPTION}    = substr($transaction_data, 376, 10);
##    $data_ref->{COUNTRY_ID}           = substr($transaction_data, 386, 50);

## format time to sps format:
    $data_ref->{SHIP_NOT_BEFORE_DATE} = $self->get_sps_time_str( $data_ref->{SHIP_NOT_BEFORE_DATE} );
    $data_ref->{DELIVERY_DATE}        = $self->get_sps_time_str( $data_ref->{DELIVERY_DATE} );
    $data_ref->{CANCEL_DATE}          = $self->get_sps_time_str( $data_ref->{CANCEL_DATE} );

## get other missing values: TERMS--description
    my $terms_info = $self->get_term_info( $po, $po_type_ref );
    $data_ref = &merge_hashes( $terms_info, $data_ref );

## add address info:
    my $address_info;
    my $re_examined_potype;

###==> record store_code in any cases: JAN 29,09, this will be useful for bulk type ds,
    $po_type_ref->{$po}->{'store_code'} = $data_ref->{STORE_CODE};

## if DS type for sps, i.e, address_to_site_id in PO table is null, then get site_id information for the case of BULK
    if ( $po_type_ref->{$po}->{'sps_po_type'} eq 'DS' ) {
        if ( $po_type_ref->{$po}->{'db_po_type'} eq 'BULK' )
        {    ## in this case, only one destination and one qty exist:
            ## you will NOT  need to have address information,
            ## USE STORE_CODE from line 30

            $data_ref->{Destination1} = $data_ref->{STORE_CODE};
            $po_type_ref->{$po}->{'store_code'} = $data_ref->{STORE_CODE};
            ## $data_ref->{STORE_CODE} = ''; ## this will avoid the <AddressLocationNumber> tag to have any value for BULK type without <Address> ## comment out to show data in the AddressLocationNumber field, on oct29, yuc

        }
        else {    ## dropship
            ## use SITE_ID from line 50
            $data_ref->{STORE_CODE} =
              '';    ## this avoid <AddressLocationNumber> in this case. site_id will come from line 50

        }

        ##}elsif($po_type_ref->{$po}->{'sps_po_type'} eq 'SA'){ ## this is for the case that address_to_site_id is NOT NULL
    }
    else {
        ##if( $po_type_ref->{$po}->{'db_address_to_site_id'} ne ''){
        $address_info = $self->get_address_info(
            $po,
            $data_ref->{'STORE_CODE'},
            $po_type_ref->{$po}->{'db_address_to_site_id'}
        );
        $data_ref->{STORE_CODE}             = $data_ref->{STORE_CODE};
        $data_ref->{ADDRESS_ID}             = 'ST';
        $po_type_ref->{$po}->{'store_code'} = $data_ref->{STORE_CODE};
        ##}
    }

    my $merged_ref = &merge_hashes( $address_info, $data_ref );
    if ( $self->{'debug'} ) {
        print "result from parse_typ_30:";
        print Dumper($merged_ref);
    }
    return $merged_ref;
}

## this is an important function, it assign sps_po_type based on the address_to_site_id,
## then, it dispatch different ways to get site_id or address information

sub re_exam_po_type {
    my ( $self, $po ) = @_;
    my $query1 =
"                                                                                                                                                                                                         
select   po_type, 
         address_to_site_id                                                                                                     
from     $self->{'TABLE_PO'}  p                                                                                        
where    business_unit_id = 30                                                                                        
         and po_id = $po       
";
    my $value_ref = $self->{'dbh'}->selectall_arrayref($query1);

    if ( $self->{'debug'} ) {
        print "po_type result:\n";
        print Dumper($value_ref);
    }

## if the address_to_site_id is null,
## then, sps_PO_TYPE is 'DS'. we do not need address information.
## if po_type value from purchase_orders is 'bulk' get additional site_id from type 10
## if po_type value is 'dropship', then get site_id from type 50
## from Karen's instruction

    my $po_type_ref;

    if ( $value_ref->[0][1] eq '' ) {
        $po_type_ref->{'sps_po_type'}           = 'DS';
        $po_type_ref->{'db_po_type'}            = $value_ref->[0][0];
        $po_type_ref->{'db_address_to_site_id'} = $value_ref->[0][1];    ## this is null, just for future use
    }
    else {
        $po_type_ref->{'sps_po_type'}           = 'SA';
        $po_type_ref->{'db_po_type'}            = $value_ref->[0][0];
        $po_type_ref->{'db_address_to_site_id'} = $value_ref->[0][1];
        ## if the db_address_to_site_id is NOT null, need to obtain address information
    }

    if ( $self->{debug} ) {
        print "query1: $query1\n";
        print Dumper($po_type_ref);
    }
    return $po_type_ref;
}

## this function can get values most of the times
sub get_term_info {
    my ( $self, $po, $po_type_ref ) = @_;
    $po =~ s/\s+//g;
    my $data_ref;
    if ($po) {
        my $query1 =
"                                                                                                        
select                                                                                                                       
t.description                                                                                                             
from                                                                                                                         
$self->{'TABLE_PO'} p,                                                                                                     
$self->{'TABLE_TERMS'} t
where                                                                                                                        
p.term_id = t.term_id                                                                                                        
and p.po_id = \'$po\'                                                                                                        
";

        my $value_ref = $self->{'dbh'}->selectall_arrayref($query1);
        $data_ref->{TERM_DESCRIPTION} = $value_ref->[0][0];

        my $header_remarks = $po_type_ref->{$po}->{'header_remarks'};

        my $query2 =
"                                                                                                                      
select                                                                                                                                      
p.ship_via_id,                                                                                                                              
p.shipped_to_site_id                                                                                                                        
from                                                                                                                                        
$self->{'TABLE_PO'} p                                                                                                                      
where                                                                                                                                       
p.po_id = \'$po\'                                                                                                                       
";
        my $value_ref2 = $self->{'dbh'}->selectall_arrayref($query2);
        $data_ref->{SCAC_CODE}  = $value_ref2->[0][0];
        $data_ref->{STORE_CODE} = $value_ref2->[0][1];
        if ( $data_ref->{'SCAC_CODE'} eq '200' ) {
            my $ship_description = $self->get_ship_via_descriptions( $data_ref->{SCAC_CODE} );
            $data_ref->{'ROUTING'}   = '';
            $data_ref->{'SCAC_CODE'} = '';
            $data_ref->{'CONDITIONS'} =
              'conditions: https://community.spscommerce.com/mcx/, routing: ' . $ship_description;
        }
        else {
            ## what to do in this case???
            my $ship_description = $self->get_ship_via_descriptions( $data_ref->{SCAC_CODE} );
            $data_ref->{'CONDITIONS'} =
              'conditions: https://community.spscommerce.com/mcx/, routing: ' . $ship_description;
            $data_ref->{'ROUTING'}   = '';
            $data_ref->{'SCAC_CODE'} = '';
        }

        if ( length($header_remarks) > 0 ) {
            $data_ref->{'CONDITIONS'} .= ", " . $header_remarks;
        }

        if ( @$value_ref == 0 ) {
            my $this_function = ( caller(0) )[3];
            my $msg           = "No term value returned from db select $query1";
            $self->record_error( $this_function, $msg );
        }
    }
    return $data_ref;
}

sub get_ship_via_descriptions {
    my ( $self, $ship_via_id ) = @_;
    $ship_via_id =~ s/\s+//g;
    my $query =
"select description from $self->{TABLE_SHIP_VIAS} where ship_via_id = \'$ship_via_id\' and business_unit_id = '30'";
    my $ret = $self->{'dbh'}->selectall_arrayref($query);
    return $ret->[0][0];
}

sub merge_hashes {
    my ( $h_a, $h_b ) = @_;
    my $merged_ref;
    foreach my $k1 ( keys %$h_a ) {
        if ( ( $k1 =~ /Destination\d+/ ) || ( $k1 =~ /Qty\d+/ ) ) {
            $merged_ref->{$k1} = $h_a->{$k1};
        }
        else {
            $merged_ref->{ uc($k1) } = $h_a->{$k1};
        }
    }
    foreach my $k2 ( keys %$h_b ) {
        $merged_ref->{$k2} = $h_b->{$k2};
    }
    return $merged_ref;
}

sub get_sps_time_str {
    my ( $self, $t_str ) = @_;
    $t_str =~ s/\s+//g;
    my $new_str;
    if ($t_str) {
        my @ary = split( //, $t_str );
        $new_str = $ary[0] . $ary[1] . $ary[2] . $ary[3] . "-" . $ary[4] . $ary[5] . "-" . $ary[6] . $ary[7];
    }
    return $new_str;
}

## why not many data items returned??
## return only one row of data:
sub get_address_info {
    my ( $self, $po_id, $store_code, $address_to_site_id ) = @_;
    $po_id      =~ s/\s+//g;
    $store_code =~ s/\s+//g;

    print "po_id: $po_id\tstore_code: $store_code\n";

    my $query = "
select 
distinct(a.address_id),
a.ADDRESS_SOURCE_ID, 
a.ADDRESS_1,                                                    
a.ADDRESS_2,                                                    
a.ADDRESS_3,                                                    
a.ADDRESS_4,                                                   
a.CITY,                                                         
a.STATE_ID,   
a.ZIP_CODE,                       
a.COUNTRY_ID, 
a.FAX,                  
a.PHONE,                
a.TELEX 

from
$self->{'TABLE_ADDRESS'} a,
$self->{'TABLE_PO'} p

where          
   p.address_to_site_id = a.address_id
and p.address_to_site_type = a.address_type_id
";

    if ($store_code) {
        $query .= " and a.address_source_id = \'$store_code\'";
    }

    if ($address_to_site_id) {
        $query .= " and p.address_to_site_id = $address_to_site_id";
    }

    print "ADDress query: $query\n";
    my $value_ref = $self->{'dbh'}->selectall_hashref( $query, "address_id" );
    print "Address informaiton:\n";
    print Dumper($value_ref) if ( $self->{'debug'} );

    if ( keys(%$value_ref) == 0 ) {
        my $this_function = ( caller(0) )[3];
        my $msg           = "No value returned from db select $query";
        $self->record_error( $this_function, $msg );
    }

    my $key;

    ### here, we assume only one row of data will be returned here.
    foreach my $k ( keys %$value_ref ) {
        if ( $k ne '' ) {
            $key = $k;
        }
    }
    if ( $self->{'debug'} ) {
        print "address information:";
        print Dumper($value_ref);
    }
    return $value_ref->{$key};
}

sub parse_type_40 {
    my ( $self, $transaction_data, $po, $po_type_ref ) = @_;
    my $data_ref;
    $data_ref->{PARTNERSHIP_CODE} = substr( $transaction_data, 0,  16 );
    $data_ref->{RECORD_TYPE}      = substr( $transaction_data, 16, 2 );
    $data_ref->{QUANTITY_ORDERED} = substr( $transaction_data, 18, 10 );
    ##$data_ref->{Qty1}                 = substr($transaction_data, 18,10);
    $data_ref->{UNIT_MEASURE}     = substr( $transaction_data, 28, 2 );
    $data_ref->{UNIT_PRICE_NC}    = substr( $transaction_data, 30, 14 );
    $data_ref->{BASIS_UNIT_PRICE} = substr( $transaction_data, 44, 2 );
    $data_ref->{QUALIFIER_ST}     = substr( $transaction_data, 46, 2 );
    $data_ref->{BAR_CODE}         = substr( $transaction_data, 48, 13 );
    $data_ref->{QUALIFIER_STYLE}  = substr( $transaction_data, 61, 2 );
    ##   $data_ref->{STYLE}                = substr($transaction_data, 63,30);  ## current value wrong, need to get correct one by function
    $data_ref->{QUALIFIER_COLOR}      = substr( $transaction_data, 93,  2 );
    $data_ref->{COLOR_VENDOR}         = substr( $transaction_data, 95,  20 );
    $data_ref->{QUALIFIER_SIZE}       = substr( $transaction_data, 115, 2 );
    $data_ref->{SIZE}                 = substr( $transaction_data, 117, 10 );
    $data_ref->{QUALIFIER_PRICE}      = substr( $transaction_data, 127, 3 );
    $data_ref->{UNIT_PRICE_RP}        = substr( $transaction_data, 130, 14 );    ## need test! how??
    $data_ref->{QUALIFIER_FF}         = substr( $transaction_data, 144, 1 );
    $data_ref->{PRODUCT_CHAR_CODE_CD} = substr( $transaction_data, 145, 2 );
    $data_ref->{DESCRIPTION_COLOR}    = substr( $transaction_data, 147, 30 );
    $data_ref->{PRODUCT_CHAR_CODE_PD} = substr( $transaction_data, 177, 2 );
    $data_ref->{DESCRIPTION_STYLE}    = substr( $transaction_data, 179, 30 );
##    $data_ref->{INNER_PACK_QUANTITY}  = substr($transaction_data, 209,4);
##    $data_ref->{SIZE_PACK_QUANTITY}   = substr($transaction_data, 213,4);
    $data_ref->{UNIT_MEASURE_EA} = substr( $transaction_data, 217, 2 );
##    $data_ref->{COLOR_RMS}            = substr($transaction_data, 219,20);

    my $biz_unit_id = '30';

## All the following processes are for correcting the junky data in edi_peding_transactions.

## re-asign destination and qty for special case
    if ( $po_type_ref->{$po}->{'sps_po_type'} eq 'DS' ) {
        if ( $po_type_ref->{$po}->{'db_po_type'} eq 'BULK' ) {
            $data_ref->{Destination1} = $po_type_ref->{$po}->{'store_code'};
            $data_ref->{Qty1} = substr( $transaction_data, 18, 10 );
        }
        else {
            ## there will be 50 lines in this case
        }
    }
    else {    ## if SA case, not dropship only one destination and its quantity
        ##, Because there can not be destination and qty in this case, nothing will be done.

        ## REMOVE SITE_ID IN LINE ITEMS:
        ##$data_ref->{Destination1}         = $po_type_ref->{$po}->{'store_code'};
        ##$data_ref->{Qty1}                 = substr($transaction_data, 18,10);
        $data_ref->{UNIT_MEASURE_EA} = '';
    }

## Obtain 'STYLE' or 'vendor_style_no' and color_id from a view, THESE DATA ITEMS ARE NECESSARY AND  because it is missing in the original transaction data:
    if ( $data_ref->{BAR_CODE} ) {
        $data_ref->{STYLE_ID} = '';
        $data_ref->{COLOR_ID} = '';
## get RMS style_id from bar_code:
        ( $data_ref->{STYLE_ID}, $data_ref->{COLOR_ID} ) =
          $self->get_style_id_color_id_from_upc( $data_ref->{BAR_CODE} );

    }
    print Dumper( $data_ref->{STYLE}, $data_ref->{COLOR_ID} ) if $self->{debug};

## get vendor_id from po
    my $rms_vendor_id = $self->get_rms_vendor_id_from_po($po);

## get vendor_style_id from rms_style, and vendor_id
    ##print "before: $data_ref->{STYLE}\n";

    $data_ref->{STYLE} =
      $self->get_first_available_vendor_style_no_by_rms_styleno( $data_ref->{STYLE_ID}, $rms_vendor_id );

    ##print "afterrr\n";
    if ( $self->{'debug'} ) {
        print "vendor_style_nooooooooooooooooo:\n";
        print Dumper( $data_ref->{STYLE} );
    }

    my $siteid = $po_type_ref->{$po}->{'store_code'};

    ## here, we only use store_code from line 30, this is going to be changed soon!!!!! Jan 21, 09

    my ( $cost, $retail_price, $comb_cr_id ) =
      $self->get_rp_cost_from_po_style_site( $po, $data_ref->{STYLE_ID}, $siteid );

    $data_ref->{UNIT_PRICE_NC} = $cost;
    $data_ref->{UNIT_PRICE_RP} = $retail_price;

## adding missing color infor as they were missing in the edi850 file
    my $color_ref = $self->collect_color_infor( $data_ref->{STYLE_ID}, $data_ref->{COLOR_ID}, $biz_unit_id );
    $data_ref->{DESCRIPTION_COLOR}    = $color_ref->[0][0];
    $data_ref->{QUALIFIER_COLOR}      = '73' if ( $data_ref->{COLOR_VENDOR} );
    $data_ref->{COLOR_VENDOR}         = $color_ref->[0][1];
    $data_ref->{PRODUCT_CHAR_CODE_CD} = '75'
      if ( $data_ref->{DESCRIPTION_COLOR} );

## set default for 'COLOR_VENDOR'
    if ( $self->{'debug'} ) {
        print "parsed value from type 40:";
        print Dumper($data_ref);
    }
    return $data_ref;
}

sub get_rms_vendor_id_from_po {
    my ( $self, $po ) = @_;
    $po =~ s/\s+//g;
    my $query = "select vendor_id from $self->{TABLE_PO} where po_id = \'$po\'";
    my $ret   = $self->{'dbh'}->selectall_arrayref($query);
    if ( $self->{'debug'} ) {
        print $query;
        print Dumper($ret);
    }
    return $ret->[0][0];
}

### GET_COLOR_BY_upC
sub get_colorid_by_upc {
    my ( $self, $upc ) = @_;
    $upc =~ s/\s+//g;
    my $query = "
select color_id from bar_codes where bar_code_id = \'$upc\'
";
    my $ret = $self->{'dbh'}->selectall_arrayref($query);
    if ( $self->{'debug'} ) {
        print $query;
        print Dumper($ret);
    }
    return $ret->[0][0];
}

## GET COLOR information from style_id
sub collect_color_infor {
    my ( $self, $style_id, $color_id, $business_unit_id ) = @_;

    ##print "incollecting color infor:";
    ##print " stayle_id: $style_id, color_id: $color_id, business_unit_id: $business_unit_id\n";

    $style_id         =~ s/\s+//g;
    $business_unit_id =~ s/\s+//g;

    my $query =
"select                                                                                                                                                                           
sc.vendor_color,
c.description
from                                                                                                                                                                               style_colors sc,
 colors c
where                                                                                                                                                                             sc.business_unit_id = \'$business_unit_id\'
and sc.style_id = \'$style_id\'
and c.color_id = sc.color_id
and c.color_id = \'$color_id\'
";
    my $ret = $self->{'dbh'}->selectall_arrayref($query);

    ##print Dumper($ret);

    return $ret;
}

## get retail price from barcode, as Karen said the price from edi_pending_transaction table is not correct now. Jan 12, 09
sub get_retail_price_from_styleid {
    my ( $self, $style_id, $business_unit_id, $site_id ) = @_;
    my $query = "select     
     style_id,
     get_permanent_retail_price(\'$business_unit_id\',\'$site_id\', \'$style_id\', null, null, null, sysdate, null)
from 
     styles
where 
     business_unit_id = \'$business_unit_id\'
     and style_id = \'$style_id\'
";
    my $ret = $self->{'dbh'}->selectall_arrayref($query);

    if ( $self->{'debug'} ) {
        print "Query: $query\n";
        print Dumper($ret);
    }
    return $ret->[0][1];
}

sub get_style_id_color_id_from_upc {
    my ( $self, $upc ) = @_;
    $upc =~ s/\s+//g;
    my $query =
"select                                                                                                                                    
style_id,     
color_id
                                                                                                                           
from                                                                                                                                        
$self->{'TABLE_BAR_CODES'}                                                                                                                
where
bar_code_id = \'$upc\'
";

    ##print "QQQQQQQ===>>: $query\n";
    my $ret = $self->{'dbh'}->selectall_arrayref($query);
    ##print "styleid and colorrrrrrrrrrrrrrrrrrrrrrrrid:\n";
    ##print Dumper($ret);

    return ( $ret->[0][0], $ret->[0][1] );
}

### feb 3, 09 for new request of changes
sub get_first_available_vendor_style_no_by_rms_styleno {
    my ( $self, $rms_style, $vendor_id ) = @_;

    $rms_style =~ s/\s+//g;
    $vendor_id =~ s/\s+//g;

##    my  $query = "
#         SELECT
#         coalesce(vendor_style_no,
#                 (SELECT vendor_style_no FROM styles WHERE style_id = \'$rms_style\' AND business_unit_id = '30'))
#         vendor_style_no
#         FROM
#            (
#         select vendor_style_no from style_vendors where
#         style_id = \'$rms_style\' and vendor_id =  \'$vendor_id\' and business_unit_id = '30'
#          ORDER BY end_date DESC
#         ) WHERE rownum = 1 ";
#

## select * from MERCH.V_PO_STYLE_COLOR_ITEMS
## where business_unit_id = 30
## and po_id = '304756'

    my $query = "
SELECT coalesce(
          (SELECT * FROM
          (
            select vendor_style_no from style_vendors
            WHERE                                                  
            (style_id = \'$rms_style\' and vendor_id =  \'$vendor_id\' and business_unit_id = '30')
            ORDER BY end_date DESC                                                                          
          ) WHERE rownum = 1),
          (SELECT vendor_style_no FROM styles WHERE style_id = \'$rms_style\' AND business_unit_id = '30')
        ) vendor_style_no
FROM                                                                                             
dual
";

    my $ret = $self->{'dbh'}->selectall_arrayref($query);
    return $ret->[0][0];
}

### Jan 23, 09 per karen's email, getting infor from view:
sub get_vendor_style_no_from_view {
    my ( $self, $po, $style_id ) = @_;
## remove possible white spaces
    $po       =~ s/\s+//g;
    $style_id =~ s/\s+//g;

    my $query = "
select 
vendor_style_no
from 
$self->{'VIEW_PO_STYLE_COLOR_ITEMS'}
where 
business_unit_id = 30
and po_id = \'$po\'
and style_id = \'$style_id\'
";

    my $ret = $self->{'dbh'}->selectall_arrayref($query);
    if ( $self->{'debug'} ) {
        print "SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS\n";
        print Dumper($query);
        print Dumper($ret);
    }

    if ( @$ret == 0 ) {
        my $this_function = ( caller(0) )[3];
        my $msg           = "No value returned from db select $query";
        $self->record_error( $this_function, $msg );
    }
    return $ret->[0][0];
}

## get vendor_style_no from upc
## deprecated on Jan 23, 09
sub get_vendor_style {
    my ( $self, $upc, $internal_vendor ) = @_;
    my $data_ref;
    $upc =~ s/\s+//g;
    my $query = "";
    $query .= "select     
s.vendor_style_no,
s.inner_pack_qty,
s.outer_pack_qty, 
s.style_id
from
$self->{'TABLE_STYLE_VENDORS'}  s,
$self->{'TABLE_BAR_CODES'}  b
where
b.style_id = s.style_id
and b.bar_code_id = \'$upc\'
and s.vendor_id = \'$internal_vendor\'
order by start_date desc
";

    my $query2 = "
select * from MERCH.V_PO_STYLE_COLOR_ITEMS
where business_unit_id = 30
and po_id = '304756'
and style_id = '00000000005510'
";
    print "VVVVVVVV_vendor_style: $query\n";

    my $value_ref = $self->{'dbh'}->selectall_arrayref($query);
    my $flag      = 0;

    $data_ref->{STYLE} = $value_ref->[0][0];
    ##    $data_ref->{INNER_PACK_QUANTITY}        =  $value_ref->[$i][1];
    ##    $data_ref->{OUTER_PACK_QUANTITY}        =  $value_ref->[$i][2];
    $data_ref->{STYLE_ID} = $value_ref->[0][3];

    if ( @$value_ref == 0 ) {
        my $this_function = ( caller(0) )[3];
        my $msg           = "No value returned from db select $query";
        $self->record_error( $this_function, $msg );
    }

    return $data_ref;
}

### this is to write site_id and quantities list
sub write_site_and_qty {
    my ( $self, $site_ref ) = @_;
    my $i   = 1;
    my $des = "Destination";
    my $qty = "Qty";
    my $data_ref;
    foreach my $site_id ( sort { $a <=> $b } keys %$site_ref ) {
        my $full_dest = $des . "$i";
        my $full_qty  = $qty . "$i";
        $data_ref->{$full_dest} = $site_ref->{$site_id}->{'site_id'};
        $data_ref->{$full_qty}  = $site_ref->{$site_id}->{'qty'};
        $i++;
    }
    return $data_ref;
}

### this is to chop up type_50 line for site_id and quanty list:
### assuming the length of type_50 line is correct
sub get_site_and_qty_list {
    my ( $self, $transaction_data ) = @_;
    my $rem_str = substr( $transaction_data, 22 );
    my $site_ref;
    my $len = length($rem_str);
    while ( $len > 0 ) {
        my $site_id = substr( $rem_str, 0, 5 );
        $site_ref->{$site_id}->{'site_id'} = $site_id;
        my $qty = substr( $rem_str, 5, 10 );
        $qty =~ s/\s+//g;
        $site_ref->{$site_id}->{'qty'} = $qty;
        $rem_str = substr( $rem_str, 15 );
        $len = length($rem_str);
    }
    return $site_ref;
}

sub split_siteqty_2_hash_ref {
    my ( $self, $transaction_data ) = @_;

    my $cur_pos = 0;
    my $tot_len = length($transaction_data);
    my $site_ref;
    while ( $cur_pos < $tot_len ) {
        my $site_qty = substr( $transaction_data, $cur_pos, 15 );
        $cur_pos = $cur_pos + 15;
        ##$ctr = $ctr + 1;
        my @ary = split( /\s+/, $site_qty );
        if ( $ary[0] ) {
            $site_ref->{ $ary[0] }->{'site'} = $ary[0];
            $site_ref->{ $ary[0] }->{'qty'}  = $ary[1];
        }
    }
    return $site_ref;
}

## finding the next 50 lines for this 40 lin

sub split_siteqty_2_hash_ref_2 {
    my ( $self, $edi_seq_id, $dtls_data_ref ) = @_;

    my $tot_site_qty = $self->get_next_50_edi_seq_id(
        $dtls_data_ref->{$edi_seq_id}->{'pondate'},
        $dtls_data_ref->{$edi_seq_id}->{'line_50_seq_id'},
        $dtls_data_ref->{$edi_seq_id}->{'site_qty_comb'}
    );
    my $cur_pos = 0;
    my $tot_len = length($tot_site_qty);
    my $site_ref;
    while ( $cur_pos < $tot_len ) {
        my $site_qty = substr( $tot_site_qty, $cur_pos, 15 );
        $cur_pos = $cur_pos + 15;

        my @ary = split( /\s+/, $site_qty );
        if ( $ary[0] ) {
            $site_ref->{ $ary[0] }->{'site'} = $ary[0];
            $site_ref->{ $ary[0] }->{'qty'}  = $ary[1];
        }
    }
    return $site_ref;
}

## recursive call to collect all the 50 lines related

sub get_next_50_edi_seq_id {
    my ( $self, $pondate, $edi_seq_id, $tot_siteqty_str ) = @_;

=head 
    my $query = "
      select 
           b.edi_sequence_id, 
           b.site_qty_comb
      from $self->{'VIEW_TYPE_50'} a, 
           $self->{'VIEW_TYPE_50'} b
      where a.pondate = \'$pondate\'
      and a.pondate = b.pondate
      and (a.transaction_sequence_id + 1) = b.transaction_sequence_id
      and a.edi_sequence_id = \'$edi_seq_id\'";
=cut

    my $query = "
      select 
           b.edi_sequence_id, 
           b.site_qty_comb
      from $self->{'VIEW_TYPE_50'} a, 
           $self->{'VIEW_TYPE_50'} b
      where 
           a.po_id = b.po_id
           and a.date_created = b.date_created 
           and (a.transaction_sequence_id + 1) = b.transaction_sequence_id
           and a.edi_sequence_id = \'$edi_seq_id\'";

    my $value_ref = $self->{'dbh'}->selectall_arrayref($query);
    print Dumper($value_ref) if $self->{'debug'};

    if ( $value_ref->[0][0] =~ /\d+/g ) {
        $tot_siteqty_str .= $value_ref->[0][1];
        $self->get_next_50_edi_seq_id( $pondate, $value_ref->[0][0], $tot_siteqty_str );
    }
    else {
        return $tot_siteqty_str;
    }
}

sub parse_type_50 {
    my ( $self, $transaction_data, $link_to_40, $po, $po_type_ref ) = @_;
    my $data_ref;
    $data_ref->{PARTNERSHIP_CODE} = substr( $transaction_data, 0,  16 );
    $data_ref->{RECORD_TYPE}      = substr( $transaction_data, 16, 2 );
    $data_ref->{UNIT_MEASURE}     = substr( $transaction_data, 18, 2 );
    $data_ref->{ASSIGNED_BP}      = substr( $transaction_data, 20, 2 );

    ## following fields are changed from first specs:
    #  $data_ref->{SHIP_TO_STREET_ADDRESS1}= substr($transaction_data, 27,60);
    #  $data_ref->{SHIP_TO_STREET_ADDRESS2}= substr($transaction_data, 87,60);
    #  $data_ref->{SHIP_TO_CITY}           = substr($transaction_data, 147,50);
    #  $data_ref->{SHIP_TO_STATE}          = substr($transaction_data, 197,10);
    #  $data_ref->{SHIP_TO_ZIPCODE}        = substr($transaction_data, 207,30);
    #  $data_ref->{QUANTITY_ORDERED}       = substr($transaction_data, 237,10);

    ## processing site list:

    my $site_ref  = $self->get_site_and_qty_list($transaction_data);
    my $site_data = $self->write_site_and_qty($site_ref);
    $data_ref = &merge_hashes( $data_ref, $site_data );

    $data_ref->{LINK_TO_40} = $link_to_40;
    if ( $self->{'debug'} ) {
        print "parsed value from type 50:\n";
        print Dumper($data_ref);
    }
    return $data_ref;
}

sub parse_type_90 {
    my ( $self, $transaction_data ) = @_;
    my $data_ref;
    $data_ref->{PARTNERSHIP_CODE} = substr( $transaction_data, 0,  16 );
    $data_ref->{RECORD_TYPE}      = substr( $transaction_data, 16, 2 );
    $data_ref->{SUB_PO_SEGMENTS}  = substr( $transaction_data, 18, 6 );
    return $data_ref;
}

sub record_error {
    my ( $self, $function_name, $msg ) = @_;
    $self->{'log_obj'}->log_info("\nError from $function_name : $msg");
}

sub get_850_POs_to_skip {
    my ($self) = @_;
    my @skip_list;
    my $query = "      
select 
       distinct e1.key_data
from   
$self->{'TABLE_EPT'} e1,
$self->{'TABLE_EPT'} e2
where
e1.key_data = e2.key_data
and (substr(e1.transaction_data, 19, 2) = '00' OR substr(e1.transaction_data, 19, 2) = '05')
and substr(e2.transaction_data, 19, 2) = '05'
and e1.key_data in 
(
 select 
     e4.key_data
from                                                                                      
$self->{'TABLE_EPT'} e3,
$self->{'TABLE_EPT'} e4,
$self->{'TABLE_E8VE'}  x
where
    e3.key_data = e4.key_data
and e4.business_unit_id = 30
and e3.business_unit_id = 30
and e3.transaction_sequence_id = 1
and e4.transaction_sequence_id = 1
and e3.transaction_set = '850' 
and e3.transaction_type ='OUT'
and x.vendor_id || '850' = e4.partnership_id  
and e4.date_processed is null
and e3.ride_session_id ='100'
and e4.ride_session_id is null
)
";

    print $query;
    my $ret = $self->{'dbh'}->selectall_arrayref($query);
    for ( my $i = 0 ; $i < @$ret ; $i++ ) {
        for ( my $j = 0 ; $j < @{ $ret->[$i] } ; $j++ ) {
            if ( $ret->[$i][$j] ) { push( @skip_list, $ret->[$i][$j] ); }
        }
    }
    return \@skip_list;
}

sub check_PO_for_skip {
    my ( $self, $po_id, $type ) = @_;

    $po_id =~ s/\s+//g;
    $type  =~ s/\s+//g;

    if ( $self->{'debug'} ) {
        print "po: $po_id type: $type\n";
    }

##    my $query = "
##select
##   e3.key_data, substr(e3.transaction_data, 19, 2)
##from
##$self->{'TABLE_EPT'} e3,
##$self->{'TABLE_EPT'} e4,
##$self->{'TABLE_E8VE'}  x
##where
##    e3.key_data = e4.key_data
##and (substr(e3.transaction_data, 19, 2) = '00'
##OR substr(e3.transaction_data, 19, 2) = '05'
##OR substr(e3.transaction_data, 19, 2) = '01')
##and substr(e4.transaction_data, 19, 2) = '05'
##and e4.business_unit_id = 30
##and e3.business_unit_id = 30
##and e3.transaction_sequence_id = 1
##and e4.transaction_sequence_id = 1
##and e3.transaction_set = '850'
##and e3.transaction_type ='OUT'
##and x.vendor_id || '850' = e4.partnership_id
##and e4.date_processed is null
##and e3.ride_session_id ='100'
##and e4.ride_session_id is null
##and e3.key_data = \'$po_id\'";

    my $query;
    if ( $type eq '05' ) {
        $query = "select 
   e3.key_data
from                                                                                      
 $self->{'TABLE_EPT'}  e3,
 $self->{'TABLE_EPT'}  e4,
 $self->{'TABLE_E8VE'} x
where
   (substr(e3.transaction_data, 19, 2) = '00' or 
 substr(e3.transaction_data, 19, 2) = '05')
and e3.business_unit_id = 30
and e3.transaction_sequence_id = 1
and e3.transaction_set = '850' 
and e3.transaction_type ='OUT'
and x.vendor_id || '850' = e3.partnership_id  
and e3.ride_session_id ='100'
and e3.key_data = e4.key_data
and e4.transaction_sequence_id = 1
and e3.edi_sequence_id < e4.edi_sequence_id
and e4.key_data = \'$po_id\' 
";

    }
    elsif ( $type eq '01' ) {

        $query = "select 
   e3.key_data
from                                                                                      
 $self->{'TABLE_EPT'}  e3,
 $self->{'TABLE_E8VE'} x
where
    e3.business_unit_id = 30
and e3.transaction_set = '850' 
and e3.transaction_type ='OUT'
and x.vendor_id || '850' = e3.partnership_id  
and e3.key_data = \'$po_id\' 
";
    }
    else {
        print "WRONG PO submission type (01, or 05) for checking skip PO. 
               You submit_type value: $type" if ( $self->{'debug'} );

    }

    my $ret;
    print "q-q-q IN PO SKIP CHECK:  $query" if ( $self->{'debug'} );

## if more than zero value returned, po_skip is true
    if ($query) {
        $ret = $self->{'dbh'}->selectall_arrayref($query);
    }
    print Dumper($ret) if ( $self->{'deubg'} );
    my $po_skip = 0;
    if ($ret) {
        if ( @$ret > 0 ) {
            $po_skip = 1;
        }
    }
    return $po_skip;
}

sub handle_skip_POs {
    my ( $self, $skip_po_href ) = @_;
    my $ret_msg;
    if ( keys %{$skip_po_href} > 0 ) {
        $ret_msg .= "\nSkipped POs:\n";
        foreach my $s_po ( sort keys %{$skip_po_href} ) {
            $ret_msg .= "\t\t$s_po\n";
        }
        my $skip_error_flag = $self->mark_850_POs_to_skip($skip_po_href);
        if ( @$skip_error_flag > 0 ) {
            my $skip_msg = "Failed to skip $ret_msg\n";
            $self->{'log_obj'}->log_info("$skip_msg");
            warn($skip_msg);
            die;
        }
        else {
            $self->{'dbh'}->commit();
            my $update_ret_msg = " have(has) been skipped.\n";
            $self->{'log_obj'}->log_info("$update_ret_msg");
        }
    }
    return $ret_msg;
}

## update ept, update e8s for the PO to skip as 'have been processed'
## insert the PO into edi_850_po_exceptions table

sub mark_850_POs_to_skip {
    my ( $self, $skip_po_ref ) = @_;
    my @errors;

    ##########!!!!!!!!!!!!!!!! you have to do it one statement, one loop ONLY
    my $statement =
      "update $self->{'TABLE_EPT'} set date_processed = sysdate, ride_session_id = '333' where key_data = ? ";
    my $sth = $self->{'dbh'}->prepare($statement);

    foreach my $po_id ( sort keys %$skip_po_ref ) {
        ##foreach my $edi_sequence_id (sort keys %{$skip_po_ref->{$po_id}}) {
        eval { $sth->execute($po_id); };
        if ($@) {
            push( @errors, $@ );
            $self->{'log_obj'}->log_info("$@");
        }
        else {
            my $msg = "Marked skiped PO $po_id \n";
            $self->{'log_obj'}->log_info("$msg");
        }
        ##}
    }

    my $statement2 =
"update $self->{'TABLE_E8S'} set ride_processed_fin_date = sysdate, ride_processed_fin_ind = 'Y' where po_id = ? ";
    my $sth2 = $self->{'dbh'}->prepare($statement2);

    foreach my $po_id ( sort keys %$skip_po_ref ) {
        ##foreach my $edi_sequence_id (sort keys %{$skip_po_ref->{$po_id}}) {
        eval {

            $sth2->execute($po_id);
        };
        if ($@) {
            push( @errors, $@ );
            $self->{'log_obj'}->log_info("$@");
        }
        else {
            my $msg = "Marked skiped PO $po_id \n";
            $self->{'log_obj'}->log_info("$msg");
        }
        ##}
    }

## do the insert/update into the PO exception table:
    if ( defined $skip_po_ref ) {
        foreach my $po_id ( sort keys %{$skip_po_ref} ) {
            $po_id =~ s/\s+//g;
            if ( $po_id =~ /\d+/ ) {
                ##print "updating po: $po_id\n";
                my $sqry = "select count(*) from  $self->{'TABLE_E8PE'} where po_id = \'$po_id\'";
                my $ret  = $self->{'dbh'}->selectall_arrayref($sqry);

                if ( $self->{'debug'} ) {
                    print "selected counter in e8pe table\n";
                    print Dumper($ret);
                }

                my $statement2;
                if ( $ret->[0][0] > 0 ) {
                    $statement2 = "update $self->{'TABLE_E8PE'} set create_date = sysdate where po_id = ?";
                }
                else {
                    $statement2 =
                      "insert into $self->{'TABLE_E8PE'} (po_id, create_date) values (? , sysdate)";
                }
                my $sth = $self->{'dbh'}->prepare($statement2);
                eval { $sth->execute($po_id); };
                if ($@) {
                    push( @errors, $@ );
                    $self->{'log_obj'}->log_info("$@");
                }
                else {
                    ##$self->{'dbh'}->commit();
                    my $msg = "insert/update $po_id successful into $self->{'TABLE_E8PE'}";
                    $self->{'log_obj'}->log_info("$msg");
                }
            }
        }
    }
    return \@errors;
}

sub just_extract_POs {
    my ( $self, $po_list_ref, $cdate ) = @_;
    my $value_ref;
    my $po_list;
    foreach my $po (@$po_list_ref) {
        if ($po) {
            $po_list .= '\''."$po".'\''.",";
        }
    }
    $po_list =~ s/\,$//g;    #remove the last ','

    my $query2 =
"                                                                                                select edi_sequence_id, po_id, transaction_data, po_type, origin 
from  
$self->{TABLE_E8S}  
where  
edi_sequence_id in (
select edi_sequence_id 
from $self->{'TABLE_EPT'}
where key_data in (" . $po_list . ")";
    if ($cdate) {
        $query2 .= "  and date_created like '%" . $cdate . "%'";
    }

    $query2 .= ")";
    print $query2 if ( $self->{'debug'} );

    $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "edi_sequence_id" );
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "EDI_SEQUENCE_ID" );
    }

    if ( keys(%$value_ref) == 0 ) {
        $self->{'log_obj'}->log_info("extract query not working: $query2");
    }
    return $value_ref;
}

sub get_po_cdate_list_from_850_stack {
    my ( $self, $e850_staging_stack_ref, $ret_type ) = @_;
    my $po_list;
    my $exist_ref;
    foreach my $edi_seq_id ( keys %$e850_staging_stack_ref ) {
        unless ( $exist_ref->{ $e850_staging_stack_ref->{$edi_seq_id}->{'po_id'} } ) {
            if ( $e850_staging_stack_ref->{$edi_seq_id}->{'po_id'} ) {
                push( @$po_list, $e850_staging_stack_ref->{$edi_seq_id}->{'po_id'} );
                $exist_ref->{ $e850_staging_stack_ref->{$edi_seq_id}->{'po_id'} } = 1;
            }
        }
    }

    my $po_str = '';
    foreach my $po (@$po_list) {
        if ($po) {
            $po_str .= '\''."$po" .'\''. ",";
        }
    }
    $po_str =~ s/\,$//g;    #remove the last ','

    if ( $ret_type eq 'r' ) {
        return $po_list;
    }
    elsif ( $ret_type eq 's' ) {
        return $po_str;
    }
    else {
        return ( $po_list, $po_str );
    }
}

sub extract_850_header {
    my ( $self, $e850_staging_stack_ref, $opt_l, $cdate ) = @_;
    my $po_list_str = $self->get_po_cdate_list_from_850_stack( $e850_staging_stack_ref, 's' );

    my $query;

    if ($opt_l) {
        $query =
"select /*+ PARALLEL(e850_outbound_header,8) */ * from $self->{VIEW_SUM_1} where date_created like \'%$cdate%\' and po_id in ($po_list_str)";
        print "query: $query\n" if $self->{debug};
    }
    else {
        ##$query = "select * from $self->{VIEW_SUM_1} where date_processed is null and po_id in ($po_list_str)";
        $query = "select /*+ PARALLEL(e850_outbound_header,8) */ * from $self->{VIEW_SUM_1} 
                 where po_id in ($po_list_str) 
                 and edi_sequence_id in (select edi_sequence_id from $self->{'TABLE_E8S'} where ride_processed_fin_date is null and data_type ='10')";
        print "query: $query\n" if $self->{debug};
    }

    my $value_ref = $self->{'dbh'}->selectall_hashref( $query, "pondate" );
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query, "pondate" );
    }

    if ( keys(%$value_ref) == 0 ) {
        $self->{'log_obj'}->log_info("extract query not working: $query");
    }

    return $value_ref;
}

sub extract_850_details {
    my ( $self, $e850_staging_stack_ref, $opt_l, $cdate ) = @_;
    my $po_list_str = $self->get_po_cdate_list_from_850_stack( $e850_staging_stack_ref, 's' );
    my $query;

    if ($opt_l) {
        $query =
'select /*+ PARALLEL(e850_outbound_details,8) */  * from  '."$self->{VIEW_SUM_2} where date_created like \'%$cdate%\' and  po_id in ($po_list_str)";
        print "query: $query\n" if $self->{debug};
    }
    else {
        $query =
'select /*+ PARALLEL(e850_outbound_details,8) */ * from  '."$self->{VIEW_SUM_2} where po_id in ($po_list_str) and edi_sequence_id in (select edi_sequence_id from $self->{'TABLE_E8S'} where ride_processed_fin_date is null and data_type = '40')";
        print "query: $query\n" if $self->{debug};
    }

    my $value_ref = $self->{'dbh'}->selectall_hashref( $query, "edi_sequence_id" );
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query, "edi_sequence_id" );
    }

    if ( keys(%$value_ref) == 0 ) {
        $self->{'log_obj'}->log_info("extract query not working: $query");
    }
    return $value_ref;
}

## this is being used for the scripts based on views

sub re_group_850_by_cost_rp {
    my ( $self, $e850_ff_dtls_ref, $header_data_ref ) = @_;
    my $regrouped_ref;

    my $struct2_ref;    ## this will hold the re-constructed data:
    my $po_type_ref;    ## this will hold the po's type value:
    my ( $dropship_data_ref, $blk_data_ref );
    my $price_zone_ref;    ## rezoned data for DS type
    my $rg_ll_ctr_ref = {};    ## line item counter after regrouping

    ## 0,  rewrite BULK , Dropship POs to 'correct' string values:

    foreach my $pondate ( keys %$header_data_ref ) {
        foreach my $data_key ( keys %{ $header_data_ref->{$pondate} } ) {
            if ( lc($data_key) eq 'purchaseordertypecode' ) {
                if ( $header_data_ref->{$pondate}->{$data_key} eq 'DROPSHIP' ) {
                    ##!!! rewrite the value for po_type to required ones:
                    $header_data_ref->{$pondate}->{$data_key} = 'DS';
                }
                else{
                    ### keep the PO type as is:
                    ## $header_data_ref->{$pondate}->{$data_key} as is
		    ## add a short name for other types:
		    if ($header_data_ref->{$pondate}->{$data_key} =~ /bulk/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
		    }elsif($header_data_ref->{$pondate}->{$data_key} =~ /packed/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'PP';
			## $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
		    }elsif($header_data_ref->{$pondate}->{$data_key} =~ /distribute/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'}  = 'PD';
			##$header_data_ref->{$pondate}->{'purchaseordertypecode'}  = 'SA';
		    }else{
			my $msg = "Unknown type of PO for $pondate $header_data_ref->{$pondate}->{'purchaseordertypecode'}  \n";
			## assign all other cases as 'SA'
			$header_data_ref->{$pondate}->{'purchaseordertypecode'}  = 'SA';
		    }
                }
            }
            else {
                ## remove front and end white space, remove invalid space
                ## (influence empty space)

                $header_data_ref->{$pondate}->{$data_key} =~ s/^\s+//;
                $header_data_ref->{$pondate}->{$data_key} =~ s/\s+$//;
            }
        }
    }

    ## 1,  separate BULK , Dropship POs, ####
    foreach my $edi_seq_id ( keys %$e850_ff_dtls_ref ) {
        my $pondate = $e850_ff_dtls_ref->{$edi_seq_id}->{'pondate'};
        if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'DS' ) {
            $dropship_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }elsif ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'SA' ) {
            $blk_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }else { ## all other types will be treated as blk_data_ref
	    $blk_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }	
    }

    if ( $self->{'debug'} ) {
        print "headerdata:\n";
        print Dumper($header_data_ref);
        print "details data:\n";
        print Dumper($e850_ff_dtls_ref);

        print "ds data:\n";
        print Dumper($dropship_data_ref);
        print "bulk data:\n";
        print Dumper($blk_data_ref);
    }

    ## 2, processing DS type data:
    foreach my $edi_seq_id ( keys %$dropship_data_ref ) {
        my $po           = $dropship_data_ref->{$edi_seq_id}->{'po_id'};
        my $pondate      = $dropship_data_ref->{$edi_seq_id}->{'pondate'};
        my $rms_style_id = $dropship_data_ref->{$edi_seq_id}->{'r_style_id'};
        my $site_qty_ref = $self->split_siteqty_2_hash_ref_2( $edi_seq_id, $dropship_data_ref );

        if ( $self->{'debug'} ) {
            print "site_qty_ref:\n";
            print Dumper($site_qty_ref);
        }

        ## loop site_qty_ref to get rp, cost, and hexkey:
        foreach my $site_id ( keys %$site_qty_ref ) {
            if ($site_id) {
                (
                    $site_qty_ref->{$site_id}->{'cost'},
                    $site_qty_ref->{$site_id}->{'retail_price'},
                    $site_qty_ref->{$site_id}->{'comb_key'}
                ) = $self->get_rp_cost_from_po_style_site( $po, $rms_style_id, $site_id );
                my $zone_key = $site_qty_ref->{$site_id}->{'comb_key'};
                if ($zone_key) {
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} .=
                      $site_qty_ref->{$site_id}->{'site'} . '_' . $site_qty_ref->{$site_id}->{'qty'} . '|';
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'} =
                      $site_qty_ref->{$site_id}->{'cost'};
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'} =
                      $site_qty_ref->{$site_id}->{'retail_price'};
                }
            }
        }
        $dropship_data_ref->{$edi_seq_id}->{'site_comb_ref'} = $site_qty_ref;
    }
## rewrite the structure based on new comb_key of seq_id, and zone_key

    foreach my $pondate ( keys %$price_zone_ref ) {
        foreach my $edi_seq_id (
            sort { $a <=> $b }
            keys %{ $price_zone_ref->{$pondate} }
          )
        {
            foreach my $zone_key ( keys %{ $price_zone_ref->{$pondate}->{$edi_seq_id} } ) {
                my $comb_key = $edi_seq_id . "_" . $zone_key;
                $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} = $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} + 1;

                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'unitprice'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'}
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'} =~ /\d+/g );
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'retailunitprice'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'}
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'} =~ /\d+/g );
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'site_qty_str'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'};
                foreach my $data_key ( keys %{ $dropship_data_ref->{$edi_seq_id} } ) {
                    if ( $data_key eq 'site_comb_ref' ) {
                        next;
                    }    ## jump over reference to avoid repeat the same section
                    ## to avoid overwrite:
                    if (   ( $data_key eq 'unitprice' )
                        or ( $data_key eq 'retailunitprice' ) )
                    {
                        next;
                    }

                    my $tmp_value = $dropship_data_ref->{$edi_seq_id}->{$data_key};
                    $tmp_value =~ s/^\s+//;    ## some format of data
                    $tmp_value =~ s/\s+$//;
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{$data_key} = $tmp_value;
                }
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'orderqty'} =
                  $self->get_orderqty_in_each_zone(
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} )
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} );
            }
        }
        ## rewrite the total number of line items for each pondate
        $header_data_ref->{$pondate}->{'totallineitemnumber'} = $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'};

    }

## modify the BULK PO without Address information to type 'DS'
## note: this value will not be refered for other purpose before writting to xml file
    foreach my $pondate ( keys %$header_data_ref ) {
       ## if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'SA' ) {
        ##if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} =~ /[SA|PP|PD]/gi) { ## excluding DS, NOT for DS!!!!
	if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} ne 'DS') { ## excluding DS, NOT for DS!!!!
            if ( $header_data_ref->{$pondate}->{'address_to_site_id'} ) { ## ## with address_to_site_id 
		if  ($header_data_ref->{$pondate}->{'purchaseordertypecode'} =~ /SA/gi){
		    $header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';    
                    ## assign this value for sa with address info

		}else{ ## this will be treated like SA too, to cover PP, PD cases.., turn the PP, PD type into SA
                   ## this case may happen in the future, as overwrite po with addresses...
		    $header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';  
		    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
		}
            }else { ## No address id, if SA type, turns to DS as is in the past code
		if($header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'SA'){ 
		    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'DS';
		}else{ ## if not address, and with PP, or PD types, need to get address infor from a special view: rdiusr.e850_pres_addresses
		   
                ## a: this case will need to get address from sites table
		    $header_data_ref->{$pondate} = 
			$self->get_address_from_site_id($header_data_ref->{$pondate});
		    my $msg = "hhhhere--gettting address information with a subroutine";
		    unless ($header_data_ref->{$pondate}->{'address1'}){
			my $msg = "FFFFFFAILED IN gettting address information with $header_data_ref->{$pondate}->{'shipped_to_sit_id'})\n";
			$self->{'log_obj'}->info($msg);
		    }
	       ## b: need to reassign the PP, PD type to SA type   
		    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
		    $header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';  
		}
            }
        }
    }

##3, Process BULK type data, put it on the same structure
    foreach my $edi_seq_id ( sort { $a <=> $b } keys %$blk_data_ref ) {
        my $pondate = $blk_data_ref->{$edi_seq_id}->{'pondate'};
        foreach my $data_key ( keys %{ $blk_data_ref->{$edi_seq_id} } ) {
            my $tmp_value = $blk_data_ref->{$edi_seq_id}->{$data_key};
            $tmp_value =~ s/^\s+//;    ## some format of data
            $tmp_value =~ s/\s+$//;
            $struct2_ref->{$pondate}->{'line_items'}->{$edi_seq_id}->{$data_key} = $tmp_value;
        }
    }

    ##print Dumper($struct2_ref);
    return $struct2_ref;
}

## recalculate orderqty for each zone:

sub get_orderqty_in_each_zone {
    my ( $self, $site_qty_str ) = @_;
    my $tot_zone_qty = 0;
    $site_qty_str =~ s/\s+//g;
    my @ary = split( /\|/, $site_qty_str );
    foreach my $pair (@ary) {
        my @twin = split( /\_/, $pair );
        if ( $twin[1] ) {
            $tot_zone_qty = $tot_zone_qty + $twin[1];
        }
    }
    return $tot_zone_qty;
}

## this function will use the header and details informaiton to form
## the data structure required for HTML::Template to write xml file:
## Note: this will be replaced by a new function: data_structure_formation_for_xml_2

sub data_structure_formation_for_xml {
    my ( $self, $header_data, $dtls_data ) = @_;
    my $ret_struct;

    my ( $base, $pos_ref ) = $self->set_struct_unit_array_ref('PurchaseOrder');

    my $sps_xml_ref = IBIS::EDI_XML::get_sps_template_ref( '', 0 );

    my $p_ctr = 0;
    foreach my $po_cdate ( keys %$header_data ) {

        #poref, po_cter, po_cdate as parameters to create an hash reference in the array at position p_ctr

        my $init_struct = $self->set_struct_unit_hash_ref( $pos_ref, $p_ctr );

        ## set header and summary:
        my $header_ref;
        my $summary_ref;
        my @ary_header;
        my @ary_summary;
        ## collect header, and summary items:
        ## pick sum elements from all, the left the header elements:
        foreach my $h_key ( keys %{ $header_data->{$po_cdate} } ) {
            foreach my $sps_sum_key ( keys %{ $sps_xml_ref->{'PurchaseOrder'}->{'Summary'} } ) {
                if ( $h_key eq lc($sps_sum_key) ) {
                    $summary_ref->{$sps_sum_key} =
                      $header_data->{$po_cdate}->{$h_key};    ## writen as cases in the template
                }
            }
        }

        $header_ref =
          {};   ## initiation blanks ###  losing all the data does not have matching keys, dangerous!!!!!!!!!!
        push( @ary_header,  $header_ref );
        push( @ary_summary, $summary_ref );

        $self->set_key_value_pairs_of_hash_ref( $init_struct, 'Header', \@ary_header );

        my $level_ref = $self->set_key_value_pairs_of_hash_ref( $init_struct, 'Summary', \@ary_summary );

        if ( $self->{'debug'} ) {
            print "base after set header and summary:";
            print Dumper($base);
            print "level_ref:";
            print Dumper($level_ref);
        }

        ## set sections under header:
        foreach my $section_key ( keys %{ $sps_xml_ref->{'PurchaseOrder'}->{'Header'} } ) {

            my $section_data_ref = $self->get_header_section_array_ref( $header_data->{$po_cdate},
                $sps_xml_ref, $section_key, '850' );

            #print Dumper($section_data_ref);

            if ($section_data_ref) {
                $self->set_key_value_pairs_of_hash_ref( ${ $level_ref->{'Header'} }[0],
                    $section_key, $section_data_ref );
            }
        }

        my $line_items = $self->construct_line_items( $dtls_data, $po_cdate, $sps_xml_ref, '850' );
        $self->set_key_value_pairs_of_hash_ref( $init_struct, 'LineItems', $line_items->{'LineItems'} );
        $p_ctr++;
    }

    return $base;
}

## this function has two extra data members than version 1. Jan 22, 10

sub data_structure_formation_for_xml_2 {
    my ( $self, $file_type ) = @_;
    my $ret_struct;
    my $rt_tag;
    my $template_file;

    my $header_data = $self->{'view_header_data_ref'};
    my $dtls_data   = $self->{'re_grouped_860_data_ref'};

    if ( $file_type eq '850' ) {
        $rt_tag = 'PurchaseOrder';
    }
    elsif ( $file_type eq '860' ) {
        $rt_tag = 'PurchaseOrderChange';
    }

    my ( $base, $pos_ref ) = $self->set_struct_unit_array_ref($rt_tag);

    ##print Dumper($base);

    my $sps_xml_ref = IBIS::EDI_XML::get_sps_template_ref( $self->{'SPS_XML_TEMPLATE'}, 0 );

    ##print Dumper($sps_xml_ref);

    my $p_ctr = 0;
    foreach my $po_cdate ( keys %$header_data ) {

        #poref, po_cter, po_cdate as parameters to create an hash reference in the array at position p_ctr

        my $init_struct = $self->set_struct_unit_hash_ref( $pos_ref, $p_ctr );

        ## set header and summary:
        my $header_ref;
        my $summary_ref;
        my @ary_header;
        my @ary_summary;
        ## collect header, and summary items:
        ## pick sum elements from all, the left the header elements:
        foreach my $h_key ( keys %{ $header_data->{$po_cdate} } ) {
            foreach my $sps_sum_key ( keys %{ $sps_xml_ref->{$rt_tag}->{'Summary'} } ) {
                if ( $h_key eq lc($sps_sum_key) ) {
                    ##if((lc($h_key) eq 'totallineitemnumber')&& (($header_data->{$po_cdate}->{$h_key} eq '0')||($header_data->{$po_cdate}->{$h_key} == 0))) {
                    ##	next;
                    ##   }else{
                    $summary_ref->{$sps_sum_key} = $header_data->{$po_cdate}->{$h_key};
                    ##  }
                }
            }
        }
        $header_ref = {};
        ###  initiation blanks
        ###  losing all the data does not have matching keys, dangerous!!!
        push( @ary_header,  $header_ref );
        push( @ary_summary, $summary_ref );

        $self->set_key_value_pairs_of_hash_ref( $init_struct, 'Header', \@ary_header );

        my $level_ref = $self->set_key_value_pairs_of_hash_ref( $init_struct, 'Summary', \@ary_summary );

        if ( $self->{'debug'} ) {
            print "base after set header and summary:";
            print Dumper($base);
            print "level_ref:";
            print Dumper($level_ref);
        }

        ## set sections under header:
        foreach my $section_key ( keys %{ $sps_xml_ref->{$rt_tag}->{'Header'} } ) {

            my $section_data_ref = $self->get_header_section_array_ref( $header_data->{$po_cdate},
                $sps_xml_ref, $section_key, $file_type );
            ##print Dumper($section_data_ref);
            if ($section_data_ref) {
                $self->set_key_value_pairs_of_hash_ref( ${ $level_ref->{'Header'} }[0],
                    $section_key, $section_data_ref );
            }
        }

        my $line_items = $self->construct_line_items( $dtls_data, $po_cdate, $sps_xml_ref, $file_type );
        $self->set_key_value_pairs_of_hash_ref( $init_struct, 'LineItems', $line_items->{'LineItems'} );
        $p_ctr++;
    }

## this is for the cleaning of the structure, removing empty summary, and lineitems in the structure

    if ( $file_type eq '860' ) {
        for ( my $i = 0 ; $i < @{ $base->{$rt_tag} } ; $i++ ) {
            if ( !$base->{$rt_tag}[$i]->{'LineItems'}[0] ) {

                delete $base->{$rt_tag}[$i]->{'LineItems'};
            }
            ## set PO number value here:

            ## if(!$base->{$rt_tag}[$i]->{'Summary'}[0]){
            ##    delete  $base->{$rt_tag}[$i]->{'Summary'};
            ##}

            my $po_id = $base->{$rt_tag}[$i]->{'Header'}[0]->{'OrderHeader'}[0]->{'PurchaseOrderNumber'};
            $base->{$rt_tag}[$i]->{'Summary'}[0]->{'PurchaseOrderNumber'} = $po_id;

        }
    }

    return $base;
}

sub get_header_section_array_ref {
    my ( $self, $header_ref, $sps_xml_ref, $section, $file_type ) = @_;
    my $ret_section_info;
    my $data_presence_flag = 0;
    my $rt_tag;
    if ( $file_type eq '850' ) {
        $rt_tag = 'PurchaseOrder';
    }
    elsif ( $file_type eq '860' ) {
        $rt_tag = 'PurchaseOrderChange';
    }
    if ( lc($section) eq 'date' ) {
        my $date_array_ref = $self->get_date_array4htmltemplate($header_ref);
        return $date_array_ref;
    }
    elsif ( lc($section) eq 'notes' ) {
        ##my $notes_array_ref = $self->get_notes_array4htmltemplate($header_ref);
        my $notes_array_ref = $self->get_notes_with_subsection_array4htmltemplate($header_ref);
        return $notes_array_ref;
    }
    else {
        foreach my $x_section_item ( keys %{ $sps_xml_ref->{$rt_tag}->{'Header'}->{$section} } ) {
            foreach my $key ( keys %$header_ref ) {
                if ( lc($x_section_item) eq lc($key) ) {

                    $ret_section_info->{$x_section_item} = $header_ref->{$key};

                    ## adding exception case here: I do not like it here, may change later...
                    if (   ( $file_type eq '860' )
                        && ( lc($x_section_item) eq 'purchaseordertypecode' ) )
                    {
                        $ret_section_info->{$x_section_item} = 'CP';
                        ##$ret_section_info->{'r_purchaseordertypecode'} =  $header_ref->{$key};
                    }

                    ## this is to set a flag for if a valid data has been retrived:
                    if ( $section =~ /address/ig ) {
                        if (   ( ( lc($key) eq 'address1' ) || ( lc($key) eq 'addresslocationnumber' ) )
                            && ( $header_ref->{$key} =~ /\w+|\d+/g ) )
                        {
                            $data_presence_flag = 1;
                        }
                    }
                    else {
                        if (   ( $header_ref->{$key} )
                            && ( lc($key) ne 'tradingpartnerid' ) )
                        {
                            $data_presence_flag = 1;
                        }

                    }
                }
            }
        }

        if ($data_presence_flag) {
            my $section_ary;
            push( @$section_ary, $ret_section_info );
            return $section_ary;
        }
        else {
            return undef;
        }
    }
}

sub get_ll_section_array_ref {
    my ( $self, $single_ll_data_ref, $sps_xml_ref, $section, $file_type ) = @_;
    my $ret_section_info;
    my $data_presence_flag = 0;
    my $rt_tag;
    if ( $file_type eq '850' ) {
        $rt_tag = 'PurchaseOrder';
    }
    elsif ( $file_type eq '860' ) {
        $rt_tag = 'PurchaseOrderChange';
    }

    if ( $self->{'debug'} ) {
        print "sps_template_reference:";
        print Dumper( $sps_xml_ref->{$rt_tag}->{'LineItems'}->{'LineItem'}->{$section} );

        print "single_ll_data_ref:\n";
        print Dumper($single_ll_data_ref);
    }

    if ( lc($section) eq 'shipdestinationqty' ) {
        my $dest_qty_ary_ref = $self->get_dest_qty_array_ref( $single_ll_data_ref->{'site_qty_str'} );
        if ( $self->{'debug'} ) {
            print "string of site_qty:\n";
            print Dumper( $single_ll_data_ref->{'site_qty_str'} );
            print Dumper($dest_qty_ary_ref);
        }

        return $dest_qty_ary_ref;
    }
    else {
        foreach
          my $l_section_item ( keys %{ $sps_xml_ref->{$rt_tag}->{'LineItems'}->{'LineItem'}->{$section} } )
        {
            foreach my $key ( keys %$single_ll_data_ref ) {

                if ( lc($l_section_item) eq lc($key) ) {

                    $ret_section_info->{$l_section_item} =
                      $single_ll_data_ref->{$key};    ## use the case in the template to save
                    ### this is bad!!!
                    if (   ( lc($key) eq 'productcolordescription' )
                        && ( $single_ll_data_ref->{$key} ) )
                    {
                        if ( $file_type eq '850' ) {
                            $ret_section_info->{'PartNumberQualifier2'} = '73';
                        }
                    }
                    if ( $single_ll_data_ref->{$key} ) {
                        if (   ( lc($key) ne 'tradingpartnerid' )
                            && ( lc($key) ne 'recordtype' ) )
                        {    ## these items alone is not valuable enough to have the section
                            $data_presence_flag = 1;
                        }
                    }
                }
            }
        }

        if ($data_presence_flag) {
            my $section_ary;
            push( @$section_ary, $ret_section_info );
            return $section_ary;
        }
        else {
            return undef;
        }
    }
}

## ret: array of hash ref about destination and qty:
## this is for the html::template
## this is modified on Feb25,10 for a bug fix.

sub get_dest_qty_array_ref {
    my ( $self, $dest_qty_str ) = @_;

## vars common to all cases:
    my @dest_qty = split( /\|/, $dest_qty_str );
    my @ret_ary;
    my $t_ctr = 0;
    my $single_set_ref;
    my $list_size = @dest_qty;

## cases:

    if ( $list_size == 0 ) {
        return undef;

    }
    elsif ( $list_size <= 10 ) {

        foreach my $pair (@dest_qty) {
            my @twin = split( /\_/, $pair );
            unless ( $twin[0] && $twin[1] ) { next; }
            if ( $twin[0] ) {
                my $ind      = $t_ctr % 10 + 1;
                my $dest_ind = "Destination" . $ind;
                my $qty_ind  = "Qty" . $ind;
                $single_set_ref->{$dest_ind} = $twin[0];
                $single_set_ref->{$qty_ind}  = $twin[1];
                $t_ctr++;
            }
        }
        ##$single_set_ref->{'QtyUOM'} = 'EA';
        if ($single_set_ref) {
            push( @ret_ary, $single_set_ref );
        }

        return \@ret_ary;

    }
    elsif ( $list_size > 10 ) {
        my $set_counter_ref;
        my $set_ctr = 0;

        foreach my $pair (@dest_qty) {
            my @twin = split( /\_/, $pair );
            unless ( $twin[0] && $twin[1] ) { next; }
            if ( $twin[0] ) {
                my $ind      = $t_ctr % 10;
                my $ind2     = $ind + 1;
                my $dest_ind = "Destination" . $ind2;
                my $qty_ind  = "Qty" . $ind2;
                $single_set_ref->{$dest_ind} = $twin[0];
                $single_set_ref->{$qty_ind}  = $twin[1];

                ## set how many set counter here as 0
                if ( $ind == 0 ) {
                    $set_counter_ref->{$set_ctr} = 0;
                }
                if ( $ind == 9 ) {
                    ##$single_set_ref->{'QtyUOM'} = 'EA';
                    if ($single_set_ref) {
                        push( @ret_ary, $single_set_ref );
                    }
                    $single_set_ref = undef;            ## refresh this reference
                    $set_counter_ref->{$set_ctr} = 1;
                    $set_ctr++;
                }
                $t_ctr++;
            }
        }

        ## take care of the last set:
        if (   ( $set_counter_ref->{$set_ctr} == 0 )
            && ( $single_set_ref->{'Qty1'} )
            && ( $single_set_ref->{'Destination1'} ) )
        {

            ##$single_set_ref->{'QtyUOM'} = 'EA';
            if ($single_set_ref) {
                push( @ret_ary, $single_set_ref );
            }
        }
        return \@ret_ary;
    }
}

### working version:
sub construct_line_items {
    my ( $self, $dtls_data, $po_cdate, $sps_xml_ref, $file_type ) = @_;

    my $rt_tag;
    if ( $file_type eq '850' ) {
        $rt_tag = 'PurchaseOrder';
    }
    elsif ( $file_type eq '860' ) {
        $rt_tag = 'PurchaseOrderChange';
    }

    my ( $base, $ll_ref );
    ( $base, $ll_ref ) = $self->set_struct_unit_array_ref('LineItems');
    my $ll_ctr = 0;

    foreach my $comb_key ( sort keys %{ $dtls_data->{$po_cdate}->{'line_items'} } ) {
        my $init_struct = $self->set_struct_unit_hash_ref( $ll_ref, $ll_ctr );    ##{

        foreach my $l_sect ( keys %{ $sps_xml_ref->{$rt_tag}->{'LineItems'}->{'LineItem'} } ) {
            my $section_data_ref =
              $self->get_ll_section_array_ref( $dtls_data->{$po_cdate}->{'line_items'}->{$comb_key},
                $sps_xml_ref, $l_sect, $file_type );
            ##print Dumper($section_data_ref);
            if ($section_data_ref) {
                $self->set_key_value_pairs_of_hash_ref( $init_struct, $l_sect, $section_data_ref );
            }
        }
        $ll_ctr++;
    }
    ##print Dumper($base);
    return $base;
}

## return an array of hash elements about date1, and datetimequalifier1

sub get_date_array4htmltemplate {
    my ( $self, $header_data_ref ) = @_;

## loop through the data ref, for each type of date,
## return a set of values in the
## form of annoymous hash

    my $array_of_date;

    foreach my $hdr_data_key ( keys %$header_data_ref ) {
        if (   ( uc($hdr_data_key) eq 'DELIVERY_DATE' )
            && ( $header_data_ref->{$hdr_data_key} ) )
        {
            my $new_date;
            $new_date->{'Date1'}              = $header_data_ref->{$hdr_data_key};
            $new_date->{'DateTimeQualifier1'} = "002";
            push( @$array_of_date, $new_date );
        }
        if (   ( uc($hdr_data_key) eq 'SHIP_NOT_BEFORE_DATE' )
            && ( $header_data_ref->{$hdr_data_key} ) )
        {
            my $new_date;
            $new_date->{'Date1'}              = $header_data_ref->{$hdr_data_key};
            $new_date->{'DateTimeQualifier1'} = "037";

            push( @$array_of_date, $new_date );
        }

        if (   ( uc($hdr_data_key) eq 'CANCEL_DATE' )
            && ( $header_data_ref->{$hdr_data_key} ) )
        {
            my $new_date;
            $new_date->{'Date1'}              = $header_data_ref->{$hdr_data_key};
            $new_date->{'DateTimeQualifier1'} = "001";
            push( @$array_of_date, $new_date );
        }
    }

    ##print Dumper($array_of_date);
    return $array_of_date;
}

## 3 values were pushed into Notes field values: ships_description (routing), foblocationdesc, and NoteInformation itself from ept

sub get_notes_array4htmltemplate {
    my ( $self, $header_ref ) = @_;
    my $array_of_notes;
    my $notes = "conditions: https://community.spscommerce.com/mcx/";

    ##1, ships
    my $tmp_scac = $header_ref->{'scac_code'};
    if ( !$tmp_scac ) {
        $tmp_scac = $header_ref->{'ship_via_id'};
    }    ## fix on March 19, 10. yuc
    $tmp_scac =~ s/\s+//g;

    my $ship_description;
    if ( $header_ref->{'ships_description'} ) {
        $ship_description = $header_ref->{'ships_description'};
    }
    else {
        $ship_description = $self->get_ship_via_descriptions($tmp_scac);
    }
    if ($ship_description) {
        $notes .= ", routing: $ship_description";
    }

    ##2, foblocation desc:
    if ( $header_ref->{'foblocationdescription'} ) {
        $notes .= ' ' . $header_ref->{'foblocationdescription'};
        delete $header_ref->{ 'foblocationdescription' };    ## to avoid the same data item repeat Dec 17,09
    }

    ##3, note field value:
    if ( $header_ref->{'noteinformationfield'} ) {
        $notes .= ' ' . $header_ref->{'noteinformationfield'};
    }
    ## put into an array
    my $new_data;
    $new_data->{'NoteInformationField'} = $notes;
    push( @$array_of_notes, $new_data );
    return $array_of_notes;
}

## this is for the changes to multiple lines under NoteInformationField
sub get_notes_with_subsection_array4htmltemplate {
    my ( $self, $header_ref ) = @_;
    my $array_of_notes;
    my $notes;

    my $temp_array1;
    if ( $header_ref->{'Conditions'} ) {
        $temp_array1->{'NoteInformationField'} = $header_ref->{'Conditions'};
        ##push(@$array_of_notes, $temp_array1);
    }
    else {
        $temp_array1->{'NoteInformationField'} = "conditions: https://community.spscommerce.com/mcx/";
        ##push(@$array_of_notes, $temp_array1);
    }
    push( @$array_of_notes, $temp_array1 );

    my $tmp_scac = $header_ref->{'scac_code'};
    if ( !$tmp_scac ) {
        $tmp_scac = $header_ref->{'ship_via_id'};
    }    ## fix on March 19, 10. yuc
    $tmp_scac =~ s/\s+//g;

    my $ship_description;
    if ( $header_ref->{'ships_description'} ) {
        $ship_description = $header_ref->{'ships_description'};
    }
    else {
        $ship_description = $self->get_ship_via_descriptions($tmp_scac);
    }
    if ($ship_description) {
        ##$notes .= ", routing: $ship_description";
        my $temp_array2;
        $temp_array2->{'NoteInformationField'} = "routing: $ship_description";
        push( @$array_of_notes, $temp_array2 );
    }
    ##2, note field value:

    if ( $header_ref->{'noteinformationfield'} ) {
        $notes .= $header_ref->{'noteinformationfield'};
    }

    ##3, foblocation desc:
    if ( $header_ref->{'foblocationdescription'} ) {
        ## remove beginning and ending spaces:
        $header_ref->{'foblocationdescription'} =~ s/^\s+//g;
        $header_ref->{'foblocationdescription'} =~ s/\s+$//g;
        $notes .= $header_ref->{'foblocationdescription'};
        delete $header_ref->{ 'foblocationdescription' };    ## to avoid the same data item repeat Dec 17,09
    }

    ## format the string into six lines, 50 character long arrays:
    $array_of_notes = $self->split_remark_to_50char_array( $notes, $array_of_notes );

    return $array_of_notes;
}

sub split_remark_to_50char_array {
    my ( $self, $str, $ret_ref ) = @_;
    $str =~ s/^\s+//g;
    $str =~ s/\s+$//g;
    my $unit_num = 50;
    ## break into unit_num, chop and assign
    my @lines = split( /\_\|\_/, $str );
    foreach my $data (@lines) {
        if ( $data =~ /\d+|\w+/g ) {
            $data =~ s/^\s+//g;
            $data =~ s/\s+$//g;
            $data =~ s/\W+/ /g;
            ## replace all non-alphaNumaric letters, leave white space, and _## follow user requres, july21,10
            $data = substr( $data, 0, $unit_num );
            if ( $data =~ /\d+|\w+/g ) {
                my $temp_ref;
                $temp_ref->{'NoteInformationField'} = $data;
                push( @$ret_ref, $temp_ref );
            }
        }
    }
    return $ret_ref;
}

## set keyed or unkeyed array_reference from ground up
sub set_struct_unit_array_ref {
    my ( $self, $key ) = @_;
    if ( $self->{'debug'} ) {
        print "inputkey in __SUBROUNTINE__ SET_STRUCT_UNIT_ARRAY_REF; $key\n";
    }
    my $base_ref;
    my $ret_ref;

    if ($key) {
        $base_ref->{$key} = [];
        $ret_ref = $base_ref->{$key};
    }
    else {
        $ret_ref = [];
    }
    return ( $base_ref, $ret_ref );
}

## put anonymous hash reference to array_reference
sub set_struct_unit_anoym_hash_ref {
    my ( $self, $array_ref, $index ) = @_;
    unless ($index) {
        $index = 0;
    }
    ${$array_ref}[$index] = {};
    return ${$array_ref}[$index];
}

## put key or non-key hash reference from an array refernce
##at certain index postion in the array

sub set_struct_unit_hash_ref {
    my ( $self, $array_ref, $index, $key ) = @_;
    unless ($index) {
        $index = 0;
    }
    if ($key) {
        ${$array_ref}[$index]->{$key} = {};
        return ${$array_ref}[$index]->{$key};
    }
    else {
        ${$array_ref}[$index] = {};
        return ${$array_ref}[$index];
    }
}

## from a hash reference set key , value pair for the hash reference

sub set_key_value_pairs_of_hash_ref {
    my ( $self, $level_ref, $key, $value ) = @_;
    $level_ref->{$key} = $value;
    return $level_ref;
}

sub pool_860_header {
    my ( $self, $po_list_ref, $cdate ) = @_;

    my $query =
      "select * from $self->{VIEW_860_HEADER} where date_processed is null and ride_session_id is null";
    print "query: $query\n" if $self->{debug};
    if ($cdate) {
        $query .= " and date_created like '%$cdate%'";
    }

    my $po_list = '';
    if ($po_list_ref) {
        foreach my $po_id (@$po_list_ref) {
            $po_list .= '\''."$po_id".'\''.",";
        }
        $po_list =~ s/\,$//g;
        $query .= " and po_id in ($po_list)";
    }

    print "QQQQQQQQQQQuery: $query\n" if ( $self->{'debug'} );

    my $value_ref = $self->{'dbh'}->selectall_hashref( $query, "pondate" );
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query, "pondate" );
    }

    if ( keys(%$value_ref) == 0 ) {
        $self->{'log_obj'}->log_info("extract query not working: $query");
        print "extract header data failed.";
    }

    $self->{'view_header_data_ref'} = $value_ref;
    return $value_ref;
}

sub pool_860_details {
    my ( $self, $po_list_ref, $cdate ) = @_;

    my $query = "select * from $self->{VIEW_860_DETAILS} where date_processed is null ";
    if ($cdate) {
        $query .= " and date_created like '%$cdate%'";
    }
    my $po_list = '';
    if ($po_list_ref) {
        foreach my $po_id (@$po_list_ref) {
            $po_list .= '\''."$po_id" .'\''.",";
        }
        $po_list =~ s/\,$//g;
        $query .= " and po_id in ($po_list) ";
    }

    print "query: $query\n" if $self->{debug};
    my $value_ref = $self->{'dbh'}->selectall_hashref( $query, "edi_sequence_id" );
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query, "edi_sequence_id" );
    }

    if ( keys(%$value_ref) == 0 ) {
        $self->{'log_obj'}->log_info("extract query not working: $query");
        ##print "extract details failed.";
    }

    $self->{'view_details_data_ref'} = $value_ref;
    return $value_ref;
}

## this is very similar to re_group_850_by_cost_rp but with minor changes, will combine into a single function later for easy maintaining:
sub re_group_860_dtls_by_cost_rp {
    my ($self) = @_;
    my $regrouped_ref;

    my $struct2_ref;    ## this will hold the re-constructed data:
    my $po_type_ref;    ## this will hold the po's type value:
    my ( $dropship_data_ref, $blk_data_ref );
    my $price_zone_ref;    ## rezoned data for DS type
    my $rg_ll_ctr_ref = {};    ## line item counter after regrouping

    my $e850_ff_dtls_ref = $self->{'view_details_data_ref'};
    my $header_data_ref  = $self->{'view_header_data_ref'};

    if ( $self->{'debug'} ) {
        print "in re_group_860_by_cost_rp, header and details:\n";
        print Dumper($e850_ff_dtls_ref);
        print Dumper($header_data_ref);
    }

    ## 0,  rewrite BULK , Dropship POs to 'correct' string values:

    foreach my $pondate ( keys %$header_data_ref ) {
        foreach my $data_key ( keys %{ $header_data_ref->{$pondate} } ) {
            if ( lc($data_key) eq 'purchaseordertypecode' ) {
                if ( $header_data_ref->{$pondate}->{$data_key} eq 'DROPSHIP' ) {
                    ##!!! rewrite the value for po_type to required ones:
                    $header_data_ref->{$pondate}->{$data_key} = 'DS';
                }
                else{
                    ### keep the PO type as is:
                    ## $header_data_ref->{$pondate}->{$data_key} as is
		    ## add a short name for other types:
		    if ($header_data_ref->{$pondate}->{$data_key} =~ /bulk/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
		    }elsif($header_data_ref->{$pondate}->{$data_key} =~ /packed/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'PP';
			## $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
		    }elsif($header_data_ref->{$pondate}->{$data_key} =~ /distribute/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'}  = 'PD';
			##$header_data_ref->{$pondate}->{'purchaseordertypecode'}  = 'SA';
		    }else{
			my $msg = "Unknown type of PO for $pondate $header_data_ref->{$pondate}->{'purchaseordertypecode'}  \n";
			## assign all other cases as 'SA'
			$header_data_ref->{$pondate}->{'purchaseordertypecode'}  = 'SA';
		    }
                }
            }
            else {
                ## remove front and end white space, remove invalid space
                ## (influence empty space)

                $header_data_ref->{$pondate}->{$data_key} =~ s/^\s+//;
                $header_data_ref->{$pondate}->{$data_key} =~ s/\s+$//;
            }
        }
    }
   
    ## 1,  separate BULK , Dropship POs, ####
    foreach my $edi_seq_id ( keys %$e850_ff_dtls_ref ) {
        my $pondate = $e850_ff_dtls_ref->{$edi_seq_id}->{'pondate'};
        if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'DS' ) {
            $dropship_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }elsif ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'SA' ) {
            $blk_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }else { ## all other types will be treated as blk_data_ref
	    $blk_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }	
    }

    if ( $self->{'debug'} ) {
        print "headerdata:\n";
        print Dumper($header_data_ref);
        print "details data:\n";
        print Dumper($e850_ff_dtls_ref);

        print "ds data:\n";
        print Dumper($dropship_data_ref);
        print "bulk data:\n";
        print Dumper($blk_data_ref);
    }

    ## 2, processing DS type data:
    foreach my $edi_seq_id ( keys %$dropship_data_ref ) {
        my $po           = $dropship_data_ref->{$edi_seq_id}->{'po_id'};
        my $pondate      = $dropship_data_ref->{$edi_seq_id}->{'pondate'};
        my $rms_style_id = $dropship_data_ref->{$edi_seq_id}->{'r_style_id'};
        my $site_qty_ref =
          $self->split_siteqty_2_hash_ref_2( $edi_seq_id, $dropship_data_ref );    ## most critical

        if ( $self->{'debug'} ) {
            print "site_qty_ref:\n";
            print Dumper($site_qty_ref);
        }

        ## loop site_qty_ref to get rp, cost, and hexkey:
        foreach my $site_id ( keys %$site_qty_ref ) {
            if ($site_id) {
                (
                    $site_qty_ref->{$site_id}->{'cost'},
                    $site_qty_ref->{$site_id}->{'retail_price'},
                    $site_qty_ref->{$site_id}->{'comb_key'}
                ) = $self->get_rp_cost_from_po_style_site( $po, $rms_style_id, $site_id );
                my $zone_key = $site_qty_ref->{$site_id}->{'comb_key'};
                if ($zone_key) {
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} .=
                      $site_qty_ref->{$site_id}->{'site'} . '_' . $site_qty_ref->{$site_id}->{'qty'} . '|';
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'} =
                      $site_qty_ref->{$site_id}->{'cost'};
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'} =
                      $site_qty_ref->{$site_id}->{'retail_price'};
                }
                else {
                    $self->{'log_obj'}->log_info(
                        "Missing cost,rp values: po: $po, edi_seq_id: $edi_seq_id, site: $site_id\n" );
                }
            }
        }
        $dropship_data_ref->{$edi_seq_id}->{'site_comb_ref'} = $site_qty_ref;
    }
## rewrite the structure based on new comb_key of seq_id, and zone_key

    foreach my $pondate ( keys %$price_zone_ref ) {
        $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} = 0;
        foreach my $edi_seq_id (
            sort { $a <=> $b }
            keys %{ $price_zone_ref->{$pondate} }
          )
        {
            foreach my $zone_key ( keys %{ $price_zone_ref->{$pondate}->{$edi_seq_id} } ) {
                my $comb_key = $edi_seq_id . "_" . $zone_key;
                $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} = $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} + 1;
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'unitprice'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'}
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'} );
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'retailunitprice'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'}
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'} );
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'site_qty_str'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'};
                foreach my $data_key ( keys %{ $dropship_data_ref->{$edi_seq_id} } ) {
                    if ( $data_key eq 'site_comb_ref' ) {
                        next;
                    }    ## jump over reference to avoid repeat the same section
                    if ( $data_key eq 'unitprice' ) { next; }

## For Mike G's request on May 3, 10, do not display retailunitprice in the case of PC or PQ for dropship
                    if ( $data_key eq 'retailunitprice' ) {
                        if (   ( $dropship_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PC' )
                            || ( $dropship_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PQ' ) )
                        {
                            $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{$data_key} = '';
                        }
                        next;
                    }
                    my $tmp_value = $dropship_data_ref->{$edi_seq_id}->{$data_key};
                    $tmp_value =~ s/^\s+//;    ## some format of data
                    $tmp_value =~ s/\s+$//;
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{$data_key} = $tmp_value;
                }

## asign deleted item's qtylefttoreceive as 0 according to sps requirement
                if ( $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'linechangecode'} eq 'DI' ) {
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'qtylefttoreceive'} =
                      0;                       ## asign deleted item's qtylefttoreceive as 0
                }
                else {

## reasign the qtyleft to receive according to the value in the 50 lines, instead of the values in the 40 lines view which parsed from the 40 line in ept table.
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'qtylefttoreceive'} =
                      $self->get_orderqty_in_each_zone(
                        $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} )
                      if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} );
                }
            }
        }
        ## rewrite the total number of line items for each pondate
        $header_data_ref->{$pondate}->{'totallineitemnumber'} = $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'};

    }

## modify the BULK PO without Address information to type 'DS'
## this part is copied from re_group_850... sub, March 18, 15
## note: this value will not be refered for other purpose before writting to xml file
    foreach my $pondate ( keys %$header_data_ref ) {
       ## if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'SA' ) {
        ##if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} =~ /[SA|PP|PD]/gi) { ## excluding DS, NOT for DS!!!!
	if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} ne 'DS') { ## excluding DS, NOT for DS!!!!
            if ( $header_data_ref->{$pondate}->{'address_to_site_id'} ) { ## ## with address_to_site_id 
		if  ($header_data_ref->{$pondate}->{'purchaseordertypecode'} =~ /SA/gi){
		    $header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';    
                    ## assign this value for sa with address info

		}else{ ## this will be treated like SA too, to cover PP, PD cases.., turn the PP, PD type into SA
                   ## this case may happen in the future, as overwrite po with addresses...
		    $header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';  
		    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
		}
            }else { ## No address id, if SA type, turns to DS as is in the past code
		if($header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'SA'){ 
		    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'DS';
		}else{ ## if not address, and with PP, or PD types, need to get address infor from a special view: rdiusr.e850_pres_addresses
		   
                ## a: this case will need to get address from sites table
		    $header_data_ref->{$pondate} = 
			$self->get_address_from_site_id($header_data_ref->{$pondate});
		    ##my $msg = "hhhhere--gettting address information with a subroutine";
		    unless ($header_data_ref->{$pondate}->{'address1'}){
			my $msg = "FFFFFFAILED IN gettting address information with $header_data_ref->{$pondate}->{'shipped_to_sit_id'})\n";
			$self->{'log_obj'}->info($msg);
		    }
	       ## b: need to reassign the PP, PD type to SA type   
		    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
		    $header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';  
		}
            }
        }
    }

##3, Process BULK type data, put it on the same structure
    foreach my $edi_seq_id ( sort { $a <=> $b } keys %$blk_data_ref ) {
        my $pondate = $blk_data_ref->{$edi_seq_id}->{'pondate'};
        if (   $blk_data_ref->{$edi_seq_id}->{'po_id'}
            && $blk_data_ref->{$edi_seq_id}->{'r_style_id'}
            && $header_data_ref->{$pondate}->{'addresslocationnumber'} )
        {

            print
"1:$blk_data_ref->{$edi_seq_id}->{'po_id'} 2:$blk_data_ref->{$edi_seq_id}->{'r_style_id'} 3:$header_data_ref->{$pondate}->{'addresslocationnumber'}\n";

            (
                $blk_data_ref->{$edi_seq_id}->{'unitprice'},
                $blk_data_ref->{$edi_seq_id}->{'retailunitprice'},
                $blk_data_ref->{$edi_seq_id}->{'comb_key'}
              )
              = $self->get_rp_cost_from_po_style_site(
                $blk_data_ref->{$edi_seq_id}->{'po_id'},
                $blk_data_ref->{$edi_seq_id}->{'r_style_id'},
                $header_data_ref->{$pondate}->{'addresslocationnumber'}
              );

            ## Mike's request on May 3, 10

            if (   ( $blk_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PC' )
                || ( $blk_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PQ' ) )
            {
                $blk_data_ref->{$edi_seq_id}->{'retailunitprice'} = '';

            }

        }
        else {
            $self->{'log_obj'}->info(
                "missing data for getting price info:	
                po: $blk_data_ref->{$edi_seq_id}->{'po_id'}, 
		r_style_id: $blk_data_ref->{$edi_seq_id}->{'r_style_id'}, 
		addresslocationnumber: $header_data_ref->{$pondate}->{'addresslocationnumber'}"
            );
        }
    }

    if ( $self->{'debug'} ) {
        print "after price inforrrrrrr\n";
        print Dumper($blk_data_ref);
    }

    ## put it into the same structure:
    foreach my $edi_seq_id ( keys %$blk_data_ref ) {
        my $pondate = $blk_data_ref->{$edi_seq_id}->{'pondate'};
        foreach my $data_key ( keys %{ $blk_data_ref->{$edi_seq_id} } ) {
            my $tmp_value = $blk_data_ref->{$edi_seq_id}->{$data_key};
            $tmp_value =~ s/^\s+//;    ## some format of data
            $tmp_value =~ s/\s+$//;
            $struct2_ref->{$pondate}->{'line_items'}->{$edi_seq_id}->{$data_key} = $tmp_value;
        }
    }

    $self->{'re_grouped_860_data_ref'} = $struct2_ref;
    return $struct2_ref;
}

sub update_edi_tabs_by_header_data {
    my ( $self, $hdr_data_ref, $table_type ) = @_;
    my @errors;
    my $statement;
    if ( $table_type =~ /e8s/ig ) {
##$statement =
        ##"update $self->{'TABLE_E8S'} s set s.ride_processed_fin_ind ='Y', s.ride_processed_fin_date = sysdate where s.po_id = ? and to_char(a.date_created, 'DD-MON-YY') = ? and s.edi_sequence_id = a.edi_sequence_id";

        $statement =
"update $self->{'TABLE_E8S'} s set s.ride_processed_fin_ind ='Y', s.ride_processed_fin_date = sysdate where s.po_id = ? and s.edi_sequence_id in (select a.edi_sequence_id from $self->{'TABLE_EPT'} a where a.key_data = s.po_id and  to_char(a.date_created, 'DD-MON-YY') = ? )";

    }
    elsif ( $table_type =~ /ept/ig ) {
        $statement =
"update $self->{'TABLE_EPT'} set ride_session_id ='100', date_processed = sysdate where key_data = ? and to_char(date_created, 'DD-MON-YY') = ?";

    }

    my $sth = $self->{'dbh'}->prepare($statement);

    foreach my $pondate ( keys %$hdr_data_ref ) {
        my $po_id        = $hdr_data_ref->{$pondate}->{'po_id'};
        my $date_created = $hdr_data_ref->{$pondate}->{'date_created'};

        if ( $po_id && $date_created ) {
            my @bding_val;
            push( @bding_val, $po_id );
            push( @bding_val, $date_created );
            eval { $sth->execute(@bding_val); };
            if ($@) {
                push( @errors, $@ );
                $self->{'log_obj'}->log_info("$@");
            }
            else {
                my $msg =
                  "update worked: table type: $table_type po_id: $po_id date_created: $date_created\n";
                $self->{'log_obj'}->log_info($msg);
            }
        }
        else {
            croak( "Warning: missing values, which one? po_id: $po_id or date_created: $date_created" );

        }
    }
    return \@errors;
}

sub clean_up_po_list {
    my ( $self, $po_list_str ) = @_;
    my @po_ary;
    $po_list_str =~ s/\s+//g;
    $po_list_str =~ s/[A-Za-z]+//g;
    @po_ary = split( /\,/, $po_list_str );
    return \@po_ary;
}

sub get_sps_ftp_object {
    my ($self) = @_;
    my $c      = IBIS::Crypt->new();
    my $pass   = $c->decrypt( $self->{PASSWORD} );
    my $sftp   = Net::SFTP::Foreign->new(
        $self->{REMOTE_SERVER},
        user     => $self->{SFTP_USER},
        password => $pass,
        port     => '10022'
    );
    $sftp->die_on_error("Unable to establish SFTP connection ");

    return $sftp;
}

sub put_single_file {
    my ( $self, $ftp, $local_file, $remote_file ) = @_;
    my $success_flag = 0;
    my $reput_ctr    = 0;
    my $max_try      = 5;
  PUT_BLOCK: {
        my $put_success = $ftp->put( $local_file, $remote_file )
          or die "put failed ", $ftp->message;
        if ( !$put_success ) {
            $self->{'log_obj'}
              ->log_info( "ftp-put failed once. retry times: $reput_ctr, file: $local_file\n" );
            if ( $reput_ctr < $max_try ) {
                $reput_ctr++;
                redo PUT_BLOCK;
            }
        }
        else {
            $success_flag = 1;
        }
    }
    return $success_flag;
}

## at this point, POs are in holding, some may need to be re-hold
## some may need to be delted from the holding table..
##
sub sync_po_in_holding_post_modification {
    my ($self) = @_;
    my $db_op_error;
    my $insert_ctr = 0;
    ## db handles for all required operations:
    my $query0     = qq{select distinct po_id from $self->{TABLE_PO_IN_HOLD} };
    my $sth_select = $self->{'dbh'}->prepare($query0);
    ## only pick modified POs...
    my $query1 = qq{select 
                       key_data,
                       edi_sequence_id,
                       ride_session_id, 
                       transaction_set
                    from 
                       $self->{TABLE_EPT} 
                    where  
                       key_data = ? 
                       and 
                       date_processed is null
                       and 
                       ride_session_id is null
                       };
    my $sth_select1 = $self->{'dbh'}->prepare($query1);

    ## update the POs that has been modified..
    my $query2 = qq{update $self->{TABLE_EPT} 
                       set ride_session_id = '200' 
                    where 
                       key_data = ?  
                       and ride_session_id is null 
                       and date_processed is null};
    my $sth_update = $self->{'dbh'}->prepare($query2);

    ## modified POs will have totally different set of edi_seq_ids from the original PO, even with
    ## the same PO number, so, this query only delete older rows from the original POs inserted in the first time
    my $query3 = qq{delete 
                  from $self->{TABLE_PO_IN_HOLD}  
                  where po_id = ? 
                       and edi_sequence_id not in (select edi_sequence_id from  $self->{TABLE_EPT})};
    my $sth_delete = $self->{'dbh'}->prepare($query3);
    ## this is using the new set of edi_sequence_ids to insert into holding table
    my $query4 = qq{INSERT INTO rdiusr.edi_po_in_hold_t(
                      po_id, edi_sequence_id, transaction_set, date_of_holding) values (?,?,?,sysdate)};
    my $sth_insert = $self->{'dbh'}->prepare($query4);

    eval {
        $sth_select->execute;
        my $ary_ref = $sth_select->fetchall_arrayref;
        ## get each PO from in hold table
        foreach my $row (@$ary_ref) {
            my $po = $row->[0];
            print "PO:$po\n";
            my @val;
            push( @val, $po );
            $sth_select1->execute(@val);
            my $ary_ref1 = $sth_select1->fetchall_arrayref;

            ## if PO is in EPT table with ride_session_id as NULL,
            ## that means this PO has been modified... so, update ride_session_id as
            ## 200, for the ones that with 200 value already, do nothing...
            print Dumper($ary_ref1);
            if ( @{$ary_ref1} > 0 ) {
                foreach my $row2 (@$ary_ref1) {
                    ## key_data,
                    ## edi_sequence_id,
                    ## ride_session_id,
                    ## transaction_set
                    my $key_data        = $row2->[0];
                    my $edi_sequence_id = $row2->[1];
                    my $ride_session_id = $row2->[2];
                    my $transaction_set = $row2->[3];
                    my @insert_values;
                    push( @insert_values, $key_data );
                    push( @insert_values, $edi_sequence_id );
                    push( @insert_values, $transaction_set );
                    print "ride_session:$ride_session_id\n";

                    unless ($ride_session_id)
                    {    ## this is the case that POs has been modified, with new edi_seq_ids
                        print "in unless condition:\n";    ## this means the PO has been modified...

                        my ( $ret1, $ret2, $ret3 );
                        $ret1 = $self->do_execute( $sth_delete, \@val )
                          ;                                ## delete old PO rows, exclude newly added lines
                        if ($ret1) {
                            push( @$db_op_error, $ret1 );
                        }

                        $ret2 = $self->do_execute( $sth_insert, \@insert_values );    ## insert new rows
                        if ($ret2) {
                            push( @$db_op_error, $ret2 );
                        }

                        $ret3 = $self->do_execute( $sth_update, \@val );
                        ## update the new POs that should be in hold..
                        if ($ret3) {
                            push( @$db_op_error, $ret3 );
                        }
                    }
                }
            }
            else {
                ## if PO is not in EPT table, delete it from holding table
                ## The PO has been cancelled in this case..., so all edi_sequence_ids in holding table are gone...
                ##
                my $ret = $self->do_execute( $sth_delete, \@val );
                if ($ret) {
                    push( @$db_op_error, $ret );
                }
            }
        }
    };

    ##if($@){
    ##	push(@$db_op_error, $@);
    ##	$self->{'dbh'}->rollback;
    ##   }else{
    ##	$self->{'dbh'}->commit;
    ## }

    return $db_op_error;
}

sub sget_debug {
    my ( $self, $debug ) = @_;
    if ($debug) {
        $self->{'is_debug'} = $debug;
    }
    else {
        return $self->{'is_debug'};
    }
}

sub do_execute {
    my ( $self, $sth, $val_ref ) = @_;
    my $db_op_error;
    my $trace = Devel::StackTrace->new;
    print $trace->as_string; # like carp
    eval { $sth->execute(@$val_ref); };
    if ($@) {
        push( @$db_op_error, $@ );
        $self->{'dbh'}->rollback;
    }
    else {
        $self->{'dbh'}->commit;
    }
    return $db_op_error;
}

## wrapper_850 functions here

## initiate the START status table for current run
## input: list of values: program_id, status
## output: db errors

sub put_status {
    my ($self) = @_;

## get values:
    my $value_ref;
    my $next_id = $self->get_next_run_id();
    ## initiate values:
    my $p_id = $self->{'PROGRAM_ID'};
    push( @$value_ref, $next_id );
    push( @$value_ref, $p_id );

## do db operation:
    my $query = qq(insert into $self->{'TABLE_PROGRAM_STATUS'} 
        (run_id, program_id, status, start_date, finish_date) 
         values 
        (?,?, null, sysdate, null));
    my $sth = $self->{'dbh'}->prepare($query);
    my $db_error = $self->do_execute( $sth, $value_ref );
    unless ($db_error) {
        $self->_sget_current_run_id($next_id);
    }
    else {
        $self->{'log_obj'}->info("failed to get status run_id: Dumper($db_error)");
        die;
    }
    return $db_error;
}

#### util function:
sub _sget_current_run_id {
    my ( $self, $cur_run_id ) = @_;
    if ($cur_run_id) {
        $self->{'CUR_RUN_ID'} = $cur_run_id;
    }
    else {
        return $self->{'CUR_RUN_ID'};
    }
}

##input: none
##output: max run id
sub get_max_run_id {
    my ($self) = @_;
    my $query = qq( 
    select max(run_id) 
    from $self->{'TABLE_PROGRAM_STATUS'} 
    );
    my $ret = $self->{'dbh'}->selectall_arrayref($query);

    if ( $self->{'debug'} ) {
        print "query: $query";
        print Dumper($ret);
    }

    if ( $ret->[0][0] ) {
        return $ret->[0][0];
    }
    else {
        $self->{'log_obj'}->info("Failed to get max run id");
        return undef;
    }

}

## input: none
## output: next run id
sub get_next_run_id {
    my ($self) = @_;
    my $max_id = $self->get_max_run_id();
    if ($max_id) {
        $max_id++;
    }
    else {
        $self->{'log_obj'}->info("Failed to get max_id from status table");
        die;
    }
    return $max_id;
}

## Application functions for wrapper:

## input: none, or start_date??
## process: get last run_id, or max start_date
## output: return the status for that run_id, or max_date
sub check_last_run_status {
    my ($self) = @_;

    my $query = qq(
   select status 
   from $self->{TABLE_PROGRAM_STATUS} 
   where run_id = (
     select max(run_id) 
     from $self->{TABLE_PROGRAM_STATUS}
     where program_id = $self->{PROGRAM_ID}
      )
   );
    $self->{'dbh'}->prepare($query);
    my $ret = $self->{'dbh'}->selectall_arrayref($query);

    $self->{'log_obj'}->info("running query: $query");

    if ( $ret->[0][0] ) {
        return $ret->[0][0];
    }
    else {
        $self->{'log_obj'}->info("Last run result status is undef, possibly failed...");
        return undef;
    }
}

## input: run_id,start_date, status
## output: db_errors for update
sub update_status {
    my ( $self, $status, $run_id ) = @_;
    my $query = qq(
    update $self->{TABLE_PROGRAM_STATUS} 
    set status = ?, finish_date = sysdate
    where run_id = ?
    );
    my $sth = $self->{'dbh'}->prepare($query);
    my $value_ref;
    push( @$value_ref, $status );
    push( @$value_ref, $run_id );
    my $db_errors = $self->do_execute( $sth, $value_ref );

    if ($db_errors) {
        $self->{'log_obj'}->info("update status error:Dumper ($db_errors)");
    }
    else {
        $self->{'log_obj'}->info("update status success: $run_id\n");
    }
    return $db_errors;
}

## input: none or run_id, or start_date
## output: status for the run_id or start_date
sub get_status_from_run_id {
    my ( $self, $run_id ) = @_;
    if ($run_id) {
        my $query = qq(
     select status 
     from $self->{TABLE_PROGRAM_STATUS} 
     where run_id = $run_id
     );
        $self->{'dbh'}->prepare($query);
        my $ret = $self->{'dbh'}->selectall_arrayref($query);

        $self->{'log_obj'}->info("running query: $query");

        if ( $ret->[0][0] ) {
            return $ret->[0][0];
        }
        else {
            $self->{'log_obj'}->info("Failed in checking last run.");
            return undef;
        }
    }
    else {
        return undef;
    }
}

## input: 1 or 0
## process: lunch the program
## output: returned values for lunching the process
## or none

sub launch_edi_850 {
    my ( $self, $opt_str ) = @_;

    ## with option of re-run
    my $process = 'perl /usr/local/mccs/bin/edi_850_to_sps_by_views.pl ' . $opt_str;
    my $ret     = system($process);
    return $ret;
}

## this should be rewritten into one fuction:
## however, the first one has been in production and hard to change it
sub launch_edi_860 {
    my ( $self, $opt_str ) = @_;

    ## with option of re-run
    my $process = 'perl /usr/local/mccs/bin/edi_860_to_sps.pl ' . $opt_str;
    my $ret     = system($process);
    return $ret;
}

## A error was found lately, Dec 2012 when the create_date for the same 860 PO are different
## That caused errors like 'PO_TYPE' not found and 860 process died out.
## This is an funtion to fix the problem.
##
sub sync_new_860_create_date {
    my ($self)    = @_;
    my $success   = 1;
    my $query_sel = "
select key_data
from merch.edi_pending_transactions 
where  transaction_set ='860' 
and date_processed is null 
and ride_session_id is null
group by key_data
having (count (distinct (to_char(date_created, 'yyyymmddhh'))) > 1)
    ";

    my $query_upd = "
update merch.edi_pending_transactions 
set date_created = 
    (select min (date_created) 
	from 
	merch.edi_pending_transactions 
	where key_data = ?
	and date_processed is null 
	and ride_session_id is null 
	and transaction_set ='860'
	)
where key_data = ?
and date_processed is null 
and ride_session_id is null
and transaction_set ='860' 
";

    my $sth_sel = $self->{'dbh'}->prepare($query_sel);
    my $sth_upd = $self->{'dbh'}->prepare($query_upd);

    my $ret1    = $sth_sel->execute();
    my @row_ary = $sth_sel->fetchrow_array;
    ## print Dumper( \@row_ary );

    if ( @row_ary > 0 ) {
        for ( my $i = 0 ; $i < @row_ary ; $i++ ) {
            if ( $row_ary[$i] ) {
                my @two_values;
                push( @two_values, $row_ary[$i] );
                push( @two_values, $row_ary[$i] );
                eval { $sth_upd->execute(@two_values); };
                if ($@) {
                    $self->{'dbh'}->rollback;
                    if ($success) {
                        $success = 0;
                        $self->{'log_obj'}->info( "Failed to sync create_date for: Dumper(\@two_values)" );
                    }
                }
                else {
                    $self->{'dbh'}->commit;
                    $self->{'log_obj'}->info("Success sync create_date from: Dumper(\@two_values)");
                }
            }
        }
    }
    else {
        $success = 1;
    }
    return $success;
}

##===============subroutines for new POs over 1000s, added Jan 2013 ===================
## input: none
## output: number of 850 POs needed to be processed.

sub check_current_850_po_size {
    my ( $self, $date_created ) = @_;
    my $query2 = "
select 
distinct key_data, edi_sequence_id
from                                                                                                                      
$self->{'TABLE_EPT'}
where                                                                                       
business_unit_id = 30
and partnership_id != '00001707694850' 
and transaction_set = '850' 
and transaction_type ='OUT' 
and date_processed is null 
and ride_session_id is null 
";

## if with a date, use it, else use current days data...
    if ($date_created) {
        $query2 .= "
and date_created like \'%$date_created%\'";
    }
    else {
        $query2 .= "
and date_created >= sysdate - 2";
    }

    $self->{'log_obj'}->log_info("extract query: $query2");

    my $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "edi_sequence_id" );
    ## CASE MATTERS HERE\!!!
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "EDI_SEQUENCE_ID" );
    }
## some sorting:
    my ( $count, $seq_ref, $po_ref );
    my $p_ctr = 0;
    if ($value_ref) {
        foreach my $seq_id ( sort keys %{$value_ref} ) {
            $seq_ref->{$seq_id} = 1 unless $seq_ref->{$seq_id};
            my $po_id = $value_ref->{$seq_id}->{'key_data'};
            unless ( $po_ref->{$po_id} ) {
                $po_ref->{$po_id}->{$seq_id} = 1;
            }
            else {
                $po_ref->{$po_id}->{$seq_id} = 1;
            }
        }
        $count = keys %{$po_ref};
        return ( $count, $po_ref, $seq_ref );
    }
    else {
        return ( undef, undef, undef );
    }
    ##return $value_ref;
}

## input: list of PO+seq_ids
## output: hashref for the second half in the po_list:
sub divide_po_list {
    my ( $self, $po_ref ) = @_;
    my $count       = keys %{$po_ref};
    my $half_po_ctr = int( $count / 2 );
    my $half_po_ref;
    my $half_seqid_ref;
    my $ctr = 0;
    foreach my $po ( sort { $a <=> $b } keys %{$po_ref} ) {
        $ctr++;
        if ( $ctr > $half_po_ctr ) {
            $half_po_ref->{$po} = $po_ref->{$po};
            foreach my $seq_id ( keys %{ $half_po_ref->{$po} } ) {
                $half_seqid_ref->{$seq_id} = 1;
            }
        }
    }
    my $count2 = keys %{$half_po_ref};
    return ( $half_po_ref, $half_seqid_ref );
}

## input: hash of POs/seq_id
## output: db_error 1(error) 0 (no error),
## update the temp table for the POs for hold, with a date_created field
sub insert_850_reserve_table {
    my ( $self, $po_ref ) = @_;
    my $query = qq(insert into $self->{'TABLE_EDI_850_PO_RESERVED'} 
        (po_id, edi_sequence_id, date_created) 
         values 
        (?,?,sysdate));
    my $sth           = $self->{'dbh'}->prepare($query);
    my $db_error_flag = 0;

    eval {
        foreach my $po ( keys %$po_ref )
        {
            foreach my $seq_id ( keys %{ $po_ref->{$po} } ) {
                my $value_ref;
                if ( $po && $seq_id ) {
                    push( @$value_ref, $po );
                    push( @$value_ref, $seq_id );
                    $sth->execute(@$value_ref);
                }
            }
        }
    };
    if ($@) {
        $db_error_flag = 1;
        $self->{'log_obj'}->info($@);
        $self->{'dbh'}->rollback;
        return $db_error_flag;    ## exit when error happens...
    }
    else {
        $self->{'dbh'}->commit;
        return $db_error_flag;
    }
}

## update_EPT_table
## use: update_EPT_table(edi_seq_id);
sub mark_ept_table_with_reserved {
    my ($self)   = @_;
    my $db_error = 0;
    my $query    = " update 
         $self->{'TABLE_EPT'} 
         set date_processed = sysdate, 
         ride_session_id = '100'     
         where transaction_set = '850'
         and edi_sequence_id in (
         select edi_sequence_id 
         from $self->{'TABLE_EDI_850_PO_RESERVED'} 
         )";
    my $sth = $self->{'dbh'}->prepare($query);
    eval { $sth->execute; };

    if ($@) {
        $self->{'log_obj'}->info($@);
        $self->{'dbh'}->rollback;
        $db_error = 1;
    }
    else {
        $self->{'dbh'}->commit;
    }
    return $db_error;
}
## input: run_id for the process
## purpose: update the status of the rdi_status to 'P', meaning pending
## output:

sub refresh_ept_table_with_reserved {
    my ($self)   = @_;
    my $db_error = 0;
    my $query    = " update 
         $self->{'TABLE_EPT'} 
         set date_processed = null, 
          ride_session_id = null     
         where transaction_set = '850'
         and edi_sequence_id in (
         select edi_sequence_id 
         from $self->{'TABLE_EDI_850_PO_RESERVED'}
         )";
    my $sth = $self->{'dbh'}->prepare($query);
    eval { $sth->execute; };
    if ($@) {
        $self->{'log_obj'}->log_info($@);
        $self->{'dbh'}->rollback();
        $db_error = 1;
    }
    else {
        $self->{'dbh'}->commit();
        $self->{'log_obj'}->log_info("Refresh ept table for resending is ok.\n");
    }
    return $db_error;
}

## input: none
## output: 1, or zero for db operation of deletion only current days' data...
sub delete_po_in_reserve {
    my ($self)   = @_;
    my $db_error = 0;
    my $query    = "delete from $self->{'TABLE_EDI_850_PO_RESERVED'} where date_created >= trunc(sysdate)";
    my $sth      = $self->{'dbh'}->prepare($query);
    eval { $sth->execute; };
    if ($@) {
        $self->{'log_obj'}->log_info($@);
        $self->{'dbh'}->rollback();
        $db_error = 1;
    }
    else {
        $self->{'dbh'}->commit();
        $self->{'log_obj'}->log_info("delete po reserved table data successful\n");
    }
    return $db_error;
}

## input: none
## output: refresh and delete result status
sub refresh_and_delete {
    my ($self) = @_;
    my $refresh_success = 0;
### refresh and delete...
    my $refresh_error = $self->refresh_ept_table_with_reserved();
    if ($refresh_error) {
        my $msg = "DB errors in refreshing POs for sending...";
        $self->{'log_obj'}->info( $msg . $refresh_error );
    }
    else {
        my $msg = "Refresh POs for reseinding successful. Ready for the rest of rerun";
        $self->{'log_obj'}->info($msg);
        my $del_error = $self->delete_po_in_reserve();
        if ($del_error) {
            my $msg = "db failed to delete data in po_reserved table";
            $self->{'log_obj'}->info( $msg . $del_error );
        }
        else {
            my $msg = "Success in deleting po_reserved table temp data";
            $self->{'log_obj'}->info($msg);
            $refresh_success = 1;
        }

    }
    return $refresh_success;
}

## input: po->{edi_sequence_id}
## output: insert and mark result status: 1, success; 0 failure
sub insert_and_mark {
    my ( $self, $po_ref ) = @_;
    my $success = 1;

    my ( $second_half, $half_seqid_ref ) = $self->divide_po_list($po_ref);
    if ($second_half) {
        my $insert_error = $self->insert_850_reserve_table($second_half);
        if ($insert_error) {
            my $msg = "Failed in inserting data to 850 reserve table:";
            $self->{'log_obj'}->info( $msg . $insert_error );
            $success = 0;
        }
        else {
            #### mark the half in ept able
            my $update_error = $self->mark_ept_table_with_reserved();
            if ($update_error) {
                my $msg = "DB operation failed for mark_EPT_table_with_reserved";
                $self->{log_obj}->info( $msg . $update_error );
                $success = 0;
            }
            else {
                my $msg = "Some data marked, and will send only half of the POs";
                $self->{log_obj}->info($msg);
                ####will mark the status flag as splited PO list happend
                $success = 1;
            }
        }
    }
    else {
        my $msg = 'failed in calling function divide_po_list';
        $self->{'log_obj'}->info($msg);
        $success = 0;
    }
    return $success;
}

## in:none
##out: count of any row of data in the po_reserved table

sub count_850_po_in_reserved {
    my ($self) = @_;
    my $sqry = "select count(distinct po_id) from  $self->{'TABLE_EDI_850_PO_RESERVED'}";
    my $count;
    my $ret;
    eval { $ret = $self->{'dbh'}->selectall_arrayref($sqry); };

    if ($@) {
        my $msg = "select error:";
        $self->{'log_obj'}->info( $msg . $@ );
    }
    $count = $ret->[0][0];
    return $count;
}

#### New changes, April 2, 2014
### adding code to exclude wrong PO types (other than bulk, and dropship)...

sub extract_wrong_potype_of_850 {
    my ( $self, $date_created ) = @_;
    my $ret_ref;
    my $query2 =
"                                                                                                              
select             
distinct 
e.edi_sequence_id,                                                                                                              
e.key_data,                                                                                                                     
e.business_unit_id,                                                                                                             
p.po_type,                                                                                                                      
p.origin,

to_char(e.date_created, 'yyyymmdd') as date_created
                                                                                                                       
from                                                                                                                            
$self->{TABLE_EPT} e,                                                                                                           
$self->{TABLE_PO}  p

where                                                                                       
e.business_unit_id = 30
AND p.business_unit_id = e.business_unit_id
and e.partnership_id != '00001707694850' 
and e.transaction_set = '850'                                                                                                   
and e.transaction_type ='OUT'                                                                                                   
and e.date_processed is null                                                                                                    
and e.ride_session_id is null                                                                                                   
and p.po_type not in ('DROPSHIP','BULK')
and p.po_id=e.key_data                                                                                                          
";
    if ($date_created) {
        $query2 .= " and e.date_created like \'%$date_created%\'";

    }
    print $query2 if ( $self->{'debug'} );
    my $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "edi_sequence_id" );
    ## CASE MATTERS HERE\!!!
    if ( !defined($value_ref) ) {
        $value_ref = $self->{'dbh'}->selectall_hashref( $query2, "EDI_SEQUENCE_ID" );
    }

    if ( keys(%$value_ref) == 0 ) {
        $self->{'log_obj'}->log_info("extract query not working: $query2");
    }
    return $value_ref;
}

### add code to exlude POs with shippers
## no input
## output: list of POs or undef
sub send_email_for_po_wt_shipper{
    my ($self, $host_time) = @_;
## get a list of POs with shippers
    my $query =  
	"select  /*+ PARALLEL(E850_PO_WT_SHIPPER_V,8) */
                distinct PO_ID, VERSION_NO, style, po_type, CREATION_DATE, CREATED_BY
	 from 
                rdiusr.E850_PO_WT_SHIPPER_V
	";
    my $value_ref ;
    my $result_ret;
    eval{
	$value_ref = $self->{'dbh'}->selectall_arrayref($query);   
    };
    if ($@){	
	my $msg = "DB selection query errored: $@";
	$self->{'log_obj'}->info($msg);
	$result_ret = 0;
    }
    
    if(@$value_ref >0){
## send emails afterwards...
	my $th_str = 'PO_ID|VERSION|STYLE|PO_TYPE|CREATION_DATE|CREATED_BY|';
	my $subject = 'EDI 850 POs with Shippers';
	my $str_size = 20;
	my $mail_content; ## = "<p>:</p>";
	$mail_content   .=  &html_table_via_sth_or_aryref($value_ref,$th_str,$subject,$str_size);
	$mail_content   .= "<p>The above PO(s) have been created for SPS and contain Shippers. Please update these purchase orders accordingly ASAP. </p> <p> $host_time </p>";	
	my $to_list = $self->{POC4SHIPPER};
	&send_html_by_email($subject, $to_list, $mail_content);

        ## MARKOFF THESE POs
	$self->markoff_ept_po_wt_shippers();
    }
    return $result_ret;
}

sub markoff_ept_po_wt_shippers{
   my ($self) = @_;
## for each of the PO not in view E850_PO_WT_SHIPPER 
## update processed_data as 20991231, 
    eval{
	my $sth1 = $self->{'dbh'}->prepare('BEGIN rdiusr.E850_MARKOFF_PO_WT_SHIPPER(); END;');
	$sth1->execute || die $self->{'log_obj'}->info($self->{'dbh'}->errstr);
    };
    if($@){
	my $msg = "Database errors when EXEC: MARKOFF SHIPPER PROC. Process abort...";
	$self->{'log_obj'}->info($msg);
	die;
    }
}

## load address info for PP, PD types of POs into the header reference that has values already
## input: reference from header->pondate
## output: the same reference back

sub get_address_from_site_id{
    my ($self, $ret_ref) = @_;
   
    my $shipped_to_site_id = $ret_ref->{'shipped_to_site_id'};
    unless($shipped_to_site_id){ ## this is for cover the case of 860 wo shipped_to_site_id..
	$shipped_to_site_id = $ret_ref->{'addresslocationnumber'};
    } 
    unless($shipped_to_site_id){
	my $msg = "WARNING: shipped to site_id for: $shipped_to_site_id\n";
	$self->{'log_obj'}->info($msg);
	return $ret_ref;  ## do nothing... here..
    }

    my $query = " 
    select 
       SHIPPED_TO_SITE_ID,  
       ADDRESS1, 
       ADDRESS2, 
       ADDRESS3, 
       ADDRESS4, 
       CITY, 
       STATE, 
       POSTALCODE, 
       COUNTRY 
      from 
       E850_PRES_ADDRESSES
      where 
       shipped_to_site_id = \'$shipped_to_site_id\'";
    
    my $value_ref = $self->{'dbh'}->selectall_hashref( $query, "shipped_to_site_id" );

    foreach my $shipped_to_site_id (keys %{$value_ref} ) {
	foreach my $key(keys %{$value_ref->{$shipped_to_site_id}}) {
	    $ret_ref->{$key} = $value_ref->{$shipped_to_site_id}->{$key};
			}	
    }
    return $ret_ref;
}

## load address info for PP, PD types of POs into the header reference that has values already
## input: reference from header->pondate
## output: the same reference back

sub get_address_by_address_to_site{
    my ($self, $ret_ref) = @_;
   
    my $address_to_site_id = $ret_ref->{'address_to_site_id'};
    unless($address_to_site_id){
	my $msg = "WARNING: missing address_to_site_id for Dumper($ret_ref)";
	$self->{'log_obj'}->info($msg);
	return $ret_ref;  
    }

    my $query = " 
    select 
       distinct
       ADDRESS1, 
       ADDRESS2, 
       ADDRESS3, 
       ADDRESS4, 
       CITY, 
       STATE, 
       POSTALCODE, 
       COUNTRY 
      from 
       e850_address_from_AddrToId_v
      where 
       address_to_site_id = \'$address_to_site_id\'";
    
    my $value_ref = $self->{'dbh'}->selectall_hashref( $query, "address_to_site_id" );

    foreach my $address_to_site_id (keys %{$value_ref} ) {
	foreach my $key(keys %{$value_ref->{$address_to_site_id}}) {
	    $ret_ref->{$key} = $value_ref->{$address_to_site_id}->{$key};
			}	
    }
    return $ret_ref;
}

## this is a bkup before March 18's change for 860
sub v1_re_group_860_dtls_by_cost_rp {
    my ($self) = @_;
    my $regrouped_ref;

    my $struct2_ref;    ## this will hold the re-constructed data:
    my $po_type_ref;    ## this will hold the po's type value:
    my ( $dropship_data_ref, $blk_data_ref );
    my $price_zone_ref;    ## rezoned data for DS type
    my $rg_ll_ctr_ref = {};    ## line item counter after regrouping

    my $e850_ff_dtls_ref = $self->{'view_details_data_ref'};
    my $header_data_ref  = $self->{'view_header_data_ref'};

    if ( $self->{'debug'} ) {
        print "in re_group_860_by_cost_rp, header and details:\n";
        print Dumper($e850_ff_dtls_ref);
        print Dumper($header_data_ref);
    }

    ## 0,  rewrite BULK , Dropship POs to 'correct' string values:

    foreach my $pondate ( keys %$header_data_ref ) {
        foreach my $data_key ( keys %{ $header_data_ref->{$pondate} } ) {
            if ( lc($data_key) eq 'purchaseordertypecode' ) {
                if ( $header_data_ref->{$pondate}->{$data_key} eq 'DROPSHIP' ) {
                    ##!!! rewrite the value for po_type to required ones:
                    $header_data_ref->{$pondate}->{$data_key} = 'DS';
                }
                ##elsif ( $header_data_ref->{$pondate}->{$data_key} eq 'BULK' ) {
		 else{ ## all other types are SA from now on, March 11, 2015
                    ##!!! rewrite the value for po_type to required ones:
                    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
                }
            }
            else {
                ## remove front and end white space, remove invalid space
                ## (influence empty space)

                $header_data_ref->{$pondate}->{$data_key} =~ s/^\s+//;
                $header_data_ref->{$pondate}->{$data_key} =~ s/\s+$//;
            }
        }
    }

    ## 1,  separate BULK , Dropship POs, ####
    foreach my $edi_seq_id ( keys %$e850_ff_dtls_ref ) {
        my $pondate = $e850_ff_dtls_ref->{$edi_seq_id}->{'pondate'};
        if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'DS' ) {
            $dropship_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }
        elsif ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'SA' ) {
            $blk_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }
        else {
            $self->{'log_obj'}->info(
"unkonw PO_TYPE:$header_data_ref->{$pondate}->{'purchaseordertypecode'}  for pondate: $pondate\n"
            );
            die;
        }
    }

    if ( $self->{'debug'} ) {
        print "headerdata:\n";
        print Dumper($header_data_ref);
        print "details data:\n";
        print Dumper($e850_ff_dtls_ref);

        print "ds data:\n";
        print Dumper($dropship_data_ref);
        print "bulk data:\n";
        print Dumper($blk_data_ref);
    }

    ## 2, processing DS type data:
    foreach my $edi_seq_id ( keys %$dropship_data_ref ) {
        my $po           = $dropship_data_ref->{$edi_seq_id}->{'po_id'};
        my $pondate      = $dropship_data_ref->{$edi_seq_id}->{'pondate'};
        my $rms_style_id = $dropship_data_ref->{$edi_seq_id}->{'r_style_id'};
        my $site_qty_ref =
          $self->split_siteqty_2_hash_ref_2( $edi_seq_id, $dropship_data_ref );    ## most critical

        if ( $self->{'debug'} ) {
            print "site_qty_ref:\n";
            print Dumper($site_qty_ref);
        }

        ## loop site_qty_ref to get rp, cost, and hexkey:
        foreach my $site_id ( keys %$site_qty_ref ) {
            if ($site_id) {
                (
                    $site_qty_ref->{$site_id}->{'cost'},
                    $site_qty_ref->{$site_id}->{'retail_price'},
                    $site_qty_ref->{$site_id}->{'comb_key'}
                ) = $self->get_rp_cost_from_po_style_site( $po, $rms_style_id, $site_id );
                my $zone_key = $site_qty_ref->{$site_id}->{'comb_key'};
                if ($zone_key) {
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} .=
                      $site_qty_ref->{$site_id}->{'site'} . '_' . $site_qty_ref->{$site_id}->{'qty'} . '|';
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'} =
                      $site_qty_ref->{$site_id}->{'cost'};
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'} =
                      $site_qty_ref->{$site_id}->{'retail_price'};
                }
                else {
### this may not be the best solution, but one of the solutions for missing data
# $zone_key = "$dropship_data_ref->{$edi_seq_id}->{'unitprice'}"."$dropship_data_ref->{$edi_seq_id}->{'retailunitprice'}";
#$zone_key =~ s/\s+//g;
#print "ZZZZZZZZZZZZZZZZZZZZZZZZonekey: $zone_key\n" if $self->{'debug'};
#$price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'}
#= $dropship_data_ref->{$edi_seq_id}->{'unitprice'};
#$price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'}
#= $dropship_data_ref->{$edi_seq_id}->{'retailunitprice'};
#$price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'}
#.= $site_qty_ref->{$site_id}->{'site'}.'_'.$site_qty_ref->{$site_id}->{'qty'}.'|';

                    $self->{'log_obj'}->log_info(
                        "Missing cost,rp values: po: $po, edi_seq_id: $edi_seq_id, site: $site_id\n" );
                }
            }
        }
        $dropship_data_ref->{$edi_seq_id}->{'site_comb_ref'} = $site_qty_ref;
    }
## rewrite the structure based on new comb_key of seq_id, and zone_key

    foreach my $pondate ( keys %$price_zone_ref ) {
        $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} = 0;
        foreach my $edi_seq_id (
            sort { $a <=> $b }
            keys %{ $price_zone_ref->{$pondate} }
          )
        {
            foreach my $zone_key ( keys %{ $price_zone_ref->{$pondate}->{$edi_seq_id} } ) {
                my $comb_key = $edi_seq_id . "_" . $zone_key;
                $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} = $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} + 1;
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'unitprice'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'}
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'} );
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'retailunitprice'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'}
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'} );
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'site_qty_str'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'};
                foreach my $data_key ( keys %{ $dropship_data_ref->{$edi_seq_id} } ) {
                    if ( $data_key eq 'site_comb_ref' ) {
                        next;
                    }    ## jump over reference to avoid repeat the same section
                    if ( $data_key eq 'unitprice' ) { next; }

## For Mike G's request on May 3, 10, do not display retailunitprice in the case of PC or PQ for dropship
                    if ( $data_key eq 'retailunitprice' ) {
                        if (   ( $dropship_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PC' )
                            || ( $dropship_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PQ' ) )
                        {
                            $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{$data_key} = '';
                        }
                        next;
                    }
                    my $tmp_value = $dropship_data_ref->{$edi_seq_id}->{$data_key};
                    $tmp_value =~ s/^\s+//;    ## some format of data
                    $tmp_value =~ s/\s+$//;
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{$data_key} = $tmp_value;
                }

## asign deleted item's qtylefttoreceive as 0 according to sps requirement
                if ( $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'linechangecode'} eq 'DI' ) {
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'qtylefttoreceive'} =
                      0;                       ## asign deleted item's qtylefttoreceive as 0
                }
                else {

## reasign the qtyleft to receive according to the value in the 50 lines, instead of the values in the 40 lines view which parsed from the 40 line in ept table.
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'qtylefttoreceive'} =
                      $self->get_orderqty_in_each_zone(
                        $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} )
                      if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} );
                }
            }
        }
        ## rewrite the total number of line items for each pondate
        $header_data_ref->{$pondate}->{'totallineitemnumber'} = $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'};

    }

## modify the BULK PO without Address information to type 'DS'
## note: this value will not be refered for other purpose before writting to xml file
    foreach my $pondate ( keys %$header_data_ref ) {
        if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'SA' ) {
            if ( !( $header_data_ref->{$pondate}->{'address_to_site_id'} ) ) {
                $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'DS';
            }
            else {
                $header_data_ref->{$pondate}->{'addresstypecode'} =
                  'ST';    ## assign this value for sa with address info
            }
        }
    }

##3, Process BULK type data, put it on the same structure
    foreach my $edi_seq_id ( sort { $a <=> $b } keys %$blk_data_ref ) {
        my $pondate = $blk_data_ref->{$edi_seq_id}->{'pondate'};
        if (   $blk_data_ref->{$edi_seq_id}->{'po_id'}
            && $blk_data_ref->{$edi_seq_id}->{'r_style_id'}
            && $header_data_ref->{$pondate}->{'addresslocationnumber'} )
        {

            print
"1:$blk_data_ref->{$edi_seq_id}->{'po_id'} 2:$blk_data_ref->{$edi_seq_id}->{'r_style_id'} 3:$header_data_ref->{$pondate}->{'addresslocationnumber'}\n";

            (
                $blk_data_ref->{$edi_seq_id}->{'unitprice'},
                $blk_data_ref->{$edi_seq_id}->{'retailunitprice'},
                $blk_data_ref->{$edi_seq_id}->{'comb_key'}
              )
              = $self->get_rp_cost_from_po_style_site(
                $blk_data_ref->{$edi_seq_id}->{'po_id'},
                $blk_data_ref->{$edi_seq_id}->{'r_style_id'},
                $header_data_ref->{$pondate}->{'addresslocationnumber'}
              );

            ## Mike's request on May 3, 10

            if (   ( $blk_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PC' )
                || ( $blk_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PQ' ) )
            {
                $blk_data_ref->{$edi_seq_id}->{'retailunitprice'} = '';

            }

        }
        else {
            $self->{'log_obj'}->info(
                "missing data for getting price info:	
                po: $blk_data_ref->{$edi_seq_id}->{'po_id'}, 
		r_style_id: $blk_data_ref->{$edi_seq_id}->{'r_style_id'}, 
		addresslocationnumber: $header_data_ref->{$pondate}->{'addresslocationnumber'}"
            );
        }
    }

    if ( $self->{'debug'} ) {
        print "after price inforrrrrrr\n";
        print Dumper($blk_data_ref);
    }

    ## put it into the same structure:
    foreach my $edi_seq_id ( keys %$blk_data_ref ) {
        my $pondate = $blk_data_ref->{$edi_seq_id}->{'pondate'};
        foreach my $data_key ( keys %{ $blk_data_ref->{$edi_seq_id} } ) {
            my $tmp_value = $blk_data_ref->{$edi_seq_id}->{$data_key};
            $tmp_value =~ s/^\s+//;    ## some format of data
            $tmp_value =~ s/\s+$//;
            $struct2_ref->{$pondate}->{'line_items'}->{$edi_seq_id}->{$data_key} = $tmp_value;
        }
    }

    $self->{'re_grouped_860_data_ref'} = $struct2_ref;
    return $struct2_ref;
}

sub e850_email_yesterday_pos{
    my ($self, $host_time) = @_;
## get a list of POs with shippers
    my $query =  
	"select 
    distinct 
    p.po_id,  
    p.version_no, 
    p.po_type, 
    (
    select emp.first_name ||' '|| emp.last_name 
    from employees emp
    where emp.business_unit_id ='30'
     and emp.employee_id = p.buyer_employee_id
    AND ROWNUM < 2
     ) as buyer_name,
    p.fob_point, 
    p.CREATED_BY,
    p.REASON_ID,
    p.shipped_to_site_id
from 
      MERCH.PURCHASE_ORDERS p,
      rdiusr.EDI_850_STAGING s
where 
    s.po_id = p.po_id
and p.shipped_to_site_id in ('60001','70001')
and TRUNC(s.date_processed) = TRUNC(sysdate - 1)
and upper(p.fob_point) = 'DESTINATION'
";
    my $value_ref ;
    my $result_ret;
    eval{
	$value_ref = $self->{'dbh'}->selectall_arrayref($query);   
    };
    if ($@){	
	my $msg = "DB selection query errored: $@";
	$self->{'log_obj'}->info($msg);
	$result_ret = 0;
    }
    
    if(@$value_ref >0){
	
## send emails afterwards...
##
	my $th_str = 'PO|Verson|PO type|BuyerName|FOB|Created by|ReasonID|ShippedTo|';
	my $subject = ' The SPS POs listed belove are Prepaid type for RDC';
	my $str_size = 20;
	my $mail_content; 
	
	$mail_content   .=  &html_table_via_sth_or_aryref($value_ref,$th_str,$subject,$str_size);
	$mail_content   .= "<br><p></p> <p> $host_time </p>";
	my $to_list = $self->{'PO_SHIPPER_EMAILS'};
	&send_html_by_email($subject, $to_list, $mail_content);

    }    
}

sub e850_860_exclude_filtered_pos{
    my ($self, $host_time, $type) = @_;
## get a list of POs with shippers
    my $query;
    if($type eq '850'){
	 $query = " 
         select
          /*+ PARALLEL(E850_EXCLUSTION_FILTER_V,8) */
          distinct 
          po_id,
          version_no,  
          po_type,
          creation_date, 
          created_by,
          reason_id,
          buyer_name,
          shipped_to_site_id,
          reject_type
	from 
          RDIUSR.E850_EXCLUSTION_FILTER_V";
    }elsif($type eq '860'){
	$query = " 
         select
          /*+ PARALLEL(E860_EXCLUSTION_FILTER_V,8) */
          distinct 
          po_id,
          version_no,  
          po_type,
          creation_date, 
          created_by,
          reason_id,
          buyer_name,
          shipped_to_site_id,
          reject_type
	from 
          RDIUSR.E860_EXCLUSTION_FILTER_V";
    }
    my $value_ref ;
    my $result_ret;
    eval{
	$value_ref = $self->{'dbh'}->selectall_arrayref($query);   
    };
    if ($@){	
	my $msg = "DB selection query errored: $@";
	$self->{'log_obj'}->info($msg);
	$result_ret = 0;
    }
    
    if(@$value_ref >0){
	
## send emails afterwards...
	my $th_str = 'PO|Version|PO type|CreateDate|CreatedBy|ReasonID|BuyeeName|ShippedTo|RejectDesc|';
	my $subject = 'EDI '. $type. ' POs listed below have not been sent to RDC (except \'BULK wo Stow Style\') ';
	my $str_size = 20;
	my $mail_content; ## = "<p>:</p>";

	$mail_content   .=  &html_table_via_sth_or_aryref($value_ref,$th_str,$subject,$str_size);
	$mail_content   .= "<p></p> <p> Sent from host: $host_time </p>";
	my $to_list = $self->{'PO_SHIPPER_EMAILS'};
	&send_html_by_email($subject, $to_list, $mail_content);
## mark off POs that are in the filtered list...

        eval{
	    if($type eq '850'){
		my $sth1 = $self->{'dbh'}->prepare('BEGIN rdiusr.e850_update_filtered_po(); END;');
		$sth1->execute || die $self->{'log_obj'}->info($self->{'dbh'}->errstr);
	    }
	    if($type eq '860'){
		my $sth1 = $self->{'dbh'}->prepare('BEGIN rdiusr.e860_update_filtered_po(); END;');
		$sth1->execute || die $self->{'log_obj'}->info($self->{'dbh'}->errstr);
	    }
        };
        if($@){
	    my $msg = "Database errors when update ept: $type  aborted, aborted...";
	    $self->{'log_obj'}->info($msg);
	    ##die;
        }

    }else{
	my $msg = "No POs was with exclusion reasons.";
	$self->{'log_obj'}->info($msg);
    }
    
}

## new code on March 30, 2015 for processing new PO types, yuc
sub re_group_850_by_cost_rp_v3 {
    my ( $self, $e850_ff_dtls_ref, $header_data_ref ) = @_;
    my $regrouped_ref;

    my $struct2_ref;    ## this will hold the re-constructed data:
    my $po_type_ref;    ## this will hold the po's type value:
    my ( $dropship_data_ref, $blk_data_ref );
    my $price_zone_ref;    ## rezoned data for DS type
    my $rg_ll_ctr_ref = {};    ## line item counter after regrouping

    ## 0,  rewrite BULK , Dropship POs to 'correct' string values:

    foreach my $pondate ( keys %$header_data_ref ) {
        foreach my $data_key ( keys %{ $header_data_ref->{$pondate} } ) {
            if ( lc($data_key) eq 'purchaseordertypecode' ) {
if (( $header_data_ref->{$pondate}->{$data_key} =~ /dropship/gi )||($header_data_ref->{$pondate}->{$data_key} eq 'DROPSHIP')||(lc($header_data_ref->{$pondate}->{$data_key}) eq 'dropship')) {
                ## if ( $header_data_ref->{$pondate}->{$data_key} =~ /dropship/gi ) {
                    ##!!! rewrite the value for po_type to required ones:
                    $header_data_ref->{$pondate}->{$data_key} = 'DS';
                }
                else{
                    ### keep the PO type as is:
                    ## $header_data_ref->{$pondate}->{$data_key} as is
		    ## add a short name for other types:
		    if ($header_data_ref->{$pondate}->{$data_key} =~ /bulk/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'BK';
		    }elsif($header_data_ref->{$pondate}->{$data_key} =~ /packed/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'PP';
		    }elsif($header_data_ref->{$pondate}->{$data_key} =~ /distribute/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'}  = 'PD';
		    }else{
			my $msg = "Unknown type of PO for $pondate $header_data_ref->{$pondate}->{'purchaseordertypecode'}  \n";
			$header_data_ref->{$pondate}->{'purchaseordertypecode'}  = 'SA';
		    }
                }
            }
            else {
                ## remove front and end white space, remove invalid space
                ## (influence empty space)

                $header_data_ref->{$pondate}->{$data_key} =~ s/^\s+//;
                $header_data_ref->{$pondate}->{$data_key} =~ s/\s+$//;
            }
        }
    }

    ## 1,  put BULK, DS+PP into separate groups
    foreach my $edi_seq_id ( keys %$e850_ff_dtls_ref ) {
        my $pondate = $e850_ff_dtls_ref->{$edi_seq_id}->{'pondate'};
        if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'DS' ) {
            $dropship_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }elsif ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'BK' ) {
            $blk_data_ref->{$edi_seq_id}      = $e850_ff_dtls_ref->{$edi_seq_id};
	}elsif ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'PP' ) {
            $dropship_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }else { ## all other types, PD,  will be treated as SA types
	    $blk_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id}; ## PD, or blanket etc...
        }	
    }

    if ( $self->{'debug'} ) {
        print "headerdata:\n";
        print Dumper($header_data_ref);
        print "details data:\n";
        print Dumper($e850_ff_dtls_ref);

        print "ds data:\n";
        print Dumper($dropship_data_ref);
        print "bulk data:\n";
        print Dumper($blk_data_ref);
    }

    ## 2, processing DS, PP type data:
    foreach my $edi_seq_id ( keys %$dropship_data_ref ) {
        my $po           = $dropship_data_ref->{$edi_seq_id}->{'po_id'};
        my $pondate      = $dropship_data_ref->{$edi_seq_id}->{'pondate'};
        my $rms_style_id = $dropship_data_ref->{$edi_seq_id}->{'r_style_id'};
        my $site_qty_ref = $self->split_siteqty_2_hash_ref_2( $edi_seq_id, $dropship_data_ref );

        if ( $self->{'debug'} ) {
            print "site_qty_ref:\n";
            print Dumper($site_qty_ref);
        }

        ## loop site_qty_ref to get rp, cost, and hexkey:
        foreach my $site_id ( keys %$site_qty_ref ) {
            if ($site_id) {
                (
                    $site_qty_ref->{$site_id}->{'cost'},
                    $site_qty_ref->{$site_id}->{'retail_price'},
                    $site_qty_ref->{$site_id}->{'comb_key'}
                ) = $self->get_rp_cost_from_po_style_site( $po, $rms_style_id, $site_id );
                my $zone_key = $site_qty_ref->{$site_id}->{'comb_key'};
                if ($zone_key) {
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} .=
                      $site_qty_ref->{$site_id}->{'site'} . '_' . $site_qty_ref->{$site_id}->{'qty'} . '|';
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'} =
                      $site_qty_ref->{$site_id}->{'cost'};
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'} =
                      $site_qty_ref->{$site_id}->{'retail_price'};
                }
            }
        }
        $dropship_data_ref->{$edi_seq_id}->{'site_comb_ref'} = $site_qty_ref;
    }
## rewrite the structure based on new comb_key of seq_id, and zone_key

    foreach my $pondate ( keys %$price_zone_ref ) {
        foreach my $edi_seq_id (
            sort { $a <=> $b }
            keys %{ $price_zone_ref->{$pondate} }
          )
        {
            foreach my $zone_key ( keys %{ $price_zone_ref->{$pondate}->{$edi_seq_id} } ) {
                my $comb_key = $edi_seq_id . "_" . $zone_key;
                $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} = $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} + 1;

                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'unitprice'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'}
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'} =~ /\d+/g );
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'retailunitprice'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'}
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'} =~ /\d+/g );
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'site_qty_str'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'};
                foreach my $data_key ( keys %{ $dropship_data_ref->{$edi_seq_id} } ) {
                    if ( $data_key eq 'site_comb_ref' ) {
                        next;
                    }    ## jump over reference to avoid repeat the same section
                    ## to avoid overwrite:
                    if (   ( $data_key eq 'unitprice' )
                        or ( $data_key eq 'retailunitprice' ) )
                    {
                        next;
                    }

                    my $tmp_value = $dropship_data_ref->{$edi_seq_id}->{$data_key};
                    $tmp_value =~ s/^\s+//;    ## some format of data
                    $tmp_value =~ s/\s+$//;
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{$data_key} = $tmp_value;
                }
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'orderqty'} =
                  $self->get_orderqty_in_each_zone(
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} )
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} );
            }
        }
        ## rewrite the total number of line items for each pondate
        $header_data_ref->{$pondate}->{'totallineitemnumber'} = $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'};

    }

## list each PO types, treat differently according to address_to_site_id value 
    foreach my $pondate ( keys %$header_data_ref ) {
	if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'BK') { ## BULK
            if ( $header_data_ref->{$pondate}->{'address_to_site_id'} ) { ## ## with address_to_site_id 
		$header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';  ## this case suppose to have address info
		$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
	    }else{
                $header_data_ref->{$pondate} =
                    $self->get_address_from_site_id($header_data_ref->{$pondate});
		$header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';
		$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
		$header_data_ref->{$pondate}->{'addresslocationnumber'} 
		=  $header_data_ref->{$pondate}->{'shipped_to_site_id'};
	    }
	}elsif($header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'PP'){	    
	    if ( $header_data_ref->{$pondate}->{'address_to_site_id'} ) { ## ## with overwrite address 
		$header_data_ref->{$pondate}->{'addresstypecode'} = 'ST'; ## this case will have address information
		$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
	    }else{ ## NO ADDRESS_TO_SITE_ID VALUE:
		$header_data_ref->{$pondate} =
		    $self->get_address_from_site_id($header_data_ref->{$pondate});
		unless ($header_data_ref->{$pondate}->{'address1'}){
		    my $msg = "FFFFFFAILED IN gettting address information with $header_data_ref->{$pondate}->{'shipped_to_sit_id'})\n";
		    $self->{'log_obj'}->info($msg);
		}
		
		$header_data_ref->{$pondate}->{'addresstypecode'} = 'ST'; 
		$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
		$header_data_ref->{$pondate}->{'addresslocationnumber'} 
		=  $header_data_ref->{$pondate}->{'shipped_to_site_id'}; ## shipped_to_site_id as address location
	    }
	}elsif($header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'DS'){
	    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
	}else{ ## this is saved for Pre Distributed needs
	    $header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';  
	    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';   
	}
    }
    
##3, Process BULK type data, put it on the same structure
    foreach my $edi_seq_id ( sort { $a <=> $b } keys %$blk_data_ref ) {
        my $pondate = $blk_data_ref->{$edi_seq_id}->{'pondate'};
        foreach my $data_key ( keys %{ $blk_data_ref->{$edi_seq_id} } ) {
            my $tmp_value = $blk_data_ref->{$edi_seq_id}->{$data_key};
            $tmp_value =~ s/^\s+//;    ## some format of data
            $tmp_value =~ s/\s+$//;
            $struct2_ref->{$pondate}->{'line_items'}->{$edi_seq_id}->{$data_key} = $tmp_value;
        }
    }
    ##print Dumper($struct2_ref);
    return $struct2_ref;
}

## this is the code changed to reflect the new request in March, 2015 for processing new PO types, April 1, 2015, yuc
## please note the code is mostly like 850, so code contains variables of e850_ff_dtles_ref for easy to change 
sub re_group_860_dtls_by_cost_rp_v3 {
    my ($self) = @_;
    my $regrouped_ref;

    my $struct2_ref;    ## this will hold the re-constructed data:
    my $po_type_ref;    ## this will hold the po's type value:
    my ( $dropship_data_ref, $blk_data_ref );
    my $price_zone_ref;    ## rezoned data for DS type
    my $rg_ll_ctr_ref = {};    ## line item counter after regrouping

    my $e850_ff_dtls_ref = $self->{'view_details_data_ref'};
    my $header_data_ref  = $self->{'view_header_data_ref'};

    if ( $self->{'debug'} ) {
        print "in re_group_860_by_cost_rp, header and details:\n";
        print Dumper($e850_ff_dtls_ref);
        print Dumper($header_data_ref);
    }

    ## 0,  rewrite BULK , Dropship POs to 'correct' string values:

    foreach my $pondate ( keys %$header_data_ref ) {
        foreach my $data_key ( keys %{ $header_data_ref->{$pondate} } ) {
            if ( lc($data_key) eq 'purchaseordertypecode' ) {
                 ## same as 850 on this part
		## if ( $header_data_ref->{$pondate}->{$data_key} =~ /dropship/gi ) { ## weired problem found in July 27, 15 yuc
 if (( $header_data_ref->{$pondate}->{$data_key} =~ /dropship/gi )||($header_data_ref->{$pondate}->{$data_key} eq 'DROPSHIP')||(lc($header_data_ref->{$pondate}->{$data_key}) eq 'dropship')) {
                    $header_data_ref->{$pondate}->{$data_key} = 'DS';
                }
                else{
                    ### keep the PO type as is:
                    ## $header_data_ref->{$pondate}->{$data_key} as is
		    ## add a short name for other types:
		    if ($header_data_ref->{$pondate}->{$data_key} eq 'BULK' ){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'BK';
		    }elsif($header_data_ref->{$pondate}->{$data_key} =~ /packed/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'PP';
		    }elsif($header_data_ref->{$pondate}->{$data_key} =~ /distribute/gi){
			$header_data_ref->{$pondate}->{'purchaseordertypecode'}  = 'PD';
		    }else{
			my $msg = "Unknown type of PO for $pondate $header_data_ref->{$pondate}->{'purchaseordertypecode'}  \n";
			$header_data_ref->{$pondate}->{'purchaseordertypecode'}  = 'SA';
		    }
                }
            }
            else {
                ## remove front and end white space, remove invalid space
                ## (influence empty space)

                $header_data_ref->{$pondate}->{$data_key} =~ s/^\s+//;
                $header_data_ref->{$pondate}->{$data_key} =~ s/\s+$//;
            }
        }
    }
    ## 1,  separate BULK , Dropship + Prepacked into separate piles
    foreach my $edi_seq_id ( keys %$e850_ff_dtls_ref ) {
        my $pondate = $e850_ff_dtls_ref->{$edi_seq_id}->{'pondate'};
	if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'DS' ) {
            $dropship_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }elsif ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'BK' ) {
            $blk_data_ref->{$edi_seq_id}      = $e850_ff_dtls_ref->{$edi_seq_id};
	}elsif ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'PP' ) {
            $dropship_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id};
        }else { ## all other types, PD,  will be treated as SA types
	    $blk_data_ref->{$edi_seq_id} = $e850_ff_dtls_ref->{$edi_seq_id}; ## PD, or blanket etc...
        }	
    }

    if ( $self->{'debug'} ) {
        print "headerdata:\n";
        print Dumper($header_data_ref);
        print "details data:\n";
        print Dumper($e850_ff_dtls_ref);

        print "ds data:\n";
        print Dumper($dropship_data_ref);
        print "bulk data:\n";
        print Dumper($blk_data_ref);
    }

    ## 2, processing DS type data:
    foreach my $edi_seq_id ( keys %$dropship_data_ref ) {
        my $po           = $dropship_data_ref->{$edi_seq_id}->{'po_id'};
        my $pondate      = $dropship_data_ref->{$edi_seq_id}->{'pondate'};
        my $rms_style_id = $dropship_data_ref->{$edi_seq_id}->{'r_style_id'};
        my $site_qty_ref =
          $self->split_siteqty_2_hash_ref_2( $edi_seq_id, $dropship_data_ref );    ## most critical

        if ( $self->{'debug'} ) {
            print "site_qty_ref:\n";
            print Dumper($site_qty_ref);
        }

        ## loop site_qty_ref to get rp, cost, and hexkey:
        foreach my $site_id ( keys %$site_qty_ref ) {
            if ($site_id) {
                (
                    $site_qty_ref->{$site_id}->{'cost'},
                    $site_qty_ref->{$site_id}->{'retail_price'},
                    $site_qty_ref->{$site_id}->{'comb_key'}
                ) = $self->get_rp_cost_from_po_style_site( $po, $rms_style_id, $site_id );
                my $zone_key = $site_qty_ref->{$site_id}->{'comb_key'};
                if ($zone_key) {
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} .=
                      $site_qty_ref->{$site_id}->{'site'} . '_' . $site_qty_ref->{$site_id}->{'qty'} . '|';
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'} =
                      $site_qty_ref->{$site_id}->{'cost'};
                    $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'} =
                      $site_qty_ref->{$site_id}->{'retail_price'};
                }
                else {
                    $self->{'log_obj'}->log_info(
                        "Missing cost,rp values: po: $po, edi_seq_id: $edi_seq_id, site: $site_id\n" );
                }
            }
        }
        $dropship_data_ref->{$edi_seq_id}->{'site_comb_ref'} = $site_qty_ref;
    }
## rewrite the structure based on new comb_key of seq_id, and zone_key

    foreach my $pondate ( keys %$price_zone_ref ) {
        $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} = 0;
        foreach my $edi_seq_id (
            sort { $a <=> $b }
            keys %{ $price_zone_ref->{$pondate} }
          )
        {
            foreach my $zone_key ( keys %{ $price_zone_ref->{$pondate}->{$edi_seq_id} } ) {
                my $comb_key = $edi_seq_id . "_" . $zone_key;
                $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} = $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'} + 1;
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'unitprice'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'}
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'cost'} );
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'retailunitprice'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'}
                  if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'retail_price'} );
                $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'site_qty_str'} =
                  $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'};
                foreach my $data_key ( keys %{ $dropship_data_ref->{$edi_seq_id} } ) {
                    if ( $data_key eq 'site_comb_ref' ) {
                        next;
                    }    ## jump over reference to avoid repeat the same section
                    if ( $data_key eq 'unitprice' ) { next; }

## For Mike G's request on May 3, 10, do not display retailunitprice in the case of PC or PQ for dropship
                    if ( $data_key eq 'retailunitprice' ) {
                        if (   ( $dropship_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PC' )
                            || ( $dropship_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PQ' ) )
                        {
                            $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{$data_key} = '';
                        }
                        next;
                    }
                    my $tmp_value = $dropship_data_ref->{$edi_seq_id}->{$data_key};
                    $tmp_value =~ s/^\s+//;    ## some format of data
                    $tmp_value =~ s/\s+$//;
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{$data_key} = $tmp_value;
                }

## asign deleted item's qtylefttoreceive as 0 according to sps requirement
                if ( $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'linechangecode'} eq 'DI' ) {
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'qtylefttoreceive'} =
                      0;                       ## asign deleted item's qtylefttoreceive as 0
                }
                else {

## reasign the qtyleft to receive according to the value in the 50 lines, instead of the values in the 40 lines view which parsed from the 40 line in ept table.
                    $struct2_ref->{$pondate}->{'line_items'}->{$comb_key}->{'qtylefttoreceive'} =
                      $self->get_orderqty_in_each_zone(
                        $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} )
                      if ( $price_zone_ref->{$pondate}->{$edi_seq_id}->{$zone_key}->{'site_qty_str'} );
                }
            }
        }
        ## rewrite the total number of line items for each pondate
        $header_data_ref->{$pondate}->{'totallineitemnumber'} = $rg_ll_ctr_ref->{$pondate}->{'ll_ctr'};

    }

## this part follows new scheme for PO type processings
## for this 860 part, no need to have any street address information, yuc April 10, 15
    foreach my $pondate ( keys %$header_data_ref ) {
	if ( $header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'BK') { ## BULK
            if ( $header_data_ref->{$pondate}->{'address_to_site_id'} ) { ## ## with address_to_site_id 
                ## no street address for bulk with address_to_site_id, on April 10, 15, yuc
		$header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';  
		$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
	    }else{
		##$header_data_ref->{$pondate} =
	        ##        $self->get_address_from_site_id($header_data_ref->{$pondate});
		$header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';
		$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
	    }
	}elsif($header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'PP'){	    
	    if ( $header_data_ref->{$pondate}->{'address_to_site_id'} ) { ## ## with overwrite address 
		$header_data_ref->{$pondate}->{'addresstypecode'} = 'ST'; ## this case will have address information
		$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
	    }else{ ## NO ADDRESS_TO_SITE_ID VALUE:
		##$header_data_ref->{$pondate} =
	        ##        $self->get_address_from_site_id($header_data_ref->{$pondate});
		##unless ($header_data_ref->{$pondate}->{'address1'}){
		##    my $msg = "FFFFFFAILED IN gettting address information with $header_data_ref->{$pondate}->{'shipped_to_sit_id'})\n";
		##    $self->{'log_obj'}->info($msg);
		##}
		
		$header_data_ref->{$pondate}->{'addresstypecode'} = 'ST'; ## this will have address information
		$header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';

	    }
	}elsif($header_data_ref->{$pondate}->{'purchaseordertypecode'} eq 'DS'){
	    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';
	}else{ ## this is saved for Pre Distributed needs
	    $header_data_ref->{$pondate}->{'addresstypecode'} = 'ST';  
	    $header_data_ref->{$pondate}->{'purchaseordertypecode'} = 'SA';   
	}
    }

##3, Process BULK type data, put it on the same structure
    foreach my $edi_seq_id ( sort { $a <=> $b } keys %$blk_data_ref ) {
        my $pondate = $blk_data_ref->{$edi_seq_id}->{'pondate'};
        if (   $blk_data_ref->{$edi_seq_id}->{'po_id'}
            && $blk_data_ref->{$edi_seq_id}->{'r_style_id'}
            && $header_data_ref->{$pondate}->{'addresslocationnumber'} )
        {

            print
"1:$blk_data_ref->{$edi_seq_id}->{'po_id'} 2:$blk_data_ref->{$edi_seq_id}->{'r_style_id'} 3:$header_data_ref->{$pondate}->{'addresslocationnumber'}\n";

            (
                $blk_data_ref->{$edi_seq_id}->{'unitprice'},
                $blk_data_ref->{$edi_seq_id}->{'retailunitprice'},
                $blk_data_ref->{$edi_seq_id}->{'comb_key'}
              )
              = $self->get_rp_cost_from_po_style_site(
                $blk_data_ref->{$edi_seq_id}->{'po_id'},
                $blk_data_ref->{$edi_seq_id}->{'r_style_id'},
                $header_data_ref->{$pondate}->{'addresslocationnumber'}
              );

            ## Mike's request on May 3, 10

            if (   ( $blk_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PC' )
                || ( $blk_data_ref->{$edi_seq_id}->{'linechangecode'} eq 'PQ' ) )
            {
                $blk_data_ref->{$edi_seq_id}->{'retailunitprice'} = '';

            }

        }
        else {
            $self->{'log_obj'}->info(
                "missing data for getting price info:	
                po: $blk_data_ref->{$edi_seq_id}->{'po_id'}, 
		r_style_id: $blk_data_ref->{$edi_seq_id}->{'r_style_id'}, 
		addresslocationnumber: $header_data_ref->{$pondate}->{'addresslocationnumber'}"
            );
        }
    }

    if ( $self->{'debug'} ) {
        print "after price inforrrrrrr\n";
        print Dumper($blk_data_ref);
    }

    ## put it into the same structure:
    foreach my $edi_seq_id ( keys %$blk_data_ref ) {
        my $pondate = $blk_data_ref->{$edi_seq_id}->{'pondate'};
        foreach my $data_key ( keys %{ $blk_data_ref->{$edi_seq_id} } ) {
            my $tmp_value = $blk_data_ref->{$edi_seq_id}->{$data_key};
            $tmp_value =~ s/^\s+//;    ## some format of data
            $tmp_value =~ s/\s+$//;
            $struct2_ref->{$pondate}->{'line_items'}->{$edi_seq_id}->{$data_key} = $tmp_value;
        }
    }

    $self->{'re_grouped_860_data_ref'} = $struct2_ref;
    return $struct2_ref;
}

## Changed by removing the PO type restrictions by request of Karen. March 22, 17. yuc

sub evaluate_edi_pending_transactions_nopotype {
    my ( $self, $value_ref ) = @_;
    my ( $log_buf, $group_ref, $valid_transaction_ref );
    my @valid_key_data;
    my @kd_with_invalid_upcs;
    my $marker1        = '10';
    my $marker2        = '90';
    my $marker3        = '50';
    my $marker30       = '30';
    my $site_id_ref;                    # store infor about site_id, nex_richter_site, nex_whse etc...
    my $td_se_ref;                      # store infor about colume 17,18 values
    my $items_wrong_potype;
    my $items_missing_upc;
    my $items_wrong_origin;

## re-group data by key_data
    foreach my $edi_sequence_id ( sort { $a <=> $b } keys %$value_ref ) {
        my $key_data = $value_ref->{$edi_sequence_id}->{key_data};
        if ( $key_data ne "" ) {
            $group_ref->{$key_data}->{$edi_sequence_id}->{transaction_data} =
		$value_ref->{$edi_sequence_id}->{transaction_data};
            $group_ref->{$key_data}->{$edi_sequence_id}->{po_type} =
		$value_ref->{$edi_sequence_id}->{po_type};
            $group_ref->{$key_data}->{$edi_sequence_id}->{business_unit_id} =
		$value_ref->{$edi_sequence_id}->{business_unit_id};
            $group_ref->{$key_data}->{$edi_sequence_id}->{origin} = 
		$value_ref->{$edi_sequence_id}->{origin};
        }
    }

## processing data markers for valid transactions
    foreach my $kd ( keys %$group_ref ) {
        my ( $flag1, $flag2, $upc_missing ) = 0;    ##flag1 for 10, flag2 for 90
        foreach my $edi_seq_id ( sort { $a <=> $b } keys %{ $group_ref->{$kd} } ) {
            my $transaction_data = $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
            ## get the line type data first:
            ## remove possible front spaces:???
            $transaction_data =~ s/^\s+//;
            my @ary = split( //, $transaction_data );
            ## 1, find out if the transaction contain 10 and 90:
            my $seq1 = "$ary[16]" . "$ary[17]";
            unless ( $seq1 =~ /[1-9]0/ ) {
                $self->{'log_obj'}->log_info("Wrong to extract 10 to 90. what is: $seq1 <-\n");
            }

            if ( $seq1 eq $marker1 ) {
                $flag1 = 1;
            }
            if ( $seq1 eq $marker2 ) {
                $flag2 = 1;
            }
            ## record site_id which only exists in 17-18 as 50 type, for dropship, prepacked POs
            if ( $seq1 eq $marker3 ) {
                if ( $site_id_ref->{$kd} eq '' ) {
                    $site_id_ref->{$kd}->{'site_id'} =
                        $ary[22]
                      . $ary[23]
                      . $ary[24]
                      . $ary[25]
                      . $ary[26];    ## collect site_id according to data specifications
                    $self->{'log_obj'}->log_info("site_id:   $site_id_ref->{$kd}->{'site_id'}\n");
                    ( $site_id_ref->{$kd}->{'nex_richter_site'}, $site_id_ref->{$kd}->{'nex_whse'} ) =
                      $self->get_store_info( $self->{'dbh'}, $self->{'log_obj'},
                        $site_id_ref->{$kd}->{'site_id'} );
                }
            }
            $td_se_ref->{$kd}->{$edi_seq_id} = $seq1;

            ## 2, for 17-18's value 40, check if there is a valid UPC, mark invalid upc, index pos 49-61, for it
            ## length is 13!!!!!!!!
            if ( ( $seq1 eq "40" ) && ( $upc_missing == 0 ) ) {
                my $upc = '';
                for ( my $j = 48 ; $j < 61 ; $j++ ) {
                    $upc .= $ary[$j];
                }
                ## 3, valid upc criterias: length<3, with letters inside, and no-exist in bar_codes tables
                my $upc_checker = $self->check_valid_upc( $self->{'dbh'}, $self->{'log_obj'}, $upc );
                if ( $upc_checker == 0 ) {
                    $group_ref->{$kd}->{$edi_seq_id}->{'upc_missing'} = 1;
                    $upc_missing = 1;
                }
            }
	    ## 3, for bulk type PO, get the site
	    if ( $seq1 eq $marker30 ){
		if ( $site_id_ref->{$kd} eq '' ) {
		    if( $group_ref->{$kd}->{$edi_seq_id}->{po_type} eq 'BULK'){
                    $site_id_ref->{$kd}->{'site_id'} =
                        $ary[83]
			. $ary[84]
			. $ary[85]
			. $ary[86]
			. $ary[87];   
                    $self->{'log_obj'}->log_info("site_id:   $site_id_ref->{$kd}->{'site_id'}\n");
                        ( $site_id_ref->{$kd}->{'nex_richter_site'}, $site_id_ref->{$kd}->{'nex_whse'} ) =
		    	$self->get_store_info( $self->{'dbh'}, $self->{'log_obj'},
                        $site_id_ref->{$kd}->{'site_id'} );
		         unless($site_id_ref->{$kd}->{'nex_richter_site'} || $site_id_ref->{$kd}->{'nex_whse'} ){
			      $self->{'log_obj'}->log_info("Missing nex_store data -"."nex_richter_site:"
						     . $site_id_ref->{$kd}->{'nex_richter_site'}
						     . "nex_whse:".   $site_id_ref->{$kd}->{'nex_whse'});
		         }
		    
		    } ## bulk
                } ## eq ''
            } ## markder30
        } ## loop edi_seq_id

        ## important to book-keeping valid data, and pos with invalid upcs:
        if ( ( $flag1 == 1 ) && ( $flag2 == 1 ) ) {
            if ( $upc_missing == 1 ) {
                push( @kd_with_invalid_upcs, $kd );    ### jump over missing upc???
                $self->{'log_obj'}->log_info(
                    "\nkey_data|po_id: $kd have invalid UPCs with one or more of its edi_seq_ids.\n" );
            }
            else {
                push( @valid_key_data, $kd );
                $self->{'log_obj'}
                  ->log_info( "\nkey_data|po_id: $kd have both 10, and 90 in the transaction data list\n" );
            }
        }
        else {
            $self->{'log_obj'}
              ->log_info( "\n $kd DO NOT HAVE BOTH 10, AND 90 IN THE TRANSACTION_DATA LIST.\n" );
            if ( ( $flag1 == 1 ) && ( $flag2 == 0 ) ) {
                $self->{'log_obj'}->log_info("-- $kd miss 90.\n");
            }
            elsif ( ( $flag1 == 0 ) && ( $flag2 == 1 ) ) {
                $self->{'log_obj'}->log_info("-- $kd miss 10.\n");
            }
            elsif ( ( $flag1 == 0 ) && ( $flag2 == 0 ) ) {
                $self->{'log_obj'}->log_info("-- $kd miss both 10 and 90.\n");
            }
        }
    }
    ## Only select data with both markers, 10 and 90. Then group the data into missing_upc, and wrong_po_types, and valid_data
    foreach my $kd (@valid_key_data) {
        foreach my $edi_seq_id ( sort { $a <=> $b } keys %{ $group_ref->{$kd} } ) {
	    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{transaction_data} =
		$group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
	    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{business_unit_id} =
		$group_ref->{$kd}->{$edi_seq_id}->{business_unit_id};
	    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{site_id} =
		$site_id_ref->{$kd}->{'site_id'};
	    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{td_se} = $td_se_ref->{$kd}->{$edi_seq_id};
	    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{nex_richter_site} =
		$site_id_ref->{$kd}->{nex_richter_site};
	    $valid_transaction_ref->{$kd}->{$edi_seq_id}->{nex_whse} =
		$site_id_ref->{$kd}->{nex_whse};
        }
    }
    ## screen out the items with missiong upc
    foreach my $kd ( keys %{$valid_transaction_ref} ) {
        foreach my $edi_seq_id (
            sort { $a <=> $b }
            keys %{ $valid_transaction_ref->{$kd} }
          )
        {
            if ( $group_ref->{$kd}->{$edi_seq_id}->{'upc_missing'} == 1 ) {
                $valid_transaction_ref->{$kd}->{$edi_seq_id} = undef;
                $items_missing_upc->{$kd}->{$edi_seq_id}->{'po_type'} =
                  $group_ref->{$kd}->{$edi_seq_id}->{'po_type'};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{transaction_data} =
                  $group_ref->{$kd}->{$edi_seq_id}->{transaction_data};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{business_unit_id} =
                  $group_ref->{$kd}->{$edi_seq_id}->{business_unit_id};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{site_id} = $site_id_ref->{$kd}->{'site_id'};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{td_se}   = $td_se_ref->{$kd}->{$edi_seq_id};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{nex_richter_site} =
                  $site_id_ref->{$kd}->{nex_richter_site};
                $items_missing_upc->{$kd}->{$edi_seq_id}->{nex_whse} = $site_id_ref->{$kd}->{nex_whse};
            }
        }
    }
    return ( $valid_transaction_ref, $items_wrong_potype, $items_missing_upc, $items_wrong_origin,
        $group_ref );
}

#### end of the package
1;

__END__
    
=pod                                                                                                                        
    
=head1 NAME

EDI.pm

Extract data from edi tables, evaluate the data for completeness, then write transaction data 
to flatfiles for sending to NEX RMS purchase orders.    
  
=head1  SYNOPSIS


=over 4  

           
use IBIS::EDI; 

$edi = IBIS::EDI->new(conf_file=>'/full/path/to/config_file');

=back

see CONFIGURATION section for the config_file content. All items are required in the config file.
        
=head1 CONFIGURATION      

=over 4                                                                                                                                                                                                                                       
=item  Here is the list of configuration details for production:


=item  WORK_DIR = /usr/local/mccs/bin  


=item  FTP_STAGING_DIR=/home/ftp/pub/rms_nex 


=item  REMOTE_DIR=/retek_nfs/rams_to_retek_pre2


=item  REMOTE_DIR_PROD=/retek_nfs/rams_to_retek


=item  NEX_VENDOR_ID=00001707694

                                                                                                                   
=item  LOG_DIR=/home/rdiusr/edi_log/


=item  MAIL_TO=esg@usmc-mccs.org


=item  MAIL_CC=yuc@usmc-mccs.org  


=item  MAIL_FROM =rdiusr@usmc-mccs.org 


=item  FTP_CONNECT=nexcom_production


=item  SFTP_USER=rdiusr                                                                                                                       
      
=item  CONNECT_INSTANCE=rms_p    


=item  REMOTE_SERVER=164.226.186.74 ## testing server for sftp


=item  PASSWORD=XXXXXXXXXXXXXXXXXXXX= ## an encrypted string                                                                                             

=item  POC=yuc@usmc-mccs.org  


=item  TABLE_EPT=edi_pending_transactions


=item  TABLE_NPS=nex_po_staging  


=item  TABLE_PO=purchase_orders 


=back 


=head1 VERSION 


This documentation refers to EDI.pm version 0.0.1.    


=head1 REQUIRED ARGUMENTS  


Full path to the configuration file as described above. 


=head1 DESCRIPTION 


EDI.pm is as part of the program  of edi_rms2nexrms.pl, which is a data extraction, and analysis program for automatic purchase order creation and submission. Each night, new purchase order data is extracted from edi_pendign_transactions, purchase_orders and nex_po_staging tables. These data will be analysed to see if they are qualified for PO submissions. The criteria including checking if both 10, and 90 exist per PO_ID, if the po_type is BULK, and if any of the UPC is missing. Data passed the checking will be written to a flat file in the format requied for NEXCOM, and send to NEXCOM via sFTP. For items send out, the script will marked them through time in the nex_po_staging table and edi_pending_transactions. The script will infor esg@usmc-mccs.org and any POC if any thing wrong during the running of the script.                                           
                                                                                                

     
=head1 REQUIREMENTS 


None. 


=head1 DIAGNOSTICS   


None.  



=head1 EXIT STATUS  


None.


=head1 DEPENDENCIES 

=over 4  



=item * use base ( 'Class::Accessor' ); 



=item * use IBIS::Config::Auto; 



=item * use Data::Dumper;



=back   

=head1  SEE ALSO 

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=head1 INCMOPATIBILITIES

Unknown.                                                                                 
