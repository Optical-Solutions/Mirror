package IBIS::AP_Genex::Wrapper4claim;
use Carp;
use File::Basename;
use IBIS::Log::File;
use strict;
use warnings;
use Data::Dumper;
use IBIS::DBI;
use IBIS::AP_Genex::Claim;
use IBIS::AP_Genex::Type_I_Record;
use IBIS::AP_Genex::DateTime;
use IBIS::AP_Genex::Type_C_Record;
use IBIS::AP_Genex::Type_D_Record;
use IBIS::AP_Genex::Field;

my $pkg = __PACKAGE__;

my $config = '/usr/local/mccs/etc/genex/claim_genex.conf';

sub new {
    my $type   = shift;
    my %params = @_;
    my $self   = {};

    bless $self, $type;

    my $config_file;
    if ( $params{conf} ) {
        $config_file = $params{conf};
    }
    else {
        $config_file = $config;
    }
    my $fh;
    open( IN, "<$config_file" ) || die "Failed to open config file: $config_file\n";
    my @pairs = <IN>;
    close IN;
    foreach my $line (@pairs) {
        chomp($line);
        my @items = split( /\=/, $line );
        if ( $items[0] && $items[1] ) {
            $self->{ $items[0] } = $items[1];
        }
    }

    return $self;
}

sub sget_dbh {
    my ($self) = @_;
    if ( $self->{'dbh_obj'} ) {
        return $self->{'dbh_obj'};
    }
    else {
        if ( $self->{'DB_CONN_ID'} ) {
            $self->{'dbh_obj'} = IBIS::DBI->connect( dbname => $self->{'DB_CONN_ID'} )
                or die "Can’t connect\n";
            return $self->{'dbh_obj'};
        }
        else {
            my $msg =
                "Failed to connect to database, missing DB_CONN_ID value in config file";
            die "$msg";
        }

    }
}

################subroutines#########################
## a new subroutine for marking off reason_id 22 for details
sub markoff_reason_22 {
    my ($self) = @_;
    my $query = "update merch.iro_po_claim_details 
                        set user_out_date = sysdate
                 where returned_reason_id ='22'
                       and remark_key ='13'
                       and user_out_date is null";
    eval{
	$self->{'dbh_obj'}->do($query);
    };

    if($@){
	$self->{'dbh_obj'}->rollback;
    }else{
	$self->{'dbh_obj'}->commit;
    }
}

sub get_claim_header_file {
    my ($self) = @_;
    my $query = "
SELECT distinct 
      business_unit_id, 
       --lpad(claim_id,10),
       group_cmd, 
       vendor_id, 
       --site_id, 
       ' ' as site_id, --site_id, 
       to_char(claim_date, 'yyyymmdd') as claim_date,
       ' ' as receipt_id, 
       ' ' as po_id,
       item_qty, 
       claim_amt, 
       on_hold_ind,
       remark_key, 
       vendor_currency_id, 
       gl_account_ap_control,
       authorization_code, 
       to_char(user_out_date, 'yyyymmdd') as update_date, 
       merch_period
FROM v_cl_grp_hdr_sum
order by group_cmd asc
";

=head
/*
	my $h_unit_id                 ,	# size = 2	1,2
	my $h_claim_id                ,	# size = 6 	3,8
	my $h_duns_nbr                ,	# size = 11 	9,19
	my $h_site_id                 ,	# size = 5 	20,24
	my $h_claim_date              ,	# size = 8 	25,32
	my $h_receipt_id              ,	# size = 10 	33,42
	my $h_po_id                   ,	# size = 10 	43,52
	my $h_item_qty                ,	# size = 11 	53,63
	my $h_claim_amt               ,	# size = 12 	64,75
	my $h_on_hold_ind             ,	# size = 1	76,76
	my $h_remark_key              ,	# size = 38 	77,114
	my $h_currency                ,	# size = 10 	115,124
	my $h_liability_acct          ,	# size = 25 	125,149
	my $h_auth_code               ,	# size = 10	150,159
	my $h_update_date            , # size = 8      160,167
	my $h_merchandising_period   , # size = 2     168,169
	) = 
m/^(.{2})(.{6})(.{11})(.{5})(.{8})(.{10})(.{10})(.{11})(.{12})(.{1})(.{38})(.{10})(.{25})(.{10})(.{8})(.{2})$/
	)
*/
=cut

    my $sth = $self->{'dbh_obj'}->prepare($query);
    $sth->execute();
    return $sth;
}

sub print_header_file {
    my ( $self, $sth, $outfile ) = @_;
    ##my $outfile = './testout_header_file';
    open( OUT, ">$outfile" ) or die "can not open file to write:$outfile\n";

    while ( my $row = $sth->fetchrow_hashref() ) {
        if ( $row->{group_cmd} ) {
            $row->{business_unit_id} = "" unless ( $row->{business_unit_id} );
            ##$row->{group_cmd}        = "" unless $row->{group_cmd};
            $row->{vendor_id}             = "" unless $row->{vendor_id};
            $row->{site_id}               = "" unless $row->{site_id};
            $row->{claim_date}            = "" unless $row->{claim_date};
            $row->{receipt_id}            = "" unless $row->{receipt_id};
            $row->{po_id}                 = "" unless $row->{po_id};
            $row->{item_qty}              = "" unless $row->{item_qty};
            $row->{claim_amt}             = "" unless $row->{claim_amt};
            $row->{on_hold_ind}           = "" unless $row->{on_hold_ind};
            $row->{remark_key}            = "" unless $row->{remark_key};
            $row->{vendor_currency_id}    = "" unless $row->{vendor_currency_id};
            $row->{gl_account_ap_control} = "" unless $row->{gl_account_ap_control};
            $row->{authorization_code}    = "" unless $row->{authorization_code};
            $row->{update_date}           = "" unless $row->{update_date};
            $row->{merch_period}          = "" unless $row->{merch_period};

            my $line = sprintf(
                "%2s%15s%11s%5s%8s%10s%10s%11s%12s%1s%38s%10s%25s%10s%8s%2s\n",
                $row->{business_unit_id},      $row->{group_cmd},
                $row->{vendor_id},             $row->{site_id},
                $row->{claim_date},            $row->{receipt_id},
                $row->{po_id},                 $row->{item_qty},
                $row->{claim_amt},             $row->{on_hold_ind},
                $row->{remark_key},            $row->{vendor_currency_id},
                $row->{gl_account_ap_control}, $row->{authorization_code},
                $row->{update_date},           $row->{merch_period}
            );
            print OUT $line;
        }
    }
    close OUT;
}

sub get_claim_detail_file {
    my ($self) = @_;
    my $query = "
SELECT 
       business_unit_id,
       group_cmd, 
       claim_id as claim_detail_id,
       chargeback_type, 
       style_id, 
       color_id, 
       dimension_id, 
       size_id, 
       size_range_id, 
       item_qty,
       returned_reason_id,
       gl_account_id, 
       unit_cost,
       landed_cost,
       site_id, 
       authorization_code,
       po_id,
       receipt_id
FROM v_cl_grp_detail_sum
order by group_id_cmd asc
";
    my $sth = $self->{'dbh_obj'}->prepare($query);
    $sth->execute();
    ##return $sth;
    ##my $ret = $self->{'dbh_obj'}->selectall_arrayref($query);
    ##print Dumper($ret);
    return $sth;
}

=head
  	my $d_unit_id                 ,	# size = 2	1,2
	my $d_claim_id                ,	# size = 6 	3,8
	my $d_seq_no                  ,	# size = 3 	9,11
	my $d_chargeback_type         ,	# size = 10 	12,21
	my $d_style_id                ,	# size = 14 	22,35
	my $d_color_id                ,	# size = 3 	36,38
	my $d_dimension_id            ,	# size = 5 	39,43
	my $d_size_id                 ,	# size = 5 	44,48
	my $d_size_range_id           ,	# size = 3 	49,51
	my $d_item_qty                ,	# size = 11 	52,62
	my $d_reason                  ,	# size = 4 	63,66
	my $d_expense_acct            ,	# size = 25 	67,91
	my $d_unit_cost               ,	# size = 13 	92,104
	my $d_landed_cost             ,	# size = 13 	105,117
	) = 
# 0,13 for costs below should be removed after production starts receiving cost fields
# in the interim, this allows support of both
m/^(.{2})(.{10})(.{3})(.{10})(.{14})(.{3})(.{5})(.{5})(.{3})(.{11})(.{4})(.{25})(.{0,13})(.{0,13})$/
	)
=cut

sub print_detail_file {
    my ( $self, $sth, $outfile ) = @_;
    ##my $outfile = './testout_detail_file';
    open( OUT2, ">$outfile" ) or die "can not open file to write:$outfile\n";

    while ( my $row = $sth->fetchrow_hashref() ) {
        if ( $row->{group_cmd} ) {
            $row->{business_unit_id} = "" unless $row->{business_unit_id};
            ##$row->{group_cmd}          = "" unless $row->{group_cmd};
            $row->{claim_detail_id}    = "" unless $row->{claim_detail_id};
            $row->{chargeback_type}    = "" unless $row->{chargeback_type};
            $row->{style_id}           = "" unless $row->{style_id};
            $row->{color_id}           = "" unless $row->{color_id};
            $row->{dimension_id}       = "" unless $row->{dimension_id};
            $row->{size_id}            = "" unless $row->{size_id};
            $row->{size_range_id}      = "" unless $row->{size_range_id};
            $row->{item_qty}           = "" unless $row->{item_qty};
            $row->{returned_reason_id} = "" unless $row->{returned_reason_id};
            $row->{gl_account_id}      = "" unless $row->{gl_account_id};
            $row->{unit_cost}          = "" unless $row->{unit_cost};
            $row->{landed_cost}        = "" unless $row->{landed_cost};
            $row->{site_id}            = "" unless $row->{site_id};
            $row->{authorization_code} = "" unless $row->{authorization_code};
            $row->{po_id}              = "" unless $row->{po_id};
            $row->{receipt_id}         = "" unless $row->{receipt_id};

            my $line = sprintf(
                "%2s%15s%10s%10s%14s%3s%5s%5s%3s%11s%4s%-25s%13s%13s%5s%10s%10s%10s\n",
                $row->{business_unit_id},   $row->{group_cmd},
                $row->{claim_detail_id},    $row->{chargeback_type},
                $row->{style_id},           $row->{color_id},
                $row->{dimension_id},       $row->{size_id},
                $row->{size_range_id},      $row->{item_qty},
                $row->{returned_reason_id}, $row->{gl_account_id},
                $row->{unit_cost},          $row->{landed_cost},
                $row->{site_id},            $row->{authorization_code},
                $row->{po_id},              $row->{receipt_id}
            );
            print OUT2 $line;
        }
    }
    close OUT2;
}
### orignial: yuc dec 12, 14: "%2s%15s%6s%10s%14s%3s%5s%5s%3s%11s%4s%-25s%13s%13s%5s%10s%10s%10s\n",
## changed the third item: from 6s to 10s please note.
## 

sub scp_head_detail_to_fmsserver {
    my ($self) = @_;
    my $server= $self->{REMOTE_SERVER};
    #Move next line to next scp   
    # 'scp -q /usr/local/mccs/data/genex/ap/source/*CL_*.DAT rdiusr@'.$server.':/usr/local/mccs/data/axsone/input/PO_RCV_CL/';
    my $scp_cmd =
        'scp -q /usr/local/mccs/data/genex/ap/source/*CL_*.DAT rdiusr@'.'hqm04ibissvr0010'.':/tmp';
    my $ret = system($scp_cmd);
    if ($ret) {
        print "WARNING: scp error happend when copy file to fms";
        return undef;
    }
    else {
        return 1;
    }
}

## from the flat file directory, get a pair of most recent claim files, or return error
sub get_most_recent_claim_flat_files {
    my ( $self, $dir_path ) = @_;
    ## get default dir path:
    unless ($dir_path) {
        $dir_path = $self->{RMT_DATA_DIR};
    }
    my $flag_success = 0;
    ## get most recent pair of claim head and detail files:

    opendir( my $DH, $dir_path ) or die "Error opening $dir_path: $!";
    my @files = map { [ stat "$dir_path/$_", $_ ] } grep( !/^\.\.?$/, readdir($DH) );
    closedir($DH);

    ##sub rev_by_date { $b->[9] <=> $a->[9] }
    my @sorted_files = sort { $b->[9] <=> $a->[9] } @files;

    my @newest = @{ $sorted_files[0] };
    my $name   = pop(@newest);
    print $name;
}

1;
__END__

=head

## directories:
WORK_DIR=/usr/local/mccs/bin/
DATA_DIR=/usr/local/mccs/data/genex/ap/source/
ARCHIVE_DIR=/usr/local/mccs/data/genex/archive/claims/

## emails:
MAIL_TO=rdistaff@usmc-mccs.org
MAIL_CC=rdistaff@usmc-mccs.org
MAIL_FROM=yuc@usmc-mccs.org

## remote program, data directories:
FTP_CONNECT=fms_dev
DB_CONN_ID=rms_p
REMOTE_SERVER=fmsdev.usmc-mccs.org
REMOTE_USER=ban
PASSWORD=U2FsdGVkX1895HT9OfER1n+UTp2AJrvY
RMT_BIN_DIR=/usr/local/mccs/scripts/
RMT_DATA_DIR=/usr/local/mccs/data/axsone/input/PO_RCV_CL/
GENEX_INPUT_DIR=/axsone/ctron12/data/apgenex/
GENEX_CL_KSH=/usr/local/mccs/scripts/Genex_CL.ksh

## TABLES, PROCEDURES:
VIEW_CL_HEADER_SUM=
VIEW_CL_DETAILS_SUM=
PROC_HEADER=create_cl_group_header
PROC_DETAILS=create_cl_group_detail

## logs
LOG_DIR=/usr/local/mccs/log/genex/



This is for creating claim files using ap_genex package...

=cut


