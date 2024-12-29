package IBIS::WEBFMSJob;
use Carp;
use File::Basename;
use IBIS::Log::File;
use IBIS::DBI;
use strict;
use Data::Dumper;
use warnings;
my $pkg = __PACKAGE__;
my $db_connection_id = 'rms_p_force';

#--------------------------------------------------------------------------
sub new {
    my $type   = shift;
    my %params;
    while (my $key = shift) {
        my $value = shift;
        $params{$key} = $value;
    }
    my $self   = {};
  
    bless $self, $type;

    if ( $params{IBISlog_object} ) {

        # use log object
        $self->{log} = $params{IBISlog_object};

    } elsif ( $params{logfile} ) {

        # use logfile;
        $self->{logfile} = $params{logfile};
        $self->{log} = IBIS::Log::File->new(
                             { file => $params{logfile}, append => 1, level => 4 } );

    } else {
        $self->{log} = undef;
    }

    if ( $params{username} ) {
        $self->{username} = $params{username};
    } else {
        $self->_croak("Must supply username argument");
    }

    # print map "$_ = $params{$_}\n", keys %params; # DEBUG delete this later
    # use Data::Dumper;
    # print Dumper $self;

    # Init database connection
    $self->_db_init();

    # Get initial sequence number from DB to form job_id
    $self->_make_job_id();

    return $self;
}

#--------------------------------------------------------------------------
sub is_processing {
    my $self = shift;
    my $sql = "SELECT WEBFMSJOB.get_status(?) from dual";
    my $sth =$self->{dbh}->prepare($sql);
    $sth->execute($self->{job_id});
    my $stat = $sth->fetchrow_array();

    if ( $stat eq 'P') {
      return 1;
    } else {
      return 0;
    }

}

#--------------------------------------------------------------------------
sub store {
    my $self = shift;
    $self->{job_type}   = shift;
    $self->{op_type}    = shift;
    $self->{items}      = shift;
    $self->{genex_date} = shift || '';

    if ( $self->{job_type} eq 'CL' ) { $self->_store_cl();
    } elsif ( $self->{job_type} eq 'PO' ) { $self->_store_po();
    } elsif ( $self->{job_type} eq 'BPO' ) { $self->_store_bpo();
    } elsif ( $self->{job_type} eq 'RCV' ) { $self->_store_rcv();
    } else { $self->_croak("Invalid Job Type \"$self->{job_type}\"");
    }
    return;
}

#--------------------------------------------------------------------------
sub _store_po {
    my $self = shift;
    return;
}

#--------------------------------------------------------------------------
sub _store_bpo {
    my $self = shift;
    return;
}

#--------------------------------------------------------------------------
sub _store_rcv {
    my $self = shift;
    return;
}

#--------------------------------------------------------------------------
sub _store_cl {
    my $self = shift;

    if ( ( $self->{op_type} eq 'A' ) || ( $self->{op_type} eq 'E' ) ) {
        if ( $self->{genex_date} ) {
            $self->{filename} = $self->_make_source_filename( $self->{genex_date}, $self->{job_type} );
        } else {
            $self->_croak("Must supply genex date for operation A or E");
        }
    }

    if ( $self->{op_type} eq 'S' ) {

    # Since $genex_date is not mandatory on 'S' op type (specific type)
    # we need to file the dates for each individual item_number and store them there.

	$self->{log}->info("item: $self->{items}");
	$self->{log}->info("job_type: $self->{job_type}");
        my $hash_ref = $self->_get_cl_genex_dates( $self->{items}, $self->{job_type} );


	my $buffer = Dumper($hash_ref);
	$self->{log}->info($buffer);

	
        $self->_insert_collection();
        foreach my $e ( sort keys %{$hash_ref} ) {
            $self->_insert_item( $e, $hash_ref->{$e} ); ## e: cl name, $hash_ref->{$e} is filename
        }

    } elsif ( $self->{op_type} eq 'A' ) {

        $self->_insert_collection();
	my $cl_list_ref  = $self->_get_cl_items_for_date( $self->{genex_date} );
	if( ($cl_list_ref) && (@$cl_list_ref > 0) ){
	    foreach my $e ( sort @$cl_list_ref) {
		## $self->_insert_item($e,  $self->{filename} );
		$self->_insert_item($e);
	    }
	}else{
	    $self->_croak("<B/>No claims found in the date you provided. maybe another date? </B>");
	}
	
    } elsif ( $self->{op_type} eq 'E' ) {

        $self->_insert_collection();

        foreach my $e ( @{ $self->{items} } ) {
            $self->_insert_item($e);
        }

    } else {

        $self->_croak("Invalid Operation type \"$self->{op_type}\"");

    }

    return 1;
}

#--------------------------------------------------------------------------
sub _insert_item {
    my $self     = shift;
    my $item     = shift;
    my $filename = shift || $self->{filename};
    $self->_log( "  ITEM " . join( ", ", ( $self->{job_id}, $item, $filename ) ) );
    $self->{dbh}->do( "BEGIN WEBFMSJOB.store_collection_item(?,?,?); END;",
                      undef, $self->{job_id}, $item, $filename );
                      return;
}

#--------------------------------------------------------------------------
sub _insert_collection {
    my $self = shift;
    $self->{dbh}->do(
             "BEGIN WEBFMSJOB.store_collection(?,?,?,?,to_date(?,'YYYYMMDD')); END;",
             undef,
             $self->{job_id},
             $self->{username},
             $self->{job_type},
             $self->{op_type},
             $self->{genex_date}
    );
    $self->_log(
                 "COLLECTION "
                   . join(
                           ", ",
                           (
                             $self->{job_id},  $self->{username}, $self->{job_type},
                             $self->{op_type}, $self->{genex_date}
                           )
                   )
    );
    return;
}

#--------------------------------------------------------------------------
sub _make_source_filename {
    my $self     = shift;
    my $date     = shift;
    my $job_type = shift;
    my $filename = '';
    my ( $yyyy, $mm, $dd );
    if ( $date =~ m/(\d{4})(\d{2})(\d{2})/ ) {
        $yyyy     = $1;
        $mm       = $2;
        $dd       = $3;
	if (($job_type eq 'cl') || ($job_type eq 'CL')) { ## 023000
	    $filename = $yyyy . '-' . $mm . $dd . '023000' . $job_type . '_HEAD.DAT';			     	      
	}else{
	    $filename = $yyyy . '-' . $mm . $dd . '010000' . $job_type . '_HEAD.DAT';
	}
        return $filename;
    } else {
        $self->_croak("Could not parse $date");
    }
    return;
}

#--------------------------------------------------------------------------
sub _get_cl_items_for_date {
    my $self = shift;
    my $date = shift;
    my $dbh  = IBIS::DBI->connect( dbname => $db_connection_id ); 

    ##my $sql = "select distinct i.group_rtv_id||substr(v.short_name, 1,2)
    ##           from iro_po_claim_headers i, 
    ##                site_3_letter_name_v v
    ##           where i.site_id = v.site_id 
    ##           and   to_char(i.user_out_date, 'YYYYMMDD') = ? 
    ##           and   i.user_out_date > (sysdate - 360)";

  ## rewrite the query: aug, 15 yuc
    my $sql = "select distinct i.group_rtv_id||s.cmd_short
               from iro_po_claim_headers i, 
                    site_cmd s
               where i.site_id = s.site_id 
               and   to_char(i.user_out_date, 'YYYYMMDD') = ? 
               and   i.user_out_date > (sysdate - 360)";

    my $sth = $dbh->prepare($sql);
    $sth->execute($date);
    my ( $row, $all );

    while ( $row = $sth->fetchrow_array() ) {
        push( @{$all}, $row );
    }
    $dbh = undef;
    return $all;
}

#--------------------------------------------------------------------------
sub _get_cl_genex_dates {
    my $self     = shift;
    my $arr_ref  = shift;
    my $job_type = shift;
    my $dbh      = IBIS::DBI->connect( dbname => $db_connection_id );
    my $h;
    my $str;
    ## $dbh->do("alter session set nls_date_format = 'YYYYMMDD'");

    for (@{$arr_ref}) {  $_ = "'" . $_ . "'"; }

    $str = join( ',', @{$arr_ref} );
#    my $sql = "select claim_id, user_out_date
#               from   iro_po_claim_headers
#               where claim_id in ($str)";

    my $sql = "select  	group_rtv_id_cmd, to_char(user_out_date,'YYYYMMDD') as user_out_date
               from     v_iro_po_claim_headers_web
               where    group_rtv_id_cmd in ($str)
                        and user_out_date > (sysdate - 365)"; ## aug 4, 15, yuc add only one year old file
    $self->{log}->info("sql: $sql");
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $r;

    
    while ( $r = $sth->fetchrow_hashref() ) {
       $self->{log}->info("user_out_date: $r->{user_out_date}");
        $h->{ $r->{group_rtv_id_cmd} } =
          $self->_make_source_filename( $r->{user_out_date}, $job_type );
    }

    $dbh = undef;
    return $h;
}

#--------------------------------------------------------------------------
sub job_id {
    my $self = shift;
    return $self->{job_id};
}

#--------------------------------------------------------------------------
sub _make_job_id {
    my $self = shift;
    my $sql  = 'select job_id_seq.nextval from dual';
    my $sth  = $self->{dbh}->prepare($sql);
    eval { $sth->execute(); };
    if ($@) {
        $self->_croak( 'Cant make job_id because ' . $@ );
    }
    $self->{job_id} = $sth->fetchrow_array();
    $self->_log( 'Job ID is ' . $self->{job_id} );
    return;
}

#--------------------------------------------------------------------------
sub _db_init {
    my $self = shift;
    $self->{dbh} = IBIS::DBI->connect( dbname => 'ibisora' );
    return;
}

#--------------------------------------------------------------------------
sub _croak {
    my $self = shift;
    my $msg  = shift;
    $self->_log($msg);
    croak $pkg . ' ' . $msg;
}

#--------------------------------------------------------------------------
sub _log {
    my $self = shift;
    my $msg  = shift;
    if ( defined( $self->{'log'} ) ) {
        $self->{'log'}->info( $pkg . ' ' . $msg );
    }
    return;
}

#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
# Dont want destroy because nothing to clean up
# Janitor is having a vacation today!
sub DESTROY {
    my $self = shift;
    $self->{dbh} = undef;
    $self->_log("End-------------");
    return;
}

1;

__END__

=pod

=head1 NAME

IBIS::WEBFMSJob - Object Class for WEBFMSJob 

=head1 SYNOPSIS

    # Using explicit log filename
    my $object = IBIS::WEBFMSJob->new( username=>'ironman', logfile=> '/log/mylogfile');

    # Using defined log object 
    my $object = IBIS::WEBFMSJob->new( username=>'ironman', IBISlog_object=> $IBIS_log_object );

    # No Logging
    my $object = IBIS::SSH2CONNECT->new( username=>'ironman');

=head1 DESCRIPTION

bla bla bla .. in-progress

=head1 CONSTRUCTOR

=over 4

=item new ()

IBIS::WEBFMSJob constructor is called. It has 3 options. Using logfile, using log object and no logging.

=back

=head1 METHODS

=over 4

=item store()

Store method.

=item is_processing()

Returns boolean, checks if particular job_id is processing or not.

=back

=head1 PL/SQL Dependencies

=over 4

=item ibisadm.WEBFMSJob.store_collection(job_id, username, job_type, operation, file_date)

Store header information.

=item ibisadm.WEBFMSJob.store_collection_item(job_id, item_id)

Store detail information down to item level.

=item ibisadm.WEBFMSJob.store_completed_job(job_id, filename)

=item ibisadm.WEBFMSJob.get_status(job_id)

Returns status of particular job_id

=back

=head1 HISTORY

=over 4

=item Mon May 24 12:04:18 EDT 2010

Conceived.

=back

=head1 BUGS

Not a chance.

=head1 AUTHOR (S)

Hanny Januarius B<januariush@usmc-mccs.org>

=cut
