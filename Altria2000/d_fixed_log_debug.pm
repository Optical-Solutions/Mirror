#######################################
# Usage can be found in 
#  package MCCS::DBI::E856_Normalization;
#  package IBIS::EmpowerIT::Query;
#######################################

package d.pm;

use strict;
use warnings;
use Carp;
use IBIS::Log::File;
use IBIS::DBI;
use Time::localtime;

#--------------------------------------------------------------------------
 my ($arg1, @rest) = @_;
sub new {
    my $class  = shift;
    my %params = @_;
   
    my $self = {};
    for my $key (keys %params) {
    	$self->{$key} = $params{$key} ;
    }

    # Optional database name
    if ( defined $params{dbname} ) {
        $self->{dbname} = $params{dbname};
    } else {
    	$self->{dbname} = 'rms_p';
    }
    
    if (defined $params{AutoCommit} ) {
    	$self->{AutoCommit} = qq($params{AutoCommit});
    } else {
    	$self->{AutoCommit} = 1;
    }
    
    if (defined $params{logFileRef}) {
    	 $self->{logFileMain} = $params{logging}->{fileRef};
    }
    
    $self->{dbh} = IBIS::DBI->connect( dbname  => $self->{dbname}, 
                                       attribs => {AutoCommit  => $self->{AutoCommit} } 
                                      ) || die "Can not connect to rms";

    # Need this special setting to handle CLOB db columns	
    $self->{dbh}->{'LongTruncOk'} = 1;
    $self->{dbh}->{'LongReadLen'} = 2000000;
    
    if ($params{debug}) {
      use Data::Dumper;    
      warn "IBIS DataAccessor new() = ".Dumper $self;
    }
    
#    $self->init();
    
    bless $self, $class;
    
    return $self;
}

sub init {};

sub get_record {}

sub set_record {}

sub close_dbh {
	my $self = shift;
    if($self->{sth}){ $self->{sth}->finish(); }
    if($self->{dbh}){ $self->{dbh}->disconnect(); }
	return $self;
}

sub commit {
	my $self = shift;
    if($self->{dbh}){ $self->{dbh}->commit(); }
	return $self;
}

sub rollback {
    my $self = shift;
    if($self->{dbh}){ $self->{dbh}->rollback(); }
	return $self;
}

#-----------------------------------------------------
# Calling Store procedure with values being return
#-----------------------------------------------------
# sub example_sp {
# 	#calls Database SP
#    my $self = shift;
#    my %params = @_;
#    
#    #has to be same as params passing from calling program
#    @{$self->{ $self->{dataSource}."bind_in_out" } } = ('site_id:in',
#                                                        'merch_year:in',
#                                                        'mc_config_id:in',
#                                                        'eff_date:in',
#                                                        'process:in',
#                                                        'modified_by:in',
#                                                        'mc_sqft_hdr_id:out',
#                                                        'found:out'
#                                                       );
#    
#    my $sql = qq( 
#      Begin create_example_sp(:site_id, :merch_year, :mc_config_id, :eff_date, :process, :modified_by, :mc_sqft_hdr_id, :found ); End;  
#    );
#
#}
#----------------------------------------------------- 
# In your calling program make sure you init then use
# below example.  All your return values will be in the corresponding
# hash ref with the name you gave in "bind_in_out" 
#-----------------------------------------------------
# $exmpRtn = $dataAccess->set_record( dataSource    => 'example_sp', 
#                                         merch_year   => $ARGS{merch_year},
#                                         site_id      => $ARGS{site_id},
#                                         eff_date     => $ARGS{eff_date},
#                                         modified_by  => $user->username(),
#                                         process      => 'exists',
#                                         mc_config_id => $ARGS{mc_config_id_fk}, );
#
# $exmpRtn->{found} would be in the ref due to it being an 'out' value of the Oracle SP



#--------------------------------------------------------------------------#
#        Global Section
#--------------------------------------------------------------------------#
sub multi_row_sql {
	#when selecting records will be givien a key using 'rec_' and counter.  Not
	#using rowid due to ordering setup by your sql as well as how you display 
	#using html..    It justs made it easier for me 
	my $self = shift;
    my $sql = shift;
    
    $self->{sth} = $self->{dbh}->prepare($sql);
    
    if ($self->{debug}) {
      print "\n<br>WMS DataAccessor MultiRow $self->{dataSource} = ".$sql;
    }
    
    $self->{sth}->execute();
    
    my $results;
    my $ctr = 0;
    
    while(my $record = $self->{sth}->fetchrow_hashref()) {
      $ctr++;
      $results->{ 'rec_'.sprintf('%.7d',$ctr) } = $record ;   
    } 
    
    $self->{sth}->finish();

    return $results
}

sub single_row_sql {
	my $self = shift;
	my $sql = shift;
	
	$self->{sth} = $self->{dbh}->prepare($sql);
	
	if ($self->{debug}) {
      print "\n<br> DataAccessor SingleRow $self->{dataSource} = ".$sql;
    }
	
    $self->{sth}->execute();
    
    my $results;

        
    while(my $record = $self->{sth}->fetchrow_hashref()) {
      $results = $record ;  
    }
    
    $self->{sth}->finish();
    
    return $results
	
}

sub simple_execute_sql {
	my $self = shift;
	my $sql = shift;
	my $params = shift;
	
	$self->{sth} = $self->{dbh}->prepare($sql);
    $self->{sth}->execute();
    return $self;
}

sub bind_execute_sql {
      my $self = shift;
      my $sql = shift;
      my $params = shift;
      
      my @sqlBind = $self->_binding_params($params, $self->{ $self->{dataSource}."BindOrder" });
      #Cache DataHandle.... 
      if (defined $params->{prepare_cache} 
            && ! defined $self->{ $params->{prepare_cache} } ) {
         
         $self->{ $params->{prepare_cache} } = $self->{dbh}->prepare_cached($sql);
         $self->{sthPrepare} =  $self->{ $params->{prepare_cache} }
         
      } elsif (defined $params->{prepare_cache} 
                 && defined $self->{ $params->{prepare_cache} } ) {
         
         $self->{sthPrepare} =  $self->{ $params->{prepare_cache} }
      
      } else {	
      	 $self->{sthPrepare} = $self->{dbh}->prepare($sql);
      }
      
      $self->{sth} = $self->{sthPrepare};
      
      #print $sql. " bind  @sqlBind<br>$params->{prepare_cache}<br>$self->{ $params->{prepare_cache} }<br><br>";
      #return;
      $self->{sth}->execute(@sqlBind);     
      return $self;	  
}

sub bind_execute_procedure
{
      my $self = shift;
      my $sql = shift;
      my $params = shift;
      my $out = ();
     
     $self->{sth} = $self->{dbh}->prepare($sql);
     
     foreach my $x (@{$self->{ $self->{dataSource}."bind_in_out" }}) {
     	my ($field, $bind_type) = split(/:/, $x);
     	if ($bind_type =~ /in/i) {
     	 	$self->{sth}->bind_param( ":$field",   $params->{$field} );    	 	
     	}
     	
     	if ($bind_type =~ /out/i) {
     		$self->{sth}->bind_param_inout( ":$field",    \$out->{$field} , 1024 );   
     		  		
     	}
     }
     
     $self->{sth}->execute();
     return $out;
     
            
}

#Use Binding to prevent sql injections
sub _binding_params {	
	my $self = shift;
	my $params = shift;
    my $order = shift;
     
    my @binding = ();
	
	foreach my $key (@{$order}) {
		if (!exists $params->{$key} ) {
			push(@binding, '');
		} else {
		  if (! defined($params->{$key}) ) {
		  	push(@binding, '');
		  } else {
		    push(@binding, $params->{$key});
		  }	
		}
	}
	return @binding;
}

sub _log_debug {
    my ($self, @args) = @_;
	my $log_entry = join( '', "(DEBUG PID $$) ", @args );
	if ($self->{logFileMain}) { $self->{logFileMain}->info($log_entry); }
	_debug($log_entry);
        return;
}

sub _log_warn {
	my $self = shift;
	my $log_entry = join( '', "(WARN PID $$) ", @_ );
	if ($self->{logFileMain}) { $self->{logFileMain}->warn($log_entry); }
	_debug($log_entry);
	return $self;
}

sub _fatal_error {
	my $self = shift;
#	send_mail( "ERROR on " . __FILE__ . ' ' . $g_long_date, $msg );
	my $log_entry = join( '', "(FATAL PID $$) ", @_ );
	if ($self->{logFileMain}) { $self->{logFileMain}->error($log_entry); }
	die $log_entry;
}

sub _debug {
		print "DEBUG: ", @_, "\n";
		return;
}

#----- do not remove "1" -----#

1;

__END__

=pod

=head1 NAME


=head1 VERSION


=head1 SYNOPSIS


=head1 DESCRIPTION


=head1 SUBROUTINES/METHODS

=over 4


=back

=head1 CONFIGURATION AND ENVIRONMENT

None.

=head1 INCOMPATIBILITIES

None.

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to RDI Staff L<rdistaff@usmc-mccs.org|mailto:rdistaff@usmc-mccs.org>.
Patches are welcome.

=head1 AUTHORS


=head1 LICENSE AND COPYRIGHT

Copyright (c) 2012 MCCS. All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut


# $resultData{savedSqftDates} = $dataAccess->get_record( dataSource => 'getSavedSqftDatesBySite', 
#                                                           site_id    => $ARGS{site_id},
#                                                           merch_year => $ARGS{merch_year},);
                                                           
#    $resultData{savedSqftHdr} = $dataAccess->get_record( dataSource => 'getSavedSqftHdrIdByDate', 
#                                                         site_id    => $ARGS{site_id},
#                                                         merch_year => $ARGS{merch_year},
#                                                         eff_date   => $ARGS{eff_date}, );

