package MCCS::Reflexis::Util;
use strict;
use warnings;
#use constant FILENAME_PATTERN => 'MCCS_RWS_%s_%s.VD0%i';
#constant FILENAME_PATTERN : String := "MCCS_RWS_%s_%s.VD0%i";
use MCCS::Reflexis::HeaderRecord;
use MCCS::Reflexis::FooterRecord;
use MCCS::Reflexis::VDDPRecord;
use MCCS::Reflexis::VDDIRecord;
use MCCS::Reflexis::DateTime;
use MCCS::Reflexis::DateTimeWithTime;
use Date::Manip;
use File::Basename;

sub new {
	my ($class, $filename) = @_;

	my $filehandle;

	my $is_stdout = 0;

	#open output file
	if($filename eq '-'){
		$filehandle = \*STDOUT;
		$is_stdout = 1;
	}else {
		#	die "Can not open '$filename'" unless open($filehandle, ">$filename");
		#open($filehandle, '>', $filename) or die "Cannot open '$filename': $!";
		close $filehandle or warn "Could not close '$filename': $!";	
	}

	my $self = bless({ filehandle=>$filehandle, filename=>$filename, is_stdout => $is_stdout },$class);
return $self;
}


sub vddp_filename{
	 return _filename('VDP',1);

}

sub vddi_filename{
	return _filename('VDI',2);
	
}


sub _filename {
	my ($first_arg, $second_arg) = @_;
	return sprintf('MCCS_RWS_%s_%s.VD0%i', $first_arg, UnixDate(ParseDate('now'), "%q"), $second_arg);
#	sprintf(FILENAME_PATTERN, $_[0], UnixDate(ParseDate('now'), "%q"),$_[1]);

}
sub make_vddp_customer_count{
	#my $self = shift;
	#	my ($site_id,$date,$value) = @_;
	my ($self, $site_id, $date, $value) = @_;
	return $self->make_vddp_record($site_id,'S','',$date,'C_TRANSACTIONS',$value);
	
}
sub make_vddp_units_sold{
	 my ($self, $site_id, $lob, $date, $value) = @_;
	return $self->make_vddp_record($site_id,'X',$lob,$date,'C_ITEMS',$value);
	
}
sub make_vddp_sales{
	my ($self, $site_id, $lob, $date, $value) = @_;
	return $self->make_vddp_record($site_id,'X',$lob,$date,'C_SALES',$value);
	
}
sub make_vddp_returns{
	 my ($self, $site_id, $lob, $date, $value) = @_;
	return $self->make_vddp_record($site_id,'X',$lob,$date,'C_RETURNS',$value);
	
}
sub make_vddi_customer_count{
	my ($self, $site_id, $date, @time_values) = @_;
	return $self->make_vddi_record($site_id,'S','',$date,'C_TRANSACTIONS',[@time_values]);

}
sub make_vddi_units_sold {
	my ($self, $site_id, $lob, $date, @time_values) = @_;
    return $self->make_vddi_record($site_id, 'D', $lob, $date, 'C_ITEMS', [@time_values]);
   
	}

sub make_vddi_sales {
    	 my ($self, $site_id, $lob, $date, @time_values) = @_;
	return $self->make_vddi_record($site_id, 'D', $lob, $date, 'C_SALES', [@time_values]);
	
}

sub make_vddi_returns{
	my ($self, $site_id, $lob, $date, @time_values) = @_;
	return $self->make_vddi_record($site_id,'D',$lob,$date,'C_RETURNS',[@time_values]);
}

sub make_vddi_record {
	#my $self = shift;
	#my ($site_id,$feed_level,$lob,$date,$metric,$time_values) = @_;
	my ($self ,$site_id, $feed_level, $lob, $date, $metric, $time_values) = @_;
	my $time_obj = MCCS::Reflexis::VDDITimeValues->new($time_values);
	my %time_hash = $time_obj->to_hash();

	$self->_track_header_create();

	return MCCS::Reflexis::VDDIRecord->new( filehandle=>$self->{'filehandle'} )->set(
		{
			'Unit ID'=>$site_id,
			'Feed Level'=>$feed_level,
			'Entity ID'=>$lob,
			'Effective Date'=>MCCS::Reflexis::DateTime->new($date),
			'Metric ID' => $metric,
			%time_hash
		}
	);
}

sub make_vddp_record{
	my ($self,$site_id,$feed_level,$lob,$date,$metric,$value) = @_;

	$self->_track_header_create();

	return MCCS::Reflexis::VDDPRecord->new( filehandle=>$self->{'filehandle'} )->set(
		{
			'Unit ID'=>$site_id,
			'Feed Level'=>$feed_level,
			'Entity ID'=>$lob,
			'Effective Date'=>MCCS::Reflexis::DateTime->new($date),
			'Metric ID' => $metric,
			'Data Value' => $value
		}
	);
}

sub finish{
	my $self = shift;
	if(! $self->{'fired_footer'}){
		$self->make_footer_record()->to_string();
		$self->{'fired_footer'} = 1;
	}
	if( ! $self->{'is_stdout'} ){ close $self->{'filehandle'}; }
return;
}

sub make_header_record{
	my $self = shift;
	return MCCS::Reflexis::HeaderRecord->new( filehandle=>$self->{'filehandle'} )->set(
		{
			'Timestamp' => MCCS::Reflexis::DateTimeWithTime->new('NOW'),
			'Filename' => basename($self->{'filename'}),
		}
	);	

}

sub make_footer_record{
	my $self = shift;
	return MCCS::Reflexis::FooterRecord->new( filehandle=>$self->{'filehandle'} )->set(
		{
			'Record count' => $self->{'count'},
		}
	);
}


sub _track_header_create{
	my $self = shift;
	if($self->{'count'} == 0){
		$self->make_header_record()->to_string();
	}
	$self->{'count'}++;
return;  #TODO ASk Hanny
}

sub DESTROY {
	my $self = shift;
	if(ref($self)){
		$self->finish();
	}
return;
}

1;

__END__

=pod

=head1 NAME

MCCS::Reflexis::Util - Utility routines for Reflexis record generation

=head1 SYNOPSIS

MCCS::Reflexis::Util 

=head1 DESCRIPTION

Static class defining utility routines for quick Reflexis record creation.

=head1 SUBROUTINES/METHODS

=over 4

=item make_vdp_record(site_id,feed_level,lob,date,metric,value)

Create a MCCS::Reflexis::VDDPRecord

=item make_vddp_customer_count(site_id,date,value)

Create a MCCS::Reflexis::VDDPRecord suitable for a customer count volume load record.

=item make_vddp_units_sold(site_id,lob,date,value)

Create a MCCS::Reflexis::VDDPRecord suitable for a sales units volume load record.

=back

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
