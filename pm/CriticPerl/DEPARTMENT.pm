package MCCS::CMS::Loads::DEPARTMENT;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::DepartmentRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return;
}

sub get_sql {
    my $self               = shift;

return "
Select distinct Department_Id, Upper(Description) Description From Departments
where business_unit_id = '30' and description is not null
"
;
}

sub make_record {
    my ($self,$dept_id, $desc ) = @_;

    my $obj =
        MCCS::CMS::RecordLayout::DepartmentRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   DeptNumber    => $dept_id,
            DeptDesc      => substr($desc,0,30),
        }
      );
    return $obj->to_string();
}

sub get_filename {
    my $self = shift;
    my $date = shift;
    return $date . '_Dept.dat';
}


1;

__END__

=pod

=head1 NAME

MCCS::CMS::Loads::DEPARTMENT - 

=head1 SYNOPSIS



=head1 DESCRIPTION

This plugin extracts the data necessary for the Department Core File for Compliance Network

=head1 AUTHOR


Larry Lewis  L<larry.d.lewis@usmc-mccs.org|mailto:larry.d.lewis@usmc-mccs.org>

=cut
