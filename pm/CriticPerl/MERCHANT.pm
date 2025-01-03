package MCCS::CMS::Loads::MERCHANT;
use strict;
use warnings;
use base ("MCCS::CMS::Loads::Base");
use MCCS::CMS::RecordLayout::MerchantRecord;
use MCCS::CMS::DateTime;

sub init {
    my $self = shift;
    my $util = $self->{'util'};
    return;
}

sub get_sql {
    my $self               = shift;

return "
select * from (
select employee_id,
employee_name,
note_1,
department_id
from
(select 
e.employee_id,
upper(e.First_Name) || ' ' || upper(e.Last_Name) employee_name,
case when e.note_1 = 'BUYER'
then 'BYR' else e.note_1 end note_1,
sub.department_id
from
employees e
join ( select div.description division, line.level_id lob_id, line.description lob_description,
              dept.department_id, dept.description, dmm.Employee_Id dmm_id, dmm.First_Name || ' ' || dmm.Last_Name dmm_name,
              byr.Employee_Id byr_id, byr.fullname byr_name
      from 
       (select description, level_id from merchandising_levels where sub_type = '10') div
      left join (select level_id, mlvl_level_id, description from merchandising_levels where sub_type = '20') line 
           on (div.level_id = line.mlvl_level_id)
      left join (select  department_id, description, mlvl_level_id from departments where mlvl_sub_type = '20') dept 
           on (line.level_id = dept.mlvl_level_id)
      left join (select * from employees where note_2 is not null) DMM 
           on (upper(dmm.note_2) = upper(div.description))
      left join (Select  E.Employee_Id, E.First_Name || ' ' || E.Last_Name Fullname,  D.Department_Id,  e.note_1 
                 From Employees E    
                 Join Departments D On (D.Buyer_Employee_Id = E.Employee_Id)
                 Where note_1 is not null) BYR 
            on (byr.department_id = dept.department_id)
      ) sub 
on (e.employee_id = sub.dmm_id or e.employee_id = sub.byr_id)
where  e.business_unit_id = '30' and
       sub.department_id is not null and
upper(e.note_1) <> upper('Used for POM Default') )
union
(select 
to_number(line.level_id) employee_id,
line.description employee_name,
'LOB' note_1,
department_id

from 
  (select description, level_id from merchandising_levels where sub_type = '10') div
left join 
  (select level_id, mlvl_level_id, description from merchandising_levels where sub_type = '20') line on (div.level_id = line.mlvl_level_id)
left join
  (select  department_id, description, mlvl_level_id from departments where mlvl_sub_type = 20) dept on (line.level_id = dept.mlvl_level_id)
where department_id is not null
  )  ) base
where not exists (select 1 from cns_merchant_exclusion where base.employee_id = employee_id)
  Order By Note_1, Employee_Id, Department_Id

"
;

}

sub make_record {
    my ($self,$emp_id, $name,  $sub_type, $dept_id ) = @_;
    my $obj =
        MCCS::CMS::RecordLayout::MerchantRecord->new( filehandle => $self->{'util'}->{'filehandle'} )
        ->set(
        {   MerchantNumber     => $emp_id,
            MerchantName       => substr($name,0,30),
            MerchantDesignator => substr($sub_type, 0,10),
            Department         => $dept_id,
            BuyerNumber        => $emp_id,
            
        }
      );
    return $obj->to_string();

}

sub get_filename {
    my $self = shift;
    my $date = shift;
return $date . '_Merchant.dat';

}


1;

__END__

=pod

=head1 NAME

MCCS::CMS::Loads::MERCHANT - MCCS::CMS MERCHANT (Buyer) record extract

=head1 SYNOPSIS

MCCS::SAS::MERCHANT->new( MCCS::CMS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the CMS Merchant Load.  Pulls employee information 
This is a daily/weekly tracking load.

=head1 AUTHOR

Larry Lewis  L<larry.d.lewis@usmc-mccs.org|mailto:larry.d.lewis@usmc-mccs.org>

=cut
