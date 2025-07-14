package MCCS::SAS::Loads::DEPARTMENT;
use strict;
use warnings;
use base qw(MCCS::SAS::Loads::Base);
use MCCS::SAS::MerchandiseLevelRecord;
use Readonly;
# Readonly my $SQL = "  #UNCOMMENT THIS LINE + 2 AND DELETE 5 LINES AFTER THAT IF NECESSARY
# 	select distinct department_id, upper(dept_name) from v_dept_class_subclass WHERE length(dept_name) > 0 and business_unit_id = '30'
# ";
Readonly my $SQL => q{
    select distinct department_id, upper(dept_name)
    from v_dept_class_subclass
    WHERE length(dept_name) > 0 and business_unit_id = '30'
};

sub get_sql {
    return $SQL;
}

sub make_record { ## no critic qw(Subroutines::RequireArgUnpacking)
    my $self = shift;
    my $string =  $self->{util}->_merchandise_level_record(@_)->to_string();
    warn defined($string) ? "String defined from dept: [$string]\n" : "String is undef\n";  #TODO added this line.
    return $string;
}

sub get_filename {
    return 'MERCH_4_' . time() . '.txt';
}

sub site_field    { return; }
sub week_limiting { return 0; }

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Loads::DEPARTMENT - MCCS::SAS DEPARTMENT hiearchy record extract

=head1 SYNOPSIS

MCCS::SAS::DEPARTMENT->new( MCCS::SAS::Util );

=head1 DESCRIPTION

This plugin extracts the data necessary for the department hiearchy in MCCS::SAS. This is a full load (i.e. all departments).

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
