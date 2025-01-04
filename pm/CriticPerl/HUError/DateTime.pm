package MCCS::CMS::DateTime;
use strict;
use warnings;
use base ("IBIS::NCR::DateTime");
sub default_pattern {
    return '%m/%d/%Y';
}
sub mdy_pattern{
#	'%m/%d/%Y';
   return '%Y%m%d';
}

sub mdy_hhmmss_pattern{
#	'%m/%d/%Y';
   return '%Y%m%d%H%M%S';
}

1;

__END__

=pod

=head1 NAME

MCCS::SAS::DateTime - Date/Time datat type for MCCS::SAS record types

=head1 SYNOPSIS

MCCS::SAS::DateTime->new( any date format supported by Date::Manip or another IBIS::NCR::DateTime object )

=head1 DESCRIPTION

Inherits from IBIS::NCR::DateTime. Defines output date/time format.

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
