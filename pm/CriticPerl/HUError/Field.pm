package MCCS::CMS::Field;
use base qw(IBIS::NCR::Field);
use strict;
use warnings;

sub convert_money{
	my $self = shift;
	my $value=shift;
	return $value;
}

1;

__END__

=pod

=head1 NAME

MCCS::SAS::Field - Class for a SAS Field object

=head1 SYNOPSIS

MCCS::SAS::Field basically inherits from IBIS::NCR::Field overloading the convert_money routine

=head1 DESCRIPTION

MCCS::SAS::Field basically inherits from IBIS::NCR::Field overloading the convert_money routine

=head1 SUBROUTINES/METHODS

=over 4

=item convert_money()

Overloaded from IBIS::NCR::Field to keep decimal format intact.

=back

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
