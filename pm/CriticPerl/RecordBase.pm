package MCCS::CMS::RecordBase;
use strict;
use warnings;
use base qw(IBIS::NCR::Record);
use IBIS::NCR::Constants;

#Overload Field Delimiter to pipe
sub FIELD_DELIMITER {
    '|';
    return;
}

#Overload Record delimiter to carriage return + line feed
sub RECORD_DELIMITER {
    chr(13) . chr(10);
    return;
}

1;

__END__

=pod

=head1 NAME

MCCS::SAS::RecordBase - Base class representing an SAS Record

=head1 SYNOPSIS

MCCS::SAS::RecordBase extends IBIS::NCR::Record

=head1 DESCRIPTION

This class inherits most of it's capability from IBIS::NCR::Record. It overrides the field and record delimiters.

=head1 SUBROUTINES/METHODS

=over 4

=item FIELD_DELIMITER()

Overloaded to pipe delimit the fields

=item RECORD_DELIMITER()

Overloaded to carriage return and line feed delimit the records.

=back

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
