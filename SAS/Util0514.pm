#Utilities for SAS Planning and Assortment

package MCCS::SAS::Util;

use strict;
use warnings;
use Date::Manip;
use Readonly;

our $NOW;
Readonly::Scalar $NOW => UnixDate(ParseDate('now'), "%q");  

use MCCS::SAS::InventoryRecord;
use MCCS::SAS::OnOrderRecord;
use MCCS::SAS::DateTime;

sub new {
    my ( $class, $database, $filename ) = @_;

    my $filehandle;
    my $is_stdout = 0;

    # Open output file
    if ( $filename eq '-' ) {
        $filehandle = \*STDOUT;
        $is_stdout  = 1;
    } else {
        open( $filehandle, '>', $filename )
            or die "Cannot open '$filename': $!";
        close $filehandle;  #TODO closing to make critic happy.
    }

    my $db = IBIS::DBI->connect( dbname => $database )
        or die "Cannot open Database $database";
    $db->{'AutoCommit'} = undef;

    return bless(
        {
            db         => $db,
            #filehandle => $filehandle,  #TODO is there a reason to keep this 
            filename   => $filename,
            is_stdout  => $is_stdout
        },
        $class
    );
}

# New method to get a filehandle when needed
sub _get_filehandle {
    my $self = shift;
    
    if ($self->{is_stdout}) {
        return \*STDOUT;
    }
    
    open(my $fh, '>>', $self->{filename}) 
        or die "Cannot open '$self->{filename}': $!";
    return $fh;
}

sub _merchandise_level_record {
    my ($self, $id, $name) = @_;
    
    die "Missing required parameters" unless defined $id && defined $name;
    $name =~ s/[^[:print:]]//g;
    
    my $fh = $self->_get_filehandle();
    my $obj = MCCS::SAS::MerchandiseLevelRecord->new(filehandle => $fh);
    
    $obj->set({
        mast_userid => $id,
        name        => $name
    });
    
    unless ($self->{is_stdout}) {
        close $fh or warn "Could not close file: $!";
    }
    
    return $obj;
}

# Other methods remain the same but should use _get_filehandle() when needed
sub set_merch_year {
    my ($self, $year) = @_;
    return $self->{merch_year} = $year;
}

sub set_merch_week {
    my ($self, $week) = @_;
    return $self->{merch_week} = $week;
}

sub get_database {
    my $self = shift;
    return $self->{db};
}

sub finish {
    my $self = shift;
    return unless ref $self;
    
    if ($self->{db}) {
        $self->{db}->disconnect;
    }
    return;
}

sub DESTROY {
    my $self = shift;
    if (ref $self && $self->can('finish')){
        $self->finish();
    }
    return;
}

1;  # Module must end with true value



__END__

=pod

=head1 NAME

MCCS::SAS::Util - Utliity methods for SAS data extract

=head1 SYNOPSIS

my $util_obj = MCCS::SAS::Util->new( DBI, filename ); 

=head1 DESCRIPTION

Various utility methods for the MCCS::SAS hierarchy

=head1 SUBROUTINES/METHODS

=over 4

=item set_merch_year()

Set the merch year of interest in this MCCS::SAS

=item set_merch_week()

Set the merch week of interest in this MCCS::SAS

=item get_database()

Get the database of thie MCCS::SAS

=back

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut
