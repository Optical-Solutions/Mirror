
#Base class for NCR records
package IBIS::NCR::Record;
use Carp;
use strict;
use warnings;
use Storable qw(dclone);

use constant DATA_KEY         => '__fields';     ## no critic qw(ValuesAndExpressions::ProhibitConstantPragma)
use constant MAP_KEY          => '__field_map';  ## no critic qw(ValuesAndExpressions::ProhibitConstantPragma)
use constant ADMIN_KEY        => '__setup';      ## no critic qw(ValuesAndExpressions::ProhibitConstantPragma)

my %record_cache = ();

sub FIELD_DELIMITER { ','; } ## no critic qw(Subroutines::RequireFinalReturn)
sub RECORD_DELIMITER { "\n"; } ## no critic qw(Subroutines::RequireFinalReturn)
sub TEXT_VALUE{ $_[1]; } ## no critic qw(Subroutines::RequireFinalReturn Subroutines::RequireArgUnpacking)
sub TEXT_VALUE_OBJECT{ $_[1]; } ## no critic qw(Subroutines::RequireFinalReturn Subroutines::RequireArgUnpacking)

sub new { ## no critic qw(Subroutines::RequireFinalReturn Subroutines::RequireArgUnpacking)
    my $class = shift;
    my $self = bless( { DATA_KEY() => [] }, $class );
    $self->_init(@_);
    $self;
}

#added to make overloading this object simpler
sub _init { ## no critic qw(Subroutines::RequireArgUnpacking Subroutines::RequireFinalReturn)
    my $self  = shift;
    my $class = ref($self);

    my $args;

    #parameters can be hash or hashref
    if ( ref( $_[0] ) eq 'HASH' ) {
        $args = $_[0];
    }
    else {
        my %t = @_;
        $args = \%t;
    }

    my @fields;
    if ( $record_cache{$class} ) {
        @fields =
            @{ dclone( $record_cache{$class} )
            }; #gotta clone it again other wise this object instance will modify the cache
    }
    else {
        @fields = $self->def();
        $record_cache{$class} = dclone( \@fields )
            ;    #gotta clone it, otherwise this object instance will modify my cache
    }

    $self->{ DATA_KEY() } = \@fields;
    my $index = 0;
    my %uniq;
    my %field_map =
        map { $uniq{ $_->get_description() }++; ( $_->get_description(), $index++ ); }
        @fields;
    if ( grep { $_ > 1 } values(%uniq) ) {
        croak("Field descriptions must be unique");
    }
    $self->{ MAP_KEY() } = \%field_map;    #convience map to more quickly find my Field objects

    my $delim = $self->FIELD_DELIMITER();
    $args->{'field_delimiter_check'} = qr($delim);
    if ( %{$args} ) { $self->{ ADMIN_KEY() } = $args; }

    $self;
}

sub to_string { ## no critic qw(Subroutines::RequireArgUnpacking Subroutines::RequireFinalReturn)
    my $self = shift;
    my $out;
     
        my @tmp_array = map { $self->TEXT_VALUE( $self->TEXT_VALUE_OBJECT($_)->get() ) } $self->fields() ;
        foreach my $z (@tmp_array) {
            unless(defined($z)) { $z = "";}
        }
        $out  = join( $self->FIELD_DELIMITER(), @tmp_array );
     
    my $fh   = $self->{ ADMIN_KEY() }->{'filehandle'};
    
    if ($fh && fileno($fh)) {  #TODO Added this line + 5, if not needed and uncomment 6 lines after that.
        print $fh $out, $self->RECORD_DELIMITER();
        print STDERR "bless Mary\n"; #TODO remove
        return;
        } else {
           return $out;
    }
    # if ($fh) {
    #     print $fh $out, $self->RECORD_DELIMITER();
    # }
    # else {
    #     $out;
    # }
}

sub get { ## no critic qw(Subroutines::RequireArgUnpacking Subroutines::RequireFinalReturn)
    my $self = shift;
    $self->_get( $_[0] );
}

sub _get { ## no critic qw(Subroutines::RequireArgUnpacking Subroutines::RequireFinalReturn)
    my $self = shift;
    my $idx  = $self->{ MAP_KEY() }->{ $_[0] };
    if ( defined $idx ) {
        $_[1] ? $self->{ DATA_KEY() }->[$idx] : $self->{ DATA_KEY() }->[$idx]->get();
    }
    else {
        croak("Unknown field $_[0]");
    }
}

sub set { ## no critic qw(Subroutines::RequireArgUnpacking Subroutines::RequireFinalReturn)
    my $self = shift;
    my %fv;
    if ( ref( $_[0] ) eq 'HASH' ) {
        %fv = %{ $_[0] };
    }
    else {
        %fv = @_;
    }
    while ( my ( $f, $v ) = each(%fv) ) { $self->_set( $f, $v ); }
    $self;
}

sub _set { ## no critic qw(Subroutines::RequireArgUnpacking Subroutines::RequireFinalReturn)
    my $self = shift;
    my $obj  = $self->_get( $_[0], 1 );
    my $val  = $_[1];
    my $field_delimiter_check = $self->{ ADMIN_KEY() }->{'field_delimiter_check'};

    if(! ref($val) && $field_delimiter_check){
        if ( defined($val) && ( $val =~ m/\d\d\d,\d\d\d/) ) {
    # Dont strip dept_id delimiter with comma

        # RWS dept_id can contains comma
        # if I dont put this, the flat file will be \d\d\d\d\d\d (this is bad)
        # instead of \d\d\d,\d\d\d
        # hopefully RWS will take the extra comma in the flat file as data value
        # rather than treating is as a delimiter.
        } else {
           $val =~ s/$field_delimiter_check//g if defined($val);    #strip delimiters from data... but don't try to strip strings from things that are not a scalar
        }
    }
    $obj->set($val);
    $self
        ; #return self so that we can chain the sets together... i.e. $obj->set()->set()
}

sub fields { ## no critic qw(Subroutines::RequireFinalReturn)
    my $self = shift;
    @{ $self->{ DATA_KEY() } };
}

1;

__END__

=pod

=head1 NAME

IBIS::NCR::Record - Base class representing an NCR Record

=head1 SYNOPSIS

IBIS::NCR::Record is an abstract class representing any NCR Record.

=head1 DESCRIPTION

IBIS::NCR::Record contains most of the IBIS::NCR::Field handling for any type of record.
It is expected to by subclassed and extended by the subclass providing the sub def that defines the field types that are contained in this record.
Logically, this class would represent one line in an NCR data file.

=head1 SUBROUTINES/METHODS

=over 4

=item new()

Usually called during contruction of a subclass to IBIS::NCR::Record. This constructor is expecting to find an implmented subroutine 'def' that returns a list of
IBIS::NCR::Field that specify the fields for this type of record.

=item get(field name)

Proxy routine that finds the correct IBIS::NCR::Field in the record (by description) and calls its get() routine. 

=item set(field name, value)

Proxy routine that finds the correct IBIS::NCR::Field in the record (by description) and calls its set() routine with the supplied value.

=item to_string()

Returns a single scalar reprentation of the record.

=item fields()

Returns the field list for the record.

=item FIELD_DELIMITER()

Returns the field delimiter used to separate a text version of the record. Defaults to comma. Override to change.

=item RECORD_DELIMITER()

Returns the record delimiter used to separate records when using the to_string() method. Defaults to new line. Override to change.

=item TEXT_VALUE_OBJECT()

Receives the IBIS::NCR::Field object before it is passed onto TEXT_VALUE().
This is guaranteed to be called right before TEXT_VALUE() and allows interogation of the field object.
The purpose of this routine could be either to modify the IBIS::NCR::Field object or read from it for further processing by TEXT_VALUE().
This routine is expected to return the object it was given.

=item TEXT_VALUE()

Receives the text value of the IBIS::NCR::Field. Returns the value of a field manipulated by this routine. Defaults to no manipulation.
Override to change.
The purpose could be to wrap the value in double quotes for proper CSV formatting (or any other value manipulations that might be necessary).

=back

=head1 DEPENDENCIES

=over

None

=back

=head1 AUTHOR

Eric Spencer L<spencere@usmc-mccs.org|mailto:spencere@usmc-mccs.org>

=cut


Thank you  
Kaveh Sari, Contractor
Email:kaveh.sari@usmc-mccs.org | Dial: 703 888 7534 

