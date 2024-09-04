package IBIS::DBI;

use warnings;
use strict;

use version; our $VERSION = qv('0.0.1');

use base qw(DBI);

use Carp;
use IBIS::Config::DBI;
use IBIS::Crypt;

my ( $dbs, $enc );


#2/6/2008 - ERS - BE ADVISED: This module will only read the dbi.conf at compile time.
#Under mod_perl, that will happen once per Perl interpreter. Therefore any change to dbi.conf will require a restart of the web server
#to ensure that this information reads the updated version. My guess would be that this is done for performance reasons.
BEGIN
{
    # Initialize the $dbs hash and environment from XML configuration file
    my $cfg = IBIS::Config::DBI->new()
        or croak 'Could not create configuraton in ', (caller(0))[3];

    # Set up %ENV (mostly for ORACLE)
    $cfg->environment();

    $dbs = $cfg->databases();

    $enc = IBIS::Crypt->new()
        or croak 'Could not create encryption object in ', (caller(0))[3];

    $enc->keyset('simple');
}

# Override the DBI->connect constructor
sub connect    ## no critic
{
    my ( $class, %args ) = @_;

    # Die quickly if no database name given
    croak 'No database name specified in call to ', (caller(0))[3]
        unless defined $args{dbname};

    # Force all arguments to lower case
    $args{dbname} = lc $args{dbname};

    unless ( defined $dbs->{ $args{dbname} } )
    {
        carp 'Unrecognized database in ', (caller(0))[3];
        return;
    }

    print "Connecting to $args{dbname}\n" if $args{debug};

    my $attribs = { RaiseError => 1, AutoCommit => 1, PrintError => 0 };

    # Allow default attributes to be overridden
    for ( keys %{ $args{attribs} } )
    {
        $attribs->{$_} = $args{attribs}->{$_};
    }

    # No point in going past here if we don't get what we want
    my $dbh = $class->SUPER::connect(
        $args{dsn}      || $dbs->{ $args{dbname} }{dsn},
        $args{username} || $dbs->{ $args{dbname} }{username},
        $args{passname} || $enc->decrypt( $dbs->{ $args{dbname} }{password} ),
        $attribs,
        ) or return undef;

    # Defaults to lower case names in functions that return hashes
    $dbh->{FetchHashKeyName} = 'NAME_lc';

    # Set date format if requested
    if($args{date_format})
    {
        if($dbh->{Driver}->{Name} =~ m/^oracle$/imosx)
        {
            $dbh->date_format($args{date_format});
        }
        else
        {
            croak "date_format unsupported for this $dbh->{Driver}->{Name} in ", (caller(0))[3]; 
        }
    }

    return $dbh;
}

sub databases
{
    return sort keys %{$dbs};
}

#---------------------------------------------------------------------------
# The following package definitions are required to properly subclass DBI.
# If we wanted to override more DBI methods, they would be defined in the
# appropriate packages below. Perl::Critic does not like to define more than
# one package per file but DBI has a different opinion on what it expects
# when you subclass from it.

## no critic

package IBIS::DBI::db;
use base qw(DBI::db);

sub databases
{
    my ($self) = @_;

    return sort keys %{$dbs};
}

sub date_format
{
    my ($self, $format) = @_;

    if($self->{Driver}->{Name} =~ m/^oracle$/imosx)
    {
        $self->do(qq{alter session set nls_date_format = '$format'});
        return 1;
    }
    else
    {
        return;
    }
}

#---------------------------------------------------------------------------
package IBIS::DBI::st;
use base qw(DBI::st);

1;

__END__

=pod

=head1 NAME

IBIS::DBI - IBIS Sub-class of Tim Bunce's Perl DBI module

=head1 VERSION

This documentation refers to IBIS::DBI version 0.0.1.

=head1 SYNOPSIS

    my $dbh = IBIS::DBI->connect(dbname => 'ibis')
        or die "Can't connect\n";

    my @dbs = $dbh->databases();

    my $sth = $dbh->prepare($sql);

=head1 DESCRIPTION

B<IBIS::DBI> is a sub-class of Perl DBI. It creates a fully functional DBI object of type IBIS::DBI::db. IBIS::DBI is configured by the IBIS::Config::DBI module which reads its configuration from dbi.conf in the IBIS B<etc> directory. The configuration module sets the environment for things like ORACLE_HOME and any other variables that are needed.

This module defaults $dbh->{FetchHashKeyName} to 'NAME_lc'. All database calls that return data in a hash structure will have lower case keys for field names. 

RaiseError and AutoCommit are on by default. PrintError is false to enable error handling at the developer's descretion. See the constructor description for overriding connection attributes.

=head1 SUBROUTINES/METHODS

=over 4

=item connect()

Object constructor. This modified version requires only one parameter, 'dbname'. This must be a valid database name from the configuration file or the constructor will carp and return undef. You may override any of the standard DBI->connect arguments with named parameters.

    # Override the standard username and password for
    # the IBIS database

    my $dbh = IBIS::DBI->connect(
        dbname   => 'ibis',
        username => 'username',
        password => 'password'
    );

The dbname parameter must be a valid configured database name. See databases() for how to get a list of configured names.

The IBIS::DBI constructor also allows the date format for the database handle to be set. Use the 'date_format' parameter to specify a valid date format string. This is only valid for connections to Oracle databases. Ex:

    my $dbh = IBIS::DBI->connect(
        dbname      => 'xxx',
        date_format => 'YYYY-MM-DD'
    );

=item databases()

Returns a list of configured databases. As a convenience and to address a chicken and egg problem, you may call IBIS::DBI::databases in list context to get a list of all databases.

    my @dbs = IBIS::DBI::databases();

=item date_format()

Set the date format for the current database handle. This method is only valid for Oracle database connections. You must specify a valid date format string.

    $dbh->date_format('YYYY-MM-DD');

=back

=head1 DIAGNOSTICS

=over 4

=item Could not create configuration in IBIS::DBI  BEGIN

This IBIS::Config::DBI object was unable read or find the xml configuration file. This file is usually /usr/local/IBIS/etc/dbi.conf and /usr/local/IBIS/data/xml/schema/dbi.xsd.

=item Could not create encryption object in IBIS::DBI  BEGIN

There was an error creating an IBIS::Crypt object for decoding of passwords. See IBIS::Crypt.

=item No database name specified in call to IBIS::DBI->connect()

The constructor call failed to pass a dbname parameter.

=item Unrecognized database in IBIS::DBI->connect()

There was a dbname parameter in the constructor call but the database is not configured in dbi.conf.

=item Connecting to 'database name'

Debugging output.

=back

=head1 CONFIGURATION AND ENVIRONMENT

This module is configured by IBIS::Config::DBI. See perldoc. The IBIS::Config::DBI module also handle setting the environmental variables to run Oracle or ODBC as needed.

The DBI handle is sets RaiseError, AutoCommit, and ShowErrorStatement to true.

A special HandleError routine is defined to 'pretty print' the database error and provide more useful information in debugging.

=head1 DEPENDENCIES

    Carp
    DBI
    IBIS::Config::DBI
    IBIS::Crypt

=head1 INCOMPATIBILITIES

None.

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to Trevor S. Cornpropst (B<tcornpropst@acm.org>)
Patches are welcome.

=head1 AUTHOR

Trevor S. Cornpropst B<tcornpropst@acm.org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2006 Trevor S. Cornpropst. All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut

