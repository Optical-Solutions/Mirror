package MCCS::Storable::Table;

use strict;
use version; our $VERSION = qv('0.0.1');
use Carp;
use File::Basename;
use IBIS::Log::File;
use Time::localtime;
use IBIS::DBI;
use MCCS::Config;
use Data::Dumper;
use Storable;
use File::Path;
my $pkg = __PACKAGE__;

#--------------------------------------------------------------------------
sub new {
    my $type   = shift;
    my %params = @_;
    my $self   = {};

    bless $self, $type;

    if ( $params{IBISlog_object} ) {

        # use log object
        $self->{log} = $params{IBISlog_object};

    }
    elsif ( $params{logfile} ) {

        # use logfile;
        $self->{logfile} = $params{logfile};
        $self->{log} = IBIS::Log::File->new( { file => $params{logfile}, append => 1, level => 4 } );

    }
    else {
        $self->{log} = undef;
    }
    $self->_log("Start-----------");

    my $cfg = new MCCS::Config;
    $self->{data_d} = $cfg->StorableTable_PM->{DATA_DIR};

    #--Database name ------------------
    if ( $params{dbname} ) {
        $self->{dbname} = $params{dbname};
        $self->{dbh} = IBIS::DBI->connect( dbname => $self->{dbname} );
    }

    #--SQL-------------------------
    if ( $params{sql} ) {
        $self->{sql} = $params{sql};
    }
    else {
        $self->{sql} = undef;
    }

    #--Storable filename-------------------------

    #print map "$_ = $params{$_}\n", keys %params; # DEBUG delete this later

    # Make necessary directories
    #---------------------------
    foreach my $d ( ( $self->{data_d} ) ) {
        unless ( -d $d ) {
            mkpath($d);
        }
    }

    return $self;
}

#--------------------------------------------------------------------------
# Private Methods
#--------------------------------------------------------------------------
sub _croak {
    my $self = shift;
    my $msg  = shift;
    $self->_log($msg);
    croak $pkg . ' ' . $msg;
}

#--------------------------------------------------------------------------
sub _log {
    my $self = shift;
    my $msg  = shift;
    if ( defined( $self->{log} ) ) {
        $self->{log}->summary( $pkg . ' ' . $msg );
    }
}

#--------------------------------------------------------------------------
# Public Methods
#--------------------------------------------------------------------------
sub setsql {
    my $self = shift;
    my $sql = shift || 'select sysdate from dual';
    $self->{sql} = $sql;
}

#--------------------------------------------------------------------------
sub get {
    my $self = shift;
    my $key  = shift;

    if ( exists( $self->{$key} ) ) {
        return $self->{$key};
    }
    else {
        return undef;
    }
}

#--------------------------------------------------------------------------
sub _build_lookup {
    my $self = shift;
    foreach my $e ( @{ $self->{data} } ) {
        foreach my $column ( sort keys %{$e} ) {
            $self->{lookup}->{$column}->{ $e->{$column} } = 1;
        }
    }
}

#--------------------------------------------------------------------------
sub store_file {
    my $self         = shift;
    my $filename     = shift || $self->_croak('Must supply storable filename.');
    my $data_file    = $self->{data_d} . '/' . $filename;
    my $table_column = $self->{data_d} . '/' . $filename . '_cols';
print ("data file is $data_file \n");
exit;
    unless ( defined( $self->{sql} ) ) {
        $self->_croak("Must setsql('select field from table') first!");
    }
    unless ( defined( $self->{dbh} ) ) {
        $self->_croak("Must Supply dbname in named params. Ex dbname=>rms_p");
    }

    $self->{sth} = $self->{dbh}->prepare( $self->{sql} ) || $self->_croak("Could not prepare sql!");
    $self->{sth}->execute;

    #------------------------
    # Acquire the column name
    #------------------------
    $self->{columns} = $self->{sth}->{NAME_lc};    # ref of array

    my @arr = ();
    while ( my $row = $self->{sth}->fetchrow_hashref() ) {
        push( @arr, $row );
    }

    $self->{sth}->finish;

    store( \@arr, $data_file );                    # Store them locally at data_d dir.
                                                   #print Dumper \@arr;

    store( $self->{columns}, $table_column );      # Store them locally at data_d dir.
    $self->_build_lookup;
    $self->{filename} = $filename;
}

#--------------------------------------------------------------------------
sub retrieve_file {
    my $self         = shift;
    my $filename     = shift || $self->_croak('Must supply storable filename which you want to read.');
    my $data_file    = $self->{data_d} . '/' . basename($filename);
    my $table_column = $self->{data_d} . '/' . basename($filename) . '_cols';

    if ( -e $data_file ) {

        # Get the data file
        $self->{data} = retrieve($data_file);

        #print Dumper $self->{lookup};
        $self->_build_lookup;

        # Get the column name
        $self->{columns} = retrieve($table_column);

    }
    else {
        $self->_croak("$data_file missing");
    }
}

#--------------------------------------------------------------------------
sub is_valid {
    my $self = shift;
    my %args = @_;

    my $col = lc( $args{column} );    # lower case it, precautions
    my $val = $args{value};

    if ( exists( $self->{lookup}->{$col}->{$val} ) ) {
        return 1;
    }
    else {
        return 0;
    }
}

#--------------------------------------------------------------------------
# Perform the pulling file action from NFI SFTP server
#--------------------------------------------------------------------------
sub DESTROY {
    my $self = shift;
    $self->{sth1}->finish    if ( defined( $self->{sth1} ) );
    $self->{dbh}->disconnect if ( defined( $self->{dbh} ) );

    $self->_log("End-------------");
}

1;

__END__

=pod

=head1 NAME

MCCS::Storable::Table - Stores table locally.

=head1 VERSION

This documentation refers to version 0.0.1.

=head1 SYNOPSIS

    use MCCS::Storable::Table;

    # To store data
    my $in_data = MCCS::Storable::Table->new(dbname=>'rms_p', 
                                       sql=>'select style_id, site_id from styles', 
                                       logfile=>'/usr/local/mccs/log/t/mccs_storable_table.t.log'
                                       );
    $in_data->store_file('dada.sto');
     
  
    # To retrieve data
    my $out_data = MCCS::Storable::Table->new();
    $out_data->retrieve_file('dada.sto');

    # Check the legitimate style_id
    $out_data->is_valid(column=>'style_id', value=>'00000003323');
    $out_data->is_valid(column=>'site_id', value=>'03323');

=head1 DESCRIPTION

Dont want to go to DB as often?  Yes, you can.  Make a storable file.
Please be sure to refresh your storable often.  A cron should be handy.
Basically, this PM store your query result in a storable perl file and 
instead of go to db to query as many as you need, we can just look it up there.

=head1 METHODS

=over 4

=item new( dbname=>'', sql=>'', logfile=>'');

Constructors.

=item store_file( $filename );

Store the query into storable file. Pass in only filename, no need path.

=item retrieve_file( $filename );

Get the query from storable file. Just a filename, no need to use full path.

=item is_valid(column=>'style_id', value=>'00000003323');

Take 2 named params, colums, and value;

=back

=head1 CONFIGURATION AND ENVIRONMENT

None.

=head1 INCOMPATIBILITIES

None.

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to RDI Staff L<rdistaff@usmc-mccs.org|mailto:rdistaff@usmc-mccs.org>.
Patches are welcome.

=head1 AUTHORS

Hanny Januarius

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2015 MCCS. All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
