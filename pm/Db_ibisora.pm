package MCCS::Db_ibisora;

use IBIS::DBI;
use Carp;
use Sys::Hostname;
use strict;
use base qw(DBI::db);
use Data::Dumper;
my $pkg = __PACKAGE__;

BEGIN {

    # This BEGIN block makes this module host aware. Various environment
    # variables can be set based on the host. The "or equals" allows an
    # existing environment to override these settings.

    my $hostname = hostname();    # Set the hostname

    $ENV{ORACLE_HOME} ||= '/usr/local/mccs/lib/oracle/product/10.2.0/client';
    $ENV{TNS_ADMIN}   ||= '/usr/local/mccs/etc';

} ## end BEGIN

use constant DEBUG => 0;

my $error_handler = sub {
    my ( $p, $f, $l, $s ) = caller(0);
    print "<table width='80%' class='e832_table_nh'>";
    print "<tr><th colspan=2>MCCS/Db_ibisora.pm Perl DBI Error </th></tr>";
    print "<tr><td>Time</td><td>";
    print scalar localtime;
    print "</td></tr>";
    print "<tr><td>Package</td><td>$p</td></tr>";
    print "<tr><td>File</td><td>$f</td></tr>";
    print "<tr><td>Line</td><td>$l</td></tr>";
    print "<tr><td>Error String</td><td><pre>$_[0]</pre></td></tr>";
    print "</table>";
};

################################################################################
sub connect {
    my $proto = shift;

    # I can't think of any reason we would be calling this as an instance
    # method unless we wanted a clone, and I can't think of any legitimate
    # parameter that would have a reference up front.

    return undef if ref($proto);

    # This allows us to be called as either a class method or a normal sub-routine
    # and also defaults the package name if needed.

    ( unshift( @_, ($proto) ), $proto = $pkg ) unless $proto->isa($pkg);

    # Let's use named arguments
    my %args = (
                 location => '',
                 username => '',
                 password => '',
                 dsn      => '',
                 attribs  => {},
                 @_,
               );

    # Die quickly if no db name given

    croak "No database name specified in call to $pkg\:\:connect()\n"
      unless defined $args{dbname};

    # Force all arguments to lower case
    $args{dbname} = lc $args{dbname};

    my ( $dsn, $user, $pass );

    # Just allow ibisora here, I will work on it with IBIS::DB_Utils module.
    if ( $args{dbname} eq 'ibisora' ) {
        $user = $args{username} || "dunno";
        $pass = $args{password} || "dunno";
        $dsn  = $args{dsn}      || "dunno";
        print "IBISORA\n" if DEBUG;
    } else {
        carp "Cannot resolve database request\n";
    }

    my $attribs = {
                    RaiseError         => 1,
                    AutoCommit         => 1,
                    ShowErrorStatement => 1
                  };

    foreach ( keys( %{ $args{attribs} } ) ) {
        $attribs->{$_} = $args{attribs}->{$_};
    }

    # $dbh is no longer a global "my" variable.

    return undef unless my $dbh = IBIS::DBI->connect( dbname=>'ibisora', attribs => $attribs );

    $dbh->{HandleError} = $error_handler;

    # Change class name as needed to incorporate new functions.
    my $newclass = $pkg . "::" . lc( $dbh->{Driver}->{Name} );
    $proto = $newclass if $newclass->isa($pkg);

    return bless $dbh, $proto;
} ## end sub connect

sub errorhandler {
    my $self = shift;
    my $cmd  = shift;

    # Instance method or sub-routine call only!  This is not a class method.
    return undef unless ref($self) && $self->isa("DBI::db");

    return $self->{HandleError} unless defined($cmd);

    if ( ref($cmd) ) {
        return undef unless ref($cmd) eq "CODE";
        $self->{HandleError} = $cmd;
    }
    elsif ($cmd) {
        $self->{HanldeError} = $error_handler;
    }
    else {
        $self->{HandleError} = undef;
    }

    return (1);
} ## end sub errorhandler

sub db_duplicate_error {
    my $self = shift;

    return (0);
}

sub db_fields {
    my $self = shift if ref( $_[0] ) && $_[0]->isa("DBI::db");

    my $tname = ( @_ ? shift : undef );

    return undef unless $tname;

    my $sth = $self->prepare("select * from $tname where 1 < 0");
    $sth->execute();
    my $flist = $sth->{NAME_lc};

    return ($flist);
} ## end sub db_fields

sub db_insert {
    my $self = shift if $_[0]->isa("DBI::db");
    my $tname   = ( @_ ? shift : undef );
    my $datasrc = ( @_ ? shift : undef );

    return undef unless $tname;
    return undef unless ref($datasrc);

#FIXME
    print "--------------------------------------------------<br>\n";
    print "Pkg: " . __PACKAGE__ . "<br>\n";
    print "METHOD: db_insert<br>\n";
    print "TABLE: $tname<br>\n";
    if ( defined($datasrc) ) {
       print "<pre>\n";
       print Dumper $datasrc;
       print "</pre>\n";
    }
    print "<pre>\n";
    print Dumper $self;
    print "</pre>\n";
    return 0;


    my $flist = $self->db_fields($tname);
    my @vlist = ();
    my $vstr  = "";
    my $vcntr = 0;
    foreach (@$flist) {
        push( @vlist, $datasrc->{$_} );
        $vstr .= "," if $vcntr++;
        $vstr .= "?";
    }

    my $qstr = "insert into $tname values ( " . $vstr . " )";

    #print "qstr = $qstr\n";
    #print "values = ".join("|",@vlist)."\n";

    my $ret = $self->do( $qstr, undef, @vlist );

    return ($ret);
} ## end sub db_insert

sub db_unique_keys {
    my $self = shift;

    return (undef);
}

sub db_update {
    my $self = shift if $_[0]->isa("DBI::db");
    my $tname   = ( @_ ? shift : undef );
    my $datasrc = ( @_ ? shift : undef );

    return undef unless $tname;
    return undef unless ref($datasrc);

#FIXME
    print "--------------------------------------------------<br>\n";
    print "Pkg   : " . __PACKAGE__ . "<br>\n";
    print "METHOD: db_update<br>\n";
    print "TABLE : $tname<br>\n";
    if ( defined($datasrc) ) {
       print "<pre>\n";
       print Dumper $datasrc;
       print "</pre>\n";
    }
    print "<pre>\n";
    print Dumper $self;
    print "</pre>\n";
    return 0;

    my $uklist = $self->db_unique_keys($tname);
    die "No UNIQUE keys in $tname, update aborted" unless scalar(@$uklist);

    my @ukvlist = ();
    my $ukcntr  = 0;
    my $ukstr   = "";
    my %uknames;
    foreach (@$uklist) {
        push( @ukvlist, $datasrc->{$_} );
        $ukstr .= " and " if $ukcntr++;
        $ukstr .= "$_ = ?";
        $uknames{$_} = 1;
    } ## end foreach (@$uklist)

    my $flist = $self->db_fields($tname);
    my @vlist = ();
    my $vstr  = "";
    my $vcntr = 0;
    foreach (@$flist) {
        next if defined( $uknames{$_} );
        push( @vlist, $datasrc->{$_} );
        $vstr .= "," if $vcntr++;
        $vstr .= "$_ = ?";
    } ## end foreach (@$flist)

    my $qstr = "update $tname set $vstr where $ukstr";

    if ( $tname eq 'bank_filetransfers' ) {
        open my $fh, '>>/usr/local/mccs/log/getapack_dbupdate.log';
        print $fh scalar( localtime() ),
          "\nSQL: [", $qstr, "]",
          "\nVLIST: ", join( ';', @vlist ),
          "\nUKVLIST:", join( ';', @ukvlist ), "\n";
        close($fh);
    } ## end if ( $tname eq 'bank_filetransfers')

    #print "qstr: $qstr\n";

    my $ret = $self->do( $qstr, undef, ( @vlist, @ukvlist ) );

    return ($ret);

} ## end sub db_update

sub db_delete {
    my $self = shift if $_[0]->isa("DBI::db");
    my $tname   = ( @_ ? shift : undef );
    my $datasrc = ( @_ ? shift : undef );

    return undef unless $tname;
    return undef unless ref($datasrc);

#FIXME
    print "--------------------------------------------------<br>\n";
    print "Pkg: " . __PACKAGE__ . "<br>\n";
    print "METHOD: db_delete<br>\n";
    print "TABLE: $tname<br>\n";
    if ( defined($datasrc) ) {
       print "<pre>\n";
       print Dumper $datasrc;
       print "</pre>\n";
    }
    print "<pre>\n";
    print Dumper $self;
    print "</pre>\n";

    return 0;

    my $uklist = $self->db_unique_keys($tname);
    die "No UNIQUE keys in $tname, delete aborted" unless scalar(@$uklist);

    my @ukvlist = ();
    my $ukcntr  = 0;
    my $ukstr   = "";
    my %uknames;
    foreach (@$uklist) {
        push( @ukvlist, $datasrc->{$_} );
        $ukstr .= " and " if $ukcntr++;
        $ukstr .= "$_ = ?";
        $uknames{$_} = 1;
    } ## end foreach (@$uklist)

    my $qstr = "delete from $tname where $ukstr";

    #print "qstr: $qstr\n";

    my $ret = $self->do( $qstr, undef, (@ukvlist) );

    return ($ret);
} ## end sub db_delete

# This is a nice short-cut.  It probably won't be used by anyone but me
# but I am lazy enough to leave here now that I have it.

sub db_genaccessors {
    my $self     = shift;
    my $tname    = shift;
    my $classobj = shift;

    # Creating closures, or other variables for that matter, in another
    # namespace is probably cheating, but if we are going to be lazy let's
    # go all the way.
    my $pkgname = ref($classobj);

    # The lexical is needed explicitly for a closure, otherwise we use $_.
    foreach my $attr ( @{ $self->db_fields($tname) } ) {
        next if $classobj->can($attr) || substr( $attr, 0, 1 ) eq "_";
        no strict 'refs';
        *{"$pkgname\::$attr"} = sub {
            my $self = shift;
            return undef unless ref($self);
            return ( @_ ? $self->{$attr} = shift : $self->{$attr} );
          }
    } ## end foreach my $attr ( @{ $self...})

} ## end sub db_genaccessors

1;

__END__

=pod

=head1 NAME

MCCS::Db_ibisora - Connect to ibisora Database

=head1 SYNOPSIS

   use MCCS::Db_ibisora;

   # Turn off error handling and reporting
   my %attribs = (
                     PrintError => 0,
                     RaiseError => 0,
                 );

   $dbh = MCCS::Db_ibisora::connect(
                              dbname   => 'database',
                              username => 'username',
                              password => 'password',
                              location => 'location',
                              dsn      => 'customdsn',
                              attribs  => \%attribs,
                            );

   my $flist = $dbh->db_fields($tname);
   my $rowcnt = $dbh->db_insert($tname,$datasrc);
   my $rowcnt = $dbh->db_update($tname,$datasrc);
   my $rowcnt = $dbh->db_delete($tname,$datasrc);
   my $boolean = $dbh->db_duplicate_error();

=head1 DESCRIPTION

"MCCS::Db_ibisora" provides a cleaner, simpler interface for connecting to databases within B<MCCS>.

"MCCS::Db_ibisora" is implemented at the top of the MCCS Perl namespace. This module enables the user to develop modules that seamlessly connect to databases without having to provide variable database connection parameters in every module. It also provides for a centrally administered method of connection to those databases. For example, user names, passwords, and DSNs are defined once and can be used anywhere.

This module offers a primary function called connect() to provide access to MCCS databases. It is the programmer's responsibility to know the database name of the desired database. A required parameter called 'dbname' must be passed in the call to connect(). Other parameters may be specified as necessary or overridden. See the B<Examples> section for usage.

There is no need to include DBI in your modules as it should be replaced by MCCS::Db_ibisora. See B<EXAMPLES>. Other than this change, your code will look and act as normal. Also, you do not need to include any DBD::XXX modules in your Perl programs.

Error Handling

If you have a need to suppress error handling and printing, pass a hash reference in your call to MCCS::Db_ibisora::connect. Set PrintError to 0 and/or RaiseError to 0 to disable error handling. See the SYNOPSIS for an example.

=head1 Database Connections

POS database applications require a "location" parameter to be passed in order to connect to the correct field POS server.

Usernames and passwords may be overridden in the connect() call. See I<Examples>.

Environment variables are set in this module to support Oracle and ODBC driver requirements.

   ORACLE_HOME = <path to Oracle libraries>
   ODBCINI     = <location of odbc.ini>
   ODBCINSTINI = <location of odbcinst.ini>

These environment variables can be overridden in your own code, however, they are set within this module for convenience. Concievably, you shouldn't need to worry about setting any of these variables. Just use the MCCS::Db_ibisora module and specify the database you want to connect to.

=head1 FUNCTIONS

=over 4

=item connect ( )

The connect() function returns a database handle for the requested database. connect() takes up to four parameters to customize your database connectivity. Default values are set for username, password, and dsn but, can be overriden using named parameters.

Returns a dbh that has be sub-classed into a database specific form of MCCS:Db if supported otherwise MCCS::Db_ibisora.  For example, connect("ibis") will return an object of type MCCS::Db_mysql but connect("axsone") will return an object of type MCCS::Db as Oracle is not fully supported yet.  This ensures that the methods db_update and db_delete will fail when unable to determine unique keys for a given table.

=back

=head1 DATABASE CONNECTORS

=over 4

=item ibis

IBIS database.

=item rectrac

RecTrac database.

=item mrx

MRX database.

=item axsone

AXS-One database.

=item essentus

Essentus database.

=item epos

POS databases. This connector also requires a location that matches the ODBC DSN for that site. For example Albany would have a location of ALM.

=item svs

SVS database.

=back

=head1 Methods

=item db_fields()

   # Called in OO mode.
   my $Db = MCCS::connect("ibis");
   my $flist = $Db->db_fields($tname);

   # Called as a normal sub-routine.
   my $flist = MCCS::Db_ibisora::db_fields($dbh,$tname);

   # Called as a class method returns undef
   my $flist = MCCS::Db_ibisora->db_fields($tname);

   foreach ( @$flist ) {
      ... do something with $_;
   }

Returns a reference to a list of column names in the table specified by C<$table>. Names are converted to lower-case automatically.  Attempts to call as a class method will return C<undef>.

=item db_insert()

   my $rowcnt = $Db->db_insert($tname,$hashref)

Returns number of rows inserted.  An insert statement for the table specified by C<$tname> is automatically generated for the data contained in the hash reference.  C<$hashref> is usually an object but can be a normal hash as well.  The values to be inserted are taken from the keys in the data source that match column names in C<$table>. (all column names are automatically converted to lower case). Any missing values will be assigned the value C<undef>, possibly causing errors of the "required field" variety.

This method eliminates the need to write specific insert statements for every object going into a table.

=item db_update()

   my $rowcnt = $Db->db_update($tname,$hashref)

Similar to C<db_insert> except that an update statement which specifies ALL unique keys in the "WHERE" clause is generated.  This method requires the presence of a method named C<db_unique_keys> in the appropriate sub-class and will FAIL without taking action if no unique keys are found.

All values for the "WHERE" clause, as well as new values for columns come from matching fields in the data source hash.

=item db_delete()

   my $rowcnt = $Db->db_delete($tname,$hashref)

Same as C<db_update> except that a delete statement is created.  The same unique key requirements apply to this method.

= item db_duplicate_error()

   eval {
      $dbh->db_insert($tname,\%values);
   };
   if ( $@ ) {
      die $dbh->errstr unless $dbh->db_duplicate_error;
   }

This method returns 1 if the value of dbh->err indicates an error caused by tyring to insert a duplicate record in a table with a unique key.  The base class method will always return 0, but db-specific versions in the appropriate sub-classes should override this.  See MCCS::Db_mysql for an example.

This method should be useful in error handling routines, especially if used in conjunction with the C<HandleError> attribute.  This assumes that you want to handle duplicate errors in some other fashion.

=head1 EXAMPLES

   use MCCS::Db_ibisora;

   my $database_name = 'axsone';
   $dbh = MCCS::Db_ibisora::connect( dbname => "$database_name" );

   $sth = $dbh->prepare($sql);

   ... your code here ...

   $sth->finish();
   $dbh->disconnect();

=head1 HISTORY

=over 4

=item '2003-xx-xx'

Module created.

=item '2003-10-01'

Moved declaration of variables outside scope of the connect() sub routine. This was causing the module to not perform cleanup on exit because it could not access the $dbh variable.

=item '2003-10-09'

Added capability to override the default DSN defined in this module.

=item '2003-10-10'

Added connector for rectrac database.

=item '2004-03-24'

Removed extinct database entries.
POS database 'location' should be specified as an MRF company code. This is because the ODBC DSN's were updated to use company codes instead of site names.

=item '2004-04-14'

Added ability to override default error handling by sending a hash reference with database handle attributes. This parameter is optional.

=item '2005-04-06'

Module is now host aware (see BEGIN block). This allows the setting of certain environment variables based on the hostname.

=item '2005-11-10 - jgb'

Changed handling of the attribs hash so that values passed in by the user are either added to or modify the default hash values instead of completely replacing the default attib hash.

Package MCCS::Db_ibisora is now a sub-class of DBI::dh.  MCCS::Db is further sub-classed into db specific packages, i.e. MCCS::Db_mysql.  These bottom sub-classes are used for methods that require specific db features or commands (such as those used to get sequence numbers or index columns).  The dbh created by connect is now re-blessed into the appropriate bottom sub-class before being returned.

Added new methods to return a list of all columns in a table, automatically generate insert, update and delete statements, and mysql specific routines to support the above.  Oracle routines are still needed.

=item '2005-11-14 jgb'

Added capability to enable, disable and reassign sub-routine for HandleError attribute.

=item '2005-11-15 jgb'

Added db_duplicate_error.

=item '2006-03-03'

Added fms_fctc target and updated tnsnames.ora.

=item '2006-03-07 jgb'

Added pos database (schema) and added grants for user application.

=back

=head1 AUTHOR (S)

Trevor S. Cornpropst B<trevor.cornpropst@usmc-mccs.org>
