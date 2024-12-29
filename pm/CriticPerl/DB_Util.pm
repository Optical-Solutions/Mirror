package IBIS::WMS::DB_Util;

use strict;
use warnings;
use version; our $VERSION = qv('0.0.8');
use Carp;
use File::Basename;
use IBIS::SSH2CONNECT;
use IBIS::Log::File;
use File::stat;
use File::Path;
use Time::localtime;
use IBIS::DBI;
use MCCS::Config;
use Data::Dumper;
use MCCS::File::Util qw(copy_to_archive move_to_archive);
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

    } elsif ( $params{logfile} ) {

        # use logfile;
        $self->{logfile} = $params{logfile};
        $self->{log} = IBIS::Log::File->new(
                             { file => $params{logfile}, append => 1, level => 4 } );

    } else {
        $self->{log} = undef;
    }

    $self->{cfg}    = new MCCS::Config;
    $self->{dbname} = $self->{cfg}->wms_global->{DBNAME};   

    $self->{dbh} = IBIS::DBI->connect(dbname => $self->{dbname});

    $self->_prepare_sql;


    return $self;
}
#--------------------------------------------------------------------------
# Private Methods
#--------------------------------------------------------------------------
sub _prepare_sql {
    my $self = shift;

    my $sql1 = <<EOFSQL1;
select  distinct description
from    wms_item_error e, v_wms_rejected_msg v
where   e.item_id = ?
and     e.rejected_id = v.rejected_id
and     e.date_error_cleared is null
EOFSQL1

    $self->{get_error_msg_sth} = $self->{dbh}->prepare_cached($sql1);

    my $sql2 = <<EOFSQL2;
select distinct reason_id, description
from reasons
where sub_type = 'INVADJ'
EOFSQL2

    $self->{get_reason_code_lookup_sth} = $self->{dbh}->prepare_cached($sql2);
    return;
}
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
        $self->{log}->info( $pkg . ' ' . $msg );
    }
    return;
}
#--------------------------------------------------------------------------
# PUBLIC Methods
#--------------------------------------------------------------------------
sub get_free_transfer_id {
    my $self    = shift;

    my $tr_id;
    eval {
      my $func = $self->{dbh}->prepare(q{
        BEGIN
            get_free_transfer_id
            (
                :parameter1,
                :parameter2
            );
        END;
      });
      $func->bind_param(":parameter1", 9);   # Dummy 9
      $func->bind_param_inout(":parameter2", \$tr_id, 8); # bind this 8 parameter as an "inout"
      $func->execute;
    };

    if( $@ ) {
      $self->_croak("Execution of stored procedure failed because:\n\t$DBI::errstr \n$@");
    } else {
      return $tr_id;
    }
return;
}
#--------------------------------------------------------------------------
sub get_reason_code_lookup {
    my $self    = shift;

    $self->{get_reason_code_lookup_sth}->execute();
    my $h;
    while (my $ref = $self->{get_reason_code_lookup_sth}->fetchrow_hashref) {
        $h->{$ref->{reason_id}} = $ref->{description};
    }
    return $h;
}
#--------------------------------------------------------------------------
sub get_error_msg {
    my $self    = shift;
    my $item_id = shift;
    my $msg     = '';
    my %uniq;

    $self->{get_error_msg_sth}->execute($item_id);

    while ( my $a = $self->{get_error_msg_sth}->fetchrow ) {
        $msg .= $a . "   ";
    }

    return $msg;

} 

#--------------------------------------------------------------------------
sub DESTROY {
    my $self = shift;
    $self->{sth1}->finish if (defined($self->{sth1}));
    $self->{sth_get_param}->finish if (defined($self->{sth_get_param}));
    $self->{dbh}->disconnect if (defined($self->{dbh}));
    $self->_log("End-------------");
    return;
}

1;

__END__

=pod

=head1 NAME

MCCS::WMS::DB_Util - blabla

=head1 VERSION

This documentation refers to version 0.0.1.

=head1 SYNOPSIS

bla

=head1 DESCRIPTION

bla

=head1 SUBROUTINES/METHODS

=over 4

=item sss();

bla

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

Copyright (c) 2014 MCCS. All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
