package MCCS::MCE::Util;

use strict;
use warnings;
#use version; our $VERSION = qv('0.0.8');
use Carp;
use JSON;

my $pkg = __PACKAGE__;

#--------------------------------------------------------------------------
our (@EXPORT_OK, %EXPORT_TAGS);

  # put your object methods here
@EXPORT_OK = qw(   
    get_secret 
);

%EXPORT_TAGS = (
    ALL => [ @EXPORT_OK ],
    );

#--------------------------------------------------------------------------
sub new { ## no critic qw(Subroutines::RequireArgUnpacking)
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

    # print map "$_ = $params{$_}\n", keys %params; # DEBUG delete this later
    # use Data::Dumper;
    # print Dumper $self;

    return $self;
}
#--------------------------------------------------------------------------
sub _croak {
    my $self = shift;
    my $msg  = shift;
    print($msg);
    croak $pkg . ' ' . $msg;
}

#--------------------------------------------------------------------------
sub _log {
    my $self = shift;
    my $msg  = shift;
    print( $pkg . ' ' . $msg );
    return;
}

#--------------------------------------------------------------------------
sub DESTROY {
    my $self = shift;
    print("End-------------");
    return;
}
#--------------------------------------------------------------------------

    # Put your methods here.

sub get_secret {
    my $self = shift;
    my $secret_var_name = shift;
    my $hash_ref;

    # Retrieve the value from the secret
    my $secret_var_value_json = $ENV{$secret_var_name};  # THIS IS JSON string

    my $json = JSON->new->allow_nonref;

    if (defined $secret_var_value_json) {
       $hash_ref = $json->decode( $secret_var_value_json );
       $self->{secret_hash}= $hash_ref;
    } else {
       warn("W A R N I N G! \nSecret variable $secret_var_name is not set.\n");
    }
    return $hash_ref;
}
#--------------------------------------------------------------------------
sub print_secret {
    my $self = shift;
    my $secret_var_name = shift;
    my $hash_ref;

    $self->get_secret($secret_var_name);
    print "--- Secret Name = $secret_var_name ----------------------------------\n";
    foreach my $key (sort keys %{$self->{secret_hash}}) {
      my $value = $self->{secret_hash}->{$key};
      if ($key =~ m/^(pw|pass)/i )  {
	      $value =~ s/./#/g;
      }
      print "$key = $value\n";
    }
    #print "--------------------------------------------------------------------- END\n";
    return;
}
#--------------------------------------------------------------------------


1;

__END__

=pod

=head1 NAME

MCCS::MCE::Util - Desc

=head1 VERSION

This documentation refers to MCCS::MCE::Util 

=head1 SYNOPSIS

    use MCCS::MCE::Util;
    my $object = MCCS::MCE::Util->new();
    my $hash_ref = $object->get_secret('SPS-DLA');  # Get SPS DLA secret information

    $object->print_secret('SPS-DLA');  # will print SPS-DLA stuff

=head1 DESCRIPTION

Various short cuts, for working on MCE dev env.

=head1 SUBROUTINES/METHODS

=over 4

=item get_secret(<secret_name>);

returns a hashref containing all the information for the secret, ie host, username, password, port, etc
This secret token will need to be set up by MCE staff.

=item print_secret(<secret_name>);

prints into standard out of the secret information

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

Copyright (c) 2025 MCCS. All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
