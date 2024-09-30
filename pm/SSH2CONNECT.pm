package IBIS::SSH2CONNECT;
use Carp;
use File::Basename;
use Net::SSH2;
use IBIS::Log::File;
use strict;
use warnings;

use constant STDOUT_PARAM => '-';

my $pkg = __PACKAGE__;
#--------------------------------------------------------------------------
# Method:       IBIS::SSH2::new()
# Description:  Constructor for IBIS::SSH2 class.
# Params:       IN - anonymous hash of user attributes, see POD
#	            OUT - blessed object
#--------------------------------------------------------------------------
sub new
{
   my $type = shift;
   my %params = @_;
   my $self = {};

   bless $self, $type;  

   if ( $params{'IBISlog_object'} ) {
      # use log object
      $self->{'log'} =  $params{'IBISlog_object'};

   } elsif ( $params{'logfile'} ) {
      # use logfile;
      $self->{'logfile'}  = $params{'logfile'};

	if($params{'logfile'} eq STDOUT_PARAM){
		$self->{'log'} = STDOUT_PARAM;
	} else {
      		$self->{'log'} = IBIS::Log::File->new( { file => $params{'logfile'}, append => 1, level => 4 } ) ;
	}

   }  else {
      $self->{'log'} = undef;
   }

#handle SSH key params
	my $has_keys = 0;
    map{ if($params{$_}){
			if(! -e $params{$_}){
				$self->_croak("$_ ($params{$_}) does not exist");
			}
			$has_keys++;
			$self->{$_} = $params{$_}; 
	} } qw(private_ssh_key public_ssh_key);
	if($has_keys && $has_keys != 2){	#need both key paramters if have one
		$self->_croak("Must supply both public and private SSH keys if you supply one.");
	}

   # print map "$_ = $params{$_}\n", keys %params; # DEBUG delete this later
   # use Data::Dumper;
   # print Dumper $self;

   return $self;
}
#--------------------------------------------------------------------------
sub connect {
   my $self = shift;
   my %params = @_;

   $self->_croak("missing hostname in the param") unless( $params{'host'} );
   $self->_croak("missing user in the param") unless( $params{'user'} ) ;

   $self->{'host'}  = $params{'host'};
   $self->{'user'}  = $params{'user'};

   if ( $params{'password'} ) {

      $self->{'password'}  = $params{'password'};
      $self->_log("on PW ");

   } else {

      $self->{'password'}  = undef;

	#unless you supply your own key paths....

	unless( $self->{'public_ssh_key'} ) {

		$self->_log("PKI with rdiusr keys");

       		# Make sure it's 'rdiusr'
       		# So you use RDIUSR public key

		unless ( $self->{'user'} eq 'rdiusr' ) {
			$self->_croak("Connecting using PKI. Invalid user \"$self->{'user'}\", will not connect. You Must be rdiusr");
		}

	} else {
		$self->_log("PKI with alternative keys");
	}
   }

   my $counter = 0;
   my $object;
   while ( $counter < 10) {

      $counter++;
      $object = $self->_zconnect();

      if ( defined($object) ) {

         # This is good, we got connection
         $self->_log('Connected: ' . $self->{'user'}.'@'. $self->{'host'} );
         return $object;

      } else {

         $self->_log("Reconnect number $counter");
         sleep 5;

      }
   }
   $self->_croak("GIVE UP! After 10 attempts connecting to " . $self->{'host'} );
}

#--------------------------------------------------------------------------
sub _croak {
   my $self = shift;
   my $msg = shift;
   $self->_log($msg);
   croak $pkg . ' ' . $msg;
}
#--------------------------------------------------------------------------
sub _log {
   my $self = shift;
   my $msg = shift;
   if ( defined($self->{'log'}) ) {
	my $tmpmsg = $pkg . ' ' . $msg;
	if($self->{'log'} eq STDOUT_PARAM){
		print $tmpmsg,"\n";
	 } else {
      		$self->{'log'}->info($tmpmsg);
	}
   }
}
#--------------------------------------------------------------------------
sub _zconnect {

   my $self = shift;

   my $ssh2 = Net::SSH2->new();


   unless ( $ssh2->connect( $self->{'host'} ) ) {
          
          $self->_log("1.1 Could not connect to " . $self->{'host'} . " : " . $ssh2->error);
          $self->_log("Retrying!");

          return undef;
   }

   if ( defined($self->{'password'}) ) {
      unless ( $ssh2->auth_password( $self->{'user'}, $self->{'password'}) ) {
          $self->_log("1.2 Could not connect to " . $self->{'host'} . " using password: " . $ssh2->error);
          $self->_log("Retrying!");
          return undef;
      } else {
          $self->_log('Password auth');
      }

   } else {

	my $pub_key = $self->{'public_ssh_key'} || '/home/'. $self->{'user'} .'/.ssh/id_ecdsa.pub';
	my $priv_key = $self->{'private_ssh_key'} || '/home/'. $self->{'user'} .'/.ssh/id_ecdsa';

	#my $pub_key = $self->{'public_ssh_key'} || '/home/'. $self->{'user'} .'/.ssh/id_rsa.pub';
	#my $priv_key = $self->{'private_ssh_key'} || '/home/'. $self->{'user'} .'/.ssh/id_rsa';

        $self->_log("pub_key = $pub_key");

      unless ( $ssh2->auth_publickey($self->{'user'}, 
                                     $pub_key, 
                                     $priv_key) ) {
          $self->_log("1.3 Could not connect to " . $self->{'host'} . " using PKI: " . $ssh2->error);
          $self->_log("Retrying!");
          return undef;
      } else {
          $self->_log('PKI auth');
      }
   }
   return $ssh2;
}
#--------------------------------------------------------------------------
# Dont want destroy because nothing to clean up
# Janitor is having a vacation today!
# sub DESTROY { my $self = shift; $self->_log("End-------------"); }


1;

__END__

=pod

=head1 NAME

IBIS::SSH2CONNECT - Overload connection on SSH2 tries 10 times, pause 5 seconds while retrying

=head1 SYNOPSIS

    # Using explicit log filename
    my $object = IBIS::SSH2CONNECT->new( logfile=> '/log/mylogfile');

    # Using defined log object 
    my $object = IBIS::SSH2CONNECT->new( IBISlog_object=> $IBIS_log_object );

    # No Logging
    my $object = IBIS::SSH2CONNECT->new();

   # Use specified key pairs for PKI instead of default for rdiusr
   my $object = IBIS::SSH2CONNECT->new(public_ssh_key => '/path/to/publickey', private_ssh_key => '/path/to/privatekey')

    # Connect with PKI, without password will try PKI
    my $ssh2 = $object->connect( user=>'rdiusr', host=>'yourremotehost.thedomain.com');

    # Connect with password
    my $ssh2 = $object->connect( user=>'rdiusr', password=>'foobar', $host=>'google.com');

    $ssh2  is your actual Net::SSH2 object

=head1 DESCRIPTION

A wrap of Net::SSH2. Due to notorious disconnect on Net::SSH2, this module is created.
To retry to connect for 10 times and pause 5 seconds in between tries.

=head1 CONSTRUCTOR

=over 4

=item new ()

IBIS::SSH2CONNECT constructor is called. It has 3 options. Using logfile, using log object and no logging.

Optional paramters public_ssh_key and private_ssh_key for specifying alternate SSH keys to use.

=back

=head1 METHODS

=over 4

=item connect ( user=>'username', host=>'remote hostname' )

Make the connection to remotehost using username PKI

=item connect ( user=>'username', password=>'asdfasf', host=>'remote hostname' )

Make the connection to remotehost using password authentification

=back

=head1 HISTORY

=over 4

=item Thu Aug 27 08:55:59 EDT 2009

Conceived.

=back

=head1 BUGS

Not a chance.

=head1 AUTHOR (S)

Hanny Januarius B<januariush@usmc-mccs.org>

=cut
