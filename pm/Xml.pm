package Config::Xml;

use strict;

my $pkgname = __PACKAGE__;

use base qw(MCCS::Config);

use XML::Simple;
use File::Basename;

my $cfgfile = "ibiscfg.xml";

my $forcenames = [ "cfgparam","application" ];
my $config = $pkgname->_load_xml($cfgfile);

my %normalize = (
   _scalar => \&_scalar,
   _array => \&_array,
   _hash => \&_hash,
);

# Compiled base configuration object.
my $pricfg = {};
# Compiled application configuration objects.
my $appcfgs = {};
# List of available applications in raw file.
my $cfglist = {};

sub _load_xml
{
my $class = shift;
my $file = shift;

  my $path = $pkgname->cfgdir()."/$file";

  if(! -e $path){
	warn("Configuration file does not exist. Reverting to standard config file.");
	$path = $pkgname->cfgdir()."/$cfgfile";
  }

   my $config =   XMLin($path,
         KeyAttr => {},
         ForceArray => $forcenames,
         SuppressEmpty => "",
         NormaliseSpace => 2,
         );

return($config);
}

sub _init
{
my $class = shift;
my $force = shift;	#optional other config file OR force reload of standard file

   return undef unless $class->isa("MCCS::Config");
   $class = ref($class) if ref($class);

   $config = $pkgname->_load_xml( File::Basename::basename( $force eq '1' ? $cfgfile : $force) ) if $force;		#since the $force paramter can mean either force the reload of the standard config or load a specific config... need to check for one here
   return undef unless ref($config);

   foreach my $param ( @{ $config->{primary}->{cfgparam}} ) {
      my $type = ( $param->{type} ? "_".$param->{type} : "_scalar" );
      $pricfg->{$param->{name}} = &{$normalize{$type}}($param->{value});
   }

   my $newcfglist = {};

   foreach ( @{$config->{application}} ) {
      my $appname = $_->{name};
      $newcfglist->{$appname} = $_;
      no strict 'refs';
      next if $pkgname->can($appname);
      *{"$pkgname\::$appname"} =
         sub {
         my $self = shift;
         my $force = shift;
         return undef unless $self->isa($pkgname);
         return($pkgname->_appcfg($appname,$force));
         }
   }

return($cfglist = $newcfglist);
}

sub _initapp
{
my $self = shift;
my $appname = shift;
my $force = shift;

   return undef unless $self->isa($pkgname);
   if ( $force ) { return undef unless $pkgname->_init($force); }
   return undef unless exists($cfglist->{$appname});

   my $xmlobj = $cfglist->{$appname};
   return undef unless $xmlobj->{name} eq $appname;
   my $cfg = {};
   foreach my $param ( @{ $xmlobj->{cfgparam}} ) {
      my $type = ( $param->{type} ? "_".$param->{type} : "_scalar" );
      $cfg->{$param->{name}} = &{$normalize{$type}}($param->{value});
   }

return($cfg);
}

sub _appcfg
{
my $self = shift;
my $appname = shift;
my $force = shift;

   return undef unless $self->isa($pkgname);

   if ( $force || !ref($appcfgs->{$appname}) ) {
      $appcfgs->{$appname} = $self->_initapp($appname,$force);
   }

return( ref($appcfgs->{$appname}) ? $appcfgs->{$appname} : undef );
}

sub _scalar
{
return(shift);
}

sub _array
{
my $vals = shift;

   return $vals if ref($vals);

return([ $vals ]);
}

sub _hash
{
my $pairs = shift;

   my $hashref = {};
   if ( ref($pairs) eq "HASH" ) {
      $hashref->{$pairs->{key}} = $pairs->{content};
   } elsif ( ref($pairs) eq "ARRAY" ) {
      foreach ( @$pairs ) {
         $hashref->{$_->{key}} = $_->{content};
      }
   }

return($hashref);
}

1;

__END__
