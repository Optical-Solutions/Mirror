package IBIS::SFTP;

use strict;
use warnings;
use version; our $VERSION = qv('0.0.1');
use Data::Dumper;
use Carp;
use IBIS::Config::NetLogin;
use IBIS::Crypt;
use IBIS::SSH2CONNECT;
use File::Basename;
use File::Spec;
use Encode;

{
#-------------------------------------------------------------------------------
    sub new {
        my ($class, $arg_ref) = @_;
	my $obj_ref = {};

        ## Get info from netlogin object:
        my $cfg = IBIS::Config::NetLogin->new()
            or croak 'Could not get NetLogin configuration in ', (caller(0))[3];	

        # Get our list of connections
        my $connections = $cfg->get_config_hashref();

        # User didn't provide a destination
        croak 'Missing required destination parameter in ', (caller(0))[3]
            unless($arg_ref->{destination});

        # The destination is not in the configured list
        croak 'Unrecognized destination in ', (caller(0))[3]
            unless(exists $connections->{$arg_ref->{destination}});

        # Need this to decrypt the password
        my $enc = IBIS::Crypt->new()
	    or croak 'Could not create IBIS encryption object in ', (caller(0))[3];

        my $host = $connections->{$arg_ref->{destination}}->{host};
        my $user = $connections->{$arg_ref->{destination}}->{username};

        ## this is necessary for login, it change from utf8 encode to iso, otherwise it will error out.
	Encode::from_to($user,'utf8','iso-8859-1');   
	Encode::from_to($host,'utf8','iso-8859-1');

my $ssh2connect_obj = IBIS::SSH2CONNECT->new();

my $pwd = length($connections->{$arg_ref->{destination}}->{password}) ? $enc->decrypt($connections->{$arg_ref->{destination}}->{password}) : '';

	#This connect will croak on it's own
	$obj_ref->{ssh2_obj} = $ssh2connect_obj->connect( user=> $user, host => $host,  password => $pwd);
	$obj_ref->{sftp_obj} = $obj_ref->{ssh2_obj}->sftp;

        ## $ftp_of{$oid}->binary();  ## commented out as sftp default as binary mode    
        # This is the list of files from the constructor
        $obj_ref->{files} = $arg_ref->{files};

	## Save remote_dir value	
        if($arg_ref->{remote_dir}) {
            $obj_ref->{remote_dir} = $arg_ref->{remote_dir};
	}else{
           ## in this case, it will go to the default loging directory of the sftp
	    $obj_ref->{remote_dir} = './';
	}
	bless $obj_ref, $class;
	return $obj_ref;
    }

#-------------------------------------------------------------------------------

    sub _get_files{
	my ($self) = @_;
	return $self->{files};
    }

    sub _set_files{
	my ($self, $files_ref) = @_;	
	$self->{files} = $files_ref;
        return $self->{files};
    }

    sub _get_remote_dir{
	my ($self) = @_;
	return $self->{remote_dir};
    }
    
    sub quit{
	my ($self) = @_;
	eval{ $self->{ssh2_obj}->disconnect(); };
	$self->{ssh2_obj} = undef;
	return;
    }

#Returns a list of files in a remote directory    
    sub ls {
	my ($self) = @_;
	my @list;
	my $remote_dir = $self->_get_remote_dir()  || "./";

	my $sftp_obj = $self->{sftp_obj};
	my $remote_dir_obj = $sftp_obj->opendir($remote_dir);
	while(my $afile = $remote_dir_obj->read()){
		push(@list, $afile->{'name'}) if $afile->{'name'} =~ /^\w+/;
	}
	#there doesn't seem to be a closedir() function in Net::SSH2
	return @list;    
    }

    ## return values:$sftp->get($remote [, $local [, \&callback ] ])
    ## Transfers files FROM the server. Returns a list of files transferred. 
    ## Takes an optional files parameter.
    sub get{
	my ($self, $args_ref) = @_;
        if($args_ref){$self->_set_files( _file_list($args_ref) );}
	my @filelist = $self->generic_sftp('get');
	return @filelist;
    }
    
    sub put{
	my ($self, $args_ref) = @_;
	if($args_ref){$self->_set_files( _file_list($args_ref) );}	
	my @filelist = $self->generic_sftp('put');
        return @filelist;
    }
    
    sub delete{
	my ($self, $args_ref) = @_;
	if($args_ref){$self->_set_files( _file_list($args_ref) );}
        my @filelist = $self->generic_sftp('delete');
        return @filelist;
    }
    

sub _file_list{
		my $arg_ref = shift;
		my @opfiles;
		if(ref($arg_ref) eq 'HASH'){	#if we got a hash, look for the 'files' key, use the array ref inside
			@opfiles = @{ $arg_ref->{files} };
		} elsif(ref($arg_ref) eq 'ARRAY'){	#if we got an array ref, use it
			@opfiles = @{ $arg_ref };
		} elsif(! ref($arg_ref) && $arg_ref){	#if we got a value, use it
			@opfiles = ($arg_ref);
		}
	\@opfiles;
}

    sub generic_sftp{
	my ($self, $method) = @_;
	## my $method = 'get';
	
        my @ret_files;
	
    ##1,  get remote_dir, @files
	my $remote_dir = $self->_get_remote_dir();
	my $rfile_ref  = $self->_get_files();
	
    ## if remote_dir, get path, else current dir as path
	my $path = $remote_dir || './';

    	##need to have file list
	 carp 'Empty file list in', ( caller(0) )[3] unless $rfile_ref && @$rfile_ref;

    ## transfering files: full path remote_file, local_file 
	foreach(@$rfile_ref){

	    ## get filename at leaf level
	    my $full_files_name = $_;
	    my $leafname = basename($_);

            ## get remote file name
            $path =~ s/\s+$//g;	#strip ending spaces
	my $rfile_name = File::Spec->catfile($path,$leafname);

            ## dispatch to different commands
	    if($method eq 'get') {
		my $ret_stat;
		eval{
		    $ret_stat  = $self->{ssh2_obj}->scp_get($rfile_name, $full_files_name); 
                    ## that will return undef if get failed
		};
		if($ret_stat){
		    push(@ret_files, $leafname);
		}
	    }elsif($method eq 'put'){
		my $put_status;
		eval{
		    $put_status = $self->{ssh2_obj}->scp_put($full_files_name, $rfile_name);
		};
		if($put_status){		
		   push (@ret_files, $leafname);
		}
	    }elsif($method eq 'delete'){
		my $del_status;
		my $sftp_obj = $self->{sftp_obj};
		eval{
		    $del_status = $sftp_obj->unlink($rfile_name);
		};
		if($del_status){
		    push(@ret_files, 1); ## this follows IBIS::FTP::put
		}
       	    }else{
		croak "Unknown method to IBIS::SFTP is called: $method\n";
	        return undef;
	    }		    
	}
	return @ret_files;
    }
}

sub DESTROY {
	my $self = shift;
	if($self){ $self->quit(); }
}

1;

__END__

=pod

=head1 NAME

IBIS::SFTP - IBIS SFTP API

=head1 VERSION

This documentation refers to IBIS::SFTP version 0.0.1.

=head1 SYNOPSIS

    use IBIS::SFTP;

    my $ftp = IBIS::SFTP->new( { destination => 'dest' } );

    my @files = IBIS::SFTP->new( { destination => 'dest' } )->ls();

    my @files_in_remote_dir = 
       IBIS::SFTP->new( { destination => 'dest', remote_dir => 'remotedir' } )->ls();

    my @upld = qw(one.txt two.txt three.txt);

    my @files = IBIS::SFTP->new( { destination => 'dest', files => \@upld } )->put();

=head1 DESCRIPTION

IBIS::SFTP was made aiming to keep the interface of all the functions in the IBIS::FTP module.

As a wrapper function for Net::SFTP, it has the following functions of Net::SFTP: ls, get, put, delete and quit. IBIS::SFTP is not a fully wapper for all the functions in Net::SFTP. This module is intended to provide a 'one-liner' type of access to common SFTP commands and be configurable by file with login information, just like IBIS::FTP.

The constructor requires a 'destination' parameter to be set. This destination will correspond with an entry in $IBIS_ROOT/etc/netlogin.conf. If a matching entry is found, the username and password will be used to login to the host in the configuration block. See the new() constructor for more details.

Each SFTP command calls a 'quit()' at its completion. This is to prevent SFTP sessions from hanging around during long running programs.

All parameters are passed via hash reference. So wrap 'em with curlies.

    IBIS::SFTP->new( { destination => 'dest', files => \@files } );

=head1 SUBROUTINES/METHODS

=over 4

=item new()

Constructor. The constructor takes four possible parameters: remote_dir, destination, files, debug. Destination is the only required parameter. All parameters are passed via a hash reference. The files parameter must be a reference to an array.

    IBIS::SFTP->new( { destination => 'dest', files => \@files } );

    or

    IBIS::SFTP->new( { destination => 'dest', files => $files } );

Set debug to a positive value for debugging output from Net::FTP.

The remote_dir option defines the file destination directory, or directory from which to fetch files from the remote server.

=item get()

Transfers files B<FROM> the server. return a list of files

    $ftp->get( { files => \@files } );   

=item put()

Transfers files B<TO> the server. Returns a list of files transferred. Takes an optional files parameter.

    $ftp->put( { files => \@files } );

=item delete()

Deletes files from the server. Does not return anything useful.

=item ls()

Returns a list of files on the server. Takes an optional agrument specifying the source directory.

=item quit()

Quits the SFTP session.

=back

=head1 EXAMPLES

    use IBIS::SFTP;

    my @files = qw(one.txt two.txt three.txt);

    # Two separate calls with the file list specified in the constructor
    my $sftp = IBIS::SFTP->new( { destination => 'dest', files => \@files } );
    $sftp->get();

    # Two separate calls with the file list specified in the method
    my $sftp = IBIS::SFTP->new( { destination => 'dest' } );
    $sftp->get( { files => \@files } );

    # All in one
    # @f contains a list of files transferred
    my @f = IBIS::SFTP->new( { destination => 'dest', files => \@files } )->get();


=head1 DIAGNOSTICS

The constructor may be called with a debug parameter set to a positive value. See Net::Cmd for more information.

    IBIS::SFTP->new( { destination => 'dest', debug => 1 } );

=head1 CONFIGURATION AND ENVIRONMENT

IBIS::SFTP is configured from $IBIS_ROOT/etc/netlogin.conf. The configuration file is an XML series of 'connection' blocks. Each block has an id and three elements: host, username, and password. See IBIS::Config::NetLogin for more details.

=head1 DEPENDENCIES

=over 4

=item * Carp

=item * Class::Std

=item * IBIS::Config::NetLogin

=item * IBIS::Crypt

=item * Net::SFTP

=item * version

=back

=head1 INCOMPATIBILITIES

As if.

=head1 BUGS AND LIMITATIONS

The know issue of Net::SFTP is its slowness when compared with Net::FTP. So is this module. In my tests, sometimes it took about one minite to login. For transfer of a 20 MB file, it took about 7 mins. For transfer of a 150 MB file, it took about 30 mins. It is very slow. 

=head1 AUTHOR

Chunhui Yu <yuc@usmc-mccs.org|chunhui_at_tigr@yahoo.com>
Eric Spencer<spencere@usmc-mccs.org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2009 Chunhui Yu, All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut

