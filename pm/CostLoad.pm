package MCCS::RMS::CostLoad;

use strict;
use warnings;

use version; our $VERSION = qv('0.0.1');

use Carp;
use IBIS::Mail;
use IBIS::DBI;
use Readonly;
use Exporter;
our @EXPORT_OK = qw( last_run log_file_contents get_file_names style_id invalid_upc mail_notify get_nex_styles get_row_count );

my $logfile = '/usr/local/mccs/log/rms_cost_load.log';

Readonly::Scalar my $NEX_VENDOR_ID => '00001707694';

sub last_run
{
    my $fh;
    eval { open $fh, '<', $logfile or croak $!; };

    return if $@;

    my @complete = grep { m/RMS\sCost\sLoad\scomplete/msx } <$fh>;
#    my @complete = grep { m/RMS\sCost\supdate\scomplete/msx } <$fh>;
#    my @complete = grep { m/Essentus\sCost\sLoad\scomplete/msx } <$fh>;

    close $fh;

    my $string = $complete[0];
    $string =~ s/(.*)\[info\]\s+(.*)\n/$2: $1/gmx;
    return $string;
}

sub log_file_contents
{
    my (@lines, $fh);

    eval { open $fh, '<', $logfile or croak $!; };

    return if $@;

    # Slurp, not so much
#    my $lines = do { local $/; <$fh> };

    while (my $line = <$fh>)
    {
        chomp $line;
        push @lines, $line;
    }

    close  $fh;

    return wantarray ? @lines : \@lines;
}

#-------------------------------------------------------------------------------
# Return a list of names for the given directory.

sub get_file_names
{
    my ($dir) = @_;

    my @files;

    opendir DIR, $dir or return;

    while ( my $file = readdir DIR )
    {
        if ($file !~ m/^AMC_LOC/xms) { next; }

        my $filename = join q{/}, $dir, $file;

        push @files, $filename;
    }

    closedir DIR;

    return @files;
}

#-------------------------------------------------------------------------------
# Fetch style_id from RAMS.BAR_CODES
 
sub style_id
{
    my ($dbh, $upc) = @_;

    $upc =~ s/^0(\d{12,12})$/$1/msx;

    my $sth = $dbh->prepare_cached(q{
       select
           style_id
       from
           bar_codes
       where
           business_unit_id = '30'
           and bar_code_id = ?
                                                                                    });

    my $r = $dbh->selectrow_arrayref($sth, undef, $upc);

    return $r->[0] ? $r->[0] : undef;
}

#-------------------------------------------------------------------------------
# UPC must be either 12 or 13 digits. Return true if the UPC is not valid.
# NEXCOM internal UPCs start with 04

sub invalid_upc
{
    my ($upc) = @_;

    if ( $upc =~ m/^04/msx )
    {
        return 1;
    }
    elsif ( $upc !~ m/^\d{12,13}$/msx )
    {
        return 1;
    }
    else
    {
        return;
    }
}

sub mail_notify
{
    my ( $cc, $subj, $body ) = @_;

    my $m = IBIS::Mail->new(
        to      => ['kaveh.sari@usmc-mccs.org'],		#1/10/2008 ERS pretty stupid to embed email addresses in a module??!!!
        from    => 'IBIS <ibis@usmc-mccs.org>',
        type    => 'text/html',
        subject => $subj,
        body    => $body,
    );

    $m->send;

    return 1;
}

sub mc2p_mail_notify
{
 &mail_notify;	#1/10/2008 ERS quick and dirty alias to mail_notify...
} 

#-------------------------------------------------------------------------------
# Cache all NEXCOM styles
sub get_nex_styles
{
    my ($dbh) = @_;

    return $dbh->selectall_hashref(
        qq{
            SELECT
                style_id,
                estimated_landed_cost
            FROM
                style_vendors
            WHERE
                business_unit_id = '30'
                and vendor_id = '$NEX_VENDOR_ID'
        },
        'style_id'
    );
}

#-------------------------------------------------------------------------------# Return the number of records for the specified table
sub get_row_count
{
    my ($dbh, $table) = @_;

    my $r =  $dbh->selectrow_arrayref(
        qq{
            SELECT
                count(*)
            FROM
                $table
        }
    );
    return $r->[0];
}

1;

__END__

=pod

=head1 NAME

CostLoad.pm - Utilties to support RMS Cost Load procedures and WWW interfaces.

=head1 VERSION

This documentation refers to MCCS::RMS::CostLoad version 0.0.1.

=head1 SYNOPSIS

    use MCCS::RMS::CostLoad;

    my $last_run    = MCCS::RMS::CostLoad::last_run();
    my $log_lines   = MCCS::RMS::CostLoad::log_file_contents();
    my @files       = MCCS::RMS::CostLoad::get_file_names($DATA_DIR); 
    my $style_id    = MCCS::RMS::CostLoad::style_id($dbh, $upc);
    my $invalid_upc = MCCS::RMS::CostLoad::invalid_upc($upc);
                      MCCS::RMS::CostLoad::mail_notify($subj, $msg);
    my $nex_styles  = MCCS::RMS::CostLoad::get_nex_styles($dbh);
    my $row_count   = MCCS::RMS::CostLoad::get_row_count($dbh, $table);

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=over 4

=item last_run()

Returns a string displaying the last time the rms_cost_load.pl was run.

=item log_file_contents()

Returns a scalar containing all lines from the rms_cost_load.log.

=item get_file_names()

Returns a list of data file names in the data directory.

=item style_id()

Returns a scalar containing a style_id from RMS.BAR_CODES.

=item invalid_upc()

Returns true if the evaluated UPC does not validate properly.

=item mail_notify()

Sends an email with a particular subject and message.

=item get_nex_styles()

Returns a hash containing all NEXCOM style ID numbers.

=item get_row_count()

Returns the number of records for the specified table;

=back

=head1 EXAMPLES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to the MCCS Help Desk B<help.desk@usmc-mccs.org>.
Patches are welcome.

=head1 AUTHOR

Trevor S. Cornpropst B<tcornpropst@acm.org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2006 MCCS. All rights reserved.

This software is the property of the Marine Corps Community Services. Redistribution is not authorized.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut

