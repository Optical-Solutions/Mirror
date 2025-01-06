package MCCS::RMS::Vouchers;
#
#===============================================================================
#
#         FILE:  Vouchers.pm
#
#  DESCRIPTION:  List of subroutined for Vouchers
#
#        FILES:  ---
#         BUGS:  ---
#        NOTES:  ---
#       AUTHOR:   (Matthew B. Pierpoint), <pierpointmb@usmc-mccs.org>
#      COMPANY:  
#      VERSION:  1.0
#      CREATED:  08/17/2006 09:03:48 AM EDT
#     REVISION:  ---
#===============================================================================

require Exporter;
use strict;
use warnings;
use Carp;

use version; our $VERSION = qv('1.0');
use base qw(Exporter);
use vars qw(@EXPORT_OK);
@EXPORT_OK = qw( get_voucher_amt );

sub get_voucher_amt
{
    my $args     = shift;
    my $po_num   = $args->{po};
    my $vendor   = $args->{vendor};
    my $dbh      = $args->{dbh};
    my $status   = $args->{status};
    my %v_status = ('C' => '0',
                    'O' => '2',
                    'U' => '6',);

    map{ croak "$_ is required" unless $args->{$_}} qw(po vendor dbh);

    $po_num = sprintf "%010d", $po_num;
    $po_num =~ s/(\d{5})(\d{1})(\d{4})/$1-$2-$3/;

    my @param;
    push @param, $vendor;
    push @param, $po_num;

    my $s_sql;
    unless($status =~ m/ALL/ )
    {
        $s_sql = 'and status_ind = ?';
        push @param, $v_status{$status};
    }

    my $amt = $dbh->selectall_arrayref("
            SELECT
                sum(original_amt)
            FROM
                ap_vouchers
            WHERE
                vendor_id = ?
                and po_number = ?
                $s_sql", undef, @param);

    $amt->[0][0] =~ s/(\d+\.\d{1})$/${1}0/;

    return $amt->[0][0] ? $amt->[0][0] : '0.00';
}

1;
