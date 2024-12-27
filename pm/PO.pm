package MCCS::RMS::PO;

#
#===============================================================================
#
#         FILE:  PO.pm
#
#  DESCRIPTION:  List of Purchase Order subroutines for Web Reports
#
#        FILES:  ---
#         BUGS:  ---
#        NOTES:  ---
#       AUTHOR:   (Matthew B. Pierpoint), <pierpointmb@usmc-mccs.org>
#      COMPANY:
#      VERSION:  1.0
#      CREATED:  05/11/2006 09:33:41 AM EDT
#     REVISION:  08/25/2006 13:00 (By: J. Stapleton Bug: 0073)
#     REVISION:  08/25/2006 15:30 (By: J. Stapleton Bug: 0075)
#     REVISION:  08/25/2006 15:30 (By: J. Stapleton Bug: 0076)
#     REVISION:  08/25/2006 16:10 (By: J. Stapleton Bug: 0077)
#===============================================================================
require Exporter;

use strict;
use warnings;
use Carp;

use version; our $VERSION = qv('1.0');
use vars qw( @EXPORT_OK );
use base qw(Exporter);
use English qw( -no_match_vars );
@EXPORT_OK = qw( po_details
              iface_po_details );

use MCCS::WebReports::Utils qw( site_sql );

sub po_details
{
    my $args    = shift;
    my $po      = $args->{po};
    my $site    = $args->{site};
    my $dbh     = $args->{dbh};
    my $IM_FLAG = $args->{NO_IM} ? 1 : 0;
    my $im_id   = 'history';

    croak 'Must supply a PO Number.' unless $po;
    croak 'Must supply a Db handle.' unless $dbh;

    $dbh->do(q{alter session set nls_date_format='YYYYMMDD'});

    my $site_sql = site_sql( 'v.site_id', $site );
    my $results;
    eval {
        $results = $dbh->selectall_arrayref(
            " 
        select
           vv.name,
           po.vendor_id,
           e.first_name,
           e.last_name,
           po.po_type,
           po.version_no,
           po.order_date,
           po.delivery_date,
           po.shipped_from,
           po.qty_ordered,
           v.color_id,
           po.cancelled_ind,
           v.site_id,
           b.bar_code_id,
           v.style_id,
           s.description,
           s.vendor_style_no,
           v.qty_ordered,
           v.qty_received,
           get_qty_onhand (30 ,v.style_id, v.color_id, v.dimension_id, v.site_id),
           c.description,
           sc.vendor_color,
           v.size_id,
           v.dimension_id,
           v.local_cost,
           v.retail_price,
           vd.department_id
           --po.approved_ind,
           --vd.dept_name,
           --po.document_type,
        from
           purchase_orders po ,
           /* v_po_stcodisi_sites v,  */
           v_po_item_sites v,
           styles s,
           colors c,
           vendors vv,
           v_dept_class_subclass vd,
           employees e,
           style_colors sc,
           sections t,
           bar_codes b
        where
           po.BUSINESS_UNIT_ID = 30
           and b.business_unit_id = 30
           and v.business_unit_id = 30
           and c.business_unit_id = 30
           and s.business_unit_id = 30
           and vv.business_unit_id = 30
           and vd.business_unit_id = 30
           and e.BUSINESS_UNIT_ID = 30
           and sc.BUSINESS_UNIT_ID = 30
           and t.BUSINESS_UNIT_ID = 30
           and po.version_no = v.version_no
           and po.BUYER_EMPLOYEE_ID = e.employee_id
           and s.style_id = v.style_id
           and s.style_id = sc.style_id
           and s.style_id = b.style_id
           and b.style_id = sc.style_id
           and v.size_id = b.size_id
           and s.section_id = t.section_id
           and c.color_id = v.color_id
           and c.color_id = b.color_id
           and c.color_id = sc.color_id
           and b.color_id = sc.color_id
           and s.section_id = vd.section_id
           and po.vendor_id = vv.vendor_id
           and po.vendor_id = v.vendor_id
           and po.po_id = v.po_id
           and po.po_id = ?
           and v.po_id = ?
           $site_sql
        order by
           po.version_no desc ", undef, ( $po, $po )
        );
    };
    return if $EVAL_ERROR;

    my @array;
    my $types = [
        qw( NUMBER NUMBER NUMBER TEXT TEXT NUMBER NUMBER NUMBER TEXT TEXT TEXT MONEY MONEY NUMBER)
    ];

    my $tot_ordered = 0;
    my $tot_rcvd    = 0;
    my $tot_cost    = 0;
    my $tot_r_amt   = 0;
    my $show        = 0;

    my $sth = $dbh->prepare(
        'select description from departments where department_id = ? ');

    my %dept_tot;
    my %site_tot;
    my @dept;
    my @site;
    my $dept_types;
    my $over = q{};

    push @array, [ 'PO Detail Report' ];
    push @array,
        [
        'Site',         'UPC',           'Style',    'Description',
        'Vendor Style', 'Order Qty',     'Rcvd Qty', 'On Hand Qty',
        'Color',        'Vendor Color',  'Size',     'DIM',
        'Cost Amount',  'Retail Amount', 'Dept'
        ];

    for ( @{$results} )
    {
        my (
            $site,    $upc,      $style,   $desc,   $v_style,
            $ord_qty, $rcvd_qty, $on_hand, $color,  $v_color,
            $size,    $dim,      $cost,    $retail, $dept
            )
            = ( map {$_} @{$_}[ 12 .. $#{$_} ] );


        my $im_link = $IM_FLAG ? $style : 
            "<a href='#' onclick='getInvHistory(\"$style\",\"$site\",\"$im_id\"); return false;'>$style</a>",
        $tot_ordered += $ord_qty;
        $tot_rcvd    += $rcvd_qty;
        $tot_cost    += ( $ord_qty * $cost );
        $tot_r_amt   += ( $ord_qty * $retail );

        $site_tot{$site}{$dept}{ordered}  += $ord_qty;
        $site_tot{$site}{$dept}{received} += $rcvd_qty;
        $site_tot{$site}{$dept}{ord_cost}   += $ord_qty * $cost;       # jjs/bug: 75/76 08/25/2006
        $site_tot{$site}{$dept}{ord_retail} += $ord_qty * $retail;     # jjs/bug: 75/76 08/25/2006

        $sth->execute($dept);
        my ($dept_name) = $sth->fetchrow_array();

        $dept_tot{$dept}{name} = $dept_name;
        $dept_tot{$dept}{ordered}    += $ord_qty;
        $dept_tot{$dept}{received}   += $rcvd_qty;
        $dept_tot{$dept}{ord_cost}   += $ord_qty * $cost;
        $dept_tot{$dept}{ord_retail} += $ord_qty * $retail;

        if($rcvd_qty > $ord_qty)
        {
            $rcvd_qty = "<span style='color:red;font-weight:bold;'>$rcvd_qty</span>";
            $over = '<span style="color:red;font-size:12pt;">*There is an overshipment for this PO*</span>';
        }

        push @array,
            [
            $site,    $upc,      $im_link, $desc,   $v_style,
            $ord_qty, $rcvd_qty, $on_hand, $color,  $v_color,
            $size,    $dim,      $cost,    $retail, $dept
            ];
        $show++;
    }


    my %hash = (
        po         => $po,
        vendor     => "$results->[0][0]($results->[0][1])",
        buyer      => "$results->[0][2] $results->[0][3]",
        po_type    => $results->[0][4],
        version    => $results->[0][5],
        order_date => $results->[0][6],
        del_date   => $results->[0][7],
        shipped    => $results->[0][8],
        cancelled  => $results->[0][11],
        ordered    => $tot_ordered,
        received   => $tot_rcvd,
        cost       => sprintf( '%.2f', $tot_cost ),
        amount     => sprintf( '%.2f', $tot_r_amt ),
        overship   => $over,
    );

    push @dept, ['Totals by Dept'];
    push @dept,
        [
        'Dept',
        'Dept Name',
        'Order Qty',
        'Rcvd Qty',
        'Total <BR>Ordered Cost',
        'Total <BR>Ordered Retail'
        ];
    $dept_types = [qw( NUMBER TEXT NUMBER NUMBER MONEY MONEY )];
    for my $d ( sort keys %dept_tot )
    {
        push @dept,
            [
            $d,                      $dept_tot{$d}{name},
            $dept_tot{$d}{ordered},  $dept_tot{$d}{received},
            $dept_tot{$d}{ord_cost}, $dept_tot{$d}{ord_retail},
            ];
    }

    push @site, ['Totals by Site'];
    push @site,
        [
        'Site',
        'Order Qty',
        'Rcvd Qty',
        'Total <BR>Ordered Cost',
        'Total <BR>Ordered Retail'
        ];
    my $site_types = [qw( NUMBER NUMBER NUMBER MONEY MONEY )];

    for my $s ( sort keys %site_tot )
    {
        for my $d ( sort keys %{ $site_tot{$s} } )
        {
            push @site,
                [
                $s,                          
                $site_tot{$s}{$d}{ordered},  $site_tot{$s}{$d}{received},
                $site_tot{$s}{$d}{ord_cost}, $site_tot{$s}{$d}{ord_retail},
                ];

        }
    }

    return $show
        ? ( \@array, $types, \%hash, \@dept, $dept_types, \@site,
        $site_types, $im_id )
        : undef;

}

sub iface_po_details
{
   my $args   = shift;
   my $po_num = $args->{po};
   my $date   = $args->{date};
   my $site   = $args->{site};
   my $dbh    = $args->{dbh};

   croak 'Must supply PO Num.' unless $po_num;
   croak 'Must fmsinteg db handle.' unless $dbh;

   $po_num =~ s/ //g;
   my $po_cnt = 0;
   $po_num =~ s/(\d)/$po_cnt++;$1/eg;
   for(1..(10-$po_cnt))
   {
      $po_num =~ s/^/ /;
   }

   my $site_sql = $site ? "and site_id in ($site)" : q{};
   my $date_sql = $date ? "and date = '$date'" : q{};

   my $results = $dbh->selectall_arrayref("
         SELECT
            line_no,
            po_id,
            site_id,
            style_id,
            po_qty_ordered,
            est_landed_cost,
            gl_accnt,
            item_desc,
            vendor_style_id
         FROM
            rms_po_det
         WHERE
            po_id = ?
            $date_sql
            $site_sql", undef, $po_num);

   my $types = [ "NUMBER", "NUMBER", "NUMBER", "TEXT", "NUMBER",
                 "MONEY", "TEXT", "TEXT", "TEXT" ];
   my @array;
   push @array, [ "RAMS PO Details Report for $po_num on $date" ];
   push @array, [ "Line #", "PO ID", "Site ID", "Style", "Qty",
                  "Est Landed Cost", "GL Accnt", "Desc",
                  "Vendor Style" ];
   my $tot_qty = 0;
   my $tot_cost = 0;
   my $cnt = 0;

   for(@$results)
   {
      my $grp = substr($_->[6], -3);

      $_->[6] = join('',substr($_->[6],0, -3),
                        "<a href='#' onclick='getBuyerDept(\"$grp\",\"buyers\");return false;'>$grp</a>");

      my $inv_link = "<a href='#' onclick='getInvHistory(\"$_->[3]\", \"$_->[2]\",\"history\");return false;'>$_->[3]</a>",

      push @array, [ map {$_} @$_[0..2], $_->[3],
                     map {$_} @$_[4..$#$_] ];
                    
      $tot_qty += $_->[4];
      $tot_cost += $_->[4] * $_->[5];
      $cnt++;
   }

   my %qtys = ( qty => $tot_qty, cost => $tot_cost, );

   return $cnt ? (\@array, $types, \%qtys) : undef;
}

1;
