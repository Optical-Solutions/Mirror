package MCCS::RMS::Inventory;

#===============================================================================
#
#         FILE:  Inventory.pm
#
#  DESCRIPTION:  List of Inventory subroutines for the Web Reports
#
#        FILES:  ---
#         BUGS:  ---
#        NOTES:  ---
#       AUTHOR:  Matthew Pierpoint (pierpointmb@usmc-mccs.org)
#      COMPANY:
#      VERSION:  1.0
#      CREATED:  04/27/2006 03:51:50 PM EDT
#     REVISION:  07/10/2006 04:50    PM EDT
#    EDITED BY: R. Roberts (robertsrl@usmc-mccs.org)
#       CHANGE: Added sub styles_summary
#
#               Added _get_fiscal_dates
#                 internal func, returns dates(start/end) for a fiscal period.
#                 -Args: number of days to be subtracted from current date. Returns start 
#                  and end date in YYYYMMDD
#
#
#===============================================================================
require Exporter;

use strict;
use warnings;
use Carp;
use POSIX qw(strftime);

use version; our $VERSION = qv('1.0');

use MCCS::Math::Retail;
use MCCS::WebReports::Utils qw( site_sql duns_sql dept_sql );

use DateTime::Format::Strptime;
use IBIS::DateTime::Retail;

use vars qw( @EXPORT_OK );

use base qw(Exporter);
@EXPORT = qw( style_info 
              inv_on_date 
              style_details 
              styles_summary
              style_mv_history 
              rcvr_history
              nonspecific_style_info );

sub style_info
{
    my ( $style, $dbh ) = @_;

    my $s_info = $dbh->selectall_arrayref( '
      SELECT
         s.SITE_ID,                 /* NOT NULL VARCHAR2(5) */
         s.QTY_ON_HAND,             /* NOT NULL NUMBER(11,3) */
         s.QTY_ON_ORDER,            /* NOT NULL NUMBER(11,3) */
         s.QTY_RECEIVED,            /* NOT NULL NUMBER(11,3) */
         s.QTY_SOLD_REGULAR,        /* NOT NULL NUMBER(11,3) */
         s.QTY_SOLD_MARKDOWN,       /* NOT NULL NUMBER(11,3) */
         s.QTY_SOLD_PROMOTION,      /* NOT NULL NUMBER(11,3) */
         s.QTY_RESERVED,            /* NOT NULL NUMBER(11,3) */
         s.QTY_INBOUND,             /* NOT NULL NUMBER(11,3) */
         s.QTY_OUTBOUND,            /* NOT NULL NUMBER(11,3) */
         s.QTY_FROZEN_OUTBOUND,     /* NUMBER(11,3) */
         s.QTY_FROZEN,              /* NUMBER(11,3) */
         s.QTY_COUNTED,             /* NUMBER(11,3) */
         c.description,
         s.SIZE_ID,                 /* NOT NULL VARCHAR2(5) */
         s.DIMENSION_ID,            /* VARCHAR2(5) */
         GET_PERMANENT_RETAIL_PRICE (30, s.SITE_ID, s.STYLE_ID, s.COLOR_ID, s.DIMENSION_ID, s.SIZE_ID, SYSDATE, NULL),
         s.SALES,                   /* NOT NULL NUMBER(13,2) */
         s.SALES_COST              /* NOT NULL NUMBER(13,2) */
    /*     (s.sales - s.sales_cost)/s.sales_cost * 100*/

         /*s.BREAKUP_ORIGINAL_UNITS,  /* NUMBER(11,3) *
         s.BREAKUP_EXPLODED_UNITS,  /* NUMBER(11,3) *
         s.DIMENSION_GROUP_ID,      /* VARCHAR2(5) *
         s.SIZE_GROUP_ID,           /* NOT NULL VARCHAR2(5) *
         s.COLOR_ID,                /* NOT NULL VARCHAR2(3) *
         s.VAT_AMOUNT,              /* NUMBER(13,2) *
         s.BU_SALES,                /* NUMBER(13,2) *
         s.BU_VAT_AMOUNT,           /* NUMBER(13,2) *
         s.GAIN_LOSS,               /* NUMBER(13,2) *
         s.PINV_BUSINESS_UNIT_ID,   /* NUMBER(2) *
         s.DOCUMENT_ID,             /* NUMBER(10) *
         s.PINV_SITE_ID,            /* VARCHAR2(5) *
         s.INVENTORY_DATE,          /* DATE *
         y.description,*/
      FROM
         site_inventories s,
         colors c,
         styles y
      WHERE
         s.business_unit_id=30
         and c.business_unit_id=30
         and y.business_unit_id=30
         and s.style_id = ?
         and s.style_id = y.style_id
         and s.color_id = c.color_id
      ORDER BY
         site_id,
         c.description,
         s.size_id', undef, $style );

    my @array;
    push @array, ["Style Information for $style"];
    push @array,
        [
        'Site ID',
        'Qty On Hand',
        'Qty On Order',
        'Qty Rcvd',
        'Sold Reg',
        'Sold Mark',
        'Sold Promo',
        'Qty Rsvrd',
        'Qty Inbnd',
        'Qty Outbd',
        'Qty FznOut',
        'Qty Fzn',
        'Qty Count',
        'Color',
        'Size',
        'DIM',
        'Retail',
        'Sales',
        'Sales Cost',
        'Margin'
        ];
    my $types = [
        'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER',
        'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER',
        'NUMBER', 'TEXT',   'TEXT',   'TEXT',   'MONEY',  'MONEY',
        'MONEY',  'MONEY'
    ];
    my $cnt = 0;

    for ( @{$s_info} )
    {
        my $tmp = $_->[18] != 0 ? ($_->[17] - $_->[18])/$_->[18] * 100 : 0; 
        $_->[10] = $_->[10] ? $_->[10] : '0';
        $_->[11] = $_->[11] ? $_->[11] : '0';
        $_->[12] = $_->[12] ? $_->[12] : '0';
        $_->[15] = $_->[15] ? $_->[15] : 'No DIM';
        push @array, [ @{$_}, $tmp ];
        $cnt++;
    }

    return $cnt ? ( \@array, $types ) : $cnt;
}

sub _get_fiscal_dates
{
    my ($months) = @_;
    my $wrkday = IBIS::DateTime::Retail->now(time_zone => 'local')->subtract(months => $months);

    my $period_start = 
      IBIS::DateTime::Retail->new(
          year  => $wrkday->year,
          month => $wrkday->month,
          day   => $wrkday->day )->current_period_start;

    my $period_end = 
      IBIS::DateTime::Retail->new(
          year  => $wrkday->year,
          month => $wrkday->month,
          day   => $wrkday->day )->current_period_end;
 
    my $period = 
      IBIS::DateTime::Retail->new(
          year  => $wrkday->year,
          month => $wrkday->month,
          day   => $wrkday->day )->current_period_number;
 

    my $start_date = substr($period_start, 0, 10);
    my $end_date   = substr($period_end, 0, 10);

    return($start_date, $end_date);
}


sub styles_summary
{
 my ($start_fiscal, $end_fiscal) = _get_fiscal_dates('0');
print "Start:$start_fiscal End:$end_fiscal <br>";

 ($start_fiscal, $end_fiscal) = _get_fiscal_dates('2');
print "Start:$start_fiscal End:$end_fiscal <br>";


    
    
    my $args      = shift;
    my $dbh       = $args->{dbh};
    my $dept      = $args->{dept};
    my $cmd       = $args->{cmd};
    my $site      = $args->{site};
    my $duns      = $args->{duns};

    $dbh->do(q{alter session set nls_date_format = 'YYYYMMDD'});

    my $site_sql = $site ? "AND sit.site_id in ($site)" : '';

    my ( $dept_sql, @depts ) = $dept ? _create_ph_sql( 'and sec.department_id', $dept ) : '';
    #my ( $dept_sql, @depts ) = _create_ph_sql( 'and c.department_id', $dept );
    
    my $duns_sql = $duns ? duns_sql('ven.vendor_id', $duns) : ''; 
    #my $duns_sql = duns_sql( 'v.vendor_id', $duns)  if $duns;

    for (qw(dept))
    {
        croak "$_ is a required field" unless $args->{$_};
    }

    my @report;
    my %totals;
    my $cur_year = strftime("%Y%m%d", localtime());
    my $c_year = strftime("%d-%b-%Y", localtime());

    push @report, [ "Style Summary" ];
    
    push @report, [ 'Vendor', 'Class','Style', 'Description', 'Vendor Style', 'Cost',
                    'Retail', 'IMU(Initial Markup)', 'AVG(Retail)', 'Prev WK. Units',
                    '2 WK Units', '30 Day Units', 'First Receipt', 'Last Receipt',
                    'Wks of Supply', 'On Hand', 'SiteID', '00 Cur Mth', '00 Mth+1', '00 Mth+2',
                    '00 Mth+3' ];
  
    my $run_total = 0;

    if ($dbh)
    {
        my $styles_sql = "SELECT
            color_id,
            size_id,
            nvl(dimension_id, '~'),
            ven.name AS vendor_name,
            sec.class_id,
            sty.style_id,
            sty.description,
            sty.vendor_style_no,
            sec.department_id,
            sit.site_id,
            sty.vendor_id,
            get_permanent_retail_price (
                30, 
                sit.site_id, 
                sit.style_id,
                null, 
                null, 
                null, 
                sysdate, 
                null) AS retail,
            query_exception_cost(30,
                sty.vendor_id,
                sty.style_id,
                sit.site_id,
                chr(255),
                null,
                null,
                null) AS cost,
            ROUND(AVG(get_permanent_retail_price (30, sit.site_id, sit.style_id,
                null, null, null, sysdate, null)), 2) AS round_retail,
            ROUND(AVG(query_exception_cost(30,
                sty.vendor_id,
                sty.style_id,
                sit.site_id,
                chr(255),
                null,
                null,
                null)), 2) AS round_cost,
            sum(sit.qty_on_hand) qty_on_h
        FROM 
            site_inventories sit,
            styles sty,
            sections sec,
            vendors ven
        WHERE 
            sit.business_unit_id = 30
        AND sit.business_unit_id = sty.business_unit_id  
        AND sit.business_unit_id = sec.business_unit_id  
        AND sit.business_unit_id = ven.business_unit_id  
        AND sit.style_id         = sty.style_id
        AND sty.section_id       = sec.section_id
        AND sty.vendor_id        = ven.vendor_id
        $dept_sql
        $site_sql
        $duns_sql
        GROUP BY
            color_id,
            size_id,
            dimension_id,
            ven.name,
            sec.class_id,
            sty.style_id,
            sty.description,
            sty.vendor_style_no,
            sec.department_id,
            sit.site_id,
            sty.vendor_id,
            get_permanent_retail_price (30,  sit.site_id, sit.style_id,
                null, null, null, sysdate, null),  
            query_exception_cost(30,
                sty.vendor_id,
                sty.style_id,
                sit.site_id,
                chr(255),
                null,
                null,
                null)
        ORDER BY sit.site_id,
            sec.class_id,
            ven.name
        ";

        my $avg_mv_sql = "SELECT 
            site_id,
            vendor_id,
            style_id, 
            color_id,
            size_id,
            nvl(dimension_id, '~'),
            avg(retail_price),
            sum(inven_move_qty)
        FROM inventory_movements im
        WHERE business_unit_id = 30
        AND inven_move_type    = 'SALES'
        AND inven_move_date between
            (sysdate - ?) And sysdate 
        AND site_id                = ?
        AND vendor_id              = ?
        AND style_id               = ?
        AND color_id               = ?
        AND size_id                = ?
        AND nvl(dimension_id, '~') = ?
        GROUP BY 
            site_id,
            vendor_id,
            style_id,
            color_id,
            size_id,
            dimension_id
        ";

        my $dates_sql = "SELECT 
            site_id,
            vendor_id,
            style_id, 
            color_id,
            size_id,
            nvl(dimension_id, '~'),
            inven_move_type,
            min(to_char(inven_move_date, 'YYYYMMDD')),
            max(to_char(inven_move_date, 'YYYMMDD'))
        FROM inventory_movements
        WHERE business_unit_id = 30
        AND site_id                = ?
        AND vendor_id              = ?
        AND style_id               = ?
        AND color_id               = ?
        AND size_id                = ?
        AND nvl(dimension_id, '~') = ?
        AND inven_move_type        = 'RECEIVING'
        GROUP BY 
            site_id,
            vendor_id,
            style_id, 
            color_id,
            size_id,
            dimension_id,
            inven_move_type
        ";

print "Styles:$styles_sql <br><br>";
print "AVG:$avg_mv_sql <br><br>";
print "$dates_sql <br>";
print "SQL:$site_sql <br>";
    
    #On Order SQL
        my $OOM_sql = "SELECT 
            v.po_id, 
            v.style_id, 
            sum(v.qty_balance)
        FROM v_po_stcodisi_sites v,
            purchase_orders p
        WHERE v.business_unit_id =30
        AND v.business_unit_id   = p.business_unit_id
        AND p.po_group_id is null
        AND trunc(p.delivery_date) between trunc(to_date(?, 'YYYY-MM-DD'))
            And trunc(to_date(?, 'YYYY-MM-DD'))
        AND v.site_id              = ?
        AND v.vendor_id            = ?
        AND v.style_id             = ?
        AND color_id               = ?
        AND size_id                = ?
        AND nvl(dimension_id, '~') = ?
        AND v.po_id                = p.po_id
        AND v.version_no           = p.version_no
        AND v.vendor_id            = p.vendor_id
        GROUP BY v.po_id,
            v.style_id
        ";
print "OOM:$OOM_sql <br><br>";

        my $sth_avg_retail = $dbh->prepare($avg_mv_sql);
        my $sth_sum_mv_qty = $dbh->prepare($avg_mv_sql);
        my $sth_dates      = $dbh->prepare($dates_sql);
        my $sth_OOM = $dbh->prepare($OOM_sql);

        my $styles_ref = $dbh->selectall_arrayref($styles_sql, undef, @depts );
        my ( $t_qty, $t_cost, $t_retail ) = undef;

        my ($min_date, $max_date) =  '';

        for(@{$styles_ref}){
            my ($colorId, $sizeId, $dimId, $vendor_name, $classId, $styleId, 
                $description, $vendor_style_no, $departmentId, $siteId, $vendorId, 
                $retail, $cost, $round_retail, $round_cost, $qty_on_h) = @$_;
            if($qty_on_h)
            {
                my ($r_cost, $r_retail) = undef;

                $r_cost   = sprintf('%.2f', $round_cost);
                $r_retail = $round_retail;
                $cost   = sprintf('%.2f', $cost);
                $retail = sprintf('%.2f', $retail);

                my $init_mrk_up = sprintf ('%.2f',((($r_retail - $r_cost)/$r_retail)*100)); 

                $sth_avg_retail->execute('30', $colorId, $sizeId, 
                                         $dimId, $styleId, $vendorId, $siteId);
           
                my ($avg_site, $avg_venId, $avg_style, $avg_col, 
                    $avg_sz, $avg_dim, $avg_retail, $sum_qty) = undef;
            
                ($avg_site, $avg_venId, $avg_style, $avg_col, 
                 $avg_sz, $avg_dim, $avg_retail, $sum_qty) 
                 = $sth_avg_retail->fetchrow_array();

                my ($dt_site, $dt_venId, $dt_styleId, $dt_color, 
                    $dt_sz, $dt_dim, $dt_type, $min_date) = undef;

                $sth_dates->execute($siteId, $vendorId, $styleId, $colorId, $sizeId, $dimId);
                ($dt_site, $dt_venId, $dt_styleId, $dt_color, 
                    $dt_sz, $dt_dim, $dt_type, $min_date) 
                    = $sth_dates->fetchrow_array();

                if(!$min_date){
                    $min_date = 'n/a';
                    $max_date = 'n/a';
                }
            
                #my ($start_fiscal, $end_fiscal) = _get_fiscal_dates('0');
                #$sth_OOM->execute($start_fiscal, $end_fiscal, $siteId, $vendorId, 
                #                  $styleId, $colorId, $sizeId, $dimId);
                #my ($po_id, $oo_styleid, $oom_qty_bal) = $sth_OOM->fetchrow_array();

                #($start_fiscal, $end_fiscal) = _get_fiscal_dates('1');
                #$sth_OOM->execute($start_fiscal, $end_fiscal, $siteId, $vendorId, 
                #                  $styleId, $colorId, $sizeId, $dimId);
                #my ($po_id30, $oo_styleid30, $oomm_qty_bal) = $sth_OOM->fetchrow_array();
                
                #($start_fiscal, $end_fiscal) = _get_fiscal_dates('2');
                #$sth_OOM->execute($start_fiscal, $end_fiscal, $siteId, $vendorId, 
                #                  $styleId, $colorId, $sizeId, $dimId);
                #my ($po_id60, $oo_styleid60, $oommm_qty_bal) = $sth_OOM->fetchrow_array();
            
                #($start_fiscal, $end_fiscal) = _get_fiscal_dates('3');
                #$sth_OOM->execute($start_fiscal, $end_fiscal, $siteId, $vendorId, 
                #                  $styleId, $colorId, $sizeId, $dimId);
                #my ($po_id90, $oo_styleid90, $oommmm_qty_bal) = $sth_OOM->fetchrow_array();
           
                $sth_sum_mv_qty->execute('7', $colorId, $sizeId, $dimId,
                                     $styleId, $vendorId, $siteId);
                my ($siteId7, $venr_Id7, $style_Id7, $colId7, $szId7, 
                    $dimId7, $avg_retail7, $sum_qty7) 
                    = $sth_sum_mv_qty->fetchrow_array();
            
                $sth_sum_mv_qty->execute('14', $colorId, $sizeId, $dimId,
                                     $styleId, $vendorId, $siteId);
                my ($siteId14, $venr_Id14, $style_Id14, $colId14, $szId14, 
                    $dimId14, $avg_retail14, $sum_qty14) 
                    = $sth_sum_mv_qty->fetchrow_array();
            
                $sth_sum_mv_qty->execute('30', $colorId, $sizeId, $dimId,
                                         $styleId, $vendorId, $siteId);
                my ($siteId30, $venr_Id30, $style_Id30, $colId30, $szId30, 
                    $dimId30, $avg_retail30, $sum_qty30) 
                    = $sth_sum_mv_qty->fetchrow_array();
            
                $sum_qty7  = 0 if(!$sum_qty7);
                $sum_qty14 = 0 if(!$sum_qty14);
                $sum_qty30 = 0 if(!$sum_qty30);

                my $wks_of_supply = 0;

                if(($qty_on_h) &&($sum_qty30)){
                    $wks_of_supply = ((($sum_qty30 / 30) * $qty_on_h) / 7);
                }
                else{
                    $wks_of_supply = "no data";
                }

#                if(($oom_qty_bal) && ($oomm_qty_bal) &&($oommm_qty_bal) && ($oommmm_qty_bal))
#                {
                    #if($sum_qty30)
                    #{
                    push @report, [ $vendor_name,
                                $classId,
                                $styleId,
                                $description,
                                $vendor_style_no,
                                $cost,
                                $retail,
                                $init_mrk_up,
                                $r_retail, 
                                $sum_qty7,
                                $sum_qty14,
                                $sum_qty30,
                                $min_date,
                                $max_date,
                                $wks_of_supply,
                                $qty_on_h, 
                                $siteId]; #,
                                #$oom_qty_bal, 
                                #$oomm_qty_bal, 
                                #$oommm_qty_bal, 
                                #$oommmm_qty_bal ];
                   
                       #for Testing ONLY
                       if($run_total == 90000){
                          %totals = (
                             total_cost   => sprintf( '%.2f', $t_cost ),
                             total_retail => sprintf( '%.2f', $t_retail )
                          );
                          return $report[2] ? ( \@report, %totals ) : undef;
                       }#Test
                    #} #if sum_qty30
                #} #if OOMM and such have values 
                $run_total++;
                $t_cost = 99999;
                $t_retail = 88888;
            }
            if(($t_cost) && ($t_retail)){
               %totals = (
                    total_cost   => sprintf( '%.2f', $t_cost ),
                    :otal_retail => sprintf( '%.2f', $t_retail )
                );
            }
        } #end of if qty_on_h
    }
   print "RUN:$run_total\n"; 

    return $report[2] ? ( \@report, \%totals, $run_total ) : undef;
}

sub inv_on_date
{
    my $args   = shift;
    my $dbh    = $args->{dbh};
    my $dept   = $args->{dept};
    my $date   = $args->{date};
    my $vendor = $args->{vendor};
    my $ex_zero= $args->{ex_zero};
    $date =~ s/-//xmg;
    my $order = $args->{order};
    my $class = $args->{class} ? "and v.class_id = $args->{class}" : q{};
    my $style = $args->{style} ? "and s.style_id = '$args->{style}'" : q{};
    my %ord_hash = (
        style       => 's.style_id',
        site        => 's.site_id',
        dept        => 'sec.department_id',
        description => 'sty.description',
    );
    my $qty_sql;
    my $zero_sql = $ex_zero ? q{} : "and s.qty_on_hand != 0";
    my $site_sql = site_sql( 's.site_id', $args->{site} );

    my $v_sql = $vendor ? "and sty.vendor_id = $vendor" : q{};

    $dbh->do(q{alter session set nls_date_format = 'YYYYMMDD'});

    my $report = $dbh->selectall_arrayref( "
         SELECT
            s.qty_on_hand,
            (select
                  sum(i.inven_move_qty)
             from
                  inventory_movements i
             where
                  i.business_unit_id = 30
                  and i.BUSINESS_UNIT_ID = s.business_unit_id
                  and s.STYLE_ID = i.style_id
                  and s.SITE_ID = i.SITE_ID
                  and to_char(i.INVEN_MOVE_DATE, 'YYYYMMDD') <= '$date'
                  and i.COLOR_ID = s.COLOR_ID
                  and i.SIZE_ID = s.SIZE_ID
                  and nvl(i.dimension_id, 0) = nvl(s.dimension_id,0)
            ) Qty_on_Date,
            s.qty_on_order,
            query_exception_cost(sty.business_unit_id,
               sty.vendor_id,
               sty.style_id,
               s.site_id,
               chr(255),
               null,
               null,
               null) curr_cost,
            get_permanent_retail_price(30,
               S.SITE_ID,
               sty.style_id,
                null,
                null,
                null,
                sysdate, 
                null) curr_retail,
            get_permanent_retail_price(30,
               S.SITE_ID,
               sty.style_id,
                null,
                null,
                null,
                '$date',
                null) RETAIL_ON_DATE,
            s.STYLE_ID,
            sty.DESCRIPTION,
            sty.vendor_style_no,
            s.SITE_ID,
            sec.DEPARTMENT_ID,
            v.class_id,
            sty.vendor_id,
            ven.name
         from
            site_inventories s,
            sections sec,
            styles sty,
            v_dept_class_subclass v,
            vendors ven
         where
            s.BUSINESS_UNIT_ID = 30
            and sec.BUSINESS_UNIT_ID = 30
            and sty.BUSINESS_UNIT_ID = 30
            and v.BUSINESS_UNIT_ID = 30
            and sty.section_id = v.section_id
            $site_sql
            $class
            $style
            $v_sql
            $zero_sql
            and s.STYLE_ID = sty.STYLE_ID
            and sty.SECTION_ID = sec.SECTION_ID
            and ven.vendor_id = sty.vendor_id
            and sec.DEPARTMENT_ID in ($dept)
        order by
            sty.vendor_id
         /*group by
            s.SITE_ID,
            sec.DEPARTMENT_ID,
            s.STYLE_ID,
            sty.vendor_style_no,
            sty.DESCRIPTION,
            sty.vendor_id
         order by
            ord_hash{order}*/
       " );

    my @rpt;
    push @rpt, ['RMS Inventory Report'];
    push @rpt,
        [
        'Current Qty',
        'Total Qty On Date',
        'Qty On Order',
        'Current Cost',
        'Current Retail',
        'Retail On Date',
        'Style ID',
        'Description',
        'Vendor Style',
        'Site ID',
        'Dept',
        'Class ID',
        'DUNS',
        'Vendor Name',
        ];

    my $cnt        = 0;
    my $tot_qty    = 0;
    my $curr_tot_retail = 0;
    my $tot_retail = 0;
    my $curr_tot_qty = 0;

    for ( @{$report} )
    {
        push @rpt, [ @{$_} ];

        $curr_tot_qty += $_->[0];
        $tot_qty    += $_->[1];
        $curr_tot_retail += $_->[0] * $_->[4];
        $tot_retail += $_->[1] * $_->[5];
        $cnt++;
    }

    return $rpt[2]
        ? ( \@rpt,
        { cur_qty => $curr_tot_qty, 
          curr_retail => sprintf('%.2f', $curr_tot_retail),
          date_qty => $tot_qty,
          date_retail => sprintf('%.2f', $tot_retail),} )
        : undef;
}

sub style_details
{
    my $args  = shift;
    my $dbh   = $args->{dbh};
    my $dept  = $args->{dept};
    my $style = $args->{style};
    my $site  = $args->{site};
    my $date  = $args->{date};
    my $duns  = $args->{duns};
    my $class = $args->{class};
    my $type  = $args->{type};
    my $cmd   = $args->{cmd};
    my $rpt;
    my @p_holders;
    my $style_sql = q{};
    my $site_sql = site_sql( 'a.site_id', $site );
    $date =~ s/-//xmg;

    if ($style)
    {
        $style_sql = q{and b.style_id = ?};
        push @p_holders, $style;
    }

    my ( $dept_sql, @depts ) = _create_ph_sql( 'and c.department_id', $dept );
    push @p_holders, @depts;

    my $duns_sql = duns_sql( 'd.vendor_id', $duns );
    my $class_sql = $class ? "and c.parent_class_id = $class"       : q{};
    my $date_sql  = $date  ? qq{and b.last_receipt_date >= '$date'} : q{};

    my $type_sql = q{};
    if ( $type eq 'on_hand_stock' )
    {
        $type_sql = 'and a.qty_on_hand <> 0';
    }
    elsif ( $type eq 'out_of_stock' )
    {
        $type_sql = 'and a.qty_on_hand = 0';
    }

    for (qw(dept))
    {
        croak "$_ is a required field" unless $args->{$_};
    }

    my @report;
    my %totals;
    # removed site and dept display for now. display and printing are crappy
    # when either is a long list. -hrwl
    #push @report, [ "Inventory Style Details $site  Dept: $dept " ];
    push @report, [ "Inventory Style Details" ];
    push @report, [ '*', 'Qty On Hand', 'Current Primary Vendor Cost', 'Retail',
                    'Last Rcvd', 'Style', 'Description', 'Vendor Style',
                    'Color', 'Size', 'DIM', 'Dept', 'Class', 'Site', 'Duns',
                    'Primary Vendor' ];

    $dbh->do(q{alter session set nls_date_format = 'YYYYMMDD'});

    if ($dbh)
    {
         my $sth = $dbh->prepare("
select
         sum(a.qty_on_hand) AS qty_on_hand,
         b.estimated_landed_cost AS current_primary_vendor_cost,
         get_permanent_retail_price (a.business_unit_id, a.site_id, a.style_id
, a.color_id, a.dimension_id, a.size_id, sysdate, null) AS retail_price,
         b.last_receipt_date,
         a.style_id,
         b.description,
         b.vendor_style_no,
         colors.description AS color_description,
         a.size_id,
         a.dimension_id,
         c.department_id,
         c.parent_class_id,
         a.site_id,
         b.vendor_id,
         d.name AS vendor_name,
         a.color_id
         /*sum(a.qty_on_order),
         sum(a.qty_received),
         sum(a.qty_sold_regular),
         sum(a.qty_sold_markdown),
         sum(a.qty_sold_promotion),
         a.color_id*/
      from
         site_inventories a,
         styles b,
         sections c,
         vendors d,
         sites sit,
         colors colors
      where
         a.BUSINESS_UNIT_ID=30
         and a.business_unit_id = 30
         and b.business_unit_id = 30
         and c.business_unit_id = 30
         and d.business_unit_id = 30
         and colors.business_unit_id = 30
         and sit.business_unit_id = 30
         $style_sql
         $dept_sql
         $site_sql
         $duns_sql
         and a.style_id = b.style_id
         and b.section_id = c.section_id
         and b.vendor_id = d.vendor_id
         and a.site_id = sit.site_id
         and a.color_id = colors.color_id
         $class_sql
         $date_sql
         $type_sql
         /*and a.qty_on_hand >= 0 */
         and (sit.date_last_sale is not null or sit.sub_type = 'W')
      group by
         a.style_id,
         c.department_id,
         b.vendor_id,
         b.description,
         a.site_id,
         b.estimated_landed_cost,
         d.name,
         b.vendor_style_no,
         get_permanent_retail_price (a.business_unit_id, a.site_id, a.style_id, a.color_id, a.dimension_id, a.size_id, sysdate, null),
         b.last_receipt_date,
         sit.name,
         c.parent_class_id,
         colors.description,
         a.dimension_id,
         a.size_id,
         a.color_id
            ");

        $sth->execute( @p_holders );
        my ( $total_qty, $total_cost, $total_retail ) = ( 0, 0, 0 );
        while ( my $row = $sth->fetchrow_hashref() )
        {
            $total_qty += $row->{qty_on_hand};
            $total_cost += $row->{current_primary_vendor_cost} * $row->{qty_on_hand};
            $total_retail +=  $row->{retail_price} * $row->{qty_on_hand};

            my $style2 = $row->{style_id};
            my $size = $row->{size_id};
            my $color = $row->{color_id};

            my $link = "<a href=\"inventory_history.html\?cmds=$cmd&sites=$site&style=$style2&size=$size&color=$color&type=ALL&submit=Submit\" onclick=\"newWindow(this.href,'', '', '', 'yes', 'yes', 'no', 'no'); return false;\" title=\"Show transfer details.\">*</a>";

            push @report, [ $link, $row->{qty_on_hand},
                            $row->{current_primary_vendor_cost},
                            $row->{retail_price}, $row->{last_receipt_date},
                            $row->{style_id}, $row->{description},
                            $row->{vendor_style_no}, $row->{color_description},
                            $row->{size_id}, $row->{dimension_id},
                            $row->{department_id}, $row->{parent_class_id},
                            $row->{site_id}, $row->{vendor_id},
                            $row->{vendor_name} ] ;
        }

        my $markup =
            ( $total_retail != 0 )
            ? markup_pct( retail => $total_retail, cost => $total_cost ) * 100
            : q{};

        %totals = (
            total_qty    => $total_qty,
            total_cost   => sprintf( '%.2f', $total_cost ),
            total_retail => sprintf( '%.2f', $total_retail ),
            markup       => sprintf( '%.2f', $markup )
        );
    }

    return $report[2] ? ( \@report, \%totals ) : undef;
}
################
#
sub style_mv_history
{
    my $args = shift;
    my $dbh  = $args->{dbh};

    my $type  = $args->{type}  ? $args->{type}  : q{};
    my $site  = $args->{site}  ? $args->{site}  : q{};
    my $style = $args->{style} ? $args->{style} : q{};
    my $site_sql = site_sql( 'a.site_id', $site );
    my $size_sql = $args->{size} ? qq{and a.size_id = '$args->{size}'} : q{};
    my $color_sql
        = $args->{color} ? qq{and a.color_id = '$args->{color}'} : q{};
    my $dim_sql = $args->{dim} ? qq{and a.dimension_id '$args->{dim}'} : q{};
    my $dun_style =
        $args->{dun_style}
        ? qq{and a.style_id in (select style_id from style_vendors where business_unit_id = 30 and a.vendor_id = '$args->{dun_style}')}
        : q{};

    my $sql_type = q{};
    unless ( $type eq 'ALL' )
    {
        $sql_type = qq{and a.inven_move_type = '$type'} if $type;
    }

    my $date = q{};

    if ( $args->{sdate} && $args->{edate} )
    {
        $date
            = qq{and a.inven_move_date between '$args->{sdate}' and '$args->{edate}'};
    }
    elsif ( $args->{sdate} )
    {
        $date = qq{and a.inven_move_date >= '$args->{sdate}'};
    }
    elsif ( $args->{edate} )
    {
        $date = qq{and a.inven_move_date <= '$args->{edate}'};
    }

    my $duns_sql = q{};
    if ( $args->{duns} && $args->{dun_style} )
    {
        $duns_sql
            = qq{and a.vendor_id in ('$args->{duns}', '$args->{dun_style}')};
    }
    elsif ( $args->{duns} )
    {
        $args->{duns} =~ s/\, \z//xm;
        $duns_sql = qq{and a.vendor_id = '$args->{duns}'};
        $duns_sql = "and a.vendor_id in ($args->{duns})"
            if $args->{duns} =~ m/\,/mx;
    }

    $dbh->do(q{alter session set nls_date_format='YYYYMMDD'});

    my $report;
    eval
    {
       $report = $dbh->selectall_arrayref( "
            SELECT
               a.site_id,
               a.transfer_site_id,
               a.inven_move_qty,
               a.retail_price,
               a.color_id,
               c.description,
               a.size_id,
               a.dimension_id,
               a.inven_move_date,
               a.inven_move_type,
               a.vendor_id,
               a.landed_unit_cost,
               a.po_id,
               a.source_document_id,
               a.created_by_username,
               s.description,
               a.system_date,
               to_char(a.system_date, 'HH:MI:SS'),
               s.vendor_style_no
            FROM
               inventory_movements a,
               colors c,
               styles s
            WHERE
               a.business_unit_id = 30
               and c.business_unit_id = 30
               and s.business_unit_id = 30
               and a.color_id = c.color_id
               and a.style_id = ? 
               and a.style_id = s.style_id 
               $color_sql
               $site_sql
               $size_sql
               $dim_sql
               $dun_style 
               $duns_sql
               $sql_type
               $date
            ORDER BY
               a.inven_move_date
               ", undef, $style );
    };
    if($@)
    {
        return;
    }

    my @rpt;
    my $types = [ 'NUMBER', 'TEXT', 'NUMBER', 'MONEY', 'NUMBER', 'TEXT',
                  'TEXT', 'TEXT', 'DATE', 'TEXT', 'NUMBER', 'MONEY', 'NUMBER',
                  'NUMBER', 'TEXT', 'TEXT', 'DATE' ];

    my %hdr_info = ( description => "$report->[0][15]",
                     vendor      => $report->[0][18] ? "
                                        $report->[0][18]" : 'None',
                     style       => "$style" );

    if ( $report->[0][0] )
    {
        push @rpt,
            [
            'Inventory Movement History'
            ];
        push @rpt,
            [
            'Site',     'Transfer Site', 'Qty',  'Retail',
            'Color ID', 'Color',         'Size',  'DIM',
            'Date',     'Type',          'DUNS', 'Unit Cost', 
            'Qty On Date', 'PO ID', 'SrcDoc',   'Created By', 'System Date',
            ];
        my $cnt        = 0;
        my $tot_qty    = 0;
        my $tot_retail = 0;
        for ( @{$report} )
        {
            my $unit_cost = $_->[11] ? $_->[11] : 0;
            my $src_doc = $_->[9] =~ 'RECEIVING' ? 
               "<a href='#' onclick='rcvrInfo(\"$_->[13]\", \"rcvr_info\");return false;'>$_->[13]</a>" : $_->[13];
            my $po = $_->[12] ? "<a href='#' onclick='newWindow(\"po_details.html?po_num=$_->[12]&sites=$_->[0]&submit=Submit\",\"po_de_name\",\"800\",\"500\",\"yes\",\"yes\",\"no\",\"no\"); return false;'>$_->[12]</a>" : q{};

            $tot_qty += $_->[2];
            push @rpt,
                [
                $_->[0], $_->[1],
                $_->[2], sprintf( '%12.2f', $_->[3] ),
                $_->[4], $_->[5],
                $_->[6], $_->[7],
                $_->[8], $_->[9],
                $_->[10], sprintf( '%12.2f', $unit_cost ),
                $tot_qty, $po,
                $src_doc, $_->[14],
                "$_->[16] $_->[17]",
                ];
            my $retail = $_->[3] ? $_->[3] : 0;
            $tot_retail += $retail * $_->[2];
            $cnt++;
        }

        return ( (\@rpt, $types),\%hdr_info, 
            { qty => $tot_qty, retail => sprintf('%.2f', $tot_retail) } );
    }

    return;
}

sub rcvr_history
{
    my $args = shift;
    my $dbh  = $args->{dbh};
    my $rcvr = $args->{receiver};
    croak 'Receiver Number is required.' unless $rcvr;

    $rcvr =~ s/\s//xmg;

    $dbh->do(q{alter session set nls_date_format = 'YYYYMMDD'});

    my $rcvr_info = $dbh->selectall_arrayref(
        q{
      (
         select
            a.site_id,
            a.po_id,
            a.style_id,
            s.description,
            s.vendor_style_no,
            a.inven_move_qty,
            c.description color_description,
            a.size_id,
            a.dimension_id,
            query_exception_cost(30,a.vendor_id, a.style_id,a.SITE_ID, chr(255),null,null,null)*a.inven_move_qty,
            t.department_id,
            a.inven_move_date,
            a.source_document_id,
            v.name,
            a.vendor_id,
            a.created_by_username
            /*a.inven_move_type,
            a.LANDED_UNIT_COST,
            a.color_id,
            a.transfer_site_id,*/
            /* count (distinct a.style_id) */
         from
            inventory_movements a,
            colors c,
            styles s,
            vendors v,
            sections t
         where
            a.BUSINESS_UNIT_ID=30
            and c.BUSINESS_UNIT_ID=30
            and s.BUSINESS_UNIT_ID=30
            and v.BUSINESS_UNIT_ID=30
            and t.BUSINESS_UNIT_ID=30
            and a.color_id = c.color_id
            and a.style_id = s.style_id
            and a.inven_move_type = 'RECEIVING'
            and a.source_document_id = ?
            and a.vendor_id = v.vendor_id
            and a.section_id = t.section_id
         /* group by
            a.color_id,
            a.dimension_id,
            a.size_id,
            a.inven_move_date,
            a.inven_move_type,
            a.vendor_id,
            a.LANDED_UNIT_COST,
            a.transfer_site_id,
            a.po_id,
            a.source_document_id,
            c.description,
            a.site_id,
            s.vendor_style_no,
            s.description */
      )
      UNION
      (
         select
            a.site_id,
            a.po_id,
            a.style_id,
            s.description,
            s.vendor_style_no,
            a.inven_move_qty,
            c.description color_description,
            a.size_id,
            a.dimension_id,
            query_exception_cost(30,a.vendor_id, a.style_id,a.SITE_ID, chr(255),null,null,null)*a.inven_move_qty,
            null,
            a.inven_move_date,
            a.source_document_id,
            v.name,
            a.vendor_id,
            a.created_by_username
            /*a.inven_move_type,
            a.LANDED_UNIT_COST,
            a.color_id,
            a.transfer_site_id,*/
            /* count (distinct a.style_id) */
         from
            inventory_movements a,
            colors c,
            styles s,
            vendors v
         where
            a.BUSINESS_UNIT_ID=30
            and c.BUSINESS_UNIT_ID=30
            and s.BUSINESS_UNIT_ID=30
            and v.BUSINESS_UNIT_ID=30
            and a.color_id = c.color_id
            and a.style_id = s.style_id
            and a.inven_move_type = 'RECEIVING'
            and a.source_document_id = ?
            and a.vendor_id = v.vendor_id
            and a.section_id is null
         /* group by
            a.color_id,
            a.dimension_id,
            a.size_id,
            a.inven_move_date,
            a.inven_move_type,
            a.vendor_id,
            a.LANDED_UNIT_COST,
            a.transfer_site_id,
            a.po_id,
            a.source_document_id,
            c.description,
            a.site_id,
            s.vendor_style_no,
            s.description */
         )
         order by
            style_id,
            color_description,
            size_id,
            dimension_id}, undef, ( $rcvr, $rcvr )
    );

    my $ticket = $dbh->selectall_arrayref( '
        SELECT
            vendor_ticket_no
        FROM
            receipts
        WHERE
            business_unit_id = 30
            and receipt_id = ?', undef, $rcvr );

    unless ( $ticket->[0][0] )
    {
        my $n_rcvr = sprintf '%.10d', $rcvr;

        my $a_rcvr = sprintf '%5.5s-%1.1s-%4.4s', substr $n_rcvr, 0, 5,
            substr $n_rcvr, 5, 1, substr $n_rcvr, 6, 4;

        my $a_dbh = IBIS::DBI->connect( dbname => 'fms_pqry' );
        $ticket = $a_dbh->selectall_arrayref(
            q{
            SELECT
                receiver_shipper
            FROM
                receiver_hdrs
            WHERE
                company_id = 'H09'
                and receiver_number = ?}, undef, $a_rcvr
        );
    }

    my @array;
    my $rcvr_hdr = {
        receiver => $rcvr,
        rec_date => $rcvr_info->[0][11],
        ticket   => $ticket->[0][0],       #$rcvr_info->[0][12],
        vendor  => "$rcvr_info->[0][13] ($rcvr_info->[0][14])",
        creator => $rcvr_info->[0][15]
    };
    my $types = [ 'NUMBER', 'TEXT', 'NUMBER', 'TEXT', 'TEXT', 'NUMBER', 
                  'TEXT', 'TEXT', 'TEXT', 'MONEY', 'NUMBER' ];
    push @array, ["Style History for Receiver - $rcvr"];
    push @array,
        [
        'Site',         'PO #',     'Style', 'Description',
        'Vendor Style', 'Qty',      'Color', 'Size',
        'DIM',          'Cost Amt', 'Dept'
        ];

    my ( $total_qty, $total_cost ) = ( 0, 0 );
    for ( @{$rcvr_info} )
    {
        $total_qty  += $_->[5];
        $total_cost += $_->[9];
        push @array, [ map {$_} @{$_}[ 0 .. 10 ] ];
    }

    $rcvr_hdr->{total_qty} = $total_qty;
    $rcvr_hdr->{total_cost} = sprintf '%0.2f', $total_cost;
    return $rcvr_info->[0][0] ? ( \@array, $rcvr_hdr, $types ) : undef;
}

sub get_buyer_info
{
    my $args   = shift;
    my $style  = $args->{style};
    my $dbh    = $args->{dbh};
    my $report = $dbh->selectall_arrayref(
        q{
         SELECT
            e.first_name,
            e.last_name,
            d.department_id,
            d.description,
            s.vendor_style_no
         from
            styles s,
            departments d,
            employees e,
            sections t
         where
            s.business_unit_id =30
            and d.business_unit_id =30
            and e.business_unit_id =30
            and s.business_unit_id =30
            and s.style_id = ? 
            and s.section_id = t.section_id
            and t.department_id = d.department_id
            and d.buyer_employee_id = e.employee_id }, undef, $style
    );
    my %hash;

    $hash{name}   = join q{ }, $report->[0][0], $report->[0][1];
    $hash{dept}   = $report->[0][2];
    $hash{desc}   = $report->[0][3];
    $hash{vendor} = $report->[0][4];

    return \%hash;
}

sub _create_ph_sql
{
    my $sql  = shift;
    my $dept = shift;

    $dept =~ s/\, \z//xm;
    $dept =~ s/\A \,//xm;
    my @depts = split ',', $dept;

    my $place;
    if ($#depts)
    {
        $place = join ',', map {q[?]} @depts;
    }
    else
    {
        $place = q[?];
    }

    my $dept_sql = "$sql in (";
    $dept_sql .= $place;
    $dept_sql .= ')';

    return ( $dept_sql, @depts );
}

sub nonspecific_style_info
{
    my $args = shift;
    my $style = $args->{style};
    my $dbh   = $args->{dbh};

    my $sth = $dbh->prepare("
        select
         VENDOR_ID,
         SEASON_ID,
         PRICE_STATUS, /*                    NOT NULL VARCHAR2(1) */
         CONSOLIDATED_TO_STYLE_ID,   /*                 VARCHAR2(14) */
         CONSOLIDATED_COLOR_ID,   /*                    VARCHAR2(3) */
         CONSOLIDATED_SIZE_IND,   /*           NOT NULL VARCHAR2(1) */
         TICKET_FORMAT_ID,   /*                NOT NULL NUMBER(2) */
         LOCAL_FIRST_COST,   /*                NOT NULL NUMBER(12,2) */
         LANDED_COST_ID,   /*                  NOT NULL VARCHAR2(4) */
         ESTIMATED_LANDED_COST,   /*           NOT NULL NUMBER(12,4) */
         VALUATION_COST,   /*                  NOT NULL NUMBER(12,4) */
         AVAILABILITY_STATUS,   /*             NOT NULL VARCHAR2(1) */
         COST_METHOD,   /*                     NOT NULL VARCHAR2(1) */
         HISTORY_IND,   /*                     NOT NULL VARCHAR2(1) */
         SEASON_WIPE_OUT_IND,   /*             NOT NULL VARCHAR2(1) */
         SIZE_RANGE_ID,   /*                   NOT NULL VARCHAR2(3) */
             VENDOR_STYLE_NO,   /*                          VARCHAR2(25) */
             DESCRIPTION,   /*                              VARCHAR2(30) */
             FOREIGN_DESCRIPTION,   /*                      VARCHAR2(30) */
             COORDINATE_ID,   /*                            NUMBER(5) */
             FOREIGN_FIRST_COST,   /*                       NUMBER(12,2) */
             COMPARATIVE_PRICE,   /*                        NUMBER(12,2) */
             MINIMUM_LEVEL,   /*                            NUMBER(5) */
             NOTE_1,   /*                                   VARCHAR2(30) */
             NOTE_2,   /*                                   VARCHAR2(30) */
             NOTE_3,   /*                                   VARCHAR2(30) */
             PRIMARY_BIN_LOCATION,   /*                     VARCHAR2(6) */
             SPIFF_ID,   /*                                 VARCHAR2(2) */
             SPIFF_START_DATE,   /*                         DATE */
             SPIFF_END_DATE,   /*                           DATE */
             DISPLAY_PATTERN,   /*                 NOT NULL VARCHAR2(1) */
             CURRENCY_ID,   /*                     NOT NULL VARCHAR2(10) */
             CARTON_QTY,   /*                               NUMBER(3) */
             AVERAGE_COST,   /*                             NUMBER(12,2) */
             FIRST_DISTRIBUTION_DATE,   /*                  DATE */
             LAST_PO_QTY,   /*                              NUMBER(11,3) */
             LAST_PO_ISSUED_ID,   /*                        NUMBER(10) */
             LAST_PO_DISCOUNT_RATE,   /*                    NUMBER(6,3) */
             LAST_PO_DISCOUNT,   /*                         NUMBER(13,2) */
             LAST_DISTRIBUTION_DATE,   /*                   DATE */
             LAST_CANCEL_DATE,   /*                         DATE */
             ORDERED_RETAIL_VALUE,   /*            NOT NULL NUMBER(13,2) */
             ON_ORDER_COST,   /*                   NOT NULL NUMBER(13,2) */
             MARKDOWN_VALUE,   /*                  NOT NULL NUMBER(13,2) */
             LATEST_LANDED_COST,   /*                       NUMBER(12,2) */
             LAST_RECEIPT_DATE,   /*                        DATE */
             LAST_PO_RECEIVED_ID,   /*                      NUMBER(10) */
             QTY_SOLD_REGULAR,   /*                NOT NULL NUMBER(11,3) */
             QTY_SOLD_PROMOTION,   /*              NOT NULL NUMBER(11,3) */
             QTY_SOLD_MARKDOWN,   /*               NOT NULL NUMBER(11,3) */
             QTY_RECEIVED,   /*                    NOT NULL NUMBER(11,3) */
             QTY_ORDERED,   /*                     NOT NULL NUMBER(11,3) */
             QTY_ON_HAND,   /*                     NOT NULL NUMBER(11,3) */
             QTY_ON_ORDER,   /*                    NOT NULL NUMBER(11,3) */
             SALES_REGULAR,   /*                   NOT NULL NUMBER(13,2) */
             SALES_PROMOTION,   /*                 NOT NULL NUMBER(13,2) */
             SALES_MARKDOWN,   /*                  NOT NULL NUMBER(13,2) */
             SALES_COST,   /*                      NOT NULL NUMBER(13,2) */
             RECEIVED_RETAIL_VALUE,   /*           NOT NULL NUMBER(13,2) */
             RECEIVED_LANDED_COST,   /*            NOT NULL NUMBER(12,2) */
             FIRST_RECEIPT_DATE,   /*                       DATE */
             IMAGE_FILENAME,   /*                           VARCHAR2(32) */
             IMAGE_FORMAT,   /*                             VARCHAR2(4) */
             HAZARDOUS_ID,   /*                             VARCHAR2(5) */
             BREAKUP_ORIGINAL_UNITS,   /*                   NUMBER(11,3) */
             BREAKUP_EXPLODED_UNITS,   /*                   NUMBER(11,3) */
             EXPLODED_RETAIL_PRICE,   /*                    NUMBER(12,2) */
             EXPLODED_COST,   /*                            NUMBER(12,2) */
             INNER_PACK_QTY,   /*                  NOT NULL NUMBER(11,3) */
             OUTER_PACK_QTY,   /*                  NOT NULL NUMBER(11,3) */
             OUTER_PACK_DESCRIPTOR,   /*                    VARCHAR2(5) */
             WEIGHT,   /*                                   NUMBER(11,3) */
             CUBE,   /*                                     NUMBER(11,3) */
             PLU_DESCRIPTOR,   /*                           VARCHAR2(30) */
             DATE_CREATED,   /*                    NOT NULL DATE */
             DATE_DISCONTINUED,   /*                        DATE */
             COST_FACTOR,   /*                     NOT NULL NUMBER(11,3) */
             COST_DESCRIPTOR,   /*                 NOT NULL VARCHAR2(5) */
             FOR_RESALE_IND,   /*                  NOT NULL VARCHAR2(1) */
             MARKDOWN_QTY,   /*                    NOT NULL NUMBER(11,3) */
             ACTIVITY_IND,   /*                    NOT NULL VARCHAR2(1) */
             CURRENCY_USAGE_ID,   /*                        VARCHAR2(2) */
             CURRENCY_TYPE_ID,   /*                         VARCHAR2(10) */
             HARMONIZED_TARIFF_ID,   /*                     VARCHAR2(30) */
             VAT_REGULAR,   /*                              NUMBER(13,2) */
             VAT_PROMOTION,   /*                            NUMBER(13,2) */
             VAT_MARKDOWN,   /*                             NUMBER(13,2) */
             COUNTRY_OF_SUPPLY_ID,   /*                     VARCHAR2(10) */
             COUNTRY_OF_ORIGIN_ID,   /*                     VARCHAR2(10) */
             PRICING_LEVEL,   /*                   NOT NULL VARCHAR2(1) */
             STYL_BUSINESS_UNIT_ID,   /*                    NUMBER(2) */
             SCLR_BUSINESS_UNIT_ID,   /*                    NUMBER(2) */
             HMTR_BUSINESS_UNIT_ID,   /*                    NUMBER(2) */
             CRDN_BUSINESS_UNIT_ID,   /*                    NUMBER(2) */
             SPFF_BUSINESS_UNIT_ID,   /*                    NUMBER(2) */
             STYLE_TYPE,   /*                      NOT NULL VARCHAR2(7) */
             MULTI_STYLE_ID,   /*                           VARCHAR2(14) */
             MULTI_QTY    /*                       NOT NULL NUMBER(11,3) */
            from styles
            where
               business_unit_id=30
               and style_id = ?");

    $sth->execute($style);

    my $cnt = 0;
    my $h_ref;
    while(my $a = $sth->fetchrow_hashref())
    {
        $h_ref = $a;
        $cnt++;
    }

    return $cnt ? $h_ref : undef;
}

1;

