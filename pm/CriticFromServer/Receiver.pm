package MCCS::RMS::Receiver;

#
#===============================================================================
#
#         FILE:  Receiver.pm
#
#  DESCRIPTION:  List of subroutines for Receiver Web Reports
#
#        FILES:  ---
#         BUGS:  ---
#        NOTES:  ---
#       AUTHOR:   (Matthew B. Pierpoint), <pierpointmb@usmc-mccs.org>
#      COMPANY:
#      VERSION:  1.0
#      CREATED:  05/12/2006 10:34:18 AM EDT
#     REVISION:  ---
#===============================================================================
require Exporter;

use strict;
use warnings;
use version; our $VERSION = qv('1.0');
use English qw( -no_match_vars );

use vars qw(@EXPORT_OK);
use base qw(Exporter);

@EXPORT_OK = qw( receiver_info 
              receiver_details
              receiver_cost );

use Carp;

sub receiver_info
{
   my $args = shift;
   my $rcvr = $args->{receiver};
   my $dbh = $args->{dbh};

   croak 'Must supply a Receiver #.' unless $rcvr;
   croak 'Must supply FMS Database handle.' unless $dbh;

   $rcvr = _pad_receiver($rcvr);

   my $rcvr_info;
   eval
   {
      $rcvr_info = $dbh->selectall_arrayref("
            SELECT
               r.receipt_id,
               r.po_id,
               r.vendor_id,
               v.name,
               r.received_date,
               r.vendor_ticket_no,
               b.style_id
            FROM
               receipts r,
               vendors v,
               received_items b
            WHERE
               r.business_unit_id = 30
               and v.business_unit_id = 30
               and b.business_unit_id = 30
               and r.receipt_id = b.receipt_id
               and v.vendor_id = r.vendor_id
               and r.receipt_id = ?
            GROUP BY
               r.receipt_id,
               r.po_id,
               r.vendor_id,
               v.name,
               r.received_date,
               r.vendor_ticket_no,
               b.style_id",undef, $rcvr);
   };
   return if $EVAL_ERROR;


   my @r_array;
   push @r_array, [ 'Receiver Info' ];
   push @r_array, [ 'Interface Date', 'Receipt ID', 'PO ID', 'Vendor ID', 
                    'Vendor', 'Recvd Date', 'Ticket #', 'Buyer', 
                    'Received By' ];

   my $types = [ qw(DATE NUMBER TEXT TEXT TEXT DATE NUMBER DATE NUMBER) ];

   my %rcv;
   my $cnt = 0;
   for(@{$rcvr_info})
   {
       my ($rec, $po, $ven_id, $ven, $rdate, $ven_t, $style) = @{$_};

       $po = !$po ? 'No PO' : $po;

       my $buyer = $dbh->selectall_arrayref("
         SELECT
            e.first_name,
            e.last_name
         FROM
            employees e,
            departments d,
            sections t,
            styles st
         WHERE
            e.business_unit_id = 30
            and d.business_unit_id = 30
            and t.business_unit_id = 30
            and e.employee_id = d.buyer_employee_id
            and d.department_id = t.department_id
            and t.section_id = st.section_id
            and st.style_id = ?", undef, $style);

       my $r_name = $dbh->selectall_arrayref("
         SELECT
            i.created_by_username
         FROM
            inventory_movements i
         WHERE
            i.source_document_id = ?", undef, $rec);

        $rcv{$rec} = [ $rec, $po, $ven_id, $ven, $rdate, $ven_t,
                         "$buyer->[0][0] $buyer->[0][1]", $r_name->[0][0] ];
        $cnt++;
   }
  
   my $fms_dbh = IBIS::DBI->connect( dbname => 'fmsinteg' );

   my $iface_date;
   eval
   {
        $iface_date = $fms_dbh->selectall_arrayref('
              SELECT
                date
              FROM
                rms_rcv_hdr 
              WHERE
                receiver_id = ?', undef, $rcvr);
   };
   return if $EVAL_ERROR;

   for(keys %rcv)
   {
      push @r_array, [ $iface_date->[0][0], @{$rcv{$_}} ];
   }

   return $cnt ? (\@r_array, $types) : undef; 
}

sub receiver_details
{
    my $args = shift;
    my $rcvr = $args->{receiver};
    my $dbh  = $args->{dbh};
    $rcvr =~ s/ //g;

    croak 'Must supply a receiver #.'             unless $rcvr;
    croak 'Must supply Essentus Database handle.' unless $dbh;

    my $ajax_id = 'inv_mv_history';

    $dbh->do(q(alter session set nls_date_format = 'YYYYMMDD'));

    my $results;
        $results = $dbh->selectall_arrayref( '
                   SELECT
   --                   a.update_ind,
                      b.FIN_LINE_NO,
                      b.receipt_id,
                      a.po_id,
                      a.site_id,
                      b.ESTIMATED_LANDED_COST,
                      b.QTY_RECEIVED,
                      b.ESTIMATED_LANDED_COST*b.QTY_RECEIVED,
                      b.size_id,
                      cc.description,
                      b.dimension_id,
                      c.vendor_style_no,
                      c.description,
                      b.style_id,
                      query_exception_cost(30,a.vendor_id, b.style_id,a.SITE_ID, chr(255),null,null,null),
                      a.VENDOR_ID
   --                   a.status,
   --                   a.vendor_ticket_no,
   --                   v.name,
   --                   b.color_id,
   --                   a.received_date,
                   FROM
                      receipts a ,
                      received_items b ,
                      styles c,
                      vendors v,
                      colors cc
                   WHERE
                      a.receipt_id = b.receipt_id
                      and a.BUSINESS_UNIT_ID = 30
                      and b.business_unit_id = 30
                      and c.business_unit_id = 30
                      and v.business_unit_id = 30
                      and cc.BUSINESS_UNIT_ID = 30
                      and a.receipt_id = ?
                      and b.style_id = c.style_id
                      and a.vendor_id = v.vendor_id
                      and b.color_id = cc.color_id
                  ORDER BY
                      b.FIN_LINE_NO,
                      b.style_id', undef, $rcvr );

    my @array;
    push @array, ["Receiver Details for $rcvr"];
    push @array,
        [
        'Line', 'Rec ID', 'PO',   'Vendor-Remit', 'Site', 'EstLanded Cost',
        'Qty',  'Credit', 'Total',  'Size', 'Color', 'DIM',  'Vendor Style',
        'Description',  'Style ID', 'Curr Cost', 'Total Curr Cost' 
        ];
        
    my $types = [ qw(NUMBER NUMBER NUMBER TEXT NUMBER MONEY NUMBER MONEY MONEY NUMBER TEXT TEXT TEXT TEXT NUMBER MONEY MONEY) ];

    my $cnt = 0;
    for ( @{$results} )
    {
       my ( $line, $rec_id, $po, $site, $landed_cost, $qty, $total,  
            $size, $color, $dim, $vendor_style, $desc,  $style_id, $cost,
            $vendor)
           =  @{$_};

       $style_id = "<a href='#' onclick='getInvHistory(\"$style_id\", \"$site\", \"$ajax_id\"); return false;'>$style_id</a>";

       my $fms_dbh = IBIS::DBI->connect( dbname => 'fmsinteg' );
       my $duns = $fms_dbh->selectall_arrayref("
             SELECT
                vendor_id 
             FROM
                fms_vendors
             where 
                duns_number = ?", undef, $vendor);

       my $dbh = IBIS::DBI->connect( dbname => 'fms_pctc' );

       my $remit = $dbh->selectall_arrayref('
             SELECT
                remit_id
             FROM
                vendor_remits
             WHERE
                vendor_id = ?', undef, $duns->[0][0]);

       my $tot_credit = ($total < 0) ? $total : q{};
       my $tot = ($total >= 0) ? $total : q{};
       my $tot_cost = $qty * $cost;

       push @array, [ $line, $rec_id, $po, "$duns->[0][0]-$remit->[0][0]", 
                      $site, $landed_cost, $qty, $tot_credit, $tot,  $size, 
                      $color, $dim,  $vendor_style, $desc,  $style_id, $cost, 
                      $tot_cost ];
       $cnt++;
    }

    return $cnt ? ( \@array, $types, $ajax_id ) : undef;

}

sub receiver_cost
{
    my $args = shift;
    my $rcvr = $args->{receiver};
    my $dbh = $args->{dbh};

    croak 'Must supply a Receiver #.' unless $rcvr;
    croak 'Must supply Essentus Db handle.' unless $rcvr;

    my $results;
    eval
    {
        $results = $dbh->selectall_arrayref("
                (SELECT
                   i.site_id,
                   i.source_document_id,
                   I.INVEN_MOVE_DATE,
                   i.vendor_id,
                   i.po_id,
                   v.department_id,
                   e.last_name,
                   v.class_id,
                   v.sub_class_id,
                   i.style_id,
                   s.description,
                   s.vendor_style_no,
                   query_exception_cost(30,i.vendor_id, i.style_id,I.SITE_ID, chr(255),null,null,null),
                   i.landed_unit_cost,
                   sum(i.inven_move_qty)
                FROM
                   inventory_movements i,
                   styles s,
                   v_dept_class_subclass v,
                   departments d,
                   employees e
                WHERE i.business_unit_id = 30 and
                   s.business_unit_id = 30 and
                   v.business_unit_id = 30 and
                   d.business_unit_id = 30 and
                   e.business_Unit_id = 30 and
                   i.style_id = s.style_id and
                   i.section_id = v.section_id and
                   v.department_id = d.department_id and
                   d.buyer_employee_id = e.employee_id and
                   i.inven_move_type = 'RECEIVING' AND
                   i.source_document_id = ?
                GROUP BY
                   i.site_id,
                   i.source_document_id,
                   i.inven_move_date,
                   i.vendor_id,
                   i.po_id,
                   e.last_name,
                   v.department_id,
                   v.class_id,
                   v.sub_class_id,
                   i.style_id,
                   s.description,
                   s.vendor_style_no,
                   i.landed_unit_cost
             )
             ORDER BY
                i.style_id ", undef, $rcvr );

    };
    return if $EVAL_ERROR;

    my @array;
    push @array, [ "Receiver Cost for $rcvr" ];
    push @array, [ 'Site', 'Recvr', 'Date', 'Vendor #', 'PO', 'Dept', 'Buyer',
                  'Class', 'SubClass', 'Style', 'Description', 'Vendor Style',
                  'Current Cost', 'Cost', 'Units', 'Diff' ];
    my $types = [ 'TEXT', 'NUMBER', 'DATE', 'NUMBER', 'TEXT', 'NUMBER','TEXT',
                  'NUMBER','NUMBER','TEXT', 'TEXT','TEXT','MONEY','MONEY',
                  'NUMBER', 'NUMBER' ];

    my $cnt = 0;
    for(@{$results})
    {
        push @array, [ @{$_}, q{} ];
        $cnt++;
    }

    return $cnt ? (\@array, $types) : undef;
}

sub _pad_receiver
{
    my $receiver = shift; 
    my $space = 0;
    $receiver =~ s/(\d)/$space++;$1/eg;
    for(1..(10-$space))
    {
       $receiver =~ s/^/ /;
    }

    return $receiver;
}
1;
