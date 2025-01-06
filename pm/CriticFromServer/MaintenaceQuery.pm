#######################################
#This is for WMS and using the same queries in multiple spots
#Ibis, BIN perl program, etc
# 
#######################################

package IBIS::WMS::MaintenaceQuery;

use strict;
use warnings;
use Carp;
use base qw(IBIS::xController::DataAccessor);
use IBIS::Log::File;
use Data::Dumper;
use Time::localtime;
my $pkg = __PACKAGE__;

#--------------------------------------------------------------------------
our (@EXPORT_OK, %EXPORT_TAGS);

@EXPORT_OK = qw();

%EXPORT_TAGS = (
    ALL => [ @EXPORT_OK ],
    );

    
#--------------------------------------------------------------------------
# Hey what I am calling in the BASE has the missing link that
# makes this all work so easily.... Code once, Use Anywhere, 
#--------------------------------------------------------------------------

sub get_record {
    my $self = shift;
    my %params;
    while (my $key = shift) {
        my $value = shift;
        $params{$key} = $value;
    }    
    for my $key (keys %params) {
        $self->{$key} = $params{$key} 
    }
    
    my $sql;      
    $sql = $self->flow_picks() if ('getFlowPicks' eq $self->{dataSource}) ;
    
    my $results;
    $results = $self->multi_row_sql( $sql, \%params) if ('getFlowPicks' eq $self->{dataSource}) ;  
    
    return $results;   
      
}

sub set_record {
    my $self = shift;
    my %params;
    while (my $key = shift) {
        my $value = shift;
        $params{$key} = $value;
    }    
    for my $key (keys %params) {
        $self->{$key} = $params{$key} 
    }
    
    my $sql;      
    $sql = $self->insert_rdi_close_picks() if ('insClosePicks' eq $self->{dataSource}) ;
    $sql = $self->update_rdi_close_picks() if ('updClosePicks' eq $self->{dataSource}) ;
    
    my $results;
    $results = $self->bind_execute_sql( $sql, \%params) if ('insClosePicks' eq $self->{dataSource}) ;
    $results = $self->bind_execute_sql( $sql, \%params) if ('updClosePicks' eq $self->{dataSource}) ;    
    
    return $results;   
}

#--------------------------------------------------------------------------#
#     Select Query Section
#--------------------------------------------------------------------------#
sub flow_picks {
    my $self = shift;
    my %params = %{$self};
    
    my $substitue = '';
    
    $substitue .= " and d.site_id = $params{rdc_id}" if (defined $params{rdc_id});
    $substitue .= " and dp.site_id = $params{site_id}" if ($params{site_id});
    $substitue .= " and dpd.style_id = $params{style_id}" if ($params{style_id});
    $substitue .= " and d.po_id = $params{po_id}" if ($params{po_id});
    $substitue .= " and d.receipt_id = $params{receipt_id}" if ($params{receipt_id});
    $substitue .= " and d.distribution_id = $params{distribution_id}" if ($params{distribution_id});
    $substitue .= " and dpd.date_created <= trunc(sysdate - $params{days_past})" if ($params{days_past});
#    $substitue .= qq( and dpd.date_created between to_date('$params{from_date}','yyyymmdd') 
#                      and to_date('$params{to_date}','yyyymmdd')) if ($params{to_date} && $params{from_date});
#      
    my $sql = qq(

SELECT 
  d.site_id rdc_id, d.po_id, d.receipt_id, 
  d.distribution_id, dp.site_id, dpd.style_id, 
  dpd.color_id, dpd.size_id, dpd.dimension_id, 
  dpd.qty_distributed, dpd.qty_picked, dpd.total_qty_picked, 
  to_char(dpd.date_created,'yyyy-mm-dd') date_created, 
  dpd.completed_ind, d.completed_date, dpd.status, cp.close_pick_found, cp.status close_status
FROM distributions d
Left Join Dist_Picks Dp On (d.business_unit_id = dp.business_unit_id and
                            d.distribution_id = dp.source_id)
Left Join Dist_Pick_Details Dpd On (dp.business_unit_id = dpd.business_unit_id and
                                    dp.distribution_pick_id = dpd.distribution_pick_id)
left join (select w.*, '1' close_pick_found from wms_ibis_close_picks w) cp 
    on (cp.rdc_id = d.site_id and
        cp.po_id = d.po_id and
        cp.receipt_id = d.receipt_id and
        cp.distribution_id = d.distribution_id and
        cp.site_id = dp.site_id and
        cp.style_id = dpd.style_id and
        cp.color_id =  dpd.color_id and
        cp.size_id = dpd.size_id and
        nvl('cp.dimension_id','null') = nvl('cp.dimension_id','null') and
        cp.status <> 'D' and
        cp.close_type = 'FL')
                                      
WHERE d.business_unit_id = '30'
AND d.release_for_picking_ind = 'Y'
AND d.po_id IS NOT NULL 
And D.Distribution_Source = 'RECEIVING' 
And Dpd.Status < '4'
%s
order by d.po_id, dpd.style_id, dpd.color_id, dpd.size_id, dpd. dimension_id, dp.site_id
    );

 $sql = sprintf $sql, $substitue;

return $sql;

 }
 
 

#--------------------------------------------------------------------------#
#     Insert/Update Query Section
#--------------------------------------------------------------------------#


sub insert_rdi_close_picks {
    my $self = shift;
    my %params;
    while (my $key = shift) {
        my $value = shift;
        $params{$key} = $value;
    }
    #the order that the values will be place in the value bind
    @{$self->{ $self->{dataSource}."BindOrder" } } = ('rdc_id', 
                                                       'receipt_id', 
                                                       'po_id',
                                                       'distribution_id', 
                                                       'site_id', 
                                                       'style_id', 
                                                       'color_id', 
                                                       'size_id', 
                                                       'dimension_id',
                                                       'close_type',
                                                       'created_by', 
                                                       'status'
                                                      );
    
    my $sql = qq( 
insert into wms_ibis_close_picks  
  (rdc_id, receipt_id, po_id, distribution_id, site_id, style_id, color_id, size_id, dimension_id,
   close_type, created_by, status, created_ts)
values (?,?,?,?,?,?,?,?,?,?,?,?,sysdate )
    );
    return $sql;  #TODO should this return $sql.
 }

sub update_rdi_close_picks {
    my $self = shift;
    my %params;
    while (my $key = shift) {
        my $value = shift;
        $params{$key} = $value;
    }
    #the order that the values will be place in the value bind
    @{$self->{ $self->{dataSource}."BindOrder" } } = ( 'status',
                                                       'updated_by',
                                                       'rdc_id', 
                                                       'receipt_id', 
                                                       'po_id',
                                                       'distribution_id', 
                                                       'site_id', 
                                                       'style_id', 
                                                       'color_id', 
                                                       'size_id', 
                                                       'dimension_id',
                                                       'close_type',
                                                       );
    
    my $sql = qq( 
update wms_ibis_close_picks set status= ?, updated_by = ?, updated_ts = sysdate 
 where rdc_id = ? and
       receipt_id = ? and
       po_id =  ? and 
       distribution_id = ? and 
       site_id = ? and
       style_id = ? and 
       color_id = ? and
       size_id = ? and
       nvl(dimension_id, 'null') = nvl( ? , 'null') and
       close_type = ? and
       status = 'A'  
    );
    return $sql;  #TODO should this be returning $sql.
 }

#-----------------------------------------------------
# Calling Store procedure with values being return
#-----------------------------------------------------
# sub example_sp {
#   #calls Database SP
#    my $self = shift;
#    my %params = @_;
#    
#    #has to be same as params passing from calling program
#    @{$self->{ $self->{dataSource}."bind_in_out" } } = ('site_id:in',
#                                                        'merch_year:in',
#                                                        'mc_config_id:in',
#                                                        'eff_date:in',
#                                                        'process:in',
#                                                        'modified_by:in',
#                                                        'mc_sqft_hdr_id:out',
#                                                        'found:out'
#                                                       );
#    
#    my $sql = qq( 
#      Begin create_example_sp(:site_id, :merch_year, :mc_config_id, :eff_date, :process, :modified_by, :mc_sqft_hdr_id, :found ); End;  
#    );
#
#}
#----------------------------------------------------- 
# In your calling program make sure you init then use
# below example.  All your return values will be in the corresponding
# hash ref with the name you gave in "bind_in_out" 
#-----------------------------------------------------
# $exmpRtn = $dataAccess->set_record( dataSource    => 'example_sp', 
#                                         merch_year   => $ARGS{merch_year},
#                                         site_id      => $ARGS{site_id},
#                                         eff_date     => $ARGS{eff_date},
#                                         modified_by  => $user->username(),
#                                         process      => 'exists',
#                                         mc_config_id => $ARGS{mc_config_id_fk}, );
#
# $exmpRtn->{found} would be in the ref due to it being an 'out' value of the Oracle SP





#----- do not remove "1" -----#

1;

__END__

