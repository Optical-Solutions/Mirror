--------------------------------------------------------
--  DDL for Procedure MAKE_TABLE_P8_ONWARD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."MAKE_TABLE_P8_ONWARD" IS

v_sql varchar2(5000); 

BEGIN

v_sql := q'[ create table im_p8_onward   COMPRESS PARALLEL 8 
  TABLESPACE ERICDATA
  PARTITION BY RANGE(merchandising_week)
  INTERVAL (1)	--only works in 11g, 
                --will automatically create partitions 
                --with interval as needed
  (
	PARTITION p1 VALUES LESS THAN (2)
  ) as 
   select site_id, style_id, color_id, size_id, dimension_id,
              inven_move_type, inven_move_qty, inven_move_date, retail_price,
              landed_unit_cost, merchandising_year, merchandising_week          
   from inventory_movements@MC2R
   where inven_move_date >= to_date('28-AUG-2011','DD-MON-YYYY') ]';
execute immediate v_sql;
commit;

END;

/
