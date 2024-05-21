--------------------------------------------------------
--  DDL for Procedure P_UPD_LVL321_SPACE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_UPD_LVL321_SPACE" (local_lv4loc_row IN lv4loc%ROWTYPE)
AS

-- Procedure to update space fields at levels 3,2,1 on delete of a store.

BEGIN

 update maxdata.lv3loc
       set tot_lv4flr = (tot_lv4flr - decode(nvl(local_lv4loc_row.shape_lkup_id,0),2,local_lv4loc_row.aso_area,
                         local_lv4loc_row.width*local_lv4loc_row.depth)),
           tot_lv4cub = (tot_lv4cub - decode(nvl(local_lv4loc_row.shape_lkup_id,0),2,
                         local_lv4loc_row.aso_area*local_lv4loc_row.height,
                         local_lv4loc_row.width*local_lv4loc_row.depth*local_lv4loc_row.height)),
           tot_lv7dsp =  tot_lv7dsp - local_lv4loc_row.tot_lv7dsp,
           tot_lv7lin =  tot_lv7lin - local_lv4loc_row.tot_lv7lin
  where lv3loc.lv3loc_id = local_lv4loc_row.lv3loc_id;



update maxdata.lv2loc
        set (tot_lv4flr, tot_lv4cub, tot_lv7dsp, tot_lv7lin) =
          ( select sum(tot_lv4flr), sum(tot_lv4cub), sum(tot_lv7dsp), sum(tot_lv7lin)
            from   maxdata.lv3loc
            where  lv3loc.lv2loc_id = lv2loc.lv2loc_id
            group by lv3loc.lv2loc_id ) ;



 update maxdata.lv1loc
        set (tot_lv4flr, tot_lv4cub, tot_lv7dsp, tot_lv7lin) =
          ( select sum(tot_lv4flr), sum(tot_lv4cub), sum(tot_lv7dsp), sum(tot_lv7lin)
            from   maxdata.lv2loc
            where  lv2loc.lv1loc_id = lv1loc.lv1loc_id
            group by lv2loc.lv1loc_id ) ;

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_UPD_LVL321_SPACE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_UPD_LVL321_SPACE" TO "MAXUSER";
