--------------------------------------------------------
--  DDL for Procedure P_RECALC_LV5LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_RECALC_LV5LOC" (p_lv5loc_id numeric)
as
--Change History
-- 06/13/2003 Sachin 	Modified for Bugfix #: 15534
begin
  -- floor
  update maxdata.lv5loc
     set tot_lv7flr = ( select sum(decode(nvl(shape_lkup_id,0),2,aso_area,width*depth) * alloc_space)
                        from maxdata.lv7loc
                        where lv7loc.lv5loc_id = p_lv5loc_id )
   where lv5loc.lv5loc_id = p_lv5loc_id ;

  -- display
  update maxdata.lv5loc
     set tot_lv7dsp = ( select sum((height*width*(on_dsp_front+on_dsp_back))+(height*depth*(on_dsp_left+on_dsp_right)) * alloc_space)
                        from maxdata.lv7loc
                        where lv7loc.lv5loc_id = p_lv5loc_id )
   where lv5loc.lv5loc_id = p_lv5loc_id ;

  -- cubic
  update maxdata.lv5loc
     set tot_lv7cub = ( select sum(decode(nvl(shape_lkup_id,0),2,aso_area*height,height*width*depth) * alloc_space)
                        from maxdata.lv7loc
                        where lv7loc.lv5loc_id = p_lv5loc_id )
   where lv5loc.lv5loc_id = p_lv5loc_id ;

  -- linear
  update maxdata.lv5loc
     set tot_lv7lin = ( select sum((width*(on_dsp_front+on_dsp_back))+(depth*(on_dsp_left+on_dsp_right)) * alloc_space)
                        from maxdata.lv7loc
                        where lv7loc.lv5loc_id = p_lv5loc_id )
   where lv5loc.lv5loc_id = p_lv5loc_id ;

  -- update lv7loc
  update maxdata.lv7loc
     set lv7loc.alloc_sq_meters =
      ( select (decode(nvl(shape_lkup_id,0),2,aso_area,width*depth) / lv5loc.tot_lv7flr) * (lv7loc.width*lv7loc.depth)* lv7loc.alloc_space
        from   maxdata.lv5loc
        where  lv5loc.lv5loc_id = p_lv5loc_id )
  where lv7loc.lv5loc_id = p_lv5loc_id ;
end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_RECALC_LV5LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_RECALC_LV5LOC" TO "MAXUSER";
