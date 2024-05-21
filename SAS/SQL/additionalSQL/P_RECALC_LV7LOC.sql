--------------------------------------------------------
--  DDL for Procedure P_RECALC_LV7LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_RECALC_LV7LOC" (p_lv7loc_id numeric)
as

  local_used_cubic  NUMBER(25,9) ;
  local_used_dsp    NUMBER(25,9) ;
  local_used_flr    NUMBER(25,9) ;
  local_used_linear NUMBER(25,9) ;

begin

  select sum(used_cubic_meters)
  into   local_used_cubic
  from   maxdata.lv10loc
  where  lv10loc.lv7loc_id = p_lv7loc_id ;

  select sum(used_dsp_sqmeters)
  into   local_used_dsp
  from   maxdata.lv10loc
  where  lv10loc.lv7loc_id = p_lv7loc_id ;

  select sum(used_flr_sqmeters)
  into   local_used_flr
  from   maxdata.lv10loc
  where  lv10loc.lv7loc_id = p_lv7loc_id ;

  select sum(used_linear_meters)
  into   local_used_linear
  from   maxdata.lv10loc
  where  lv10loc.lv7loc_id = p_lv7loc_id ;

  update maxdata.lv7loc
     set lv7loc.used_cubic_meters = local_used_cubic,
         lv7loc.used_dsp_sqmeters = local_used_dsp,
         lv7loc.used_flr_sqmeters = local_used_flr,
         lv7loc.used_linear_meters = local_used_linear
   where lv7loc.lv7loc_id = p_lv7loc_id ;

  if nvl(local_used_cubic,0) <> 0 then
    update maxdata.lv10loc
       set pct_used_cubic = (used_cubic_meters / local_used_cubic) * 100
     where lv10loc.lv7loc_id = p_lv7loc_id ;
  end if ;

  if nvl(local_used_dsp,0) <> 0 then
    update maxdata.lv10loc
       set pct_used_dsp = (used_dsp_sqmeters / local_used_dsp) * 100
     where lv10loc.lv7loc_id = p_lv7loc_id ;
  end if ;

  if nvl(local_used_flr,0) <> 0 then
    update maxdata.lv10loc
       set pct_used_flr = (used_flr_sqmeters / local_used_flr) * 100
     where lv10loc.lv7loc_id = p_lv7loc_id ;
  end if ;

  if nvl(local_used_linear,0) <> 0 then
    update maxdata.lv10loc
       set pct_used_linear = (used_linear_meters / local_used_linear) * 100
     where lv10loc.lv7loc_id = p_lv7loc_id ;
  end if ;

 update maxdata.lv10loc a
  set est_prod_pos =
    ( select count(*)
      from   maxdata.lv10loc b
      where  b.lv7loc_id = p_lv7loc_id and
             a.lv10mast_id = b.lv10mast_id
      group  by a.lv10mast_id )
  where a.lv7loc_id = p_lv7loc_id ;

end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_RECALC_LV7LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_RECALC_LV7LOC" TO "MAXUSER";
