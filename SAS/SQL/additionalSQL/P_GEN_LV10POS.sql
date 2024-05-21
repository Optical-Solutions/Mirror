--------------------------------------------------------
--  DDL for Procedure P_GEN_LV10POS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GEN_LV10POS" 
AS
begin
  insert into maxdata.lv10positions
    select pogmaster.pog_lv4loc_id, lv10loc.lv10mast_id, count(*)
    from   maxdata.pogmaster,
           maxdata.lv10loc
    where  pogmaster.pog_model_id = lv10loc.lv7loc_id and
           pogmaster.current_pog = 1 and
           nvl(lv10loc.no_mvmt_flag,0) <> 1
    group  by pogmaster.pog_lv4loc_id, lv10loc.lv10mast_id ;
  commit ;
end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_GEN_LV10POS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_GEN_LV10POS" TO "MAXUSER";
