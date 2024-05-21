--------------------------------------------------------
--  DDL for Procedure P_REBUILD_TEMP_POS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_REBUILD_TEMP_POS" 
AS

BEGIN

  insert into maxdata.temp_pos
    select distinct store_loc_userid, lv10mast_id, 0
    from   maxdata.batmvmt_ko
    where  ko_type = 'NO DEF SEC' ;
  commit ;

  delete from maxdata.batmvmt_ko where  ko_type = 'NO DEF SEC';
  commit ;
END ;

/

  GRANT EXECUTE ON "MAXDATA"."P_REBUILD_TEMP_POS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_REBUILD_TEMP_POS" TO "MAXUSER";
