--------------------------------------------------------
--  DDL for Procedure F_GET_PERIOD_CYCLE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."F_GET_PERIOD_CYCLE" 
  ( p_date IN DATE,  p_period IN OUT INTEGER,  p_cycle IN OUT INTEGER)AS
-----------------------------------
--change history
-- V5.4
-- 5.4.0-031 12/10/02    Sachin          Ported from 5.3.4 - 143_03. No changes made.
--5.3.4
-- 11/26/02 Sachin Added drop public synonym.
-- 11/25/02 DR Changed table period_lkup to lv5time.
----------------------------------
begin
  if p_date is NULL then
     p_period := NULL ;
     p_cycle  := NULL ;
  elsif TO_CHAR(p_date,'DD-MON-YY') = '01-JAN-80' then
     p_period := 1 ;
     p_cycle  := 1980 ;
  else
    select lv5time_id, cycle_id
    into   p_period, p_cycle
    from   maxapp.lv5time
    where  p_date between lv5time_start_date and lv5time_end_date and
           lv5time_type = (select period_type from maxapp.mmax_config) ;
  end if ;
end ;

/

  GRANT EXECUTE ON "MAXDATA"."F_GET_PERIOD_CYCLE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."F_GET_PERIOD_CYCLE" TO "MAXUSER";
  GRANT EXECUTE ON "MAXDATA"."F_GET_PERIOD_CYCLE" TO "MAXAPP";
