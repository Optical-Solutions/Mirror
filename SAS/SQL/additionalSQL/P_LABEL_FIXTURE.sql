--------------------------------------------------------
--  DDL for Procedure P_LABEL_FIXTURE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_LABEL_FIXTURE" (p_lv7loc_id number)
as
  cursor c_lv8loc is
    select lv8loc_id
    from   maxdata.lv8loc
    where  lv7loc_id = p_lv7loc_id ;

  cursor c_lv10loc(p_lv8loc_id in number) is
    select distinct lv10mast_id
    from   maxdata.lv10loc
    where  lv8loc_id = p_lv8loc_id ;

  t_name    maxdata.lv10mast.name%TYPE ;
begin
  for c1 in c_lv8loc loop
    for c2 in c_lv10loc(c1.lv8loc_id) loop
      select name
      into   t_name
      from   maxdata.lv10mast
      where  lv10mast_id = c2.lv10mast_id ;
      update maxdata.lv8loc
         set bigchar_user14 = bigchar_user14 || rtrim(t_name) || ','
       where lv8loc_id = c1.lv8loc_id ;
    end loop ;
  end loop ;
end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_LABEL_FIXTURE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_LABEL_FIXTURE" TO "MAXUSER";
