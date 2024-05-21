--------------------------------------------------------
--  DDL for Procedure MCX_PURGE_USER_PA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."MCX_PURGE_USER_PA" 
as
--- Date: 5/4/2012
--- Description: this process is used to delete the PA view owned by individual 
iSql long;
iCtr  int;

begin

select count(*) into iCtr from sys.all_tables where table_name = 'MCX_PA_PURGE';

if iCtr = 0 then
  iSql:='create table maxdata.mcx_pa_purge as select a.planworksheet_id seq_no, a.create_date execute_date, a.* from maxdata.planworksheet a where 1=2';
  execute immediate iSql;
end if;
if iCtr>0 then
  iSql:='select nvl(max(seq_no),0) from maxdata.mcx_pa_purge';
  execute immediate iSql into iCtr;
end if;
iCtr:=iCtr+1;
iSql:='insert into maxdata.mcx_pa_purge'||
  ' select '||iCtr||', sysdate, planworksheet.* from maxdata.planworksheet where planversion_id=-3 and planworksheet_id not in ('||
  '  select distinct primary_col1_no from maxdata.wlfo_favorite_object)'||
  ' and max_group_ID is null and max_user_id is not null';
  execute immediate iSql;

delete from maxdata.planworksheet where planversion_id=-3 and planworksheet_id not in (
 Select Distinct Primary_Col1_No From Maxdata.Wlfo_Favorite_Object)
-- where object_type_cd = 26) 
 and max_group_ID is null and max_user_id is not null; 

 commit;
 
end;

/
