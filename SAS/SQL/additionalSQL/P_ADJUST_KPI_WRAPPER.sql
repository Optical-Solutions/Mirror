--------------------------------------------------------
--  DDL for Procedure P_ADJUST_KPI_WRAPPER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_ADJUST_KPI_WRAPPER" (iProcess int)
-- Program: p_adjust_kpi_wrapper.sql
as

iCtr            int;
iFlag		int;
iWKID		number(10);

iTimeId	        number(10);
iTimeLevel      number(5);
iLev        	number(5);

iSql 		long;
iWKtab   	varchar2(30);


begin


iWKtab:='t_fes_wk';

iSql:='select count(1) from sys.all_tables where owner=''MAXDATA'' and table_name =upper('''||iWKtab||''')';
execute immediate iSql into iCtr;
			
if iCtr > 0 then

	iSql:='select count(*) from maxdata.'||iWKtab||' where status=''C''';
	execute immediate iSql into iCtr;
	
	if iCtr = 0 then
		iSql:='drop table maxdata.'||iWKtab;
		execute immediate iSql;
	end if;
	
end if;

if iCtr = 0 then
	iSql:='create table maxdata.'||iWKtab||' as select mod(rownum,4)+1 Process_id, planworksheet_id, ''C'' status from maxdata.planworksheet'||
		' where planversion_id > 0 and PLANWORK_STAT_ID>0 order by planversion_id';
	execute immediate iSql;
end if;

iSql:='select count(*) from maxdata.'||iWKtab||' where status=''C'' and Process_id='||iProcess;
execute immediate iSql into iCtr;


while iCtr > 0 loop
-- get a worksheet to process
	iSql:='select planworksheet_id from maxdata.'||iWKtab||' where status=''C'' and Process_id='||iProcess||' and rownum=1 for update';
	execute immediate iSql into iWKID;

	maxdata.p_adjust_kpi_records(iWKID);

	iSql:='update maxdata.'||iWKtab||' set status=''P'' where planworksheet_id='||iWKID||' and Process_id='||iProcess;
	execute immediate iSql;
	commit;
	
	dbms_lock.sleep(10);
	iSql:='select count(*) from maxdata.'||iWKtab||' where status=''C'' and Process_id='||iProcess;
	execute immediate iSql into iCtr;

end loop;

end;

/
