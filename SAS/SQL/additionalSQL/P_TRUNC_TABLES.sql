--------------------------------------------------------
--  DDL for Procedure P_TRUNC_TABLES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_TRUNC_TABLES" ( 
-- $Id: 005_01_maxdata_p_trunc_tables.sql,v 1.0 2005/02/11 18:01:01 cbarr Exp $
-- Copyright (c) 2005 SAS Institute Inc., Cary, NC, USA
-- Date      By      Comment
-- 08/30/04  TS      Created at rev 2.0.4.0.
-- ----------------------------------------------------------------------------


--- 1: for truncate table, 2: for truncate Partition
v_tab_part IN int,
v_tableName IN VARCHAR2,
v_partitionName IN VARCHAR2)
is

iSql	 VARCHAR2(2000);
msg	 VARCHAR2(2000);
iCount	 int;

BEGIN

select count(*) into iCount from datamgr.truncate_table_logs;
if iCount > 100 then
	iSql:='delete from datamgr.truncate_table_logs where rownum < '||(iCount-1);
	execute immediate iSql;
end if;


if v_tab_part = 2
then
	iSql := 'ALTER TABLE maxdata.'||v_tableName||' TRUNCATE PARTITION '||v_partitionName;
else
	iSql := 'TRUNCATE TABLE maxdata.'||v_tableName;
end if;

msg:=to_char(sysdate,'yyyy-mm-dd hh:mi:ss')||isql;
insert into datamgr.truncate_table_logs values(msg);

execute immediate iSql;

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_TRUNC_TABLES" TO "DATAMGR";
