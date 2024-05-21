--------------------------------------------------------
--  DDL for Procedure P_TRUNCATE_TABLE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_TRUNCATE_TABLE" (
	in_table_name varchar2
) AS
/*----------------------------------------------------------------
$Log: 2167_p_truncate_table.sql,v $
Revision 1.5  2007/06/19 14:39:34  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.1  2005/10/04 21:53:43  joscho
FIXID : Replace p_clear/create_tmp_tbl


Usage:

This procedure is used by the app to truncate a table.
-----------------------------------------------------------------*/

v_sql varchar2(1000);

BEGIN

v_sql := 'truncate table ' || in_table_name;

EXECUTE IMMEDIATE v_sql;

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_TRUNCATE_TABLE" TO "MADMAX";
