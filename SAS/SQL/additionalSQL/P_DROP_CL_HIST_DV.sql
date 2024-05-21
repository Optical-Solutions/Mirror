--------------------------------------------------------
--  DDL for Procedure P_DROP_CL_HIST_DV
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DROP_CL_HIST_DV" (
	in_pw_id	NUMBER,
	in_kpi_dv_id	NUMBER,
	in_future1	NUMBER,
	in_future2	NUMBER,
	in_future3	NUMBER

) AS
/*----------------------------------------------------------------------
$Log: 2147_p_drop_cl_hist_dv.sql,v $
Revision 1.7  2007/06/19 14:39:41  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2005/11/23 13:46:49  joscho
Ignore error with DROP TABLE so that even when the table doesn't exist, the procedure may go ahead and drop the CL_HIST_STATUS entry.



Change History
V6.1
6.1.0-001  07/15/05 Diwakar	Re Written for 6.1

Usage : Both External and Internal

Description:

This procedure drops cluster history tables and deletes entries from
maxdata.cl_hist_status table for the given worksheet ID and Data version ID.

Parameters:

in_pw_id	: Planworksheet ID
in_kpi_dv_id	: Dataversion ID
in_future1	: placeholder. Pass in -1.
in_future2	: placeholder. Pass in -1.
in_future3	: placeholder. Pass in -1.
--------------------------------------------------------------------------------*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_drop_cl_hist_dv';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_table_nm			VARCHAR2(64)		:= NULL;

BEGIN

n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_pw_id, -1)  || ',' ||
	COALESCE (in_kpi_dv_id, -1)  || ',' ||
	COALESCE (in_future1, -1)  || ',' ||
	COALESCE (in_future2, -1)  || ',' ||
	COALESCE (in_future3, -1)  ||
	' ) ';

n_sqlnum := 2000;

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
COMMIT;

n_sqlnum := 3000;

SELECT table_nm INTO t_table_nm
FROM maxdata.cl_hist_status
WHERE planworksheet_id = in_pw_id
AND kpi_dv_id = in_kpi_dv_id;

-- Ignore any drop table error.

BEGIN
n_sqlnum := 4000;
v_sql := 'DROP TABLE ' || t_table_nm;
EXECUTE IMMEDIATE v_sql;

EXCEPTION
	WHEN OTHERS THEN
		NULL;
END;

n_sqlnum := 6000;

DELETE maxdata.cl_hist_status
WHERE planworksheet_id = in_pw_id
AND kpi_dv_id = in_kpi_dv_id;

COMMIT;

EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := substr(v_sql,1,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := substr(v_sql,1,255);
		t_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
		COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_DROP_CL_HIST_DV" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_DROP_CL_HIST_DV" TO "MAXUSER";
