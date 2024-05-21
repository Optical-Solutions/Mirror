--------------------------------------------------------
--  DDL for Procedure P_SPECIAL_PLANWORKSHT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_SPECIAL_PLANWORKSHT" (
	in_pw_id	NUMBER,
	in_future1	NUMBER,		-- placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_future2	NUMBER,		-- placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_future3	NUMBER,		-- placeholder. Pass in -1. Use it only for EXTERNAL procedure
	out_id		OUT NUMBER	-- placeholder. output param
) AS
/*----------------------------------------------------------------------------
$Log: 2368_p_special_planworksht.sql,v $
Revision 1.7  2007/06/19 14:38:51  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2006/03/28 19:14:06  anchan
Moved the code to PROMOTE a task to a migration procedure: p_mig_stitch_wksht_template.sql

Revision 1.2  2006/03/27 16:50:54  anchan
Moved error-handler code

Revision 1.1  2006/01/20 16:32:29  anchan
Renamed from: 2370_p_special_planworksht.sql
Added for rename from 2370_p_special_planworksht.sql.
See originally named file for history prior to the rename.

Revision 1.3  2006/01/20 16:25:55  anchan
Coded to handle "11" cases

Revision 1.1  2006/01/04 16:10:17  anchan
Created.


==============================================================================
DESCRIPTION:
   A skeleton procedure for handling of any special processing of a worksheet.
   Intended to be run at the time of opening a worksheet.
   Reads the SPECIAL_ACTION_CD column in the PLANWORKSHEET table to perform any special processing.
   Afterwards, it sets this column to 0.
--------------------------------------------------------------------------------*/



n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 		:= 'p_special_planworksht';
t_error_level      	VARCHAR2(6) 		:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(1000) 		:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);
t_str_null		VARCHAR2(255)		:= NULL;
t_int_null		NUMBER(10)		:= NULL;

t_special_action_cd	NUMBER(2);
t_businessview_id	NUMBER(10);
t_task_no			NUMBER(10);
t_new_template_id	NUMBER(10);
t_error_msg      	VARCHAR2(255);
t_cnt				NUMBER(10,0);
t_max_user_id		NUMBER(10);
t_max_group_id		NUMBER(10);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_pw_id,-1) || ',' ||		-- COALESCE(int, 'NULL') returns error because of diff datatype.
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
	COALESCE(in_future3, -1) || ',' ||
	'OUT' ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;
-- Dynamic SQL example with bind variable
-- Data Not Found is caught by the error handler.

BEGIN
SELECT special_action_cd,businessview_id,max_user_id,max_group_id
INTO t_special_action_cd,t_businessview_id,t_max_user_id,t_max_group_id
FROM maxdata.planworksheet
WHERE planworksheet_id=in_pw_id;
EXCEPTION
	WHEN NO_DATA_FOUND THEN
		t_error_msg := 'Specified worksheet does not exist: ' || in_pw_id;
  		RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;


CASE COALESCE(t_special_action_cd,0)
WHEN 0 THEN
	RETURN;
END CASE;

--Mark the worksheet as 'PROCESSED', so no further action is taken the next time:--
UPDATE maxdata.planworksheet
	SET special_action_cd=0
WHERE planworksheet_id=in_pw_id;

COMMIT;

EXCEPTION
  	WHEN CASE_NOT_FOUND THEN
  		t_error_msg := 'Invalid "special_action_cd": ' || t_special_action_cd;
  		RAISE_APPLICATION_ERROR (-20001,t_error_msg);

	WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := substr(v_sql,1,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := substr(v_sql,1,255);
		t_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
		--COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_SPECIAL_PLANWORKSHT" TO "MADMAX";
