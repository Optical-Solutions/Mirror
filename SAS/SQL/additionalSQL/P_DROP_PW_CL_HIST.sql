--------------------------------------------------------
--  DDL for Procedure P_DROP_PW_CL_HIST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DROP_PW_CL_HIST" (
	in_pw_id	NUMBER,
	in_future1	NUMBER,
	in_future2	NUMBER

) AS
/*----------------------------------------------------------------------
$Log: 2148_p_drop_pw_cl_hist.sql,v $
Revision 1.7  2007/06/19 14:39:40  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2006/02/17 22:18:52  healja
Replace $id with $Log
 2148_p_drop_pw_cl_hist.sql,v 1.2 2005/07/27 14:08:36 joscho Exp $


Change History
V6.1
6.1.0-001  07/15/05 Diwakar	Re Written for 6.1

Usage: Both external and internal

Description:

This procedure drops cluster history tables for all data versions assoiciated
with the passed in worksheet id parameter.

Dependant on:

Calls p_drop_cl_hist_dv procedure to drop a particular data version table.

Parameters:

in_pw_id	: Planworksheet ID
in_kpi_dv_id	: Dataversion ID
in_future1	: placeholder. Pass in -1.
in_future2	: placeholder. Pass in -1.
--------------------------------------------------------------------------------*/


n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_drop_pw_cl_hist';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_future_param_int		NUMBER(10,0)		:= -1;

BEGIN

n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_pw_id, -1)  || ',' ||
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_future2, -1) ||
	' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
COMMIT;

IF in_future1 <> -1 AND in_future2 <> -1 THEN
	RAISE_APPLICATION_ERROR(-20001,'Parameter values should be -1 for Future Parametrs');
END IF;

-- Loop through cl_hist_status entries for the worksheet and delete the entries and T_CL tables by
-- calling a subprocedure.

n_sqlnum := 2000;

DECLARE CURSOR c_cl_status IS
SELECT kpi_dv_id
FROM maxdata.cl_hist_status
WHERE planworksheet_id = in_pw_id;

BEGIN
FOR c1 IN c_cl_status LOOP
	maxdata.p_drop_cl_hist_dv(in_pw_id,c1.kpi_dv_id,t_future_param_int,t_future_param_int,t_future_param_int);
END LOOP;
END;

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

  GRANT EXECUTE ON "MAXDATA"."P_DROP_PW_CL_HIST" TO "MADMAX";
