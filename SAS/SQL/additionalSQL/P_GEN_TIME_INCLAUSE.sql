--------------------------------------------------------
--  DDL for Procedure P_GEN_TIME_INCLAUSE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GEN_TIME_INCLAUSE" (
	in_cube_id 	NUMBER,
	in_kpi_dv_id	NUMBER,		-- Pass in -1 if not specified.
	in_future1	NUMBER,		-- Placeholder. Pass in -1.
	in_future2	NUMBER,		-- Placeholder. Pass in -1.
	in_future3	NUMBER,		-- Placeholder. Pass in -1.
	out_time_inclause OUT VARCHAR2
) AS
/*------------------------------------------------------------------------------
$Log: 2139_p_gen_time_inclause.sql,v $
Revision 1.5  2007/06/19 14:39:45  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.1  2006/03/03 17:07:36  vejang
From 2304_p_gen_time_inclause.sql
TO    2139_p_gen_time_inclause.sql
Added for rename from 2304_p_gen_time_inclause.sql.
See originally named file for history prior to the rename.

Revision 1.3  2005/08/11 13:28:56  anchan
Replaced parameter values with variables to make code UDB-compatible

Revision 1.2  2005/08/05 18:12:41  anchan
Raises error if missing TIME_ID's

===========================
Change History
V6.1
6.1.0-001 07/11/05 Andy	Initial Entry
Description:
This procedure reads the T_CUBE_TIME table and returns the "time-inclause".
--------------------------------------------------------------------------------*/
n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 		:= 'p_gen_time_inclause';
t_error_level      	VARCHAR2(6) 		:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(1000) 		:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);
t_str_null		VARCHAR2(255)		:= NULL;
t_int_null		NUMBER(10)		:= NULL;

c_inclause_max CONSTANT INTEGER :=1000; --Oracle limit for inclause

t_level_no NUMBER(6);
t_inclause_collection VARCHAR2(8000);
t_inclause_single VARCHAR2(8000);

CURSOR c_cube_time IS
	SELECT t_lev, t_id
	FROM maxdata.t_cube_time
	WHERE cube_id=in_cube_id AND t_lev>0 -- skip any t_lev not greater than 0...
	AND(kpi_dv_id=in_kpi_dv_id OR in_kpi_dv_id=-1)
	ORDER BY t_lev;
BEGIN

-- Don't bother with logging the parameters of the procedure, as this can be called many times.

out_time_inclause := NULL;
t_level_no := 0;
t_inclause_collection:='';
FOR r_cube_time IN c_cube_time /* no open+fetch needed inside a for-loop... */
LOOP
	IF (c_cube_time%ROWCOUNT > c_inclause_max) THEN
	BEGIN
		 RAISE_APPLICATION_ERROR (-20001, 'Too many TIME_IDs specified. Maximum is: '
								 ||TO_CHAR(c_inclause_max));
	END;
	END IF;
	IF (t_level_no=r_cube_time.t_lev) THEN
	BEGIN
		 t_inclause_single := t_inclause_single||','||TO_CHAR(r_cube_time.t_id);
	END;
	ELSE
   	BEGIN
		 IF (t_level_no > 0) THEN
		 BEGIN
 		 	  t_inclause_collection := t_inclause_collection||t_inclause_single||')) OR ';
		 END;
		 END IF;
		 t_level_no:=r_cube_time.t_lev;
		 t_inclause_single := '(time_level='||TO_CHAR(r_cube_time.t_lev)
		 				  || ' AND time_id IN('||TO_CHAR(r_cube_time.t_id);
	END;
	END IF;
END LOOP;

IF (t_level_no > 0) THEN
BEGIN
	out_time_inclause := '( '||t_inclause_collection||t_inclause_single||')) )';
END;
ELSE
BEGIN
	RAISE_APPLICATION_ERROR (-20001, 'No TIME_IDs found in T_CUBE_TIME table for CUBE_ID='||TO_CHAR(in_cube_id));
END;
END IF;

EXCEPTION
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

  GRANT EXECUTE ON "MAXDATA"."P_GEN_TIME_INCLAUSE" TO "MADMAX";
