--------------------------------------------------------
--  DDL for Procedure P_WL_VALIDATE_OBJECT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_VALIDATE_OBJECT" (
	in_cube_id			NUMBER,  	-- required if source or target object is in WORKING tables;
						 	-- else pass -1 if both of them are in PERMANENT tables.
	in_object_type_cd 		NUMBER, 	-- 1=template; 2=task; 4=kpi_set.
	in_template_id 			NUMBER, 	-- of the source object.
	in_object_no 			NUMBER, 	-- of the source object.
	in_config_type_id 		NUMBER, 	-- Pass -1 if not applicable.
	in_future1			NUMBER,		-- placeholder. Pass in -1.
	in_future2			NUMBER,		-- placeholder. Pass in -1.
	in_future3			NUMBER,		-- placeholder. Pass in -1.
	out_complete_flg 	OUT 	NUMBER    	-- Returns 1=COMPLETE OR aborts if INCOMPLETE.
) AS
/*----------------------------------------------------------------------------
$Log: 2364_p_wl_validate_object.sql,v $
Revision 1.5  2007/06/19 14:38:52  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.1  2006/04/13 13:24:22  anchan
Script renumbered to make room for two new sub-procs.
Added for rename from 2362_p_wl_validate_object.sql.
See originally named file for history prior to the rename.

Revision 1.8  2006/04/13 13:22:49  anchan
Added checking of WORKING objects; pushed out the bulk of the code into two sub-procs.

==============================================================================
Description:

This procedure is to be used check if the specified <object> is COMPLETE.
Currently only three types of objects are supported:
	1=template; 2=task; 4=kpi_set.

Calls appropriate sub-procedure depending on whether a PERMANENT or WORKING object.

--------------------------------------------------------------------------------*/



n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 		:= 'p_wl_validate_object';
t_error_level      	VARCHAR2(6) 		:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(1000) 		:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);
t_str_null		VARCHAR2(255)		:= NULL;
t_int_null		NUMBER(10)		:= NULL;
t_error_msg		VARCHAR2(1000);

t_rowcount			NUMBER(10);
t_pa_worksheet_cd	 CONSTANT NUMBER(6) :=105;

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_cube_id, -1) || ',' ||		-- COALESCE(int, 'NULL') returns error because of diff datatype.
	COALESCE(in_object_type_cd, -1) || ',' ||
	COALESCE(in_template_id, -1) || ',' ||
	COALESCE(in_object_no, -1) || ',' ||
	COALESCE(in_config_type_id, -1) || ',' ||
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
	COALESCE(in_future3, -1) || ',' ||
	'OUT' || 'out_complete_flg' ||
	' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;

IF in_object_type_cd IN(2,4)
AND COALESCE(in_object_no,-1)=-1 THEN
	RAISE_APPLICATION_ERROR (-20001, '[AA_NO_ARGUMENT] Must specify "in_object_no".');
END IF;

CASE in_cube_id
WHEN -1 THEN
	n_sqlnum := 2000;
	maxdata.p_wl_validate_perm_subtree(
		in_object_type_cd, 	-- 1=template; 2=task; 4=kpi_set.
		in_template_id, 	-- of the source object.
		in_object_no, 	-- of the source object.
		in_config_type_id, 	-- Pass -1 if not applicable.
		out_complete_flg-- Returns 1=COMPLETE OR aborts if INCOMPLETE.
	);
ELSE
	n_sqlnum := 3000;
	maxdata.p_wl_validate_work_subtree(
		in_cube_id,
		in_object_type_cd, 	-- 1=template; 2=task; 4=kpi_set.
		in_template_id, 	-- of the source object.
		in_object_no, 	-- of the source object.
		in_config_type_id, 	-- Pass -1 if not applicable.
		out_complete_flg-- Returns 1=COMPLETE OR aborts if INCOMPLETE.
	);

END CASE;

--If it reached here, then everything is AOK...
out_complete_flg :=1;

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

  GRANT EXECUTE ON "MAXDATA"."P_WL_VALIDATE_OBJECT" TO "MADMAX";
