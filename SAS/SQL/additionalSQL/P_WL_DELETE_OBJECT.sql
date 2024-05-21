--------------------------------------------------------
--  DDL for Procedure P_WL_DELETE_OBJECT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_DELETE_OBJECT" (
	in_cube_id			NUMBER,  	-- Pass -1 for PERMANENT object
	in_object_type_cd	 	NUMBER,		-- Table prefix type of the object.
	in_permanent_flg	 	NUMBER,		-- 1 for permanent , 0 for Working are the only valid values
	in_template_id 			NUMBER, 	-- of the object.
	in_primary_col2 		NUMBER, 	-- Pass -1 for NULL
	in_primary_col3 		NUMBER, 	-- Pass -1 for NULL
	in_primary_col4 		NUMBER, 	-- Pass -1 for NULL
	in_dimension_type_cd 		VARCHAR2, 	-- M,L,T or NULL
	in_future1			NUMBER,		-- Placeholder. Pass in -1.
	in_future2			NUMBER,		-- Placeholder. Pass in -1.
	in_future3			NUMBER		-- Placeholder. Pass in -1.
) AS

/*--------------------------------------------------------------------------------
$Log: 2326_p_wl_delete_object.sql,v $
Revision 1.8  2007/06/19 14:40:13  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.4.12.1  2007/06/05 15:32:48  vejang
Moved from 6121 to 612HF4

Revision 1.4.10.1  2007/04/23 15:49:11  anchan
S0418756: "Cross-out" the name of deleted model tasks

Revision 1.4  2006/02/17 17:39:26  anchan
Append W for WORKING objects

Revision 1.3  2006/02/16 18:08:07  anchan
PERFORMANCE-ENHANCED PACKAGE

=========================================================
6.1.0-006
6.1.0-001 07/01/05 Sachin	Initial Entry

Description:

This is a wrapper procedure used to delete working and permanent objects in the worksheet template model.

--------------------------------------------------------------------------------*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_wl_delete_object';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_int_null			NUMBER(10,0)		:= NULL;
t_int_one			NUMBER(1,0)		:= 1;

t_object_type			VARCHAR2(20)		:= 'OBJECT_TYPE';
t_object_prefix_cd		VARCHAR2(5);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call:= t_proc_name || ' ( ' ||
	COALESCE(in_cube_id,-1) || ',' ||
	COALESCE(in_object_type_cd,-1) 	|| ',' ||
	COALESCE(in_permanent_flg,-1) 	|| ',' ||
	COALESCE(in_template_id, -1) || ',' ||
	COALESCE(in_primary_col2, -1) || ',' ||
	COALESCE(in_primary_col3, -1) || ',' ||
	COALESCE(in_primary_col4, -1) || ',' ||
	in_dimension_type_cd || ',' ||
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
	COALESCE(in_future3, -1) ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;

n_sqlnum := 2000;
IF in_permanent_flg NOT IN (0,1) THEN
	RAISE_APPLICATION_ERROR(-20001,'Valid values for in_permanent_flg are 0 or 1.');
END IF;

n_sqlnum := 3000;
IF in_permanent_flg = 0 AND in_cube_id = -1 THEN
	RAISE_APPLICATION_ERROR(-20001,'Cube ID must be supplied to delete from Working Area.');
END IF;


n_sqlnum := 4000;
IF in_template_id IS NULL OR in_template_id = -1 THEN
	RAISE_APPLICATION_ERROR(-20001,'Parameter in_template_id cannot be NULL or -1.');
END IF;

n_sqlnum := 4500;
t_object_prefix_cd := maxdata.f_lookup_code(t_object_type,in_object_type_cd);
IF (in_permanent_flg=0) THEN
BEGIN
	t_object_prefix_cd:=t_object_prefix_cd||'W';
END;
END IF;

n_sqlnum := 4600;
--Not necessary to delete non-MODEL/non-PREDEFINED objects as they're deleted via triggers.
IF (t_object_prefix_cd IN('WLD1','WLDF','WLKV','WLKS','WLPL') )
AND (in_template_id>0) THEN
	RETURN;
END IF;

-------BEGIN special code to prevent deadlocks--

n_sqlnum := 5000;
--If a MODEL task, just mark it DELETED, so that the clean-up job can remove them later.
IF in_cube_id=-1 AND in_template_id=0 AND in_object_type_cd=2 THEN
BEGIN
	-- "Cross-out" the name, to allow new ones with the same name later:
	UPDATE maxdata.wlw1_worksheet_task
	SET usage_type_cd='D',
        	task_nm='~DELETED~'||'['||TO_CHAR(worksheet_task_no)||']'
	WHERE worksheet_template_id=0
	AND worksheet_task_no=in_primary_col2
	AND usage_type_cd='M';
	IF SQL%ROWCOUNT=1 THEN
		RETURN;
	END IF;
END;
END IF;

-------END special code to prevent deadlocks--


--Signal the beginning of a procedure operation:
BEGIN
	n_sqlnum := 9000;
	DELETE FROM maxdata.WLOOW_object_operation
	WHERE cube_id = in_cube_id
	AND worksheet_template_id = in_template_id
	AND object_type_cd=in_object_type_cd
	AND object_no=in_primary_col2;

	n_sqlnum := 9100;
	INSERT INTO maxdata.WLOOW_object_operation (cube_id,worksheet_template_id,object_type_cd,object_no)
	VALUES (in_cube_id, in_template_id,in_object_type_cd,in_primary_col2);

	COMMIT;
END;

n_sqlnum := 10000;
IF t_object_prefix_cd IN('WLWT','WLWTW') THEN
	--For both PERMANENT and WORKING rows:
	maxdata.p_wl_delete_template (
		in_cube_id,
		in_template_id,
		t_int_one,		-- in_logical_delete_flg
		in_future2,
		in_future3
		);
ELSE
	IF in_permanent_flg=1 THEN
		maxdata.p_wl_delete_permanent_rows (
			t_object_prefix_cd,
			in_template_id,
			in_primary_col2,
			in_primary_col3,
			in_primary_col4,
			in_dimension_type_cd,
			in_future1,
			in_future2,
			in_future3
			);
	ELSE
		maxdata.p_wl_delete_working_rows (
			in_cube_id,
			t_object_prefix_cd,
			in_template_id,
			in_primary_col2,
			in_primary_col3,
			in_primary_col4,
			in_dimension_type_cd,
			in_future1,
			in_future2,
			in_future3
			);
	END IF;
END IF;

--Signal the end of a procedure operation:
n_sqlnum := 20000;
DELETE FROM maxdata.WLOOW_object_operation
WHERE cube_id = in_cube_id
AND worksheet_template_id = in_template_id
AND object_type_cd=in_object_type_cd
AND object_no=in_primary_col2;

COMMIT;


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

  GRANT EXECUTE ON "MAXDATA"."P_WL_DELETE_OBJECT" TO "MADMAX";
