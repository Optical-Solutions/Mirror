--------------------------------------------------------
--  DDL for Procedure P_WL_COPY_OBJECT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_COPY_OBJECT" (
	in_cube_id			NUMBER,  	-- required if source or target object is in WORKING tables;
						 	-- else pass -1 if both of them are in PERMANENT tables.
	in_src_object_type_cd	 	NUMBER,		-- Table prefix type of the source object.
	in_src_permanent_flg	 	NUMBER,		-- 1=PERMANENT, 0=WORKING
	in_src_template_id 		NUMBER, 	-- of the source object.
	in_src_object_no 		NUMBER, 	-- of the source object.
	in_tar_object_type_cd	 	NUMBER,		-- Table prefix type of the target object.
	in_tar_permanent_flg	 	NUMBER,		-- 1=PERMANENT, 0=WORKING
	in_tar_template_id 		NUMBER, 	-- 0=MODEL, template_id=ACTIVE/WORKING
	in_tar_new_object_nm 		VARCHAR2, 	-- New unique name of the target object.
	in_max_user_id			NUMBER,		-- Max User id
	in_max_group_id			NUMBER,		-- Max Group id
	in_future1			NUMBER,		-- placeholder. Pass in -1.
        in_tar_wk_task_no               NUMBER,      	-- Pass in the target worksheet_task_no for only WLKS(W)_kpi_set.
        						-- Pass 0 when copying to MODEL KPI_Set. Else pass in -1.
	in_future3			NUMBER,		-- placeholder. Pass in -1.
	out_new_object_no 	OUT 	NUMBER    	-- the newly created object.
--NOTE: in_src_object_type_cd and in_tar_object_type_cd must be the same value.
) AS

/*
$Id: 2342_p_wl_copy_object.sql,v 1.9 2007/06/19 14:39:03 clapper Exp $
----------------------------------------------------------------------

Change History

V6.1
6.1.0-001 06/15/05 Sachin	Initial Entry

Description:

This is a wrapper procedure used to copy all objects in the worksheet template model.

--------------------------------------------------------------------------------*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_wl_copy_object';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_int_null			NUMBER(10,0)		:= NULL;

t_debug_flg			NUMBER(1)		:= -1;
t_date_null			DATE			:= NULL;
t_object_type			VARCHAR2(20)		:= 'OBJECT_TYPE';
t_src_object_prefix_cd		VARCHAR2(5);
t_tar_object_prefix_cd		VARCHAR2(5);
t_src_wk_perm			VARCHAR2(1);
t_tar_wk_perm			VARCHAR2(1);
t_tar_active_flg		NUMBER(1);
t_deleted_flg			NUMBER(1);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call:= t_proc_name || ' ( ' ||
	COALESCE(in_cube_id,-1) || ',' ||
	COALESCE(in_src_object_type_cd,-1) 	|| ',' ||
	COALESCE(in_src_permanent_flg,-1) 	|| ',' ||
	COALESCE(in_src_template_id, -1) || ',' ||
	COALESCE(in_src_object_no, -1) || ',' ||
	COALESCE(in_tar_object_type_cd,-1) 	|| ',' ||
	COALESCE(in_tar_permanent_flg,-1) 	|| ',' ||
	COALESCE(in_tar_template_id, -1) || ',''' ||
	in_tar_new_object_nm || ''',' ||
	COALESCE(in_max_user_id, -1) || ',' ||
	COALESCE(in_max_group_id, -1) || ',' ||
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_tar_wk_task_no, -1) || ',' ||
	COALESCE(in_future3, -1) || ',' ||
	'OUT' ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;

n_sqlnum := 2000;
IF in_src_permanent_flg NOT IN (0,1) THEN
	RAISE_APPLICATION_ERROR(-20001,'Valid values for in_src_permanent_flg are 0 or 1.');
END IF;

n_sqlnum := 3000;
IF in_tar_permanent_flg NOT IN (0,1) THEN
	RAISE_APPLICATION_ERROR(-20001,'Valid values for in_tar_permanent_flg are 0 or 1.');
END IF;


n_sqlnum := 3500;
-- Check if target worksheet task_no has been supplied when copying to Non-Model Kpi Sets
IF in_src_object_type_cd = 4 AND  COALESCE(in_tar_wk_task_no,-1) = -1 AND in_tar_template_id <> 0 THEN
        RAISE_APPLICATION_ERROR(-20001,'Target Worksheet Task No needs to be supplied when copying Kpi Set.');
END IF;

n_sqlnum := 4000;

IF in_src_permanent_flg = 0 THEN
	t_src_wk_perm := 'W' ;
ELSE
	t_src_wk_perm := '';
END IF;


n_sqlnum := 5000;
IF in_tar_permanent_flg = 0 THEN
	t_tar_wk_perm := 'W';
ELSE
	t_tar_wk_perm := '';
END IF;



--Get the source and target prefix codes
n_sqlnum := 6000;
t_src_object_prefix_cd := maxdata.f_lookup_code(t_object_type,in_src_object_type_cd) || t_src_wk_perm ;

n_sqlnum := 7000;
t_tar_object_prefix_cd := maxdata.f_lookup_code(t_object_type,in_tar_object_type_cd) || t_tar_wk_perm ;


n_sqlnum := 10000;
IF SUBSTR(t_src_object_prefix_cd,1,4) <> SUBSTR(t_tar_object_prefix_cd,1,4) THEN
	RAISE_APPLICATION_ERROR(-20001,'Source and Target Objects must be the same.');
END IF;

n_sqlnum := 11000;
IF COALESCE(in_src_template_id,-1) = -1 THEN
	RAISE_APPLICATION_ERROR(-20001,'Parameter Source Template Id cannot be NULL or -1.');
END IF;

n_sqlnum := 12000;
IF in_src_object_no IS NULL THEN
	RAISE_APPLICATION_ERROR(-20001,'Parameter Source Object No cannot be NULL. Pass in -1.');
END IF;

n_sqlnum := 13000;
IF SUBSTR(t_src_object_prefix_cd,1,4) <> 'WLWT' AND COALESCE(in_tar_template_id,-1) = -1 THEN
	RAISE_APPLICATION_ERROR(-20001,'Parameter Target Template Id cannot be NULL or -1.');
END IF;


-- Check whether source worksheet template has been logically deleted.
-- i.e. marked to be deleted by the nightly job.

n_sqlnum := 15000;
IF in_src_permanent_flg = 0 THEN
	BEGIN
		n_sqlnum := 15100;
		SELECT deleted_flg
		INTO t_deleted_flg
		FROM maxdata.WLWTW_worksheet_template
		WHERE cube_id = in_cube_id
		AND worksheet_template_id=in_src_template_id;
	EXCEPTION
	WHEN NO_DATA_FOUND THEN
		v_sql :='Worksheet Template '||CAST(in_src_template_id AS VARCHAR2)||' does not exist in the working table for Cube ID '|| CAST(in_cube_id AS VARCHAR2)|| '.';
		RAISE_APPLICATION_ERROR(-20001,v_sql);
	END;

ELSE
	BEGIN
		n_sqlnum := 15200;
		SELECT deleted_flg
		INTO t_deleted_flg
		FROM maxdata.WLWT_worksheet_template
		WHERE worksheet_template_id=in_src_template_id;
	EXCEPTION
	WHEN NO_DATA_FOUND THEN
		v_sql :='Worksheet Template '||CAST(in_src_template_id AS VARCHAR2)||' does not exist in the permanent table.';
		RAISE_APPLICATION_ERROR(-20001,v_sql);
	END;

END IF;

n_sqlnum := 15500;
IF t_deleted_flg = 1 THEN
	v_sql :='Unable to Copy. Source Worksheet Template '||CAST(in_src_template_id AS VARCHAR2)||' has been marked for delete.';
	RAISE_APPLICATION_ERROR(-20001,v_sql);
END IF;


t_deleted_flg := NULL;

-- Check whether target worksheet template has been logically deleted.
-- i.e. marked to be deleted by the nightly job.

n_sqlnum := 16000;
IF in_tar_permanent_flg = 0 THEN
	BEGIN
		n_sqlnum := 16100;
		SELECT deleted_flg
		INTO t_deleted_flg
		FROM maxdata.WLWTW_worksheet_template
		WHERE cube_id = in_cube_id
		AND worksheet_template_id=in_tar_template_id;
	EXCEPTION
	WHEN NO_DATA_FOUND THEN
		v_sql :='Worksheet Template '||CAST(in_tar_template_id AS VARCHAR2)||' does not exist in the working table for Cube ID '|| CAST(in_cube_id AS VARCHAR2)|| '.';
		RAISE_APPLICATION_ERROR(-20001,v_sql);
	END;

ELSE
	BEGIN
		n_sqlnum := 16200;
		SELECT deleted_flg
		INTO t_deleted_flg
		FROM maxdata.WLWT_worksheet_template
		WHERE worksheet_template_id=in_tar_template_id;
	EXCEPTION
	WHEN NO_DATA_FOUND THEN
		v_sql :='Worksheet Template '||CAST(in_tar_template_id AS VARCHAR2)||' does not exist in the permanent table.';
		RAISE_APPLICATION_ERROR(-20001,v_sql);
	END;

END IF;

n_sqlnum := 16500;
IF t_deleted_flg = 1 THEN
	v_sql :='Unable to Copy. Target Worksheet Template '||CAST(in_tar_template_id AS VARCHAR2)||' has been marked for delete.';
	RAISE_APPLICATION_ERROR(-20001,v_sql);
END IF;



IF SUBSTR(t_src_object_prefix_cd,1,4) ='WLWT' THEN

	IF in_tar_template_id = 0 THEN
		t_tar_active_flg := 0;
	ELSE
		t_tar_active_flg := 1;
	END IF;
	n_sqlnum := 20000;
	maxdata.p_wl_copy_template (	in_cube_id,
					in_src_permanent_flg,
					in_src_template_id,
					in_tar_permanent_flg,
					t_tar_active_flg,		-- 1=ACTIVE/WORKING, 0=MODEL
					in_tar_new_object_nm,
					in_max_user_id,			-- in_max_user_id
					in_max_group_id,		-- in_max_group_id
					in_future1,
					in_tar_wk_task_no,		-- Only for WLKS else -1.
					in_future3,
					out_new_object_no
				);
ELSE


	n_sqlnum := 21000;

	maxdata.p_wl_copy_subtree ( 	in_cube_id,
					t_src_object_prefix_cd,
					in_src_template_id,
					in_src_object_no,
					t_tar_object_prefix_cd,
					in_tar_template_id,
					in_tar_new_object_nm,
					t_date_null,			-- in_last_post_time,
					in_max_user_id,			-- in_max_user_id
					in_max_group_id,		-- in_max_group_id
					t_debug_flg,			-- in_debug_flg
					in_future1,
					in_tar_wk_task_no,		-- Only for WLKS else -1.
					in_future3,
					out_new_object_no
				);
END IF;

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

  GRANT EXECUTE ON "MAXDATA"."P_WL_COPY_OBJECT" TO "MADMAX";
