--------------------------------------------------------
--  DDL for Procedure P_WL_COPY_TEMPLATE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_COPY_TEMPLATE" (
	in_cube_id			NUMBER,  	-- required if source or target object is in WORKING tables;
						 	-- else pass -1 if both of them are in PERMANENT tables.
	in_src_permanent_flg	 	NUMBER,		-- 1=PERMANENT, 0=WORKING
	in_src_template_id 		NUMBER, 	-- of the source object.
	in_tar_permanent_flg	 	NUMBER,		-- 1=PERMANENT, 0=WORKING
	in_tar_active_flg 		NUMBER, 	-- 1=ACTIVE/WORKING, 0=MODEL
	in_tar_new_template_nm 		VARCHAR2, 	-- New unique name of the target template.
	in_max_user_id			NUMBER,		-- Max User id
	in_max_group_id			NUMBER,		-- Max Group id
	in_future1			NUMBER,		-- placeholder. Pass in -1.
	in_future2			NUMBER,		-- placeholder. Pass in -1.
	in_future3			NUMBER,		-- placeholder. Pass in -1.
	out_new_template_no 	OUT 	NUMBER    	-- the newly created template.
) AS

/*
$Log: 2340_p_wl_copy_template.sql,v $
Revision 1.8  2007/06/19 14:39:04  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.4  2006/03/20 19:49:14  saghai
changed f_wl_transfer_case to p_wl_transfer_case

Revision 1.3  2006/02/16 22:26:04  saghai
PERFORMANCE-ENHANCED PACKAGE



Change History

V6.1
6.1.0-001 07/07/05 Sachin	Initial Entry

Description:

This is a wrapper procedure used to copy a worksheet template.

*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_wl_copy_template';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_int_null			NUMBER(10,0)		:= NULL;
t_int_negative_one		NUMBER(10,0)		:= -1;

t_debug_flg			NUMBER(1)		:= -1;
t_char_null			CHAR(1)			:= NULL;
t_date_null			DATE			:= NULL;
t_object_type			VARCHAR2(20)		:= 'OBJECT_TYPE';
t_new_wk_task_no		NUMBER(10) 		:= 0;
t_src_object_prefix_cd		VARCHAR2(5);
t_tar_object_prefix_cd		VARCHAR2(5);
t_src_wk_perm			VARCHAR2(1);
t_tar_wk_perm			VARCHAR2(1);
t_tar_template_id		NUMBER(10);
t_new_template_id		NUMBER(10);
t_deleted_flg			NUMBER(1);
t_to_transfer_case		VARCHAR2(10) := NULL;
t_transfer_case			VARCHAR2(20) := NULL;
t_object_name			VARCHAR2(30);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call:= t_proc_name || ' ( ' ||
	COALESCE(in_cube_id,-1) || ',' ||
	COALESCE(in_src_permanent_flg,-1) 	|| ',' ||
	COALESCE(in_src_template_id, -1) || ',' ||
	COALESCE(in_tar_permanent_flg,-1) 	|| ',' ||
	COALESCE(in_tar_active_flg, -1) || ',''' ||
	in_tar_new_template_nm || ''',' ||
	COALESCE(in_max_user_id, -1) || ',' ||
	COALESCE(in_max_group_id, -1) || ',' ||
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
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
t_src_object_prefix_cd := maxdata.f_lookup_code(t_object_type,1) || t_src_wk_perm ;

n_sqlnum := 7000;
t_tar_object_prefix_cd := maxdata.f_lookup_code(t_object_type,1) || t_tar_wk_perm ;


n_sqlnum := 9000;
IF SUBSTR(t_src_object_prefix_cd,1,4) <> SUBSTR(t_tar_object_prefix_cd,1,4) THEN
	RAISE_APPLICATION_ERROR(-20001,'Source and Target Objects must be the same.');
END IF;

n_sqlnum := 10000;
IF COALESCE(in_src_template_id,-1) = 0 THEN
	RAISE_APPLICATION_ERROR(-20001,'Cannot copy System Worksheet Template.');
END IF;

n_sqlnum := 11000;
IF COALESCE(in_src_template_id,-1) = -1 THEN
	RAISE_APPLICATION_ERROR(-20001,'Parameter Source Template Id cannot be NULL or -1.');
END IF;


n_sqlnum := 13000;
IF SUBSTR(t_src_object_prefix_cd,1,4) <> 'WLWT' THEN
	RAISE_APPLICATION_ERROR(-20001,'Please use this procedure only for copying a Worksheet Template.');
END IF;

n_sqlnum := 14000;
IF in_tar_active_flg = 0 THEN
	t_tar_template_id := 0;
ELSE
	-- Setting dummy value. Handled by maxdata.p_wl_copy_subtree procedure.
	t_tar_template_id := -9999;
END IF;

-- Check whether worksheet template has been logically deleted.
-- i.e. marked to be deleted by the nightly job.

n_sqlnum := 15000;
IF t_src_object_prefix_cd = 'WLWT' THEN
	BEGIN
		n_sqlnum := 15100;
		SELECT deleted_flg
		INTO t_deleted_flg
		FROM maxdata.WLWT_worksheet_template
		WHERE worksheet_template_id=in_src_template_id;
	EXCEPTION
	WHEN NO_DATA_FOUND THEN
		v_sql :='Worksheet Template '||CAST(in_src_template_id AS VARCHAR2)||' does not exist in the permanent table.';
		RAISE_APPLICATION_ERROR(-20001,v_sql);
	END;
ELSIF t_src_object_prefix_cd = 'WLWTW' THEN
	BEGIN
		n_sqlnum := 15200;
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

END IF;

n_sqlnum := 15500;
IF t_deleted_flg = 1 THEN
	v_sql :='Worksheet Template '||CAST(in_src_template_id AS VARCHAR2)||' cannot be copied. It has been marked for delete.';
	RAISE_APPLICATION_ERROR(-20001,v_sql);
END IF;



n_sqlnum := 20000;

maxdata.p_wl_copy_subtree ( 	in_cube_id,
				t_src_object_prefix_cd,
				in_src_template_id,
				t_int_negative_one,		-- in_src_object_no,
				t_tar_object_prefix_cd,
				t_tar_template_id,
				in_tar_new_template_nm,
				t_date_null,			-- in_last_post_time,
				in_max_user_id,			-- in_max_user_id
				in_max_group_id,		-- in_max_group_id
				t_debug_flg,			-- in_debug_flg
				in_future1,
				in_future2,
				in_future3,
				t_new_template_id
			);


-- Get the transfer case based on input parameters
n_sqlnum := 20000;
t_object_name := 'worksheet_template';
maxdata.p_wl_transfer_case(	in_cube_id,
				t_object_name,
				t_src_object_prefix_cd,
				in_src_template_id,
				t_int_negative_one,	-- in_src_object_no,
				t_tar_object_prefix_cd,
				t_tar_template_id,
				t_date_null,		-- in_last_post_time,
				t_transfer_case
				);

n_sqlnum := 22000;
t_to_transfer_case := SUBSTR(t_transfer_case,INSTR(t_transfer_case, '_', 1, 2)+1);

-- When copying to Model Template, copy worksheet task's individually
IF SUBSTR(t_src_object_prefix_cd,1,4) = 'WLWT' AND t_to_transfer_case = 'PMMODEL' THEN

	n_sqlnum := 57100;
	UPDATE maxdata.WLWT_worksheet_template
	SET 	next_task_no = 0,
		next_dimension_layout_no = 0,
		next_kpi_set_no = 0,
		next_pane_layout_no = 0,
		next_display_format_no = 0
	WHERE worksheet_template_id = t_new_template_id;

	-- Call p_wl_copy_subtree for each worksheet_task_no

	IF LENGTH(t_src_object_prefix_cd) > 4 THEN
		n_sqlnum := 57400;
		DECLARE CURSOR wlw1w_cur IS
		SELECT worksheet_task_no
		FROM maxdata.WLW1W_worksheet_task
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_src_template_id;
		BEGIN
		FOR c_row IN wlw1w_cur
		LOOP
			maxdata.p_wl_copy_subtree ( 	in_cube_id,
							REPLACE(t_src_object_prefix_cd,'T','1'),
							in_src_template_id,
							c_row.worksheet_task_no,
							REPLACE(t_tar_object_prefix_cd,'T','1'),
							t_tar_template_id,
							t_char_null,			-- in_tar_new_template_nm,
							t_date_null,			-- in_last_post_time,
							in_max_user_id,			-- in_max_user_id
							in_max_group_id,		-- in_max_group_id
							t_debug_flg,			-- in_debug_flg
							t_new_template_id,		-- in_special_template_id
							in_future2,
							in_future3,
							t_new_wk_task_no
						);

		END LOOP ;
		END;

	ELSE
		n_sqlnum := 57600;
		DECLARE CURSOR wlw1_cur IS
		SELECT worksheet_task_no
		FROM maxdata.WLW1_worksheet_task
		WHERE worksheet_template_id = in_src_template_id;
		BEGIN
		FOR c_row IN wlw1_cur
		LOOP
			maxdata.p_wl_copy_subtree ( 	in_cube_id,
							REPLACE(t_src_object_prefix_cd,'T','1'),
							in_src_template_id,
							c_row.worksheet_task_no,
							REPLACE(t_tar_object_prefix_cd,'T','1'),
							t_tar_template_id,
							t_char_null,			-- in_tar_new_template_nm,
							t_date_null,			-- in_last_post_time,
							in_max_user_id,			-- in_max_user_id
							in_max_group_id,		-- in_max_group_id
							t_debug_flg,			-- in_debug_flg
							t_new_template_id,		-- in_special_template_id
							in_future2,
							in_future3,
							t_new_wk_task_no
						);

		END LOOP ;
		END;
	END IF;
END IF;

out_new_template_no :=  t_new_template_id;
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

		-- Delete the unfinished target worksheet template
		-- that we were unsuccessful in copying
		IF COALESCE(t_new_template_id,-1) <> -1 THEN
			maxdata.p_wl_delete_template (
				in_cube_id,
				t_new_template_id,
				t_int_negative_one,			--in_logical_delete_flg
				in_future2,
				in_future3
			);
		END IF;

		RAISE_APPLICATION_ERROR(-20001,v_sql);

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_WL_COPY_TEMPLATE" TO "MADMAX";
