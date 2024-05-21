--------------------------------------------------------
--  DDL for Procedure P_WL_POST_TEMPLATE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_POST_TEMPLATE" (
	in_cube_id			NUMBER,  	-- Required
	in_template_id 			NUMBER, 	-- Required (of the source object)
	in_future1			NUMBER,		-- placeholder. Pass in -1.
	in_future2			NUMBER,		-- placeholder. Pass in -1.
	in_future3			NUMBER		-- placeholder. Pass in -1.
) AS

/*
$Log: 2346_p_wl_post_template.sql,v $
Revision 1.12  2007/06/19 14:39:01  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.8  2006/09/11 13:37:30  anchan
S0366552: push out the changes to calling procs

Revision 1.7  2006/03/22 16:16:43  anchan
Moved DELETE statements from p_wl_copy_subtree to p_wl_post_template as part of changes to fix
S0350517: "delete before insert of re-added rows".

----------------------------------------------------------------------

Change History

V6.1
6.1.0-001 06/15/05 Sachin	Initial Entry

Description:

This is a wrapper procedure used to post a worksheet template.

--------------------------------------------------------------------------------*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_wl_post_template';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_int_null			NUMBER(10,0)		:= NULL;
t_int_negative_one		NUMBER(10,0)		:= -1;

t_debug_flg			NUMBER(1)		:= -1;
t_char_null			VARCHAR2(20)		:= NULL;
t_src_object_prefix_cd		VARCHAR2(5)		:= 'WLWTW';
t_tar_object_prefix_cd		VARCHAR2(5)		:= 'WLWT';

t_last_post_time 		DATE;
t_deleted_flg	 		NUMBER(1);
t_out_dummy			NUMBER(10,0);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_cube_id, -1) || ',' ||
	COALESCE(in_template_id, -1) || ',' ||
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
	COALESCE(in_future3, -1) ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;


n_sqlnum := 2000;
BEGIN
	SELECT deleted_flg
	INTO t_deleted_flg
	FROM maxdata.WLWT_worksheet_template
	WHERE worksheet_template_id=in_template_id;

EXCEPTION
WHEN NO_DATA_FOUND THEN
  	v_sql :='Worksheet Template '||CAST(in_template_id AS VARCHAR2)||' does not exist in the permanent table.';
	RAISE_APPLICATION_ERROR (-20001, v_sql);
END;


-- Check whether worksheet template has been logically deleted.
-- i.e. marked to be deleted by the nightly job.

n_sqlnum := 3000;
IF t_deleted_flg = 1 THEN
	v_sql :='Worksheet Template '||CAST(in_template_id AS VARCHAR2)||' cannot be posted. It has been marked for delete.';
	RAISE_APPLICATION_ERROR(-20001,v_sql);
END IF;



n_sqlnum := 4000;
BEGIN

SELECT  COALESCE(posted_dttm,update_dttm,create_dttm)
INTO t_last_post_time
FROM maxdata.WLWTW_worksheet_template
WHERE cube_id=in_cube_id
AND worksheet_template_id=in_template_id;

EXCEPTION
WHEN NO_DATA_FOUND THEN
  	v_sql :='WORKSHEET_TEMPLATE_ID '||CAST(in_template_id AS VARCHAR2)||' does not exist in the working table.';
	RAISE_APPLICATION_ERROR (-20001, v_sql);
END;



n_sqlnum := 5000;
IF in_cube_id IS NULL OR in_cube_id = -1 THEN
	RAISE_APPLICATION_ERROR(-20001,'Parameter in_cube_id cannot be NULL or -1.');
END IF;

--Signal beginning of an object-operation(POSTING=0):
--BEGIN--
n_sqlnum := 9000;
DELETE FROM maxdata.WLOOW_object_operation
WHERE cube_id = -1
AND worksheet_template_id = in_template_id;
n_sqlnum := 9100;
INSERT INTO maxdata.WLOOW_object_operation (cube_id,worksheet_template_id,object_type_cd,object_no)
VALUES (-1,in_template_id,0,-1);
COMMIT;
--END;--

--BEGIN of Pre-posting cleanup.  Old and newly-added rows must be deleted before copying rows:--
BEGIN
n_sqlnum := 10100;
DELETE FROM maxdata.WLLA_level_assignment
WHERE worksheet_template_id=in_template_id
AND (worksheet_task_no,pane_no,dimension_level_no,dimension_type_cd) NOT IN
	(SELECT worksheet_task_no,pane_no,dimension_level_no,dimension_type_cd
	FROM maxdata.WLLAW_level_assignment
	WHERE worksheet_template_id=in_template_id
	AND cube_id=in_cube_id
	);

n_sqlnum := 10110;
--Rows that were deleted and re-added:
DELETE FROM maxdata.WLLA_level_assignment
WHERE worksheet_template_id=in_template_id
AND (worksheet_task_no, pane_no, dimension_level_no,dimension_type_cd) IN
	(SELECT worksheet_task_no, pane_no, dimension_level_no,dimension_type_cd
	FROM maxdata.WLLAW_level_assignment
	where worksheet_template_id=in_template_id
	AND cube_id=in_cube_id
	AND create_dttm>t_last_post_time);

n_sqlnum := 10200;
DELETE FROM maxdata.WLTP_task_pane
WHERE worksheet_template_id=in_template_id
AND (worksheet_task_no,pane_no) NOT IN
	(SELECT worksheet_task_no,pane_no
	FROM maxdata.WLTPW_task_pane
	WHERE worksheet_template_id = in_template_id
	AND cube_id = in_cube_id
	);

n_sqlnum := 10300;
DELETE FROM maxdata.WLW1_worksheet_task
WHERE worksheet_template_id=in_template_id
AND (worksheet_task_no) NOT IN
	(SELECT worksheet_task_no
	FROM maxdata.WLW1W_worksheet_task
	WHERE worksheet_template_id = in_template_id
	AND cube_id = in_cube_id
	);

n_sqlnum := 10400;
DELETE FROM maxdata.WLKS_kpi_set
WHERE worksheet_template_id=in_template_id
AND (kpi_set_no) NOT IN
	(SELECT kpi_set_no
	FROM maxdata.WLKSW_kpi_set
	WHERE worksheet_template_id = in_template_id
	AND cube_id = in_cube_id
	);

n_sqlnum := 10410;
DELETE FROM maxdata.WLKF_kpi_field
WHERE worksheet_template_id=in_template_id
AND (kpi_set_no,kpi_field_no,kpi_dv_id) NOT IN
	(SELECT kpi_set_no,kpi_field_no,kpi_dv_id
	FROM maxdata.WLKFW_kpi_field
	WHERE worksheet_template_id = in_template_id
	AND cube_id = in_cube_id
	);

n_sqlnum := 10450;
--Rows that were deleted and re-added:
DELETE FROM maxdata.WLKF_kpi_field
WHERE worksheet_template_id=in_template_id
AND (kpi_set_no, kpi_field_no, kpi_dv_id) IN
	(SELECT kpi_set_no, kpi_field_no, kpi_dv_id
	FROM MAXDATA.WLKFw_kpi_field
	WHERE worksheet_template_id=in_template_id
	AND cube_id=in_cube_id
	AND create_dttm>t_last_post_time);

n_sqlnum := 10500;
DELETE FROM maxdata.WLTD_template_dataversion
WHERE worksheet_template_id=in_template_id
AND (kpi_dv_id) NOT IN
	(SELECT kpi_dv_id
	FROM maxdata.WLTDW_template_dataversion
	WHERE worksheet_template_id = in_template_id
	AND cube_id = in_cube_id
	);

n_sqlnum := 10600;
DELETE FROM maxdata.WLD1_dimension_layout
WHERE worksheet_template_id=in_template_id
AND (dimension_layout_no) NOT IN
	(SELECT dimension_layout_no
	FROM maxdata.WLD1W_dimension_layout
	WHERE worksheet_template_id = in_template_id
	AND cube_id = in_cube_id
	);

n_sqlnum := 10700;
DELETE FROM maxdata.WLDF_display_format
WHERE worksheet_template_id=in_template_id
AND (display_format_no) NOT IN
	(SELECT display_format_no
	FROM maxdata.WLDFW_display_format
	WHERE worksheet_template_id = in_template_id
	AND cube_id = in_cube_id
	);

n_sqlnum := 10800;
DELETE FROM maxdata.WLPL_pane_layout
WHERE worksheet_template_id=in_template_id
AND (pane_layout_no) NOT IN
	(SELECT pane_layout_no
	FROM maxdata.WLPLW_pane_layout
	WHERE worksheet_template_id = in_template_id
	AND cube_id = in_cube_id
	);

n_sqlnum := 10900;
DELETE FROM maxdata.WLPN_pane_node
WHERE worksheet_template_id=in_template_id
AND (pane_layout_no,pane_node_no) NOT IN
	(SELECT pane_layout_no,pane_node_no
	FROM maxdata.WLPNW_pane_node
	WHERE worksheet_template_id = in_template_id
	AND cube_id = in_cube_id
	);

n_sqlnum := 10950;
DELETE FROM maxdata.WLDRW_deleted_row
WHERE cube_id = in_cube_id
AND worksheet_template_id = in_template_id;

EXCEPTION
	WHEN NO_DATA_FOUND THEN
     		NULL;
--END of Pre-posting cleanup:--
END;

n_sqlnum := 11000;

maxdata.p_wl_copy_subtree ( 	in_cube_id,
				t_src_object_prefix_cd,		-- in_src_object_prefix_cd
				in_template_id,
				t_int_negative_one,		-- in_src_object_no
				t_tar_object_prefix_cd,		-- in_tar_object_prefix_cd
				in_template_id,			-- in_tar_template_id
				t_char_null,			-- in_tar_new_object_nm
				t_last_post_time,
				t_int_null,			-- in_max_user_id
				t_int_null,			-- in_max_group_id
				t_debug_flg,			-- in_debug_flg
				in_future1,
				in_future2,
				in_future3,
				t_out_dummy			-- out_new_object_no
			);

n_sqlnum := 11100;

t_last_post_time := SYSDATE;
UPDATE maxdata.WLWT_worksheet_template
SET posted_dttm = t_last_post_time
WHERE worksheet_template_id=in_template_id;

n_sqlnum := 12000;
UPDATE maxdata.WLWTW_worksheet_template
SET posted_dttm = t_last_post_time
WHERE cube_id=in_cube_id
AND worksheet_template_id=in_template_id;

n_sqlnum := 13000;
--Signal end of the object-operation:
DELETE FROM maxdata.WLOOW_object_operation
WHERE cube_id = -1
AND worksheet_template_id = in_template_id;

COMMIT;

--Make sure that new CREATE/UPDATE times are later than STAGE/POST time, so wait one second:
--Not necessary in UDB, since TIMESTAMP precision is microseconds?
n_sqlnum := 14000;
sys.dbms_lock.sleep(1);

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

		-- Cleanup status entry after failure:
		DELETE FROM maxdata.WLOOW_object_operation
		WHERE cube_id = -1
		AND worksheet_template_id = in_template_id;
		COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_WL_POST_TEMPLATE" TO "MADMAX";
