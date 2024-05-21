--------------------------------------------------------
--  DDL for Procedure P_WL_NEXT_OBJECT_NO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_NEXT_OBJECT_NO" (
	in_cube_id 	NUMBER,		-- Pass in -1 for PERMANENT objects
	in_template_id 	NUMBER,		-- first column of (composite) primary key.
	in_keycol2_no 	NUMBER,  	-- second column, if composite primary key.
	in_object_type_cd NUMBER,	-- Table prefix type of the source object.
	in_increment 	NUMBER,		-- number of "slots" to reserve
	in_future1	NUMBER,		-- placeholder. Pass in -1.
	in_future2	NUMBER,		-- placeholder. Pass in -1.
	in_future3	NUMBER,	-- placeholder. Pass in -1.
	out_new_no  	OUT NUMBER
) AS
/*---------------------------------------------------------------------------------------
$Log: 2330_p_wl_next_object_no.sql,v $
Revision 1.19  2007/06/19 14:39:07  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.14  2006/04/24 17:18:43  anchan
Do not log this procedure call

Revision 1.13  2006/02/16 18:08:08  anchan
PERFORMANCE-ENHANCED PACKAGE

Revision 1.6  2006/02/16 14:29:58  anchan

========================================================================
6.1.0-006

Description:
There are 2 types of "next numbers": a globally-unique number stored in the SEQUENCE table and
a locally-unique numbers stored in the WORKING or PERMANENT tables.  A "next number" for the
specified table is maintained in only one of these places.
The "next number" is used by the application to construct the primary/unique key columns of
a newly inserted row.  This procedure retrieves a "next number" for the specified table
(as identified by the table prefix code).  The local "next numbers" are stored in the PARENT
table of the specified table (either WORKING or PERMANENT).

Note that the global numbers for both PERMANENT and WORKING tables are maintained in the one
and the same place in the SEQUENCE table--this has to be the case, so that there is no duplicates
when WORKING rows are "posted" back to the PERMANENT rows.
---------------------------------------------------------------------------------------*/

PRAGMA AUTONOMOUS_TRANSACTION;

n_sqlnum		NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 			:= 'p_wl_next_object_no';
t_error_level      	VARCHAR2(6) 			:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(1000) 			:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);
t_error_msg		VARCHAR2(1000);
t_str_null		VARCHAR2(255)			:= NULL;
t_int_null		NUMBER(10)			:= NULL;

t_entity_no		NUMBER(10)			:=100;
t_table_cd		CHAR(4);

t_local_dim_start_no CONSTANT NUMBER(10) :=100000; --10000~99999 is reserved for GLOBAL only.

BEGIN
n_sqlnum := 1000;
/*
--Procedure called 1000's of times a day. Uncomment this block for debugging purposes only--
t_call := t_proc_name || ' ( ' ||
	COALESCE(in_cube_id, -1) || ',' ||		-- COALESCE(int, 'NULL') returns error because of diff datatype.
	COALESCE(in_template_id, -1) || ',' ||		-- COALESCE(int, 'NULL') returns error because of diff datatype.
	COALESCE(in_keycol2_no, -1)  || ',' ||
	COALESCE(in_object_type_cd, -1)  || ',' ||
	COALESCE(in_increment, -1) || ',' ||
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
	COALESCE(in_future3, -1) || ',' ||
	'OUT' ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
*/

t_table_cd:=maxdata.f_lookup_code('OBJECT_TYPE',in_object_type_cd);
IF (t_table_cd IN('WLD1','WLW1','WLTP','WLPN','WLPL','WLDF','WLKS'))
AND (COALESCE(in_template_id,-1) < 0) THEN
BEGIN
	t_error_msg := '"in_template_id" must be 0 or greater for:'||t_table_cd||'/W';
	RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
END IF;

IF (t_table_cd IN('WLTP','WLPN'))
AND (COALESCE(in_keycol2_no,-1) < 1) THEN
BEGIN
	t_error_msg := '"in_keycol2_no" value must be greater than 0 for: '||t_table_cd||'/W';
	RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
END IF;

IF (t_table_cd IN('WLLA'))
AND (COALESCE(in_keycol2_no,-1) < 0) THEN
BEGIN
/* If 0, then the row belongs to a TASK, not a PANE */
	t_error_msg := '"in_keycol2_no" value must be 0 or greater for: '||t_table_cd||'/W';
	RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
END IF;

IF (COALESCE(in_increment,-1) <1) THEN
BEGIN
	t_error_msg := '"in_increment" value must be greater than 0.';
	RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
END IF;

BEGIN
CASE
	WHEN in_object_type_cd in(1,12,15,18,30) THEN
		maxapp.p_get_next_key(in_object_type_cd,t_entity_no,in_increment,out_new_no,t_error_msg);

	WHEN in_object_type_cd =2 AND in_cube_id=-1 THEN --'WLW1''
 		-- Reserve the 'in_increment' number of IDs.
 		UPDATE  maxdata.wlwt_worksheet_template
 		SET   next_task_no = COALESCE(next_task_no,0) + in_increment
		WHERE worksheet_template_id = in_template_id;
		-- Return the next usable ID.
		SELECT next_task_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlwt_worksheet_template
		WHERE worksheet_template_id = in_template_id;

	WHEN in_object_type_cd =2 AND in_cube_id>0 THEN --'WLW1W'
 		-- Reserve the 'in_increment' number of IDs.
 		UPDATE  maxdata.wlwtw_worksheet_template
 		SET   next_task_no = COALESCE(next_task_no,0) + in_increment
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;
		-- Return the next usable ID.
		SELECT next_task_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlwtw_worksheet_template
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;

	WHEN in_object_type_cd =3 AND in_cube_id=-1 THEN --'WLPL''
 		-- Reserve the 'in_increment' number of IDs.
 		UPDATE  maxdata.wlwt_worksheet_template
 		SET   next_pane_layout_no = COALESCE(next_pane_layout_no,0) + in_increment
		WHERE worksheet_template_id = in_template_id;
		-- Return the next usable ID.
		SELECT next_pane_layout_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlwt_worksheet_template
		WHERE worksheet_template_id = in_template_id;

	WHEN in_object_type_cd =3 AND in_cube_id>0 THEN --'WLPLW'
 		-- Reserve the 'in_increment' number of IDs.
 		UPDATE  maxdata.wlwtw_worksheet_template
 		SET   next_pane_layout_no = COALESCE(next_pane_layout_no,0) + in_increment
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;
		-- Return the next usable ID.
		SELECT next_pane_layout_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlwtw_worksheet_template
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;

	WHEN in_object_type_cd =4 AND in_cube_id=-1 THEN --'WLKS''
 		-- Reserve the 'in_increment' number of IDs.
 		UPDATE  maxdata.wlwt_worksheet_template
 		SET   next_kpi_set_no = COALESCE(next_kpi_set_no,0) + in_increment
		WHERE worksheet_template_id = in_template_id;
		-- Return the next usable ID.
		SELECT next_kpi_set_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlwt_worksheet_template
		WHERE worksheet_template_id = in_template_id;

	WHEN in_object_type_cd =4 AND in_cube_id>0 THEN --'WLKSW'
		-- Reserve the 'in_increment' number of IDs.
 		UPDATE  maxdata.wlwtw_worksheet_template
 		SET   next_kpi_set_no = COALESCE(next_kpi_set_no,0) + in_increment
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;
		-- Return the next usable ID.
		SELECT next_kpi_set_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlwtw_worksheet_template
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;

	WHEN in_object_type_cd =5 AND in_cube_id=-1 THEN --'WLD1''
 		-- Reserve the 'in_increment' number of IDs.
 		UPDATE  maxdata.wlwt_worksheet_template
 		SET   next_dimension_layout_no = COALESCE(next_dimension_layout_no,0) + in_increment
		WHERE worksheet_template_id = in_template_id;
		-- Return the next usable ID.
		SELECT next_dimension_layout_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlwt_worksheet_template
		WHERE worksheet_template_id = in_template_id;

	WHEN in_object_type_cd =5 AND in_cube_id>0 THEN --'WLD1W'
		-- Reserve the 'in_increment' number of IDs.
 		UPDATE  maxdata.wlwtw_worksheet_template
 		SET   next_dimension_layout_no = COALESCE(next_dimension_layout_no,0) + in_increment
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;
		-- Return the next usable ID.
		SELECT next_dimension_layout_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlwtw_worksheet_template
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;

	WHEN in_object_type_cd =9 AND in_cube_id=-1 THEN --'WLDF''
 		-- Reserve the 'in_increment' number of IDs.
 		UPDATE  maxdata.wlwt_worksheet_template
 		SET   next_display_format_no = COALESCE(next_display_format_no,0) + in_increment
		WHERE worksheet_template_id = in_template_id;
		-- Return the next usable ID.
		SELECT next_display_format_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlwt_worksheet_template
		WHERE worksheet_template_id = in_template_id;

	WHEN in_object_type_cd =9 AND in_cube_id>0 THEN --'WLDFW'
		-- Reserve the 'in_increment' number of IDs.
 		UPDATE  maxdata.wlwtw_worksheet_template
 		SET   next_display_format_no = COALESCE(next_display_format_no,0) + in_increment
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;
		-- Return the next usable ID.
		SELECT next_display_format_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlwtw_worksheet_template
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;

	WHEN in_object_type_cd =13 AND in_cube_id=-1 THEN --'WLTP'
		-- Reserve the 'in_increment' number of IDs.
		UPDATE  maxdata.wlw1_worksheet_task
		SET   next_pane_no = COALESCE(next_pane_no,0) + in_increment
		WHERE worksheet_template_id = in_template_id
		AND worksheet_task_no= in_keycol2_no;
		-- Return the next usable ID.
		SELECT next_pane_no - in_increment + 1 INTO out_new_no
		FROM  maxdata.wlw1_worksheet_task
		WHERE worksheet_template_id = in_template_id
		AND worksheet_task_no= in_keycol2_no;

	WHEN in_object_type_cd =13 AND in_cube_id>0 THEN --'WLTPW'
		-- Reserve the 'in_increment' number of IDs.
		UPDATE  maxdata.wlw1w_worksheet_task
		SET   next_pane_no = COALESCE(next_pane_no,0) + in_increment
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id
		AND worksheet_task_no= in_keycol2_no;
		-- Return the next usable ID.
		SELECT next_pane_no - in_increment + 1 INTO out_new_no
		FROM  maxdata.wlw1w_worksheet_task
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id
		AND worksheet_task_no= in_keycol2_no;

	WHEN in_object_type_cd =14 AND in_cube_id=-1 THEN --'WLPN'
		-- Reserve the 'in_increment' number of IDs.
		UPDATE  maxdata.wlpl_pane_layout
		SET   next_pane_node_no = COALESCE(next_pane_node_no,0) + in_increment
		WHERE worksheet_template_id = in_template_id
		AND  pane_layout_no= in_keycol2_no;
		-- Return the next usable ID.
		SELECT next_pane_node_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlpl_pane_layout
		WHERE worksheet_template_id = in_template_id
		AND  pane_layout_no= in_keycol2_no;

	WHEN in_object_type_cd =14 AND in_cube_id>0 THEN --'WLPNW'
		-- Reserve the 'in_increment' number of IDs.
		UPDATE  maxdata.wlplw_pane_layout
		SET   next_pane_node_no = COALESCE(next_pane_node_no,0) + in_increment
		WHERE  cube_id = in_cube_id
		AND worksheet_template_id = in_template_id
		AND  pane_layout_no= in_keycol2_no;
		-- Return the next usable ID.
		SELECT next_pane_node_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlplw_pane_layout
		WHERE  cube_id = in_cube_id
		AND worksheet_template_id = in_template_id
		AND  pane_layout_no= in_keycol2_no;

	WHEN in_object_type_cd =17 AND in_cube_id=-1 THEN --'WLLA'
		-- Reserve the 'in_increment' number of IDs.
		UPDATE  maxdata.wlw1_worksheet_task
		SET   next_dimension_level_no = COALESCE(next_dimension_level_no,t_local_dim_start_no) + in_increment
		WHERE  worksheet_template_id = in_template_id
		AND worksheet_task_no= in_keycol2_no;
		-- Return the next usable ID.
		SELECT next_dimension_level_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlw1_worksheet_task
		WHERE  worksheet_template_id = in_template_id
		AND worksheet_task_no= in_keycol2_no;


	WHEN in_object_type_cd =17 AND in_cube_id>0 THEN --'WLLAW'
		-- Reserve the 'in_increment' number of IDs.
		UPDATE  maxdata.wlw1w_worksheet_task
		SET   next_dimension_level_no = COALESCE(next_dimension_level_no,t_local_dim_start_no) + in_increment
		WHERE  cube_id = in_cube_id
		AND worksheet_template_id = in_template_id
		AND worksheet_task_no= in_keycol2_no;
		-- Return the next usable ID.
		SELECT next_dimension_level_no - in_increment + 1 INTO out_new_no
		FROM   maxdata.wlw1w_worksheet_task
		WHERE  cube_id = in_cube_id
		AND worksheet_template_id = in_template_id
		AND worksheet_task_no= in_keycol2_no;

	WHEN in_object_type_cd =19 THEN --'WLDM'
		-- Reserve the 'in_increment' number of IDs.
		UPDATE  maxdata.wldl_dynamic_level
		SET   next_dynamic_member_id = COALESCE(next_dynamic_member_id,1) + in_increment
		WHERE  dynamic_level_no= in_keycol2_no;
		-- Return the next usable ID.
		SELECT next_dynamic_member_id - in_increment + 1 INTO out_new_no
		FROM   maxdata.wldl_dynamic_level
		WHERE  dynamic_level_no= in_keycol2_no;

END CASE;
EXCEPTION
  	WHEN CASE_NOT_FOUND THEN
  		t_error_msg := 'Invalid "in_object_type_cd": ' || in_object_type_cd;
  		RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
COMMIT;

IF (t_error_msg IS NOT NULL) THEN
BEGIN
	RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
END IF;

EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;
		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := SUBSTR(v_sql,1,255);
			maxdata.Ins_Import_Log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
		END IF;
		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';
		t_sql2 := SUBSTR(v_sql,1,255);
		t_sql3 := SUBSTR(v_sql,256,255);
		maxdata.Ins_Import_Log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_WL_NEXT_OBJECT_NO" TO "MADMAX";
