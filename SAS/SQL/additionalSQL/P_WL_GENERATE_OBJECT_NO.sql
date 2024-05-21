--------------------------------------------------------
--  DDL for Procedure P_WL_GENERATE_OBJECT_NO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_GENERATE_OBJECT_NO" (
	in_session_id			NUMBER,
	in_cube_id			NUMBER,
	in_src_object_prefix_cd		VARCHAR2,	-- Table prefix of the source object.
	in_src_template_id 		NUMBER, 	-- of the source object.
	in_src_object_no 		NUMBER, 	-- of the source object.
	in_tar_object_prefix_cd		VARCHAR2,	-- Table prefix of the target object.
	in_tar_template_id 		NUMBER, 	-- of the target object.
	in_debug_flg			NUMBER,		-- Internal. Only for debugging.
	in_future1			NUMBER,		-- placeholder. Pass in -1.
	in_future2			NUMBER,		-- placeholder. Pass in -1.
	in_future3			NUMBER		-- placeholder. Pass in -1.
) AS
/*
$Log: 2332_p_wl_generate_object_no.sql,v $
Revision 1.17  2007/06/19 14:39:06  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.13  2006/09/12 14:18:05  anchan
Added WLLA alias for UDB

Revision 1.12  2006/09/11 13:41:47  anchan
S0376596: filter out duplicate WLLA dim levels

Revision 1.11  2006/08/30 16:15:18  anchan
Pass the cube_id

Revision 1.10  2006/08/29 20:05:49  anchan
S0362566: Added session_id column to allow for rewrite of performance-enhanced UDB version and straightforward porting

Revision 1.9  2006/08/14 20:21:10  anchan
S0344510: Added ORDER BY to each INSERT stmt.

Revision 1.8  2006/06/08 15:00:55  anchan
Added order by clause in the UPDATE statement to guarantee correct sequence of new objects

Revision 1.7  2006/02/23 18:28:02  saghai
removed WLKV

Revision 1.6  2006/02/16 22:26:03  saghai
PERFORMANCE-ENHANCED PACKAGE



Change History

V6.1
6.1.0-001 06/01/05 Sachin	Initial Entry

This procedure inserts records into the maxdata.sess_new_object_no temporary table
and assigns new object nos.
This procedure is called from p_wl_copy_subtree procedure.

*/



n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_wl_generate_object_no';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_int_minus_one			NUMBER(10,0)		:= -1;
t_int_null			NUMBER(10,0)		:= NULL;

t_object_type			VARCHAR2(20)		:= 'OBJECT_TYPE';
t_object_type_id		NUMBER(2);

t_cube_id			NUMBER(10);
t_cube_where			VARCHAR2(100);
t_working_prefix_cd		CHAR(1);
t_obj_cnt			NUMBER;
t_start_obj_no			NUMBER;
t_object_no			NUMBER(10);

t_comma_loc			NUMBER(2);
t_comma				CHAR(1) := ',';
t_table_order			VARCHAR2(75);
t_table_prefix_cd		VARCHAR2(5);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure
IF in_debug_flg > 0 THEN
	t_call := t_proc_name || ' ( ' ||
		COALESCE(in_session_id,-1) || ',''' ||
		COALESCE(in_cube_id,-1) || ',''' ||
		in_src_object_prefix_cd|| ''',' ||
		COALESCE(in_src_template_id, -1) || ',' ||
		COALESCE(in_src_object_no, -1) || ',' ||
		COALESCE(in_tar_template_id, -1) || ',' ||
		COALESCE(in_debug_flg, -1) || ',' ||
		COALESCE(in_future1, -1) || ',' ||
		COALESCE(in_future2, -1) || ',' ||
		COALESCE(in_future3, -1) ||
		' ) ';
	maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
	--COMMIT;
END IF;

-- Check fifth character in table prefix code and the cube where clause
n_sqlnum := 2000;
IF LENGTH(in_src_object_prefix_cd) = 5 THEN
	t_working_prefix_cd := 'W';
	t_cube_where := ' cube_id = '||CAST(in_cube_id AS VARCHAR2);
ELSE
	t_working_prefix_cd := NULL;
	t_cube_where := '-1 = -1';
END IF;


CASE UPPER(SUBSTR(in_src_object_prefix_cd,1,4))
-- IF Worksheet Task Object is being copied
WHEN 'WLW1' THEN

	t_table_order := 'WLDF,WLD1,WLKS,WLPL,WLPN,WLTP,WLLA';

	-- Copy all the display format	no
	n_sqlnum := 10000;
	v_sql:= ' INSERT INTO maxdata.sess_new_object_no '||
		' (session_id, table_prefix_cd, src_object_no,tar_object_no) '||
		' SELECT '||in_session_id||',''WLDF'', display_format_no, ROWNUM '||
		' FROM  maxdata.WLDF'||t_working_prefix_cd||'_display_format'||
		' WHERE '||t_cube_where||
		' AND worksheet_template_id = '||in_src_template_id||
		' AND worksheet_task_no = '||in_src_object_no||
		' ORDER BY display_format_no';

	EXECUTE IMMEDIATE v_sql;


	-- Copy all Dimension layout no
	n_sqlnum := 12000;
	v_sql:= ' INSERT INTO maxdata.sess_new_object_no '||
		' (session_id, table_prefix_cd, src_object_no,tar_object_no) '||
		' SELECT '||in_session_id||',''WLD1'', dimension_layout_no, ROWNUM'||
		' FROM  maxdata.WLD1'||t_working_prefix_cd||'_dimension_layout'||
		' WHERE '||t_cube_where||
		' AND worksheet_template_id = '||in_src_template_id ||
		' AND worksheet_task_no = '||in_src_object_no||
		' ORDER BY dimension_layout_no';

	EXECUTE IMMEDIATE v_sql;


	-- Copy all KPI Set no
	n_sqlnum := 16000;
	v_sql:= ' INSERT INTO maxdata.sess_new_object_no '||
		' (session_id, table_prefix_cd, src_object_no,tar_object_no) '||
		' SELECT '||in_session_id||',''WLKS'', kpi_set_no, ROWNUM '||
		' FROM  maxdata.WLKS'||t_working_prefix_cd||'_kpi_set'||
		' WHERE '||t_cube_where||
		' AND worksheet_template_id = '||in_src_template_id||
		' AND worksheet_task_no = '||in_src_object_no||
		' ORDER BY kpi_set_no';

	EXECUTE IMMEDIATE v_sql;


	--One and only one WLPL row must exist for every task:--
	-- Copy all Pane layout no
	n_sqlnum := 18000;
	v_sql:= ' INSERT INTO maxdata.sess_new_object_no '||
		' (session_id, table_prefix_cd, src_object_no,tar_object_no) '||
		' SELECT '||in_session_id||',''WLPL'', pane_layout_no, 1 '||
		' FROM  maxdata.WLPL'||t_working_prefix_cd||'_pane_layout'||
		' WHERE '||t_cube_where||
		' AND worksheet_template_id = '||in_src_template_id||
		' AND worksheet_task_no = '||in_src_object_no;

	EXECUTE IMMEDIATE v_sql;


	-- Copy all Pane Node no
	n_sqlnum := 19000;
	v_sql:= ' INSERT INTO maxdata.sess_new_object_no '||
		' (session_id, table_prefix_cd, src_object_no,tar_object_no) '||
		' SELECT '||in_session_id||',''WLPN'', pane_node_no, ROWNUM '||
		' FROM  maxdata.WLPN'||t_working_prefix_cd||'_pane_node'||
		' WHERE '||t_cube_where||
		' AND worksheet_template_id = '||in_src_template_id||
		' AND worksheet_task_no = '||in_src_object_no||
		' ORDER BY pane_node_no';

	EXECUTE IMMEDIATE v_sql;

	-- Copy all Pane no from Task Pane:--
	n_sqlnum := 20000;

	v_sql:= ' INSERT INTO maxdata.sess_new_object_no '||
		' (session_id, table_prefix_cd, src_object_no,tar_object_no) '||
		' SELECT '||in_session_id||',''WLTP'', pane_no, ROWNUM '||
		' FROM  maxdata.WLTP'||t_working_prefix_cd||'_task_pane'||
		' WHERE '||t_cube_where||
		' AND worksheet_template_id = '||in_src_template_id||
		' AND worksheet_task_no = '||in_src_object_no||
		' ORDER BY pane_no';

	EXECUTE IMMEDIATE v_sql;

	-- Copy all Dimension_level_no greater than 20000 from Level Assigment.
	-- Same dimension_level_no may be shared between panes within a task, so DISTINCT is needed:
	n_sqlnum := 22000;
	v_sql:= ' INSERT INTO maxdata.sess_new_object_no '||
		' (session_id, table_prefix_cd, src_object_no,tar_object_no) '||
		' SELECT '||in_session_id||',''WLLA'', dimension_level_no, 20000+ROWNUM'||
		' FROM (SELECT DISTINCT dimension_level_no'||
		' 	FROM  maxdata.WLLA'||t_working_prefix_cd||'_level_assignment'||
		' 	WHERE '||t_cube_where||
		' 	AND worksheet_template_id = '||in_src_template_id||
		' 	AND worksheet_task_no = '||in_src_object_no||
		' 	AND dimension_level_no > 20000)   WLLA';


	EXECUTE IMMEDIATE v_sql;


-- IF KPI Set Object is being copied
WHEN 'WLKS' THEN

	t_table_order := 'WLDF';

	-- Copy all display format no
	n_sqlnum := 25000;
	v_sql:= ' INSERT INTO maxdata.sess_new_object_no '||
		' (session_id, table_prefix_cd, src_object_no,tar_object_no) '||
		' SELECT '||in_session_id||',''WLDF'', display_format_no, ROWNUM'||
		' FROM  maxdata.WLDF'||t_working_prefix_cd||'_display_format'||
		' WHERE '||t_cube_where||
		' AND worksheet_template_id = '||in_src_template_id||
		' AND kpi_set_no = '||in_src_object_no||
		' ORDER BY display_format_no';

	EXECUTE IMMEDIATE v_sql;

END CASE;

LOOP
	-- Find the comma position.
	t_comma_loc := INSTR(t_table_order, t_comma, 1, 1);

	IF t_comma_loc = 0 THEN
		t_table_prefix_cd := t_table_order;
		t_table_order := 'EXIT';
	ELSE
		t_table_prefix_cd := SUBSTR(t_table_order,1,t_comma_loc - 1);
		t_table_order := SUBSTR(t_table_order,t_comma_loc+1);
	END IF;


	-- Check the number of records which need new object numbers.
	n_sqlnum := 35000;
	SELECT COUNT(*) INTO t_obj_cnt
	FROM maxdata.sess_new_object_no
	WHERE session_id= in_session_id
	AND table_prefix_cd = t_table_prefix_cd	AND (tar_object_no > 0) ;

	IF t_obj_cnt > 0 THEN
		-- Set the correct cube_id depending on the target prefix code
		IF LENGTH(in_tar_object_prefix_cd) = 5 THEN
			t_cube_id := in_cube_id;
		ELSE
			t_cube_id := -1;
		END IF;

		n_sqlnum := 36000;

		-- Reserve a block of new object numbers:--
		-- Update with proper starting offset for template-wide objects.
		-- Task-wide objects cannot be updated yet since tasks not created yet.
		-- But must be updated later after tasks are created by the calling procedure, p_wl_copy_subtree:--
		n_sqlnum := 38000;
		IF t_table_prefix_cd IN('WLD1','WLPL','WLDF','WLKS') THEN
			t_object_type_id := maxdata.f_lookup_number(t_object_type,t_table_prefix_cd);
			maxdata.p_wl_next_object_no ( t_cube_id,in_tar_template_id,-1,t_object_type_id,t_obj_cnt,-1,-1,-1,t_start_obj_no);
			UPDATE maxdata.sess_new_object_no
			SET tar_object_no = t_start_obj_no + tar_object_no -1
			WHERE session_id= in_session_id
			AND table_prefix_cd = t_table_prefix_cd
			AND (src_object_no> 0 AND tar_object_no>0);
		END IF;

	END IF;

	IF t_table_order = 'EXIT' THEN
		EXIT ;
	END IF;

END LOOP;


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
