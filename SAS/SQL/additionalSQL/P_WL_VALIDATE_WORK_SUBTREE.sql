--------------------------------------------------------
--  DDL for Procedure P_WL_VALIDATE_WORK_SUBTREE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_VALIDATE_WORK_SUBTREE" (
	in_cube_id			NUMBER,
	in_object_type_cd 		NUMBER, 	-- 1=template; 2=task; 4=kpi_set.
	in_template_id 			NUMBER, 	-- of the source object.
	in_object_no 			NUMBER, 	-- of the source object.
	in_config_type_id 		NUMBER, 	-- Pass -1 if not applicable.
	out_complete_flg 	OUT 	NUMBER    	-- Returns 1=COMPLETE OR aborts if INCOMPLETE.
) AS
/*----------------------------------------------------------------------------
$Log: 2363_p_wl_validate_work_subtree.sql,v $
Revision 1.9  2007/06/19 14:38:53  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.5  2006/06/05 19:50:25  anchan
S0358881: Pushed out the code for checking PA view templates to another sub-procedure.

Revision 1.4  2006/05/09 18:56:54  anchan
Per Gary Y: WP KPIs belonging to a PA-- if the WP KPIs are submitted, they are still allowed in PA. A submitted WP KPIField has a FIELD_LEVEL_NO value "3".

Revision 1.3  2006/04/25 15:38:55  makirk
Fix spelling mistakes in error messages

Revision 1.2  2006/04/20 14:49:44  anchan
Fixed typo

Revision 1.1  2006/04/13 13:26:27  anchan
Pushed out the bulk of the validation code into two sub-procs from p_wl_validate_object

==============================================================================
Description:

This procedure is to be used check if the specified <object> is COMPLETE:

***A <template> is complete if:
The <template> has <task>s.
Each <task> has a <pane>.
Each <pane> has a complete <KPI Set>.

***A <task> is complete if:
A<task> has a <pane>.
A <pane> has a complete <KPI Set>.

***A <KPI Set> is complete if:
It has at least one <KPI Field>

--------------------------------------------------------------------------------*/



n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 		:= 'p_wl_validate_work_subtree';
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
	'OUT' || 'out_complete_flg' ||
	' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;

CASE in_object_type_cd
WHEN 1 THEN

	n_sqlnum := 2000;
	SELECT COUNT(*) INTO t_rowcount
	FROM maxdata.wlwtw_worksheet_template
	WHERE worksheet_template_id=in_template_id
	AND cube_id=in_cube_id;
	IF (t_rowcount = 0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_NO_TEMPLATE] Specified <worksheet-template> does not exist.');
	END IF;

	n_sqlnum := 2000;
	SELECT COUNT(*) INTO t_rowcount
	FROM maxdata.wlw1w_worksheet_task
	WHERE worksheet_template_id=in_template_id
	AND cube_id=in_cube_id;
	IF (t_rowcount = 0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_TEMPLATE_NO_TASK] A <worksheet-template> must have at least one <task>.');
	END IF;


	n_sqlnum := 2200;
	SELECT COUNT(*) INTO t_rowcount
	FROM maxdata.wlw1w_worksheet_task w1
	WHERE worksheet_template_id=in_template_id
	AND cube_id=in_cube_id
	AND NOT EXISTS
	      (SELECT * FROM maxdata.wltp_task_pane
	      WHERE worksheet_template_id=in_template_id);
	IF (t_rowcount>0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_TASK_NO_PANE] Each <task> must have at least one <pane>.');
	END IF;

	n_sqlnum := 2300;
	SELECT COUNT(*) INTO t_rowcount
	FROM maxdata.wlksw_kpi_set
	WHERE worksheet_template_id=in_template_id
	AND cube_id=in_cube_id
	AND NOT EXISTS
		(SELECT * FROM maxdata.wlkf_kpi_field
		WHERE worksheet_template_id=in_template_id);
	IF (t_rowcount>0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_KPISET_NO_KPIFIELD] Each <kpi set> must have at least one <kpi field>.');
	END IF;

	n_sqlnum := 2400;
	IF (in_config_type_id=t_pa_worksheet_cd) THEN
	BEGIN
		maxdata.p_wl_validate_work_kpi_pa(in_cube_id,in_object_type_cd,in_template_id,in_object_no,out_complete_flg);
	END;
	END IF;

WHEN 2 THEN

	n_sqlnum := 3000;
	SELECT COUNT(*) INTO t_rowcount
	FROM maxdata.wlw1w_worksheet_task
	WHERE worksheet_template_id=in_template_id
	AND worksheet_task_no=in_object_no
	AND cube_id=in_cube_id;
	IF (t_rowcount = 0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_NO_TASK] Specified <task> does not exst.');
	END IF;

	n_sqlnum := 3200;
	SELECT COUNT(pane_no) INTO t_rowcount
	FROM maxdata.wltpw_task_pane
	WHERE worksheet_template_id=in_template_id
	AND worksheet_task_no=in_object_no
	AND cube_id=in_cube_id;
	IF (t_rowcount=0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_TASK_NO_PANE] Each <task> must have at least one <pane>.');
	END IF;

	n_sqlnum := 3300;
	SELECT COUNT(*) INTO t_rowcount
	FROM maxdata.wlksw_kpi_set
	WHERE worksheet_template_id=in_template_id
	AND worksheet_task_no=in_object_no
	AND cube_id=in_cube_id
	AND NOT EXISTS
		(SELECT * FROM maxdata.wlkfw_kpi_field
		WHERE worksheet_template_id=in_template_id
		AND worksheet_task_no=in_object_no
		AND cube_id=in_cube_id);
	IF (t_rowcount>0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_KPISET_NO_KPIFIELD] Each <kpi set> must have at least one <kpi field>.');
	END IF;

	n_sqlnum := 3400;
	IF (in_config_type_id=t_pa_worksheet_cd) THEN
	BEGIN
		maxdata.p_wl_validate_work_kpi_pa(in_cube_id,in_object_type_cd,in_template_id,in_object_no,out_complete_flg);
	END;
	END IF;

WHEN 4 THEN

	n_sqlnum := 4000;
	SELECT COUNT(*) INTO t_rowcount
	FROM maxdata.wlksw_kpi_set
	WHERE worksheet_template_id=in_template_id
	AND kpi_set_no=in_object_no
	AND cube_id=in_cube_id;
	IF (t_rowcount=0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_NO_KPISET] Specified object does not exist: kpi_set_no='||in_object_no);
	END IF;

	n_sqlnum := 4100;
	SELECT COUNT(*) INTO t_rowcount
	FROM maxdata.wlkfw_kpi_field
	WHERE worksheet_template_id=in_template_id
	AND kpi_set_no=in_object_no
	AND cube_id=in_cube_id;
	IF (t_rowcount=0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_KPISET_NO_KPIFIELD] Each <kpi set> must have at least one <kpi field>.');
	END IF;

	n_sqlnum := 4200;
	IF (in_config_type_id=t_pa_worksheet_cd) THEN
	BEGIN
		maxdata.p_wl_validate_work_kpi_pa(in_cube_id,in_object_type_cd,in_template_id,in_object_no,out_complete_flg);
	END;
	END IF;

END CASE;

--If it reached here, then everything is AOK...
out_complete_flg :=1;

EXCEPTION
  	WHEN CASE_NOT_FOUND THEN
  		t_error_msg := 'Invalid "in_object_type_cd": ' || in_object_type_cd;
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
