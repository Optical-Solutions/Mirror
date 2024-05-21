--------------------------------------------------------
--  DDL for Procedure P_WL_VALIDATE_WORK_KPI_PA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_VALIDATE_WORK_KPI_PA" (
	in_cube_id			NUMBER,
	in_object_type_cd 		NUMBER, 	-- 1=template; 2=task; 4=kpi_set.
	in_template_id 			NUMBER, 	-- of the source object.
	in_object_no 			NUMBER, 	-- of the source object.
	out_complete_flg 	OUT 	NUMBER    	-- Returns 1=COMPLETE OR aborts if INCOMPLETE.
) AS
/*----------------------------------------------------------------------------
$Log: 2361_p_wl_validate_work_kpi_pa.sql,v $
Revision 1.9  2007/06/19 14:38:55  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.5  2006/08/21 20:26:33  anchan
No change check-in

Revision 1.4  2006/08/18 16:55:03  anchan
S0372944: Fixed typo

Revision 1.3  2006/07/07 20:11:58  anchan
Rolled back prior to 6/30 changes; also do not exclude "submitted" WP KPIs.

Revision 1.1  2006/06/05 19:47:27  anchan
FIXID : S0362390: Check if template has variances for use with PA views.

Revision 1.3  2006/05/09 18:56:55  anchan
Per Gary Y: WP KPIs belonging to a PA-- if the WP KPIs are submitted,
 they are still allowed in PA. A submitted WP KPIField has a FIELD_LEVEL_NO value "3"--NOT.
--------------------------------------------------------------------------------*/



n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 		:= 'p_wl_validate_work_kpi_pa';
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
	COALESCE(in_cube_id, -1) || ',' ||
	COALESCE(in_object_type_cd, -1) || ',' ||
	COALESCE(in_template_id, -1) || ',' ||
	COALESCE(in_object_no, -1) || ',' ||
	'OUT' || 'out_complete_flg' ||
	' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;

CASE
WHEN in_object_type_cd IN(1,2,4) THEN

	SELECT COUNT(*) INTO t_rowcount
	FROM maxdata.wlkfw_kpi_field wlkf
	JOIN maxdata.wlkd_kpi_dataversion wlkd ON(wlkf.kpi_dv_id=wlkd.kpi_dv_id)
	WHERE worksheet_template_id=in_template_id
	AND (  (in_object_type_cd<>2)
	     OR(in_object_type_cd=2 AND worksheet_task_no=in_object_no) )
	AND (  (in_object_type_cd<>4)
	     OR(in_object_type_cd=4 AND kpi_set_no=in_object_no) )
	AND(wlkf.kpi_dv_id IN(5,6,7,8,10) OR wlkd.dv_id IN(1,3) )
	--AND wlkf.field_level_no<>3 --exclude submitted KPI's:NOT
	AND wlkf.variance_id IS NULL
	AND cube_id=in_cube_id;
	IF (t_rowcount>0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_INVALID_KPISET] Invalid <KPI Set>.  A PA worksheet cannot have working plans.');
	END IF;

/*
    Per Gary Y: If a template has a variance, check its components (FROM_KPI_ID and TO_KPI_ID).
   If none of them is a WP KPI, then the template can be assigned to a PA view
   Else if either one is a WP KPI:
	  If the WP KPIs are all "submitted", then the template can be assigned to a PA view.
*/
	--PA worksheet variances ALWAYS have kpi_dv_id of "10".--
	SELECT COUNT(*) INTO t_rowcount
	FROM maxdata.wlkfw_kpi_field wlkf
	JOIN maxdata.wlkv_kpi_variance wlkv ON(wlkf.variance_id=wlkv.kpi_variance_id
					AND (wlkv.from_kpi_dv_id=10 OR wlkv.to_kpi_dv_id=10 ) )
	WHERE worksheet_template_id=in_template_id
	AND (  (in_object_type_cd<>2)
	     OR(in_object_type_cd=2 AND worksheet_task_no=in_object_no) )
	AND (  (in_object_type_cd<>4)
	     OR(in_object_type_cd=4 AND kpi_set_no=in_object_no) )
	--AND wlkf.field_level_no<>3 --exclude submitted KPI's:NOT
	AND wlkf.variance_id IS NOT NULL
	AND cube_id=in_cube_id;
	IF (t_rowcount>0) THEN
		RAISE_APPLICATION_ERROR (-20001, '[WL_INVALID_VARIANCE] Invalid <KPI Variance>.  A PA worksheet cannot have working plans.');
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
