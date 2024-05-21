--------------------------------------------------------
--  DDL for Procedure P_WL_PRUNE_OBJECT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_PRUNE_OBJECT" (
	in_cube_id		NUMBER, -- Required Pass in -1 for PERMANENT.
	in_template_id 		NUMBER, -- Required.
	in_task_no 		NUMBER, -- Required if template_id=0, else pass in -1.
	in_future1		NUMBER,	-- placeholder. Pass in -1.
	in_future2		NUMBER,	-- placeholder. Pass in -1.
	in_future3		NUMBER	-- placeholder. Pass in -1.
) AS

/*
$Log: 2350_p_wl_prune_object.sql,v $
Revision 1.5.8.1  2008/06/30 15:54:46  anchan
FIXID S0515760: remove kpi_dv_id from WLTD table if not used by KPI's or variance KPI's


Revision 1.5  2007/06/19 14:40:15  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.1.10.1  2007/06/05 15:59:03  vejang
Moved From 6121 to 612HF5

Revision 1.1.6.1  2007/05/30 17:32:40  anchan
S0419208: Remove temporary tasks marked 'X'

Revision 1.1  2006/11/21 22:38:28  anchan
S0391218: Replaces 2350_p_wl_prune_kpiset.sql.   Prune orphaned WLDF/w rows also.

Revision 1.2  2006/11/15 20:25:04  anchan
S0389727: remove all WLOOW rows belonging to template/task,
	so that TR_WLKS_BEF_D does not encounter too_many_rows error.

Revision 1.1  2006/11/09 15:57:58  anchan
S0390252: remove any hanging kpisets.

--------------------------------------------------------------------------------
Prunes or cleans up any hanging kpiset/display_format, not attached to a pane or level.
These hanging kpisets must be removed; otherwise, invalid kpi_dv_id's
may be displayed by the app to the user.
--------------------------------------------------------------------------------
*/

n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 		:= 'p_wl_prune_object';
t_error_level      	VARCHAR2(6) 		:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(1000) 		:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);

t_debug_flg		NUMBER(1)		:= -1;
t_char_null		VARCHAR2(20)		:= NULL;

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_cube_id, -123) || ',' ||
	COALESCE(in_template_id, -123) || ',' ||
	COALESCE(in_task_no, -123) || ',' ||
	COALESCE(in_future1, -123) || ',' ||
	COALESCE(in_future2, -123) || ',' ||
	COALESCE(in_future3, -123) ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, -1);
-- COMMIT;

-- Signal beginning of the object-operation(CLEANUP-KPISET=0):
-- BEGIN --
--   Remove all rows from previous failed attempt for this template/task.
n_sqlnum := 2000;
DELETE FROM maxdata.WLOOW_object_operation
WHERE cube_id = in_cube_id
AND worksheet_template_id = in_template_id
AND (object_no=in_task_no OR in_template_id>0);

n_sqlnum := 2100;
INSERT INTO maxdata.WLOOW_object_operation (cube_id,worksheet_template_id,object_type_cd,object_no,procedure_nm)
VALUES (in_cube_id,in_template_id,0,in_task_no,t_proc_name);
COMMIT;
-- END; --

n_sqlnum := 3000;
IF in_cube_id=-1 AND in_template_id=0 THEN
	BEGIN
	n_sqlnum := 3100;
	DELETE FROM maxdata.WLKS_kpi_set
	WHERE worksheet_template_id=0
	AND worksheet_task_no=in_task_no
	AND kpi_set_no NOT IN
		(SELECT kpi_set_no FROM maxdata.WLLA_level_assignment
		WHERE worksheet_template_id=0
		AND worksheet_task_no=in_task_no
		AND kpi_set_no IS NOT NULL)
	AND kpi_set_no NOT IN
		(SELECT kpi_set_no FROM maxdata.WLTP_task_pane
		WHERE worksheet_template_id=0
		AND worksheet_task_no=in_task_no);
	COMMIT;

	n_sqlnum := 3200;
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id=0
	AND worksheet_task_no=in_task_no
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLKF_kpi_field
		WHERE worksheet_template_id=0
		AND worksheet_task_no=in_task_no
		AND display_format_no IS NOT NULL)
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLLA_level_assignment
		WHERE worksheet_template_id=0
		AND worksheet_task_no=in_task_no
		AND display_format_no IS NOT NULL)
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLTP_task_pane
		WHERE worksheet_template_id=0
		AND worksheet_task_no=in_task_no
		AND display_format_no IS NOT NULL)
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLW1_worksheet_task
		WHERE worksheet_template_id=0
		AND worksheet_task_no=in_task_no
		AND display_format_no IS NOT NULL);
	COMMIT;

	END;
END IF;

n_sqlnum := 4000;
IF in_cube_id=-1 AND in_template_id>0 THEN
	BEGIN
	n_sqlnum := 4100;
	DELETE FROM maxdata.WLKS_KPI_SET
	WHERE worksheet_template_id=in_template_id
	AND kpi_set_no NOT IN
		(SELECT kpi_set_no FROM maxdata.WLLA_LEVEL_ASSIGNMENT
		WHERE worksheet_template_id=in_template_id
		AND kpi_set_no IS NOT NULL)
	AND kpi_set_no NOT IN
		(SELECT kpi_set_no FROM maxdata.WLTP_TASK_PANE
		WHERE worksheet_template_id=in_template_id);
	COMMIT;

	n_sqlnum := 4150;
    --S0515760: remove unused kpi_dv_id's from WLKD--
    DELETE FROM maxdata.WLTD_template_dataversion
    WHERE worksheet_template_id=in_template_id
    AND kpi_dv_id NOT IN
       (SELECT kpi_dv_id
        FROM maxdata.WLKF_kpi_field
        WHERE worksheet_template_id=in_template_id)
    AND kpi_dv_id NOT IN
       (SELECT to_kpi_dv_id
       FROM maxdata.WLKV_kpi_variance
       JOIN maxdata.WLKF_kpi_field ON (kpi_variance_id=kpi_field_no)
       WHERE worksheet_template_id=in_template_id
       AND kpi_field_no>=100000)
    AND kpi_dv_id NOT IN
       (SELECT from_kpi_dv_id
       FROM maxdata.WLKV_kpi_variance
       JOIN maxdata.WLKF_kpi_field ON (kpi_variance_id=kpi_field_no)
       WHERE worksheet_template_id=in_template_id
       AND kpi_field_no>=100000);
    COMMIT;

	n_sqlnum := 4200;
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id=in_template_id
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLKF_kpi_field
		WHERE worksheet_template_id=in_template_id
		AND display_format_no IS NOT NULL)
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLLA_level_assignment
		WHERE worksheet_template_id=in_template_id
		AND display_format_no IS NOT NULL)
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLTP_task_pane
		WHERE worksheet_template_id=in_template_id
		AND display_format_no IS NOT NULL)
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLW1_worksheet_task
		WHERE worksheet_template_id=in_template_id
		AND display_format_no IS NOT NULL);
	COMMIT;
	END;
END IF;

n_sqlnum := 5000;
IF in_cube_id>0 AND in_template_id>0 THEN
	BEGIN
	--App temporarily marks it X when creating NEW tasks within worksheet editor.
	--If it is still X, it mean the user did not close gracefully, so remove it:
		DECLARE CURSOR c_temp_task IS
		SELECT worksheet_task_no
		FROM maxdata.wlw1w_worksheet_task
		WHERE cube_id=in_cube_id
		AND worksheet_template_id=in_template_id
		AND usage_type_cd='X';
	BEGIN
	FOR r_tt IN c_temp_task
	LOOP
		maxdata.p_wl_delete_working_rows
			(in_cube_id,'WLW1W',in_template_id,r_tt.worksheet_task_no,-1,-1,NULL,-1,-1,-1);
	END LOOP;
	END;

	n_sqlnum := 5100;
	DELETE FROM maxdata.WLKSw_KPI_SET
	WHERE worksheet_template_id=in_template_id
	AND cube_id=in_cube_id
	AND kpi_set_no NOT IN
		(SELECT kpi_set_no FROM maxdata.WLLAw_LEVEL_ASSIGNMENT
		WHERE worksheet_template_id=in_template_id
		AND cube_id=in_cube_id
		AND kpi_set_no IS NOT NULL)
	AND kpi_set_no NOT IN
		(SELECT kpi_set_no FROM maxdata.WLTPw_TASK_PANE
		WHERE worksheet_template_id=in_template_id
		AND cube_id=in_cube_id);
	COMMIT;

	n_sqlnum := 5150;
    --S0515760: remove unused kpi_dv_id's from WLKDw--
    DELETE FROM maxdata.WLTDw_template_dataversion
    WHERE worksheet_template_id=in_template_id
    AND kpi_dv_id NOT IN
       (SELECT kpi_dv_id
        FROM maxdata.WLKFw_kpi_field
        WHERE worksheet_template_id=in_template_id
        AND cube_id=in_cube_id)
    AND kpi_dv_id NOT IN
       (SELECT to_kpi_dv_id
       FROM maxdata.WLKV_kpi_variance
       JOIN maxdata.WLKFw_kpi_field ON (kpi_variance_id=kpi_field_no)
       WHERE worksheet_template_id=in_template_id
       AND kpi_field_no>=100000
       AND cube_id=in_cube_id)
    AND kpi_dv_id NOT IN
       (SELECT from_kpi_dv_id
       FROM maxdata.WLKV_kpi_variance
       JOIN maxdata.WLKFw_kpi_field ON (kpi_variance_id=kpi_field_no)
       WHERE worksheet_template_id=in_template_id
       AND kpi_field_no>=100000
       AND cube_id=in_cube_id)
    AND cube_id=in_cube_id;
    COMMIT;

	n_sqlnum := 5200;
	DELETE FROM maxdata.WLDFw_display_format
	WHERE worksheet_template_id=in_template_id
	AND cube_id=in_cube_id
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLKFw_kpi_field
		WHERE worksheet_template_id=in_template_id
		AND cube_id=in_cube_id
		AND display_format_no IS NOT NULL)
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLLAw_level_assignment
		WHERE worksheet_template_id=in_template_id
		AND cube_id=in_cube_id
		AND display_format_no IS NOT NULL)
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLTPw_task_pane
		WHERE worksheet_template_id=in_template_id
		AND cube_id=in_cube_id
		AND display_format_no IS NOT NULL)
	AND display_format_no NOT IN
		(SELECT display_format_no FROM maxdata.WLW1w_worksheet_task
		WHERE worksheet_template_id=in_template_id
		AND cube_id=in_cube_id
		AND display_format_no IS NOT NULL);
	COMMIT;
	END;
END IF;

--Signal end of the object-operation(CLEANUP-KPISET=0):
n_sqlnum := 8000;
DELETE FROM maxdata.WLOOW_object_operation
WHERE cube_id = in_cube_id
AND worksheet_template_id = in_template_id
AND object_type_cd=0
AND object_no=in_task_no;

COMMIT;


EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := substr(v_sql,1,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, -1);
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := substr(v_sql,1,255);
		t_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, -1);
		--COMMIT;

		-- Cleanup status entry after failure:
		n_sqlnum := 9000;
		DELETE FROM maxdata.WLOOW_object_operation
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id
		AND object_type_cd=0
		AND object_no=in_task_no;
		COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_WL_PRUNE_OBJECT" TO "MADMAX";
