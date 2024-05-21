--------------------------------------------------------
--  DDL for Procedure P_WL_DELETE_PERMANENT_ROWS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_DELETE_PERMANENT_ROWS" (
	in_object_prefix_cd		VARCHAR2,
	in_template_id			NUMBER,
	in_primary_col2			NUMBER,		-- Pass -1 for NULL
	in_primary_col3			NUMBER,		-- Pass -1 for NULL
	in_primary_col4			NUMBER,		-- Pass -1 for NULL
	in_dimension_type_cd		VARCHAR2,
	in_future1			NUMBER,		-- Placeholder.Pass in -1.
	in_future2			NUMBER,		-- Placeholder.Pass in -1.
	in_future3			NUMBER		-- Placeholder.Pass in -1.
) AS

/*----------------------------------------------------------------------
$Log: 2322_p_wl_delete_permanent_rows.sql,v $
Revision 1.15.8.3  2008/10/09 14:52:16  anchan
No-change check-in

Revision 1.15.8.1  2008/09/30 21:17:13  anchan
FIXID S0532959: Only delete format belonging to the single KPI field.

Revision 1.15  2007/06/19 14:39:09  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.11  2006/11/21 21:44:11  saghai
Removed updates as per Andy

Revision 1.10  2006/11/14 16:26:59  saghai
S0362566, S0376286 More Changes

Revision 1.9  2006/11/10 21:45:47  saghai
S0362566, S0376286 Performance Changes

Revision 1.8  2006/03/17 21:40:07  anchan
Only delete MODEL templates from user favorites/assignments.

Revision 1.7  2006/02/16 18:08:06  anchan
PERFORMANCE-ENHANCED PACKAGE

----------------------------------------------------------------------

Change History

==========
6.1.0-006
6.0.0-001 03/03/05 Sachin	Initial Entry

Description:

This procedure will delete records from the working template tables
for worksheet template functionality

***For proper control of ROLLBACK of transactions from the calling procedures,
this procedure must not issue any COMMITs.***
----------------------------------------------------------------------------------*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_wl_delete_permanent_rows';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_int_null			NUMBER(10,0)		:= NULL;

t_worksheet_task_no		NUMBER(10,0);
t_pane_layout_no		NUMBER(10,0);
t_pane_no			NUMBER(10,0);
t_dimension_layout_no		NUMBER(10,0);
t_cnt				NUMBER(10,0);
t_usage_type_cd			CHAR(1);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_object_prefix_cd, 'NULL')  || ',' ||
	COALESCE(in_template_id, -1) || ',' ||		-- COALESCE(int, 'NULL') returns error because of diff datatype.
	COALESCE(in_primary_col2, -1) || ',' ||
	COALESCE(in_primary_col3, -1) || ',' ||
	COALESCE(in_primary_col4, -1) || ',' ||
	COALESCE(in_dimension_type_cd, 'NULL')  || ',' ||
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
	COALESCE(in_future3, -1) ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);


-- Parameter sanity checks
IF in_object_prefix_cd IS NULL THEN
	RAISE_APPLICATION_ERROR(-20001,' Table prefix code cannot be NULL.');
END IF;

--IF in_cube_id IS NULL THEN
--	RAISE_APPLICATION_ERROR(-20001,' Cube Id cannot be NULL.');
--END IF;

IF in_template_id IS NULL THEN
	RAISE_APPLICATION_ERROR(-20001,' Template Id cannot be NULL.');
END IF;

IF in_object_prefix_cd = 'WLWT' AND in_template_id = 0 THEN
	RAISE_APPLICATION_ERROR(-20001,' Cannot delete System Dummy Template record.');
END IF;

BEGIN
SELECT 1 INTO t_cnt
FROM maxdata.wlwt_worksheet_template
WHERE worksheet_template_id=in_template_id;
--AND cube_id=in_cube_id;
EXCEPTION
	WHEN NO_DATA_FOUND THEN
		 RETURN;
END;


CASE  UPPER(in_object_prefix_cd)

-- If Display Format is deleted
WHEN 'WLDF' THEN

	IF COALESCE(in_primary_col2,-1) = -1 THEN
		RAISE_APPLICATION_ERROR(-20001,' "Display Format No" cannot be NULL or -1.');
	END IF;

	n_sqlnum := 3000;
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id
	AND display_format_no = in_primary_col2;
	--AND cube_id = in_cube_id;


-- If Dimension Layout is deleted
WHEN 'WLD1' THEN

	IF COALESCE(in_primary_col2,-1) = -1 THEN
		RAISE_APPLICATION_ERROR(-20001,' "Dimension Layout No" cannot be NULL or -1.');
	END IF;

	n_sqlnum := 4000;
	DELETE FROM maxdata.WLD1_dimension_layout
	WHERE worksheet_template_id = in_template_id
	AND dimension_layout_no = in_primary_col2;
	--AND cube_id = in_cube_id;

-- If KPI Field is deleted
WHEN 'WLKF' THEN

	n_sqlnum := 6000;

	IF COALESCE(in_primary_col2, -1) = -1 OR COALESCE(in_primary_col3,-1) = -1 THEN
		RAISE_APPLICATION_ERROR(-20001,' "KPI Set No" and "KPI Field No" cannot be NULL or -1.');
	END IF;

    n_sqlnum :=  6100;
	-- Delete Display Format of KPI_field
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id
    AND display_format_no =
        (SELECT display_format_no
        FROM maxdata.WLKF_kpi_field
            WHERE worksheet_template_id = in_template_id
            AND kpi_set_no = in_primary_col2
            AND kpi_field_no = in_primary_col3
            AND kpi_dv_id = in_primary_col4);

	n_sqlnum := 6200;
	DELETE FROM maxdata.WLKF_kpi_field
	WHERE worksheet_template_id = in_template_id
	AND kpi_set_no = in_primary_col2
	AND kpi_field_no = in_primary_col3
	AND kpi_dv_id = in_primary_col4;
	--AND cube_id = in_cube_id;


-- If KPI Set is deleted
WHEN 'WLKS' THEN

	n_sqlnum := 7000;

	IF COALESCE(in_primary_col2,-1) = -1 THEN
		RAISE_APPLICATION_ERROR(-20001,' "KPI Set No" cannot be NULL or -1.');
	END IF;

        n_sqlnum :=  7100;
	-- Delete Display Format of KPI_fields for KPI Set
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id
        AND kpi_set_no = in_primary_col2;

	n_sqlnum := 7200;
	DELETE FROM maxdata.WLKS_kpi_set
	WHERE worksheet_template_id = in_template_id
	AND kpi_set_no = in_primary_col2;
	--AND cube_id = in_cube_id;

	IF (in_template_id=0) THEN
	BEGIN
		 n_sqlnum := 7300;
   		 DELETE FROM MAXDATA.WLFO_favorite_object
   		 WHERE object_type_cd=4
   		 AND primary_col1_no=in_primary_col2;

		 n_sqlnum := 7400;
		 DELETE FROM MAXDATA.WLWS_workflow_snapshot
   		 WHERE object_type_cd=4
   		 AND primary_col1_no=in_primary_col2;
	END;
	END IF;

-- If Level Assignment is deleted
WHEN 'WLLA' THEN

	n_sqlnum := 8000;

	IF (COALESCE(in_primary_col2,-1) = -1)
	OR (COALESCE(in_primary_col3,-1) = -1)
	OR (COALESCE(in_primary_col4,-1) = -1)
	OR (in_dimension_type_cd IS NULL) THEN
		RAISE_APPLICATION_ERROR(-20001,' "Worksheet Task No", "Pane No", "Dimension Level No", and "Dimension Type CD" cannot be NULL or -1.');
	END IF;


	n_sqlnum := 8100;
	-- Delete Display Format's of Level Assignments
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id
        AND worksheet_task_no = in_primary_col2
	AND display_format_no = ( 	SELECT display_format_no
					FROM maxdata.WLLA_level_assignment
        				WHERE worksheet_template_id = in_template_id
        				AND worksheet_task_no = in_primary_col2
        				AND pane_no = in_primary_col3
        				AND dimension_level_no = in_primary_col4
        				AND dimension_type_cd = in_dimension_type_cd
        				);


	n_sqlnum := 8200;
 	-- Delete Display Format of KPI_fields for KPI Sets of Level Assignments
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id
	AND kpi_set_no = ( 	SELECT kpi_set_no
				FROM maxdata.WLLA_level_assignment
        			WHERE worksheet_template_id = in_template_id
        			AND worksheet_task_no = in_primary_col2
        			AND pane_no = in_primary_col3
        			AND dimension_level_no = in_primary_col4
        			AND dimension_type_cd = in_dimension_type_cd
        		);



	n_sqlnum := 8300;
	-- Delete KPI Set of Level Assignments
	DELETE FROM maxdata.WLKS_kpi_set
	WHERE worksheet_template_id = in_template_id
        AND worksheet_task_no = in_primary_col2
	AND kpi_set_no = (	SELECT kpi_set_no
				FROM maxdata.WLLA_level_assignment
        			WHERE worksheet_template_id = in_template_id
        			AND worksheet_task_no = in_primary_col2
        			AND pane_no = in_primary_col3
        			AND dimension_level_no = in_primary_col4
        			AND dimension_type_cd = in_dimension_type_cd
        			);


	n_sqlnum := 8400;
	DELETE FROM maxdata.WLLA_level_assignment
	WHERE worksheet_template_id = in_template_id
	AND worksheet_task_no = in_primary_col2
	AND pane_no = in_primary_col3
	AND dimension_level_no = in_primary_col4
	AND dimension_type_cd = in_dimension_type_cd;
	--AND cube_id = in_cube_id;


-- If Task Pane is deleted
WHEN 'WLTP' THEN

	n_sqlnum := 9000;

	IF COALESCE(in_primary_col2,-1) = -1 OR COALESCE(in_primary_col3,-1) = -1 THEN
		RAISE_APPLICATION_ERROR(-20001,' "Worksheet Task No" and "Pane No" cannot be NULL or -1.');
	END IF;


	n_sqlnum := 9100;
	-- Delete Display Format's of Level Assignments
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id
        AND worksheet_task_no = in_primary_col2
	AND display_format_no IN ( 	SELECT display_format_no
					FROM maxdata.WLLA_level_assignment
        				WHERE worksheet_template_id = in_template_id
        				AND worksheet_task_no = in_primary_col2
        				AND pane_no = in_primary_col3
        				);


	n_sqlnum := 9200;
	-- Delete Display Format of Task Pane
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id
        AND worksheet_task_no = in_primary_col2
	AND display_format_no = ( 	SELECT display_format_no
					FROM maxdata.WLTP_task_pane
        				WHERE worksheet_template_id = in_template_id
        				AND worksheet_task_no = in_primary_col2
        				AND pane_no = in_primary_col3
        				);


	n_sqlnum := 9300;
	-- Delete Display Format of KPI_fields for KPI Sets of Level Assignments
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id
	AND kpi_set_no IN ( 	SELECT kpi_set_no
				FROM maxdata.WLLA_level_assignment
        			WHERE worksheet_template_id = in_template_id
        			AND worksheet_task_no = in_primary_col2
        			AND pane_no = in_primary_col3
   			);


	n_sqlnum := 9400;
	-- Delete Display Format of KPI_fields for KPI Sets of Task Pane
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id
	AND kpi_set_no = ( 	SELECT kpi_set_no
				FROM maxdata.WLTP_task_pane
        			WHERE worksheet_template_id = in_template_id
        			AND worksheet_task_no = in_primary_col2
        			AND pane_no = in_primary_col3
   			);


	n_sqlnum := 9500;
	-- Delete KPI Set of Task Pane
	DELETE FROM maxdata.WLKS_kpi_set
	WHERE worksheet_template_id = in_template_id
        AND worksheet_task_no = in_primary_col2
	AND kpi_set_no = (	SELECT kpi_set_no
				FROM maxdata.WLTP_task_pane
        			WHERE worksheet_template_id = in_template_id
        			AND worksheet_task_no = in_primary_col2
        			AND pane_no = in_primary_col3
        			);


	n_sqlnum := 9600;
	-- Delete KPI Sets of Level Assignments
	DELETE FROM maxdata.WLKS_kpi_set
	WHERE worksheet_template_id = in_template_id
        AND worksheet_task_no = in_primary_col2
	AND kpi_set_no IN (	SELECT kpi_set_no
				FROM maxdata.WLLA_level_assignment
        			WHERE worksheet_template_id = in_template_id
        			AND worksheet_task_no = in_primary_col2
        			AND pane_no = in_primary_col3
        			);



	n_sqlnum := 9700;
	-- Delete Dimension Layout of Task Pane
	DELETE FROM maxdata.WLD1_dimension_layout
	WHERE worksheet_template_id = in_template_id
        AND worksheet_task_no = in_primary_col2
	AND dimension_layout_no = ( 	SELECT dimension_layout_no
					FROM maxdata.WLTP_task_pane
        				WHERE worksheet_template_id = in_template_id
        				AND worksheet_task_no = in_primary_col2
        				AND pane_no = in_primary_col3
        				);

	n_sqlnum := 9800;
	DELETE FROM maxdata.WLLA_level_assignment
	WHERE worksheet_template_id = in_template_id
	AND worksheet_task_no = in_primary_col2
	AND pane_no = in_primary_col3;
	--AND cube_id = in_cube_id;

	n_sqlnum := 9900;
	DELETE FROM maxdata.WLTP_task_pane
	WHERE worksheet_template_id = in_template_id
	AND worksheet_task_no = in_primary_col2
	AND pane_no = in_primary_col3;
	--AND cube_id = in_cube_id;


-- If Pane Node is deleted
WHEN 'WLPN' THEN

	n_sqlnum := 10000;

	IF COALESCE(in_primary_col2,-1) = -1 OR COALESCE(in_primary_col3,-1) = -1 THEN
		RAISE_APPLICATION_ERROR(-20001,' "Pane Layout No" and "Pane Node No" cannot be NULL or -1.');
	END IF;

	n_sqlnum := 10100;
	DELETE FROM maxdata.WLPN_pane_node
	WHERE worksheet_template_id = in_template_id
	AND pane_layout_no = in_primary_col2
	AND pane_node_no = in_primary_col3;
	--AND cube_id = in_cube_id;


-- If Pane Layout is deleted
WHEN 'WLPL' THEN

	n_sqlnum := 11000;

	DELETE FROM maxdata.WLPL_pane_layout
	WHERE worksheet_template_id = in_template_id
	AND pane_layout_no = in_primary_col2;
	--AND cube_id = in_cube_id;

	-- WLLI_layout_image will not be deleted here
	-- since multiple pane layouts are using the same layout image
	--DELETE FROM maxdata.WLLI_layout_image
	--WHERE layout_image_id = t_layout_image_id;

-- If Worksheet Task is deleted
WHEN 'WLW1' THEN

	n_sqlnum := 12000;

	IF COALESCE(in_primary_col2,-1) = -1 THEN
		RAISE_APPLICATION_ERROR(-20001,' "Worksheet Task No" cannot be NULL or -1.');
	END IF;

	n_sqlnum := 12100;
	DELETE FROM maxdata.WLLA_level_assignment
	WHERE worksheet_template_id = in_template_id
	AND worksheet_task_no = in_primary_col2;
	--AND cube_id = in_cube_id;

	n_sqlnum := 12200;
	DELETE FROM maxdata.WLTP_task_pane
	WHERE worksheet_template_id = in_template_id
	AND worksheet_task_no = in_primary_col2;
	--AND cube_id = in_cube_id;

	n_sqlnum := 12300;
	DELETE FROM maxdata.WLW1_worksheet_task
	WHERE worksheet_template_id = in_template_id
	AND worksheet_task_no = in_primary_col2;
	--AND cube_id = in_cube_id;

	n_sqlnum := 12400;
	DELETE FROM maxdata.WLKS_kpi_set
	WHERE worksheet_template_id = in_template_id
	AND worksheet_task_no = in_primary_col2;
	--AND cube_id = in_cube_id;

	n_sqlnum := 12500;
	DELETE FROM maxdata.WLD1_dimension_layout
	WHERE worksheet_template_id = in_template_id
	AND worksheet_task_no = in_primary_col2;
	--AND cube_id = in_cube_id;

	n_sqlnum := 12600;
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id
	AND worksheet_task_no = in_primary_col2;
	--AND cube_id = in_cube_id;


	--WLPN_pane_node rows will be deleted via FK w/ DELETE CASCADE:
	n_sqlnum := 12700;
	DELETE FROM maxdata.WLPL_pane_layout
	WHERE worksheet_template_id = in_template_id
	AND worksheet_task_no = in_primary_col2;
	--AND cube_id = in_cube_id;

	IF (in_template_id=0) THEN
	BEGIN
		 n_sqlnum := 12800;
   		 DELETE FROM MAXDATA.WLFO_favorite_object
   		 WHERE object_type_cd=2
   		 AND primary_col1_no=in_primary_col2;
		 n_sqlnum := 12900;
		 DELETE FROM MAXDATA.WLWS_workflow_snapshot
   		 WHERE object_type_cd=2
   		 AND primary_col1_no=in_primary_col2;
	END;
	END IF;



-- If Worksheet Template is deleted
WHEN 'WLWT' THEN

	n_sqlnum := 13100;
	DELETE FROM maxdata.WLLA_level_assignment
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

	n_sqlnum := 13110;
	DELETE FROM maxdata.WLTP_task_pane
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

	n_sqlnum := 13120;
	DELETE FROM maxdata.WLW1_worksheet_task
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

	n_sqlnum := 13200;
	DELETE FROM maxdata.WLKS_kpi_set
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

	n_sqlnum := 13400;
	DELETE FROM maxdata.WLD1_dimension_layout
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

	n_sqlnum := 13500;
	DELETE FROM maxdata.WLDF_display_format
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

	n_sqlnum := 13600;
	DELETE FROM maxdata.WLPL_pane_layout
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

	n_sqlnum := 13700;
	DELETE FROM maxdata.WLTD_template_dataversion
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

	n_sqlnum := 13800;
	SELECT usage_type_cd INTO t_usage_type_cd
	FROM maxdata.WLWT_worksheet_template
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

	n_sqlnum := 13900;
	DELETE FROM maxdata.WLWT_worksheet_template
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

	IF (t_usage_type_cd='M') THEN
	BEGIN
	n_sqlnum := 13990;
		DELETE FROM MAXDATA.WLFO_favorite_object
		WHERE object_type_cd=1
		AND primary_col1_no=in_template_id;

		n_sqlnum := 13991;
		DELETE FROM MAXDATA.WLWS_workflow_snapshot
		WHERE object_type_cd=1
		AND primary_col1_no=in_template_id;
	END;
	END IF;

-- If Template DataVersion is deleted:
WHEN 'WLTD' THEN

	n_sqlnum := 14000;
	DELETE FROM maxdata.WLTD_template_dataversion
	WHERE worksheet_template_id = in_template_id;
	--AND cube_id = in_cube_id;

-- ELSE CASE is handled by the CASE_NOT_FOUND exception. See Exception Handler below.

END CASE;

n_sqlnum := 15000;


EXCEPTION
	WHEN CASE_NOT_FOUND THEN
		RAISE_APPLICATION_ERROR(-20001,' Not a valid object prefix code');

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
