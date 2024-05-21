--------------------------------------------------------
--  DDL for Procedure P_WL_DELETE_TEMPLATE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_DELETE_TEMPLATE" (
	in_cube_id			NUMBER,  	-- -1 for PERMANENT template
	in_template_id 			NUMBER, 	-- of the template.
	in_logical_delete_flg		NUMBER,		-- 1 if called from p_wl_delete_object , else -1.
	in_future2			NUMBER,		-- Placeholder. Pass in -1.
	in_future3			NUMBER		-- Placeholder. Pass in -1.
) AS

/*----------------------------------------------------------------------
$Log: 2324_p_wl_delete_template.sql,v $
Revision 1.8  2007/06/19 14:39:08  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.4  2006/08/22 18:07:10  anchan
S0344513: Mark the old template "DELETED" after assigning a new one.

Revision 1.3  2006/02/16 18:08:07  anchan
PERFORMANCE-ENHANCED PACKAGE


Change History

==========
6.1.0-006
6.1.0-001 07/01/05 Sachin	Initial Entry

Description:

This is a wrapper procedure used to delete working and permanent objects in the worksheet template model.

--------------------------------------------------------------------------------*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_wl_delete_template';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_int_null			NUMBER(10,0)		:= NULL;
t_int_negative_one		NUMBER(10,0)		:= -1;
t_char_null			VARCHAR2(20)		:= NULL;

t_object_prefix_cd		VARCHAR2(5);
t_planworksheet_id		NUMBER(10);
t_deleted_flg			NUMBER(1);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call:= t_proc_name || ' ( ' ||
	COALESCE(in_cube_id,-1) || ',' ||
	COALESCE(in_template_id, -1) || ',' ||
	COALESCE(in_logical_delete_flg, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
	COALESCE(in_future3, -1) ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;

n_sqlnum := 2000;
IF in_template_id IS NULL OR in_template_id = -1 THEN
	RAISE_APPLICATION_ERROR(-20001,'Parameter in_template_id cannot be NULL or -1.');
END IF;

n_sqlnum := 3000;
IF in_template_id = 0 THEN
	RAISE_APPLICATION_ERROR(-20001,'Cannot delete System Template.');
END IF;

--Signal the beginning of a procedure operation:
BEGIN
      n_sqlnum := 5000;
      DELETE FROM maxdata.WLOOW_object_operation
      WHERE cube_id = in_cube_id
      AND worksheet_template_id = in_template_id;

      n_sqlnum := 5100;
      INSERT INTO maxdata.WLOOW_object_operation (cube_id,worksheet_template_id,object_type_cd)
      VALUES (in_cube_id, in_template_id,1);

      COMMIT;
END;

IF in_cube_id = -1 THEN

	t_object_prefix_cd := 'WLWT';

	n_sqlnum := 10000;


	-- Check if already marked "DELETED":
	SELECT COUNT(*)
	INTO t_deleted_flg
	FROM maxdata.WLWT_worksheet_template
	WHERE worksheet_template_id = in_template_id
	AND deleted_flg=1;

	-- If it's not "DELETED" yet, check if Worksheet Tempalte is being used by Planworksheet.
	-- If it is DONT Delete Worksheet Template:
	IF t_deleted_flg=0 THEN
	BEGIN
		BEGIN
			SELECT planworksheet_id
			INTO t_planworksheet_id
			FROM maxdata.planworksheet
			WHERE worksheet_template_id = in_template_id;
		EXCEPTION
			WHEN NO_DATA_FOUND THEN
				NULL;
		END;

		IF t_planworksheet_id IS NOT NULL THEN
			v_sql :='Worksheet Template '||CAST(in_template_id AS VARCHAR2)||' used by planworksheet '||CAST(t_planworksheet_id AS VARCHAR2)||'. Cannot delete Worksheet Template.';
			RAISE_APPLICATION_ERROR(-20001,v_sql);
		END IF;
	END;
	END IF;


	IF in_logical_delete_flg = 1 THEN
		-- Template will be physically deleted by nightly job
		n_sqlnum := 10100;

		UPDATE maxdata.WLWT_worksheet_template
			SET deleted_flg = 1
		WHERE worksheet_template_id = in_template_id;

	ELSE
		n_sqlnum := 10200;
		maxdata.p_wl_delete_permanent_rows (
			t_object_prefix_cd,
			in_template_id,
			t_int_negative_one,		-- in_primary_col2,
			t_int_negative_one,		-- in_primary_col3,
			t_int_negative_one,		-- in_primary_col4,
			t_char_null,			-- in_dimension_type_cd,
			t_int_negative_one,		-- in_future1
			in_future2,
			in_future3
			);
	END IF;
ELSE
	n_sqlnum := 15000;
	t_object_prefix_cd := 'WLWTW';

	IF in_logical_delete_flg = 1 THEN
		-- Template will be physically deleted by nightly job
		n_sqlnum := 15100;

		UPDATE maxdata.WLWTW_worksheet_template
			SET deleted_flg = 1
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;

	ELSE
		n_sqlnum := 15200;
		maxdata.p_wl_delete_working_rows (
			in_cube_id,
			t_object_prefix_cd,
			in_template_id,
			t_int_negative_one,		-- in_primary_col2,
			t_int_negative_one,		-- in_primary_col3,
			t_int_negative_one,		-- in_primary_col4,
			t_char_null,			-- in_dimension_type_cd,
			t_int_negative_one,		--in_future1
			in_future2,
			in_future3
			);

		n_sqlnum := 15500;
		DELETE FROM maxdata.WLDRW_deleted_row
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;
	END IF;
END IF;

--Signal the end of a procedure operation:
n_sqlnum := 20000;
DELETE FROM maxdata.WLOOW_object_operation
WHERE cube_id = in_cube_id
AND worksheet_template_id = in_template_id;
COMMIT;

EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;

		--Signal the end of a procedure operation:
		DELETE FROM maxdata.WLOOW_object_operation
		WHERE cube_id = in_cube_id
		AND worksheet_template_id = in_template_id;
		COMMIT;

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

  GRANT EXECUTE ON "MAXDATA"."P_WL_DELETE_TEMPLATE" TO "MADMAX";
