--------------------------------------------------------
--  DDL for Procedure P_COPY_FAV_ASSIGN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COPY_FAV_ASSIGN" (
	in_src_planworksheet_id 		NUMBER,
	in_tar_planworksheet_id			NUMBER
) AS
/*

----------------------------------------------------------------------
$Log: 2351_p_copy_fav_assign.sql,v $
Revision 1.1  2007/10/05 19:08:22  amkatr
For Defect # S0467554 ,S0467551

Revision 1.1  2007/10/04 19:17:32  amkatr
FIXID : For enhancement, MMMR65573, MMMR65574, MMMR65575



--------------------------------------------------------------------------------
*/



n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_copy_fav_assign';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_inserted_flag			NUMBER;
t_inserted_flag1		NUMBER;


BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_src_planworksheet_id, -1) || ',' ||
	COALESCE(in_tar_planworksheet_id, -1)  || ',' ||
	 ')' ;
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);



n_sqlnum := 2000;


SELECT property_value
INTO t_inserted_flag
FROM maxdata.t_application_property
WHERE property_key = 'marketmax.planversion.copyfavoritesandassignments';


IF t_inserted_flag = 1 THEN

BEGIN

	INSERT INTO maxdata.wlfo_favorite_object
		(user_id,
		object_type_cd,
		table_prefix_cd,
		primary_col1_no,
		primary_col2_no,
		create_dttm,
		update_dttm)
	SELECT
		user_id,
		object_type_cd,
		table_prefix_cd,
		in_tar_planworksheet_id,
		primary_col2_no,
		SYSDATE,
		NULL
	FROM maxdata.wlfo_favorite_object
	WHERE primary_col1_no = in_src_planworksheet_id
	AND object_type_cd = 26;                             ------- 26 is worksheet


	INSERT INTO maxdata.wlws_workflow_snapshot
		(user_id,
		object_type_cd,
		table_prefix_cd,
		primary_col1_no,
		primary_col2_no,
		create_dttm,
		update_dttm)
	SELECT
		user_id,
		object_type_cd,
		table_prefix_cd,
		in_tar_planworksheet_id,
		primary_col2_no,
		SYSDATE,
		NULL
	FROM maxdata.wlws_workflow_snapshot
	WHERE primary_col1_no = in_src_planworksheet_id AND object_type_cd = 26;


	SELECT property_value
	INTO t_inserted_flag1
	FROM maxdata.t_application_property
	WHERE property_key = 'marketmax.planversion.removefavoriteandassignment';


	IF t_inserted_flag1 = 1 THEN
	BEGIN
		DELETE FROM maxdata.wlfo_favorite_object WHERE primary_col1_no = in_src_planworksheet_id and OBJECT_TYPE_CD = 26;

		DELETE FROM maxdata.wlws_workflow_snapshot WHERE primary_col1_no = in_src_planworksheet_id and OBJECT_TYPE_CD = 26;
	END;
	END IF;

END;
END IF;



EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := substr(v_sql,1,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := substr(v_sql,1,255);
		t_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_COPY_FAV_ASSIGN" TO "MADMAX";
