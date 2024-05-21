--------------------------------------------------------
--  DDL for Procedure P_SAVE_AS_PLANWORKSHT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_SAVE_AS_PLANWORKSHT" (
	in_cube_id			NUMBER,
	in_src_planworksheet_id 	NUMBER,
	in_tar_planworksheet_name 	VARCHAR2,
	in_tar_planworksheet_desc  	VARCHAR2,
	in_create_userid 		NUMBER,
	in_max_user_id 		NUMBER,
	in_max_group_id 		NUMBER,
 	in_parent_id 			NUMBER,		-- Planversion id
	in_copy_plan_data 		NUMBER,		-- 2 is the ONLY valid value
	in_copy_cl_hist 		NUMBER,		-- 0/1
	in_copy_submitted		NUMBER,		-- 0/1
	in_set_whatif			NUMBER,		-- 0/1
	in_future1			NUMBER,		-- placeholder. Pass in -1.
	in_future2			NUMBER,		-- placeholder
	in_future3			VARCHAR2,	-- placeholder
	out_tar_planworksheet_id OUT	NUMBER,
	out_errcode 		 OUT 	NUMBER,
	out_errmsg 		 OUT 	VARCHAR2
) AS

/*
$Log: 2354_p_save_as_planworksht.sql,v $
Revision 1.16.2.1  2008/03/12 20:52:35  vejang
613 : Just change the files datetime

Revision 1.16  2008/03/10 16:06:32  dirapa
No comment given.

Revision 1.15  2008/02/14 16:05:18  dirapa
Fixid: S0459218

Revision 1.14  2007/06/19 14:38:58  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.10  2006/09/14 17:36:19  anchan
S0371036: Do not mark DELETED during switch of worksheet_template_id

Revision 1.9  2006/06/29 14:21:18  saghai
S0364145 - Update planworksheet_id column in Dimset_template

Revision 1.8  2006/06/16 15:13:24  anchan
Set template_id to NULL before switch

Revision 1.7  2006/06/12 16:16:53  anchan
S0360935,S0351640: Undo these changes, since a unique constraint will be used(in ORA only)

Revision 1.6  2006/06/08 13:24:57  anchan
S0360935: push out handling of switching of worksheet_template_id to triggers.

Revision 1.5  2006/04/17 19:44:41  saghai
S0332260 - Cube_id will always be -1

----------------------------------------------------------------------

Change History

V6.1
6.1.0-001 06/15/05 Sachin	Initial Entry

Description:

This is a procedure used to Save a Worksheet

--------------------------------------------------------------------------------*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_save_as_planworksht';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
v_sql2              		VARCHAR2(4000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);

t_src_time_template_id		NUMBER(10);
t_src_loc_template_id		NUMBER(10);
t_src_merch_template_id		NUMBER(10);
t_src_worksheet_template_id	NUMBER(10);

t_tar_time_template_id		NUMBER(10);
t_tar_loc_template_id		NUMBER(10);
t_tar_merch_template_id		NUMBER(10);
t_tar_worksheet_template_id	NUMBER(10);
t_tar_planworksheet_id		NUMBER(10);
t_col_value			VARCHAR2(100);


BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure
t_call := t_proc_name || ' ( ' ||
		COALESCE(in_cube_id, -1) || ',' ||
		COALESCE(in_src_planworksheet_id, -1) || ',' ||
		COALESCE(in_tar_planworksheet_name, 'NULL') || ',' ||
		COALESCE(in_tar_planworksheet_desc,'NULL') || ',' ||
		COALESCE(in_create_userid, -1) || ',' ||
		COALESCE(in_max_user_id, -1) || ',' ||
		COALESCE(in_max_group_id, -1) || ',' ||
		COALESCE(in_parent_id, -1) || ',' ||
		COALESCE(in_copy_plan_data, -1) || ',' ||
		COALESCE(in_copy_cl_hist, -1) || ',' ||
		COALESCE(in_copy_submitted, -1) || ',' ||
		COALESCE(in_set_whatif, -1) || ',' ||
		COALESCE(in_future1, -1) || ',' ||
		COALESCE(in_future2, -1) || ',' ||
		COALESCE(in_future3, -1) || ',' ||
		' OUT out_tar_planworksheet_id, OUT out_errcode, OUT out_errmsg '||
		' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
--COMMIT;


n_sqlnum := 4000;
IF in_cube_id IS NULL THEN
	RAISE_APPLICATION_ERROR(-20001,'Parameter in_cube_id cannot be NULL.');
END IF;


n_sqlnum := 5000;
IF in_copy_plan_data <> 2 THEN
	RAISE_APPLICATION_ERROR(-20001,'The ONLY valid valid for Parameter in_copy_plan_data is 2.');
END IF;

n_sqlnum := 10000;

SELECT time_template_id,loc_template_id,merch_template_id,worksheet_template_id
INTO t_src_time_template_id,t_src_loc_template_id,t_src_merch_template_id,t_src_worksheet_template_id
FROM maxdata.planworksheet
WHERE planworksheet_id = in_src_planworksheet_id;

n_sqlnum := 12000;
maxdata.p_copy_planworksht(
	-1,				-- in_cube_id, Always copy from permanent tables
	in_src_planworksheet_id,
	in_tar_planworksheet_name,
	in_tar_planworksheet_desc,
	in_create_userid,
	in_max_user_id,
	in_max_group_id,
 	in_parent_id,
	in_copy_plan_data,
	in_copy_cl_hist,
	in_copy_submitted,
	in_set_whatif,
	in_future1,
	in_future2,
	in_future3,
	t_tar_planworksheet_id,
	out_errcode,
	out_errmsg
);

out_tar_planworksheet_id := t_tar_planworksheet_id;

n_sqlnum := 13000;
SELECT time_template_id,loc_template_id,merch_template_id,worksheet_template_id
INTO t_tar_time_template_id,t_tar_loc_template_id,t_tar_merch_template_id,t_tar_worksheet_template_id
FROM maxdata.planworksheet
WHERE planworksheet_id = t_tar_planworksheet_id;

-- -- NULL out the planworksheet_id columns of the worksheet_template_id's
-- --So that they can be swapped between the two planworksheets:--
-- n_sqlnum := 14000;
-- UPDATE maxdata.wlwt_worksheet_template
-- SET 	planworksheet_id=NULL
-- WHERE worksheet_template_id = t_src_worksheet_template_id;
--
-- UPDATE maxdata.wlwt_worksheet_template
-- SET 	planworksheet_id=NULL
-- WHERE worksheet_template_id = t_tar_worksheet_template_id;

-- To avoid being marked "DELETED" by the update trigger, temporarily NULL it out:
n_sqlnum := 16000;
UPDATE maxdata.planworksheet
SET 	worksheet_template_id = NULL
WHERE planworksheet_id IN(in_src_planworksheet_id,t_tar_planworksheet_id);

-- Update Source Planworksheet columns
n_sqlnum := 17000;
UPDATE maxdata.planworksheet
SET 	time_template_id = t_src_time_template_id,
	loc_template_id = t_src_loc_template_id,
	merch_template_id = t_src_merch_template_id,
	worksheet_template_id = t_src_worksheet_template_id
WHERE planworksheet_id = t_tar_planworksheet_id;

-- Update planworksheet_id column in Dimset_template
UPDATE maxdata.dimset_template
SET planworksheet_id = t_tar_planworksheet_id
WHERE template_id in (t_src_time_template_id,t_src_loc_template_id,t_src_merch_template_id);

-- Update Target Planworksheet columns
n_sqlnum := 18000;
UPDATE maxdata.planworksheet
SET 	time_template_id = t_tar_time_template_id,
	loc_template_id = t_tar_loc_template_id,
	merch_template_id = t_tar_merch_template_id,
	worksheet_template_id = t_tar_worksheet_template_id
WHERE planworksheet_id = in_src_planworksheet_id;

-- Update planworksheet_id column in Dimset_template
n_sqlnum := 19000;
UPDATE maxdata.dimset_template
SET planworksheet_id = in_src_planworksheet_id
WHERE template_id in (t_tar_time_template_id,t_tar_loc_template_id,t_tar_merch_template_id);


-- Copy Favorite Object
BEGIN
	n_sqlnum := 20000;
	v_sql := NULL;
	v_sql2 := NULL;

	DECLARE CURSOR c_wrksht_cols IS
	SELECT UPPER(column_name) column_name FROM user_tab_columns
	WHERE table_name = 'WLFO_FAVORITE_OBJECT' AND UPPER(column_name) NOT IN ('CREATE_DTTM','UPDATE_DTTM')
	ORDER BY column_name;
	BEGIN
	FOR c1 IN c_wrksht_cols LOOP
		IF v_sql IS NULL  THEN
			v_sql := 'INSERT INTO maxdata.wlfo_favorite_object ( ';
		ELSE
			v_sql := v_sql ||',';
		END IF;
		v_sql := v_sql || c1.column_name;

		-- Fill column values.

		t_col_value := 	CASE c1.column_name
						WHEN 'PRIMARY_COL1_NO' THEN ':out_tar_planworksheet_id'
						WHEN 'USER_ID' THEN ':in_max_user_id'
						ELSE c1.column_name
					END;


		IF v_sql2 IS NULL THEN
			v_sql2 := ' ) SELECT ';
		ELSE
			v_sql2 := v_sql2 ||',';
		END IF;
		v_sql2 := v_sql2 || t_col_value;
	END LOOP;
	END;

	--WARNING: Do not CHANGE THE ORDER OF THE COLUMNS in the USING clause.
	--They must be in the alphatical order of the columns.

	n_sqlnum := 21000;
	EXECUTE IMMEDIATE v_sql || v_sql2 ||
			' FROM maxdata.WLFO_FAVORITE_OBJECT ' ||
			' WHERE PRIMARY_COL1_NO = :in_src_planworksheet_id' ||
			' AND USER_ID = :in_max_user_id'
	USING 	out_tar_planworksheet_id,
			in_max_user_id,
			in_src_planworksheet_id,
			in_max_user_id;


END; -- End Copy Favorite Object

COMMIT;

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
		COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_SAVE_AS_PLANWORKSHT" TO "MADMAX";
