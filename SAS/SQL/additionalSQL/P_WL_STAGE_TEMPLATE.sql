--------------------------------------------------------
--  DDL for Procedure P_WL_STAGE_TEMPLATE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_STAGE_TEMPLATE" (
	in_cube_id			NUMBER,  	-- Required
	in_template_id 			NUMBER, 	-- Required (of the source object)
	in_future1			NUMBER,		-- placeholder. Pass in -1.
	in_future2			NUMBER,		-- placeholder. Pass in -1.
	in_future3			NUMBER		-- placeholder. Pass in -1.
) AS

/*--------------------------------------------------------------------------------
$Log: 2344_p_wl_stage_template.sql,v $
Revision 1.12  2007/06/19 14:39:02  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.8  2006/09/11 13:37:29  anchan
S0366552: push out the changes to calling procs

Revision 1.7  2005/09/26 14:12:17  saghai
funstionality to handle NULL user_id and group_id

Revision 1.6  2005/09/23 14:27:50  saghai
added max_group_id and max_user_id parameters

Revision 1.5  2005/09/02 13:24:41  anchan
Delete only if exists in WORKING area

=============================
V6.1
6.1.0-001 06/15/05 Sachin	Initial Entry
Description:
This is a wrapper procedure used to stage a worksheet template.
--------------------------------------------------------------------------------*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_wl_stage_template';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_int_null			NUMBER(10,0)		:= NULL;
t_int_negative_one		NUMBER(10,0)		:= -1;

t_debug_flg			NUMBER(1)		:= -1;
t_date_null			DATE			:= NULL;
t_char_null			VARCHAR2(20)		:= NULL;
t_src_object_prefix_cd		VARCHAR2(5)		:= 'WLWT';
t_tar_object_prefix_cd		VARCHAR2(5)		:= 'WLWTW';

t_row_cnt			NUMBER(10);
t_deleted_flg	 		NUMBER(1);
t_out_dummy			NUMBER(10);

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


--Check to make sure the specified ID exists:--
n_sqlnum := 2000;
BEGIN
	SELECT deleted_flg
	INTO t_deleted_flg
	FROM maxdata.WLWT_worksheet_template
	WHERE worksheet_template_id=in_template_id;
EXCEPTION
WHEN NO_DATA_FOUND THEN
	v_sql :='Worksheet Template '||CAST(in_template_id AS VARCHAR2)||' does not exist in the permanent table.';
	RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

-- Check whether worksheet template has been logically deleted.
-- i.e. marked to be deleted by the nightly job.

n_sqlnum := 3000;
IF t_deleted_flg = 1 THEN
	v_sql :='Worksheet Template '||CAST(in_template_id AS VARCHAR2)||' cannot be staged. It has been marked for delete.';
	RAISE_APPLICATION_ERROR(-20001,v_sql);
END IF;

n_sqlnum := 4000;
IF in_cube_id IS NULL OR in_cube_id = -1 THEN
	RAISE_APPLICATION_ERROR(-20001,'Parameter in_cube_id cannot be NULL or -1.');
END IF;


--Clean-up any old rows from previous run:
n_sqlnum := 10000;
SELECT count(*) INTO t_row_cnt
FROM maxdata.WLWTW_worksheet_template
WHERE cube_id=in_cube_id
AND worksheet_template_id=in_template_id;
IF (t_row_cnt>0) THEN
BEGIN
	n_sqlnum := 11000;
	maxdata.p_wl_delete_template(in_cube_id,in_template_id,in_future1,in_future2,in_future3);
END;
END IF;

n_sqlnum := 12000;

maxdata.p_wl_copy_subtree ( 	in_cube_id,
				t_src_object_prefix_cd,		-- in_src_object_prefix_cd
				in_template_id,
				t_int_negative_one,		-- in_src_object_no
				t_tar_object_prefix_cd,		-- in_tar_object_prefix_cd
				t_int_negative_one,		-- in_tar_template_id
				t_char_null,			-- in_tar_new_object_nm
				t_date_null,			-- in_last_post_time
				t_int_null,			-- in_max_user_id
				t_int_null,			-- in_max_group_id
				t_debug_flg,			-- in_debug_flg
				in_future1,
				in_future2,
				in_future3,
				t_out_dummy		-- out_new_object_no
			);

n_sqlnum := 20000;
UPDATE maxdata.WLWTW_worksheet_template
SET posted_dttm = SYSDATE
WHERE cube_id=in_cube_id
AND worksheet_template_id=in_template_id;

COMMIT;

--Make sure that new CREATE/UPDATE times are later than STAGE/POST time, so wait one second:
--Not necessary in UDB, since TIMESTAMP precision is microseconds?
n_sqlnum := 21000;
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

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_WL_STAGE_TEMPLATE" TO "MADMAX";
