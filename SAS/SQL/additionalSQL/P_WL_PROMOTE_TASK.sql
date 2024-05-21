--------------------------------------------------------
--  DDL for Procedure P_WL_PROMOTE_TASK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_PROMOTE_TASK" (
	in_task_no	NUMBER,
	in_max_user_id	NUMBER,
	in_max_group_id	NUMBER,
	in_future1	NUMBER,		-- placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_future2	NUMBER,		-- placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_future3	NUMBER,		-- placeholder. Pass in -1. Use it only for EXTERNAL procedure
	out_template_id	OUT NUMBER	-- placeholder. output param
) AS
/*----------------------------------------------------------------------------
$Log: 2366_p_wl_promote_task.sql,v $
Revision 1.7  2007/06/19 14:38:51  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2006/03/28 19:07:48  anchan
Copy  WLTD rows also.

Revision 1.2  2006/03/27 16:48:36  anchan
Added 2 new parameters

Revision 1.1  2006/01/20 16:29:16  anchan
No comment given.

Revision 1.1  2006/01/04 16:10:17  anchan
Created.


==============================================================================
DESCRIPTION:
	"Promotes" a task by creating a new <worksheet template>
	and copying the specified MODEL task the new template.

	Intended for use only with migrated worksheets.
--------------------------------------------------------------------------------*/



n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 		:= 'p_wl_promote_task';
t_error_level      	VARCHAR2(6) 		:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(1000) 		:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);
t_str_null		VARCHAR2(255)		:= NULL;
t_int_null		NUMBER(10)		:= NULL;

t_new_task_no		NUMBER(10);
t_new_template_id	NUMBER(10);
t_error_msg      	VARCHAR2(255);
t_cnt			NUMBER(10,0);

t_default_template_id	NUMBER(10);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_task_no,-1) || ',' ||		-- COALESCE(int, 'NULL') returns error because of diff datatype.
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
	COALESCE(in_future3, -1) || ',' ||
	'OUT' || 'out_template_id' ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;

--Retrieve the  next available template_id:
maxdata.p_wl_next_object_no (
	-1,	-- Pass in -1 for PERMANENT objects
	-1,	-- first column of (composite) primary key.
	-1,  	-- second column, if composite primary key.
	1,	-- Table prefix type of the source object.
	1,	-- number of "slots" to reserve
	-1,	-- placeholder. Pass in -1.
	-1,	-- placeholder. Pass in -1.
	-1,	-- placeholder. Pass in -1.
	t_new_template_id
);

--Do not copy the entire template object; just create a row in the root of the subtree:
INSERT INTO maxdata.wlwt_worksheet_template(
worksheet_template_id,
template_nm,row_page_flg,
column_page_flg,
page_row_cnt,
page_column_cnt,
alternate_calendar_flg,
cluster_hierarchy_flg
)
VALUES
   (t_new_template_id,'Migrated Template[]', 0, 0, 0, 0, 0, 0);


maxdata.p_wl_copy_subtree (
	-1,  	-- required if source or target object is in WORKING tables;
						 	-- else pass -1 if both of them are in PERMANENT tables.
	'WLW1',	-- Table prefix of the source object.
	0, 	-- of the source object.
	in_task_no, -- of the source object. (-1 for NULL)
	'WLW1', -- Table prefix of the target object.
	t_new_template_id, 	-- 0 'PMMODEL', NOT NULL 'PMACTIVE',(Only for Save As: -1 'WKACTIVE')
	NULL, 	-- New unique name of the target object.
	NULL, 		-- Required for Posting only. Else pass in NULL.
	in_max_user_id,	-- Max User id
	in_max_group_id,	-- Max Group id
	-1,		-- Internal. Only for debugging. App. passes -1.
	-1,		-- placeholder. Pass in -1.
	-1,		-- placeholder. Pass in -1.
	-1,		-- placeholder. Pass in -1.
	t_new_task_no  	-- the newly created object.
);


SELECT value_1
INTO t_default_template_id
FROM maxapp.userpref
WHERE max_user_id=-1
AND key_1='DEFAULT_TEMPLATE';

INSERT INTO maxdata.wltd_template_dataversion
       (worksheet_template_id,kpi_dv_id,aggregate_flg)
SELECT t_new_template_id,kpi_dv_id,aggregate_flg
FROM maxdata.wltd_template_dataversion
WHERE worksheet_template_id=t_default_template_id;

COMMIT;

out_template_id := t_new_template_id;

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
