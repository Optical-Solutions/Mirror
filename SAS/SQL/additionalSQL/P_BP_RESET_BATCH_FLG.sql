--------------------------------------------------------
--  DDL for Procedure P_BP_RESET_BATCH_FLG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_BP_RESET_BATCH_FLG" (
	in_param_datatype 	NUMBER, 	-- indicates data type of the parameter, need to include the right column
	in_param_name 		VARCHAR2, 	-- parameter name, such as PLANVERSION_ID
	in_param_value 		VARCHAR2, 	-- parameter value,such as plan version id value
	in_debug_flg 		NUMBER, 	-- debug flag for DB testing purpose, App passes 0
	in_future1		NUMBER,		-- placeholder. Pass in -1.
	in_future2		NUMBER		-- placeholder. Pass in -1.

) AS
/* ----------------------------------------------------------------------------

Change History:

$Log: 2414_p_bp_reset_batch_flg.sql,v $
Revision 1.7  2007/06/19 14:38:46  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.1  2006/09/13 18:37:58  saghai
S0350587 New procedure to reset batch flag



Description:
The procedure prepares the version/worksheet for reset from batch mode.
The caller will take care of updating planversion, planworksheet tables
procedure focuses on cleaning up queue related entries scheduled against
the version/ worksheet.

Assumptions:
1. Each version or worksheet can have only one active job entry in the queue.
The job_status_cd for such one is either 0(not yet started) or 1(in-progress)
2. If a job is completed the batch processor has to set the flag back. This
procedure does not touch that category.
3. The procedure assumes only 2 level dependency for jobs in maxdata.bpjd_job_dependency table.
4. It assumes that batch processor entries in the queue tables are all relavent and
does not validate them against the master tables.

This procedure is dependent on the following tables
1. maxdata.bpjd_job_dependency
2. maxdata.bpjp_job_parameter
3. maxdata.bpjq_job_queue

job_status_cd: 0 - not yet started, 1 - in-progress, 2 - completed, 3 - error
parent_status_cd: 0 - not yet started (or IP), 1 - completed

and uses a function maxdata.f_get_dependency_list
Any structural changes to the above may impact the procedure.

---------------------------------------------------------------------------- */

n_sqlnum      		NUMBER(10,0)	:= 1000;
t_proc_name   		VARCHAR2(32)    := 'p_bp_reset_batch_flg';
t_error_level 		VARCHAR2(6)     := 'info';
t_call        		VARCHAR2(1000);
v_sql         		VARCHAR2(2000)  := NULL;
t_sql2        		VARCHAR2(255);
t_sql3        		VARCHAR2(255);

t_job_queue_id 		NUMBER(10);
t_job_status_cd 	NUMBER(1);
t_child_job_list	VARCHAR2(2000);

-- The following constants are passed by app as they are marked here
-- Do not change them
c_param_datatype_varchar 	NUMBER(1)	:= 1;
c_param_datatype_number 	NUMBER(1)	:= 2;
c_param_datatype_date 		NUMBER(1)	:= 6;

c_type_planversion_id 		VARCHAR2(30) 	:= 'PLANVERSION_ID';
c_type_planworksheet_id 	VARCHAR2(30) 	:= 'PLANWORKSHEET_ID';

-- end of app based constants


BEGIN

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_param_datatype, -123)    || ',' ||
	COALESCE(in_param_name, 'NULL') || ',' ||
	COALESCE(in_param_value, 'NULL') || ',' ||
	COALESCE(in_debug_flg, -123)    ||  ',' ||
	COALESCE(in_future1, -123)    ||  ',' ||
	COALESCE(in_future2, -123)    ||
	' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
--COMMIT;

IF  (in_debug_flg=1) THEN
	dbms_output.enable(100000);
END IF;

n_sqlnum := 2000;

IF (in_param_datatype<>c_param_datatype_number) THEN
	RAISE_APPLICATION_ERROR(-20001, 'Not implemented for this type');
END IF;

-- check for any 0, 1 status jobs in queue for which parameter is the passed in value
-- There may be many status=2 (completed) or 3 (error) entries depending on the number of times scheduled.
-- We need only 0 or 1 because we are working on switching the status.

n_sqlnum := 3000;

v_sql:= ' SELECT bpjq.job_queue_id, bpjq.job_status_cd '||
	' FROM maxdata.bpjp_job_parameter bpjp, maxdata.bpjq_job_queue bpjq '||
	' WHERE bpjp.param_name = '''|| in_param_name || ''' '||
	' AND bpjp.numeric_parameter = ' || in_param_value ||
	' AND bpjq.job_queue_id = bpjp.job_queue_id '||
	' AND bpjq.job_status_cd in (0, 1)';

-- Find the job in the queue.

BEGIN
	EXECUTE IMMEDIATE v_sql
	INTO t_job_queue_id, t_job_status_cd;
EXCEPTION
	WHEN TOO_MANY_ROWS THEN
		RAISE_APPLICATION_ERROR(-20001, 'More than one batch job scheduled for the same entity');
	WHEN NO_DATA_FOUND THEN
		RETURN;
END;

-- if the primary queue is in progress, throw error.
n_sqlnum := 5000;
IF (t_job_status_cd = 1) THEN
	RAISE_APPLICATION_ERROR(-20001, '3040116'); -- Using 'Batch Job in Progress'
END IF;

-- If status =0, the job was scheduled, but did not start.
n_sqlnum := 6000;
IF ( t_job_status_cd = 0) THEN
BEGIN
	-- Get the dependency list from dependency table
	-- The child jobs ideally should not have been marked for progress
	-- but should be verified as part of cleanup.

	n_sqlnum := 8000;
	maxdata.p_bp_get_child_jobs(t_job_queue_id, in_debug_flg, t_child_job_list);

	-- Delete the parent-child relations that belong to my job.

	n_sqlnum := 9000;
	v_sql :=  	' DELETE FROM maxdata.bpjd_job_dependency '||
			' WHERE parent_queue_id = ' || CAST(t_job_queue_id AS VARCHAR2);

	EXECUTE IMMEDIATE v_sql;

	IF (in_debug_flg=1) THEN
		dbms_output.put_line(' v_sql ' || v_sql);
	END IF;

	-- Delete all the param entries that belong to me and my children.
	n_sqlnum := 10000;
	v_sql := 	' DELETE FROM maxdata.bpjp_job_parameter '||
			' WHERE job_queue_id IN (' || t_child_job_list || ')';

	EXECUTE IMMEDIATE v_sql;

	IF (in_debug_flg=1) THEN
		dbms_output.put_line(' v_sql ' || v_sql);
	END IF;

	n_sqlnum := 11000;
	v_sql := 	' DELETE FROM maxdata.bpjq_job_queue '||
			' WHERE job_queue_id IN (' || t_child_job_list || ')';
	EXECUTE IMMEDIATE v_sql;

	IF (in_debug_flg=1) THEN
		dbms_output.put_line(' v_sql ' || v_sql);
	END IF;
END;
END IF;

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
		--COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_BP_RESET_BATCH_FLG" TO "MADMAX";
