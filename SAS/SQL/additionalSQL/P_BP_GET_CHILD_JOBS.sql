--------------------------------------------------------
--  DDL for Procedure P_BP_GET_CHILD_JOBS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_BP_GET_CHILD_JOBS" (
	in_job_queue_id 	NUMBER,	-- job id for which dependencies are checked.
	in_debug_flg 		NUMBER,	-- debug flag for DB testing, passed from calling proc
	out_job_list OUT 	VARCHAR2
) AS
/* ----------------------------------------------------------------------------

Change History:

$Log: 2412_p_bp_get_child_jobs.sql,v $
Revision 1.7  2007/06/19 14:38:46  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.1  2006/09/13 18:38:00  saghai
FIXID : S0350587 New procedure to reset batch flag


---------------------------------------------------------------------------- */

n_sqlnum      		NUMBER(10,0)	:= 1000;
t_proc_name   		VARCHAR2(32)    := 'p_bp_get_child_jobs';
t_error_level 		VARCHAR2(6)     := 'info';
t_call        		VARCHAR2(1000);
v_sql         		VARCHAR2(2000)  := NULL;
t_sql2        		VARCHAR2(255);
t_sql3        		VARCHAR2(255);

t_child_queue_id 	NUMBER(10);
t_jobs_in_progress 	NUMBER(4);
t_job_list 		VARCHAR2(2000);
t_children_list 	VARCHAR2(2000);

-- retrieve all the child jobs.
CURSOR child_jobs_cur(p_job_queue_id NUMBER) IS
SELECT child_queue_id
FROM maxdata.bpjd_job_dependency bpjd, maxdata.bpjq_job_queue bpjq
WHERE bpjd.parent_queue_id = p_job_queue_id	-- parent_q_id is my id, not my parent's id (parent-child pair).
AND bpjq.job_queue_id = bpjd.parent_queue_id;

BEGIN

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_job_queue_id, -123)    || ',' ||
	COALESCE(in_debug_flg, -123)    || ',' ||
	'OUT out_job_list' ||
	' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
--COMMIT;

n_sqlnum := 2000;

-- Start from the my id first.

t_job_list := CAST(in_job_queue_id AS VARCHAR2);

t_children_list := ' ';

OPEN child_jobs_cur(in_job_queue_id);
LOOP
	FETCH child_jobs_cur INTO t_child_queue_id;
	EXIT WHEN child_jobs_cur%NOTFOUND;

	IF (in_debug_flg=1) THEN
		dbms_output.put_line(' t_child_queue_id ' || t_child_queue_id);
	END IF;

	-- Append the child id to the list.
	n_sqlnum := 4000;
	t_job_list := t_job_list ||',' || CAST(t_child_queue_id AS VARCHAR2);

	IF (t_children_list = ' ') THEN
		t_children_list := CAST(t_child_queue_id AS VARCHAR2);
	ELSE
		t_children_list := t_children_list ||',' || CAST(t_child_queue_id AS VARCHAR2);
	END IF;

	IF (in_debug_flg=1) THEN
		dbms_output.put_line(' t_job_list ' || t_job_list);
	END IF;

END LOOP;
CLOSE child_jobs_cur;

-- check if children are marked for in-progress.
--if you have some child jobs in progress, report error.
n_sqlnum := 6000;
v_sql := 	' SELECT COUNT(*) FROM maxdata.bpjq_job_queue '||
		' WHERE job_queue_id IN (' || t_job_list ||' ) AND job_status_cd != 0 ';

EXECUTE IMMEDIATE v_sql
INTO t_jobs_in_progress;

IF (t_jobs_in_progress > 0) THEN
	RAISE_APPLICATION_ERROR(-20002, 'One or more child jobs were already started or abnormally terminated');
END IF;

-- We support two levels of dependancy for now.
-- My children have any children, then report the problem.

IF t_children_list <> ' ' THEN
	n_sqlnum := 8000;
	v_sql := 	'SELECT COUNT(*) FROM maxdata.bpjq_job_queue '||
			' WHERE job_queue_id IN (' || t_children_list ||' ) AND job_status_cd != 0 ';
	EXECUTE IMMEDIATE v_sql
	INTO t_jobs_in_progress;

	IF (t_jobs_in_progress > 0) THEN
		RAISE_APPLICATION_ERROR(-20002, 'More than two levels of job dependancies are not supported yet');
	END IF;
END IF;

n_sqlnum := 10000;
out_job_list := t_job_list;


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

  GRANT EXECUTE ON "MAXDATA"."P_BP_GET_CHILD_JOBS" TO "MADMAX";
