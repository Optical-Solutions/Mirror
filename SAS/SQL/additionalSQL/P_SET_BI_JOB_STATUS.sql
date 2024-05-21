--------------------------------------------------------
--  DDL for Procedure P_SET_BI_JOB_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_SET_BI_JOB_STATUS" (
	in_job_queue_id		NUMBER,
	in_future1		NUMBER,		--placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_future2		NUMBER,		--placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_future3		VARCHAR2,	--placeholder. Pass in -1. Use it only for EXTERNAL procedure
	out_child_queue_id OUT 	NUMBER,
	out_job_type_cd    OUT	NUMBER
) AS

/* ----------------------------------------------------------------------------

Change History:

$Log: 2430_p_set_bi_job_status.sql,v $
Revision 1.2.2.4  2008/10/13 19:12:38  saghai
S0540146 - fix to return the leaf job even if the id is smaller than its parent id

Revision 1.2.2.3  2008/06/20 14:03:46  dirapa
Return -101 when no data found in maxdata.bpjq_job_queue table.

Revision 1.2.2.2  2008/04/01 16:35:11  dirapa
removed in_debug_flg parameter

Revision 1.2.2.1  2008/03/12 20:52:39  vejang
613 : Just change the files datetime

Revision 1.2  2008/03/10 19:46:06  dirapa
--MMMR66156, MMMR65824

Revision 1.1  2008/03/06 18:32:03  dirapa
No comment given.

Revision 1.4  2008/02/21 18:33:35  dirapa
-- BI enhancement


Usage: Used by the appllication in BI Publish.

Description:

this procedure returns child queue id  for a maximum level and maximum child_queue_id in that level.
also return job type code for the returned child queue id value.



---------------------------------------------------------------------------- */

n_sqlnum        	NUMBER(10)	:= 1000;
t_proc_name     	VARCHAR2(30)    := 'p_set_bi_job_status';
t_call          	VARCHAR2(1000);
v_sql           	VARCHAR2(4000)  := NULL;
t_error_level   	VARCHAR2(6)     := 'info';
t_error_msg    		VARCHAR2(1000);

t_sql1                  VARCHAR2(255)  := NULL;
t_sql2                  VARCHAR2(255)  := NULL;
t_sql3                  VARCHAR2(255)  := NULL;
t_int_null              NUMBER(1)      := NULL;

BEGIN

-- Log the parameters of the procedure

t_call := t_proc_name                       || ' ( ' ||
	maxdata.f_num_to_char(in_job_queue_id)     || ',' ||
	maxdata.f_num_to_char(in_future1)   || ',' ||
	maxdata.f_num_to_char(in_future2)   || ',' ||
	COALESCE(in_future3, 'NULL')   	    || ',' ||
	' OUT ' || ',' ||
	' OUT ' || ',' ||
	' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
COMMIT;


-- Check for correct input params.

n_sqlnum := 2000;
IF  in_job_queue_id IS NULL THEN
BEGIN
	t_error_msg := 'JOB QUEUE ID can not be null.';
	RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
END IF;

--- End of checking input parameters.

n_sqlnum := 3000;

-- This query is Oracle Specific. UDB and SS will have different syntax.
-- CONNECT_BY_ISLEAF is a Oracle psuedo column.
-- Its value is 1 when it is the leaf node else its value is 0.

SELECT  MAX(child_queue_id)
INTO out_child_queue_id
FROM (	SELECT child_queue_id,CONNECT_BY_ISLEAF leaf_node
	FROM maxdata.BPJD_job_dependency
	CONNECT BY PRIOR child_queue_id = parent_queue_id
	START WITH parent_queue_id= in_job_queue_id
     )
WHERE leaf_node = 1;

n_sqlnum := 4000;

BEGIN
	SELECT job_type_cd
	INTO out_job_type_cd
	FROM maxdata.BPJQ_job_queue
	WHERE job_queue_id = out_child_queue_id;
EXCEPTION
WHEN NO_DATA_FOUND THEN
	out_child_queue_id := -101; /* Application is expecting -101 value. so do not change this value */
END;

COMMIT;

EXCEPTION
WHEN OTHERS THEN
	 ROLLBACK;

	IF t_sql1 IS NOT NULL THEN
	    t_error_level := 'info';
	    t_sql2 := 'Most recent dynamic SQL.  Not necessarily related with the current error';
	    t_sql3 := SUBSTR(t_sql1,1,255);
	    maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
	END IF;

	-- Log the error message. t_call may be quite long, so don't log it here.
	t_error_level := 'error';
	t_sql1 := SQLERRM || '(' || '...' ||', SQL#:' || n_sqlnum || ')';

	t_sql2 := SUBSTR(t_sql1,1,255);
	t_sql3 := SUBSTR(t_sql1,256,255);
	maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
	--COMMIT;

        RAISE_APPLICATION_ERROR(-20001,t_sql1);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_SET_BI_JOB_STATUS" TO "MADMAX";
