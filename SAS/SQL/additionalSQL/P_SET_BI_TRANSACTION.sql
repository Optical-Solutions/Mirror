--------------------------------------------------------
--  DDL for Procedure P_SET_BI_TRANSACTION
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_SET_BI_TRANSACTION" (
	in_transaction_id	NUMBER,
	in_status_no	NUMBER,
	in_future1		NUMBER,	--placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_future2		NUMBER,	--placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_future3		VARCHAR2,	--placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_debug_flg	NUMBER	--Zero=off, one=on.

) AS

/* ----------------------------------------------------------------------------

Change History:

$Log: 2428_p_set_bi_transaction.sql,v $
Revision 1.5.2.1  2008/03/12 20:52:38  vejang
613 : Just change the files datetime

Revision 1.5  2008/03/10 19:28:06  dirapa
--MMMR66156, MMMR65824

Revision 1.4  2008/02/21 18:33:35  dirapa
-- BI enhancement





Usage: Used by the appllication in BI Publish.

Description:

this procedure update the existing status value based on passed in transaction_id parameter.

Update option :
i. for update option, parameters in_transaction_id,in_status_no. Otherwise procedure will raise an error.
ii. The passed in value for in_status_flg should exist in t_global_lookkup_code table for 'JOB_STATUS' type, otherwise procedure will raise error.

Note: Temporary BI tables might be accessed by legacy system as read only. Legacy systems
	 will also call this procedure to set the status appropriately.


---------------------------------------------------------------------------- */

n_sqlnum        	NUMBER(10)	:= 1000;
t_proc_name     	VARCHAR2(30)    := 'p_set_bi_transaction';
t_call          	VARCHAR2(1000);
v_sql           	VARCHAR2(4000)  := NULL;
t_error_level   	VARCHAR2(6)     := 'info';
t_error_msg    	VARCHAR2(1000);

-- Remove the variables below if not used
t_sql1                  VARCHAR2(255)  := NULL;
t_sql2                  VARCHAR2(255)  := NULL;
t_sql3                  VARCHAR2(255)  := NULL;
t_int_null              NUMBER(1)      := NULL;


BEGIN

-- Log the parameters of the procedure

t_call := t_proc_name                       || ' ( ' ||
	maxdata.f_num_to_char(in_transaction_id)     || ',' ||
	maxdata.f_num_to_char(in_status_no)     || ',' ||
	maxdata.f_num_to_char(in_future1)   || ',' ||
	maxdata.f_num_to_char(in_future2)   || ',' ||
	COALESCE(in_future3, 'NULL')   	    || ',' ||
	maxdata.f_num_to_char(in_debug_flg) || ',' ||
	' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
COMMIT;


-- Check for correct input params.

n_sqlnum := 2000;
IF  in_transaction_id IS NULL THEN
BEGIN
	t_error_msg := 'Transaction ID can not be null.';
	RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
END IF;

--- End of checking input parameters.

n_sqlnum := 3000;

UPDATE maxdata.BIWT_workflow_transaction
SET 	status_no = in_status_no,
	update_dttm = SYSDATE
WHERE transaction_id = in_transaction_id;

IF SQL%ROWCOUNT = 0 THEN
BEGIN
	t_error_msg := 'Invalid transaction ID. No rows were updated.';
	RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
END IF;


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

  GRANT EXECUTE ON "MAXDATA"."P_SET_BI_TRANSACTION" TO "MADMAX";
