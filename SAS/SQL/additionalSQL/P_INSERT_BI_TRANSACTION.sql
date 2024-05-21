--------------------------------------------------------
--  DDL for Procedure P_INSERT_BI_TRANSACTION
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_INSERT_BI_TRANSACTION" (
	in_transaction_id	NUMBER,
	in_worksheet_id	NUMBER,
	in_task_id		NUMBER,
	in_status_no	NUMBER,
	in_metadata_nm	VARCHAR2,
	in_fact_nm		VARCHAR2,
	in_dimension_nm	VARCHAR2,
	in_property_nm	VARCHAR2,
	in_future1		NUMBER,	--placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_future2		NUMBER,	--placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_future3		VARCHAR2,	--placeholder. Pass in -1. Use it only for EXTERNAL procedure
	in_debug_flg	NUMBER	--Zero=off, one=on.

) AS

/* ----------------------------------------------------------------------------

Change History:

$Log: 2426_p_insert_bi_transaction.sql,v $
Revision 1.3.2.1  2008/03/12 20:52:36  vejang
613 : Just change the files datetime

Revision 1.3  2008/03/10 19:15:31  dirapa
--MMMR66156, MMMR65824

Revision 1.2  2008/03/10 19:06:22  dirapa
No comment given.

Revision 1.1  2008/02/20 15:22:10  dirapa
No comment given.

Revision 1.1  2008/02/18 19:11:19  dirapa
No comment given.



Usage: Used by the appllication in BI Publish.

Description:

This procedure  inserts a row into maxdata.biwt_workflow_transaction table.

Insert option:
The flow for 6.1.3 release is, Application first creates the tables, inserts the data then calls this procedure to insert a record into maxdata.biwt_workflow_transaction table.
i. for insert option, parameters in_transaction_id,in_worksheet_id,in_task_id,in_status_flg must be supplied. Otherwise procedure will raise an error.
ii. for table names, if all 4 parameters are null then procedure will raise an error. Even if one table name is supplied then procedure assumes
	that this particular BI publish is associated with only one table and accepts the passed in values.
	So always application will make sure that correct values are passed to these table name parameters.
---------------------------------------------------------------------------- */

n_sqlnum        	NUMBER(10)	:= 1000;
t_proc_name     	VARCHAR2(30)    := 'p_insert_bi_transaction';
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
	maxdata.f_num_to_char(in_worksheet_id)     || ',' ||
	maxdata.f_num_to_char(in_task_id)     || ',' ||
	maxdata.f_num_to_char(in_status_no)     || ',' ||
	COALESCE(in_metadata_nm, 'NULL')         || ',' ||
	COALESCE(in_fact_nm, 'NULL')         || ',' ||
	COALESCE(in_dimension_nm, 'NULL')         || ',' ||
	COALESCE(in_property_nm, 'NULL')         || ',' ||
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

n_sqlnum := 3000;
IF  (in_worksheet_id IS NULL OR in_task_id IS NULL OR in_status_no IS NULL) THEN
BEGIN
	t_error_msg := 'One or more parameters are null.';
	RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
END IF;

n_sqlnum := 4000;
IF  (in_metadata_nm IS NULL AND in_fact_nm IS NULL AND in_dimension_nm IS NULL AND in_property_nm IS NULL ) THEN
BEGIN
	t_error_msg := 'At least one table name should be supplied.';
	RAISE_APPLICATION_ERROR (-20001,t_error_msg);
END;
END IF;

--- End of checking input parameters.

-- Insert record into maxdata.biwt_workflow_transaction

n_sqlnum := 5000;
INSERT INTO maxdata.BIWT_workflow_transaction
	(	transaction_id,
		worksheet_id,
		task_id,
		metadata_nm,
		fact_nm,
		dimension_nm,
		property_nm,
		status_no
	 )
VALUES (	in_transaction_id,
		in_worksheet_id,
		in_task_id,
		in_metadata_nm,
		in_fact_nm,
		in_dimension_nm,
		in_property_nm,
		in_status_no
	 );



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

  GRANT EXECUTE ON "MAXDATA"."P_INSERT_BI_TRANSACTION" TO "MADMAX";
