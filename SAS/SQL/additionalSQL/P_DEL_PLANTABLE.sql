--------------------------------------------------------
--  DDL for Procedure P_DEL_PLANTABLE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DEL_PLANTABLE" (
	i_plantable_lev 	NUMBER,
	i_plantable_id 		NUMBER,
	o_errcode 	OUT	NUMBER,
	o_errmsg 	OUT	VARCHAR2
	)
AS

/*---------------------------------------------------
$Log: 2156_p_del_plantable.sql,v $
Revision 1.9.8.2  2008/09/03 01:26:30  saghai
612-HF13(HBC) change Added Logging Parameters.

Revision 1.9.8.1  2008/06/05 20:27:01  makirk
Fix for S0511130

Revision 1.9  2007/06/19 14:40:14  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.5.6.1  2007/06/05 15:32:50  vejang
Moved from 6121 to 612HF4

Revision 1.5.4.1  2007/04/11 16:07:44  amkatr
For S0416066

Revision 1.5  2006/11/17 20:20:10  dirapa
FIXID S0391204. moved maxdata.tr_wksht_aft_d trigger code to set deleted_flg=1 for wlwt_worksheet_template table

Revision 1.4  2006/02/17 22:18:54  healja
Replace $id with $Log
 2156_p_del_plantable.sql,v 1.3 2005/07/27 15:13:08 joscho Exp $

-- Change history:
--V6.1
--V6.1.0-001 07/15/05	Diwakar	Added future parameter variable for p_drop_pw_cl_hist call.
--V5.6.1
-- 5.6.1-062 01/12/05	Sachin	Enh#2193,2200,2345: Support time-out, batch, cl hist copy, and whatif changes.
--V5.4.0
-- 5.4.0-007 08/28/02 	Sachin 	Moved deleting of plantable record after p_drop_pw_cl_hist.
-- V5.3
-- 02/18/02 		Joseph 	Drop Cluster history for planversion, etc.

-- V5.2.4.3
-- 02/12/02 		Joseph 	Drop Persistent Cluster History tables.
-- 05/17/01 		Joseph 	Remove t_cnt code. It is to delete plans even with submitted worksheet.

Usage : Both External

Description:

This procedure deletes PLANGROUP, PLANMASTER, PLANVERSION, or PLANWORKSHEET entry.

Parameters:

i_plantable_lev 	Plan table level (91 through 94)
i_plantable_id 		ID of the entry to delete
o_errcode 		0:success, 1:informational, 2:warning, 3:error, other numbers: server error
o_errmsg 		error text
-----------------------------------------------------*/

n_sqlnum 	        NUMBER(10,0)		:=1000;
t_proc_name		VARCHAR2(32) 		:= 'p_del_plantable';
t_error_level      	VARCHAR2(6) 		:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(4000) 		:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);

t_tablename 		VARCHAR2(255);
t_exist			NUMBER;
t_plantable_id		NUMBER;
t_temp_id		NUMBER;
t_future_param_int	NUMBER := -1;


BEGIN

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
		COALESCE(i_plantable_lev, -1) || ',' ||
		COALESCE(i_plantable_id, -1) || ',' ||
		'OUT o_errcode, OUT o_errmsg '||
		' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
COMMIT;


-- Initialize the error code/msg.
-- 0:success, 1:informational, 2:warning, 3:error

o_errcode := 0;
o_errmsg  := '';


-- Set i_plantable_id to local variable t_plantable_id
t_plantable_id := i_plantable_id;

-- Clean up tmp table.

v_sql := 'truncate table maxdata.t_dp_wksht';
EXECUTE IMMEDIATE v_sql;


-- Check and fetch the right level_type and entity_type for the plantable
-- from sequence table.

BEGIN
	SELECT seq_name INTO t_tablename
	  FROM maxapp.sequence
	 WHERE entity_type = 31
	   AND level_type  = i_plantable_lev;

EXCEPTION
	WHEN OTHERS THEN
		o_errcode := 3;
		o_errmsg := 'Passed in table level ' || i_plantable_lev || ' is wrong.';
		ROLLBACK;
		RETURN;
END;


-- Check if any descendant planworksheet was already submitted.

IF t_tablename = 'PLANGROUP' THEN
	SELECT COUNT(*) INTO t_exist
	  FROM plangroup
	 WHERE plangroup_id = t_plantable_id;
ELSIF t_tablename = 'PLANMASTER' THEN
	SELECT COUNT(*) INTO t_exist
	  FROM planmaster
	 WHERE planmaster_id = t_plantable_id;
ELSIF t_tablename = 'PLANVERSION' then
	SELECT COUNT(*) INTO t_exist
	  FROM planversion
	 WHERE planversion_id = t_plantable_id;
ELSIF t_tablename = 'PLANWORKSHEET' THEN
	SELECT COUNT(*) INTO t_exist
	  FROM planworksheet
	 WHERE planworksheet_id = t_plantable_id;
ELSE	o_errcode := 3;
	o_errmsg := 'Passed in table level ' || t_plantable_id || ' is wrong.';
	ROLLBACK;
	RETURN;
END IF;

-- Check if the row exists.

IF t_exist <> 1 THEN
	o_errcode := 3;
	o_errmsg  := 'Passed in ' || t_tablename || ' id ' || t_plantable_id || ' not exists.';
	ROLLBACK;
	RETURN;
END IF;



-- Now, go ahead and delete the specified row.
-- NOTE: Children are deleted by ON DELETE CASCADE on the foreign key definitions.

IF t_tablename = 'PLANGROUP' THEN
	-- Collect planworksheet ids in order to
	-- drop cluster histories for them.

	INSERT INTO maxdata.t_dp_wksht
	SELECT planworksheet_id
	  FROM planworksheet
	 WHERE plangroup_id = t_plantable_id;
ELSIF t_tablename = 'PLANMASTER' THEN
BEGIN
	n_sqlnum := 7000;
	INSERT INTO maxdata.t_dp_wksht
	SELECT planworksheet_id
	  FROM planworksheet
	 WHERE planmaster_id = t_plantable_id;

	n_sqlnum := 7500;
	SELECT time_template_id INTO t_temp_id
	  FROM planmaster
	 WHERE planmaster_id = t_plantable_id;

	IF t_temp_id IS NOT NULL AND t_temp_id <> 0 AND t_temp_id <> -1 THEN
	BEGIN
		n_sqlnum := 7800;
		SELECT COUNT(*) INTO t_exist
		  FROM dimset_template
		 WHERE template_id = t_temp_id;

		IF t_exist > 0 THEN
		    n_sqlnum := 7900;
		    DELETE FROM maxdata.dimset_template
		    WHERE template_id = t_temp_id;
		END IF;

		END; --IF t_temp_id
	END IF;
END; --ELSE IF PLANMASTER

ELSIF t_tablename = 'PLANVERSION' THEN
BEGIN
	INSERT INTO maxdata.t_dp_wksht
	SELECT planworksheet_id
	FROM planworksheet
	WHERE planversion_id = t_plantable_id;

	-- Delete all fp_exception records
	DELETE FROM maxdata.fp_exception
	WHERE globalplan_id = t_plantable_id;

	-- Delete the mmax_locks entry
	DELETE FROM maxdata.mmax_locks
	WHERE lock_id = t_plantable_id;
END;
ELSIF t_tablename = 'PLANWORKSHEET' THEN
	INSERT INTO maxdata.t_dp_wksht
	VALUES (t_plantable_id);

	-- Fix for S0511130
	-- Must delete from workflow before deleting from planworksheet to prevent FK error
	DELETE FROM maxdata.biwt_workflow_transaction
	 WHERE worksheet_id = t_plantable_id;
END IF;

-- Drop Persistent Cluster History tables.

DECLARE CURSOR pw_id_cur IS
	SELECT planworksheet_id FROM maxdata.t_dp_wksht;
BEGIN
FOR c1 IN pw_id_cur LOOP

	/*
	The associated row should be deleted when a row is deleted from planworksheet table.
	However, for performance reasons, only the flag is set, and the actual deletion will be
	done by a nightly batch job.
	*/

	UPDATE maxdata.WLWT_worksheet_template
	   SET deleted_flg = 1
	 WHERE worksheet_template_id IN (SELECT worksheet_template_id
					   FROM maxdata.planworksheet
					  WHERE planworksheet_id = c1.planworksheet_id);

	-- Drop Persistent Cluster History tables.

	--dbms_output.put_line(c1.planworksheet_id);
	maxdata.p_drop_pw_cl_hist (c1.planworksheet_id, t_future_param_int, t_future_param_int);
END LOOP;
END;

-- Delete plantable rows
-- Delete this after p_drop_pw_cl_hist because
-- p_drop_pw_cl_hist requires planworksheet rows in order
-- to find out bv_id for timeshift (see p_get_dv_time_id)

v_sql :=' DELETE from '||t_tablename||' WHERE '||t_tablename||'_id = :t_plantable_id';

EXECUTE IMMEDIATE v_sql USING t_plantable_id;

-- Clean up tmp table.

v_sql := 'truncate table maxdata.t_dp_wksht';
EXECUTE IMMEDIATE v_sql;

COMMIT;

EXCEPTION
	WHEN OTHERS THEN
	    	o_errmsg := 'Error from p_del_plantable: ' || SQLERRM;
	    	o_errcode := SQLCODE;

		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := substr(v_sql,1,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := o_errmsg || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := substr(v_sql,1,255);
		t_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);

		RETURN;  -- return gracefully.  The app will display the detailed error message.

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_DEL_PLANTABLE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_DEL_PLANTABLE" TO "MAXUSER";
