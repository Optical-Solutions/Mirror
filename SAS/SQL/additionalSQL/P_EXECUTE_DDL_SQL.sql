--------------------------------------------------------
--  DDL for Procedure P_EXECUTE_DDL_SQL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_EXECUTE_DDL_SQL" (
	in_sql_stmt VARCHAR2,
	in_ignore_error  NUMBER,	-- Default -1. Application passes -1 for this.
	in_future_2 NUMBER,		-- Default -1
	in_future_3 VARCHAR2,		-- Default null
	in_future_4 VARCHAR2		-- Default null
) AS
/*------------------------------------------------------------------------
$Id: 2109_p_execute_ddl_sql.sql,v 1.5 2007/06/19 14:39:57 clapper Exp $

$Log: 2109_p_execute_ddl_sql.sql,v $
Revision 1.5  2007/06/19 14:39:57  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.1  2005/08/04 19:27:02  joscho
2214_p_execute_ddl_sql.sql renamed to 2109_p_execute_ddl_sql.sql for dependancy.
Added for rename from 2214_p_execute_ddl_sql.sql.
See originally named file for history prior to the rename.

Revision 1.5  2005/08/03 20:44:49  dirapa
6.1.0-001 08/03/05 Diwakar  Increased v_sql variable length to 4000

Revision 1.4  2005/08/03 17:18:59  dirapa
No comment given.

Revision 1.3  2005/08/03 17:10:11  dirapa
-- 6.1.0-001 08/03/05 Diwakar	Raise error if procedure called for any DML operation.


-- Change history.

-- V5.6.0-029_12 05/26/04 Sachin	# 16865 Added commit since DML statements are also being passed to this proc.
-- V5.6.0-029
-- The following changes were ported from 5.3.14
-- V5.3.14
-- 02/27/04		Sachin		Added functionality to ignore error during execution of in_sql_stmt
-- V5.3.4 (backported from 5.5)
-- 05/05/03		Sachin		Added logging info.
-- V 5.5
-- 5.5.0-all	11/26/02	Sachin	Initial Entry
-- Description:
-- This procedure will execute the SQL stmt passed in.
-- To be used for executing SQL stmts sent by the application.
-- For UDB also called from p_open_tmpl and p_close_tmpl
------------------------------------------------------------------------*/

t_proc_name		VARCHAR2(25)		:= 'p_execute_ddl_sql';
t_error_level		VARCHAR2(6)		:= 'info';
v_sql			VARCHAR2(4000);
t_call			VARCHAR2(1000);
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);
n_sqlnum		NUMBER(10);


BEGIN

	v_sql := in_sql_stmt;

	-- check if this procedure is called for any DML operation.
	-- if so raise error.

	n_sqlnum := 1000;

	IF UPPER(SUBSTR(LTRIM(v_sql),1,6)) IN ('SELECT','INSERT','UPDATE','DELETE') THEN
	BEGIN
		RAISE_APPLICATION_ERROR(-20001,'Procedure p_execute_ddl_sql is not intended for any DML operations ... ');
	END;
	END IF;

    	IF in_ignore_error =1 THEN
	BEGIN
	  	n_sqlnum := 2000;
	  	EXECUTE IMMEDIATE v_sql;

	EXCEPTION WHEN OTHERS
		THEN NULL;
	END;
	ELSE
	BEGIN
	  	n_sqlnum := 3000;
	  	EXECUTE IMMEDIATE v_sql;
	END;
	END IF;

	COMMIT;

EXCEPTION
     WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := substr(v_sql,1,255);
			t_sql3 := substr(v_sql,256,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		maxdata.ins_import_log (t_proc_name, t_error_level, v_sql, NULL, n_sqlnum, NULL);
		COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/
