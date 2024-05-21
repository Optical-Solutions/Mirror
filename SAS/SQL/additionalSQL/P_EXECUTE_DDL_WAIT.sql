--------------------------------------------------------
--  DDL for Procedure P_EXECUTE_DDL_WAIT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_EXECUTE_DDL_WAIT" (
    in_sql_stmt      VARCHAR2,
    in_max_try_cnt   NUMBER:=1,
    in_loop_wait_sec NUMBER:=60
) AS
/*
------------------------------------------------------------------------------
$Log: 2100_IDA_p_execute_ddl_wait.sql,v $
Revision 1.1.2.1  2008/11/26 17:30:07  anchan
FIXID : BASELINE check-in

------------------------------------------------------------------------------
*/

    t_proc_name        	VARCHAR2(25) := 'p_execute_ddl_wait';
    t_call            	VARCHAR2(1000);
    t_sql2              VARCHAR2(255);
    t_sql3              VARCHAR2(255);

    t_try_no            NUMBER(10):=0;
    t_tryagain_flg      NUMBER(1):=1;

    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(1000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg	 VARCHAR2(1000) := NULL;

--ORA-00054: resource busy and acquire with NOWAIT specified.
--ORA-04022: nowait requested, but had to wait to lock dictionary.

BEGIN
n_sqlnum:=10000;
v_sql := in_sql_stmt;
t_call := SUBSTR(v_sql,1,80);
WHILE (t_try_no<in_max_try_cnt) AND (t_tryagain_flg=1)
LOOP
    BEGIN
    n_sqlnum:=11000;
    t_tryagain_flg:=0;
    t_try_no:=t_try_no+1;
    IF MOD(t_try_no,5)=0 THEN
        maxdata.p_log (t_proc_name, t_error_level, t_call,'TRY#:'||t_try_no, n_sqlnum);
    END IF;

    EXECUTE IMMEDIATE v_sql;
    EXCEPTION
        WHEN OTHERS THEN
        IF (SQLCODE IN(-00054,-04022))AND(t_try_no<in_max_try_cnt) THEN
            t_tryagain_flg:=1;
            dbms_lock.sleep(in_loop_wait_sec);
        ELSE
            RAISE;
        END IF;
    END;
END LOOP;

IF(t_try_no=in_max_try_cnt)THEN
    t_error_msg:='Timed out after repeated attempts: '||in_sql_stmt;
	RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

EXCEPTION
WHEN OTHERS THEN
	t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
	ROLLBACK;
	maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
	RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
