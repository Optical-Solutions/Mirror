--------------------------------------------------------
--  DDL for Procedure P_CREATE_BI_BRIDGE_TBL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_CREATE_BI_BRIDGE_TBL" 
    (in_tbl_name       IN  VARCHAR2, -- The unqualified name of the table to be created (i.e. "T_PC_WORKSHEETS")
     in_tbl_def1       IN  VARCHAR2, -- The first 4000 characters of the table field definitions (i.e. "WORKSHEET_ID NUMBER,...")
     in_tbl_def2       IN  VARCHAR2, -- The next 4000 characters of the table field definitions
     in_tbl_def3       IN  VARCHAR2, -- The last 4000 characters of the table field definitions
     in_future1        IN  NUMBER,   -- Placeholder.  Pass in -1.
     in_future2        IN  NUMBER,   -- Placeholder.  Pass in -1.
     in_future3        IN  VARCHAR2, -- Placeholder.  Pass in NULL.
     out_full_tbl_name OUT VARCHAR2  -- Returns the full name of the table created (i.e. "MAXTEMP.T_PC_WORKSHEETS")
     ) AS


/*********************************************************************************************
 Change History:

 $Log: 2370_p_create_bi_bridge_tbl.sql,v $
 Revision 1.12  2007/06/19 14:38:50  clapper
 FIXID AUTOPUSH: SOS 1238247

 Revision 1.8  2006/09/20 21:04:06  makirk
 Changed calls from p_ins_long_import_log to p_log

 Revision 1.7  2006/09/19 17:01:46  joscho
 Fixed logging of long syntax

 Revision 1.6  2006/01/12 14:40:29  makirk
 Added "T_BI_" as a prefix to the table name parameter

 Revision 1.5  2006/01/06 15:26:56  makirk
 Changed variable names for consistency

 Revision 1.4  2006/01/06 14:40:35  makirk
 Added grant process and renamed variables

 Revision 1.3  2006/01/05 19:27:51  makirk
 Moved substr function from being directly passed as a param to a passed variable for portability reasons

 Revision 1.2  2006/01/05 16:20:03  makirk
 Added drop (ignore_errors on) table before create table

 Revision 1.1  2006/01/05 15:14:45  makirk
 To support bi bridge


 Usage: External only (used by the app)

 Description:
    This procedure acts as a "wrapper" for the application to create tables in maxtemp
    via the maxtemp.p_exec_temp_ddl procedure.
 *********************************************************************************************/

n_sqlnum                NUMBER(10,0)   := 0;
t_proc_name             VARCHAR2(32)   := 'p_create_bi_bridge_tbl';
v_tgt_schema            VARCHAR2(32)   := 'maxtemp'; -- The target schema
t_error_level           VARCHAR2(6)    := 'info';
t_call                  VARCHAR2(4000) := NULL;
t_ignore_error          NUMBER(1)      := 0;    -- 0 to raise an exception, 1 to ignore when raised

t_sql1                  VARCHAR2(255)  := NULL;
t_sql2                  VARCHAR2(255)  := NULL;
t_sql3                  VARCHAR2(255)  := NULL;
t_char_null             CHAR(1)        := NULL;
t_int_null              NUMBER(1)      := NULL;
t_future_int            NUMBER(2)      := -1;
t_err_msg               VARCHAR2(255)  := NULL;

BEGIN

n_sqlnum := 1000;

-- Log the parameters of the procedure
-- Log table definition parameters separately because they may be quite long.
t_call :=             t_proc_name || '(' ||
     COALESCE(in_tbl_name,'NULL') || ',' ||
    'tbl_def1: separately logged' || ',' ||
    'tbl_def2: separately logged' || ',' ||
    'tbl_def3: separately logged' || ',' ||
     COALESCE(in_future1, -123)   || ',' ||  -- COALESCE(int, 'NULL') returns error because of diff datatype.
     COALESCE(in_future2, -123)   || ',' ||
     COALESCE(in_future3, 'NULL') || ',' ||
                            'OUT' || ')';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, t_sql1, n_sqlnum, t_int_null);
maxdata.p_log (t_proc_name,t_error_level,in_tbl_def1,t_char_null,n_sqlnum);
maxdata.p_log (t_proc_name,t_error_level,in_tbl_def2,t_char_null,n_sqlnum);
maxdata.p_log (t_proc_name,t_error_level,in_tbl_def3,t_char_null,n_sqlnum);


n_sqlnum := 2000;
SELECT
    CASE WHEN COALESCE(in_tbl_def1, in_tbl_def2, in_tbl_def3, 'NULL') = 'NULL'
            THEN 'At least one of the 3 table definition parameters must be not null. '
         WHEN COALESCE(in_tbl_name, 'NULL') = 'NULL'
            THEN 'The table name passed was NULL. '
         ELSE NULL END
INTO t_err_msg
FROM dual;

IF t_err_msg IS NOT NULL THEN
    RAISE_APPLICATION_ERROR (-20001,t_err_msg);
END IF;

-- Expected string format: "maxtemp.MY_TABLE(field_a VARCHAR2(10), field_b NUMBER, ...) TABLESPACE tspace_x"
-- Exception block is required when the value is not present in the user preferences table else
--     it throws a "NO_DATA_FOUND" exception
n_sqlnum := 3000;
BEGIN
    SELECT ' TABLESPACE ' || NVL(value_1, ' MMAX_MAXTEMP')
    INTO t_sql3
    FROM maxapp.userpref
    WHERE UPPER(key_1) = 'TABLESPACE_BI_BRIDGE';

    EXCEPTION WHEN NO_DATA_FOUND THEN
        SELECT ' TABLESPACE MMAX_MAXTEMP'
        INTO t_sql3
        FROM dual;
    WHEN OTHERS THEN RAISE;
END;

n_sqlnum := 4000;
out_full_tbl_name := v_tgt_schema||'.'||'T_BI_'||in_tbl_name;

n_sqlnum := 5000;
--t_sql1 := SUBSTR(in_tbl_def1,1,255);
--maxdata.ins_import_log (t_proc_name, t_error_level, t_call, t_sql1, n_sqlnum, t_int_null);

n_sqlnum := 6000;
-- Try to drop the table first.  Turn off errors
t_ignore_error := 1;
t_sql1 := 'DROP TABLE '||out_full_tbl_name;
maxtemp.p_exec_temp_ddl(t_ignore_error, t_sql1, t_char_null, t_char_null, t_char_null, t_char_null, t_future_int, t_future_int, t_char_null);

n_sqlnum := 7000;
-- Create the table.  Turn on error reporting.
t_ignore_error := 0;
t_sql1 := 'CREATE TABLE '||out_full_tbl_name;
maxtemp.p_exec_temp_ddl(t_ignore_error, t_sql1, in_tbl_def1, in_tbl_def2, in_tbl_def3, t_sql3, t_future_int, t_future_int, t_char_null);

n_sqlnum := 8000;
-- We need to grant permissions on the table we just created in maxtemp
t_ignore_error := 0;
t_sql1 := 'GRANT ALL ON '||out_full_tbl_name||' TO maxdata, madmax, maxuser';
maxtemp.p_exec_temp_ddl(t_ignore_error, t_sql1, t_char_null, t_char_null, t_char_null, t_char_null, t_future_int, t_future_int, t_char_null);

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

END p_create_bi_bridge_tbl;

/

  GRANT EXECUTE ON "MAXDATA"."P_CREATE_BI_BRIDGE_TBL" TO "MADMAX";
