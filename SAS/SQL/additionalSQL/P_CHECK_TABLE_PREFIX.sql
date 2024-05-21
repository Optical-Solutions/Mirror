--------------------------------------------------------
--  DDL for Procedure P_CHECK_TABLE_PREFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_CHECK_TABLE_PREFIX" (in_prefix IN VARCHAR) AS

/* ----------------------------------------------------------------------------
$Log: 2420_p_check_table_prefix.sql,v $
Revision 1.8  2007/06/19 14:38:44  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.1  2006/09/28 18:07:39  makirk
Check for duplicate 4 char table prefixes

------------------------------------------------------------------------------ */

n_sqlnum             NUMBER(10,0);
t_proc_name          VARCHAR2(32)    := 'p_check_table_prefix';
t_error_level        VARCHAR2(6)     := 'info';
t_call               VARCHAR2(1000);
v_sql                VARCHAR2(1000)  := NULL;
t_sql2               VARCHAR2(255);

t_prefix_list        VARCHAR2(500)   := NULL;

BEGIN

-- Log the parameters of the procedure
n_sqlnum := 1000;
t_call := t_proc_name                || ' ( ''' ||
        COALESCE(in_prefix, 'NULL')  || ''' ) ';

maxdata.p_log (t_proc_name, t_error_level, t_call, NULL, n_sqlnum);

n_sqlnum := 2000;
DECLARE CURSOR c_prefix IS
SELECT DISTINCT prefix FROM
(
        (
        SELECT SUBSTR(table_name, 1, 5) prefix
        FROM all_tables
        WHERE owner IN ('MAXAPP','MAXDATA','DATAMGR','MAXMETADATA')
        AND SUBSTR(table_name, 5, 1) IN ('_','1')
        AND SUBSTR(table_name, 1, 3) <> 'MIG'
        AND SUBSTR(table_name, 1, 5) NOT IN
            ('ATTR_','HIER_','SESS_','TEMP_','TOAD_','T_LV1','BKUP_','EXEC_','FINC_','MMAX_','PROC_','TIME_',
             'T_CL_','T_PC_','T_SC_','T_TM_','T_DP_','T_PF_','T_AL_','TEST_')
        )
        UNION ALL
        (SELECT in_prefix prefix FROM dual) -- User's prefix
) tmp
GROUP BY prefix
HAVING COUNT(1) > 1;


BEGIN
        n_sqlnum := 3000;
        FOR prefix_rec IN c_prefix LOOP
                IF t_prefix_list IS NULL THEN
                        t_prefix_list := '''' || prefix_rec.prefix || '''';
                ELSE
                        t_prefix_list := t_prefix_list || ',''' || prefix_rec.prefix || '''';
                END IF;
        END LOOP;
END;

n_sqlnum := 4000;
IF t_prefix_list IS NOT NULL THEN
        RAISE_APPLICATION_ERROR(-20001,'Duplicate 4 letter table prefix(es) found: '||t_prefix_list||'; ');
END IF;

COMMIT;

EXCEPTION
        WHEN OTHERS THEN
                ROLLBACK;

                IF v_sql IS NOT NULL THEN
                        t_error_level := 'info';
                        t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
                        maxdata.p_log (t_proc_name, t_error_level, v_sql, t_sql2, n_sqlnum);
                END IF;

                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM || ' (' || t_call ||
                                ', SQL#:' || n_sqlnum || ')';
                maxdata.p_log (t_proc_name, t_error_level, v_sql, NULL, n_sqlnum);
                --COMMIT;

                RAISE_APPLICATION_ERROR(-20001,v_sql);

END p_check_table_prefix;

/

  GRANT EXECUTE ON "MAXDATA"."P_CHECK_TABLE_PREFIX" TO "MADMAX";
