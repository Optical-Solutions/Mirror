--------------------------------------------------------
--  DDL for Procedure P_AGGR_HIER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGGR_HIER" (
    in_key_tbl_name  VARCHAR2,
    in_src_tbl_name  VARCHAR2,
    in_tar_tbl_name  VARCHAR2,
    in_dim_type      CHAR,    --'L', 'M', 'T'
    in_src_lev       NUMBER,
    in_tar_lev       NUMBER,
    in_all_rows_flag NUMBER,  --1: aggregate ALL source rows without any criteria.
    in_debug_flag    NUMBER
) AS

/*
----------------------------------------------------------------------

Change History:
$Log: 2142_p_aggr_hier.sql,v $
Revision 1.11  2007/06/19 14:39:43  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.7  2006/04/14 16:39:27  makirk
Removed import_log delete code (redundant), added change history logging where needed, commented out param logging commit

Revision 1.6  2006/03/06 16:09:09  makirk
Added default tablespace

Revision 1.5  2006/01/09 19:23:01  makirk
Modified for creating temp tables under maxtemp

Revision 1.4  2005/12/01 19:27:17  joscho
Unit-tested for the change to remove PERIOD, CYCLE.

Revision 1.3  2005/10/21 20:33:19  raabuh
added the $Log key stuff
reviewed by Joseph


V6.1
6.1.0    10/20/05 Rahman Added code for replacing tablespace names with the ones set in userpref table
V5.4
5.4.0-028 10/28/02 Sachin    Added tablespace clause to temporary table.

V5.3.2
04/22/2002 Joseph Cho    New columns, agg_rule_merch/time, were added to AGGR_HIER_COLUMN.
04/04/2002 Joseph Cho    Initial entry. Copied from 42_07_p_get_cl_hist_53.

Description:
        This procedure was originated from p_get_cl_hist.
        It aggregates rows in an input source table (MPLAN,etc) for the
        the columns specified in AGGR_HIER_COLUMN table
        by joining the input source table with an input key_table.
        Currently, it is used for performance consolidation.

-------------------------------------------------------------------------
*/

t_sql                  VARCHAR2(10000);
t_sql2                 VARCHAR2(255);
t_cnt                  NUMBER;
t_cnt2                 NUMBER;
t_sqlnum               NUMBER;
t_aggr                 VARCHAR2(10);
t_col_nam              VARCHAR2(30);
t_agg_rule             VARCHAR2(30);
t_call                 VARCHAR2(1000);
t_tablespace_perf_cons VARCHAR2(200);
t_char_null            CHAR(1)         := NULL;
t_int_null             NUMBER          := NULL;
t_future_int           NUMBER(10)      := -1;
t_sub_sql1             VARCHAR2(4000)  := NULL;   -- Var used for parsing t_sql for maxtemp.p_exec_temp_ddl params
t_sub_sql2             VARCHAR2(4000)  := NULL;   -- Var used for parsing t_sql for maxtemp.p_exec_temp_ddl params
t_sub_sql3             VARCHAR2(4000)  := NULL;   -- Var used for parsing t_sql for maxtemp.p_exec_temp_ddl params
t_ignore_error         NUMBER          := 0;      -- 0 is Raise exception. 1 to ignore when called p_execute_ddl_sql


BEGIN

-- Log the parameters of the proc.

t_sqlnum := 180;

t_call := ' p_aggr_hier(' ||
          ''''||in_key_tbl_name  || ''',' ||
          ''''||in_src_tbl_name  || ''',' ||
          ''''||in_tar_tbl_name  || ''',' ||
          ''''||in_dim_type      || ''',' ||
                in_src_lev       || ','   ||
                in_tar_lev       || ','   ||
                in_all_rows_flag || ','   ||
                in_debug_flag    ||    ')';
maxdata.ins_import_log ('p_aggr_hier','info', t_call, NULL, NULL, NULL);
--COMMIT;

t_sqlnum := 190;

-- Get the pref tablespace name from userpref table.
BEGIN
    SELECT value_1 INTO t_tablespace_perf_cons
    FROM maxapp.userpref
    WHERE UPPER(key_1) = 'TABLESPACE_PERF_CONSOLIDATION';

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
             t_tablespace_perf_cons := 'MMAX_T_PERFCONSLD';
END;

t_sqlnum := 200;

-- Check if dynamic metadata table was loaded with table/column names.

SELECT COUNT(*) INTO t_cnt
FROM maxapp.aggr_hier_column
WHERE ROWNUM <= 1;

IF t_cnt = 0 THEN
    RAISE_APPLICATION_ERROR (-20001,'maxapp.aggr_hier_column not loaded yet.');
END IF;


-- Using a metadata table, dynamically build a SQL stmt that aggregates
-- the source table with a key table.

-- Drop the table. If not exists, ignore the error.
BEGIN
    t_sqlnum := 6900;
    t_sql := ' DROP TABLE ' || in_tar_tbl_name;
    t_ignore_error := 1;

    maxtemp.p_exec_temp_ddl(t_ignore_error,t_sql,t_char_null,t_char_null,t_char_null,t_char_null,t_future_int,t_future_int,t_char_null);

    EXCEPTION
        WHEN OTHERS THEN
              NULL;
END;

-- Clean up the log table.
IF in_debug_flag > 0 THEN
    t_sqlnum := 6950;
    t_sql := ' TRUNCATE TABLE maxdata.t_sc_log';
    EXECUTE IMMEDIATE t_sql;
END IF;

-- Create a table first and then insert data into it instead of
-- 'create table as select'.
-- It is to keep the datatypes of the original columns on Oracle.

t_sqlnum := 7000;

t_sql :='CREATE TABLE ' || in_tar_tbl_name ||
    ' NOLOGGING PCTFREE 0 STORAGE (NEXT 10M) TABLESPACE '||t_tablespace_perf_cons||' as ';

IF in_debug_flag > 0 THEN
    INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum,t_sql);
END IF;

t_sqlnum := 7500;

t_sql2 :='select ' ||
    't.l_lev LOCATION_LEVEL,'||  -- lv4loc_id is NOT loc_lev but just for datatype.
    't.l_id LOCATION_ID,'||
    't.m_lev MERCH_LEVEL,'||
    't.m_id MERCH_ID,'||
    't.t_lev TIME_LEVEL,'||
    't.t_id TIME_ID';

IF in_debug_flag > 0 THEN
    INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum,t_sql2);
END IF;

t_sql := t_sql || t_sql2;

t_sqlnum := 8000;

DECLARE CURSOR c_colnam is
    SELECT src_col_name,agg_rule_id
    FROM maxapp.aggr_hier_column
    ORDER BY aggr_hier_col_id;
BEGIN
    t_cnt := 0;

    FOR c1 IN c_colnam LOOP

        t_cnt := t_cnt + 1;

        t_col_nam := c1.src_col_name;

        t_sql2 := ',' || 'm1.' || t_col_nam || ' ' || t_col_nam;

        IF in_debug_flag > 0 THEN
            INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum+t_cnt, t_sql2);
        END IF;

        t_sql := t_sql || t_sql2;

        --exit when t_cnt > 10;

    END LOOP;
END; -- cursor c_colnam

t_sql2 := ' from ' || in_key_tbl_name || ' t, ' ||in_src_tbl_name||' m1 ';

t_cnt := t_cnt + 1;

IF in_debug_flag > 0 THEN
    INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum+t_cnt, t_sql2);
END IF;

t_sql := t_sql || t_sql2;

t_sqlnum := 9000;

t_sql2 := ' where 1 = 0 ';    -- dummy expression just to create a table without data.

IF in_debug_flag > 0 THEN
    INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum, t_sql2);
END IF;

t_sql := t_sql || t_sql2;

-- Create mplan table.
t_sqlnum := 10000;
t_ignore_error := 0;

-- Max SQL parameter size for p_exec_temp_ddl is 4000.  T_SQL is 10000 so we have to break it up.
t_sub_sql1 := SUBSTR(t_sql,1,4000);
t_sub_sql2 := SUBSTR(t_sql,4001,4000);
t_sub_sql3 := SUBSTR(t_sql,8001,2000);

maxtemp.p_exec_temp_ddl(t_ignore_error, t_sub_sql1, t_sub_sql2, t_sub_sql3, t_char_null, t_char_null, t_future_int, t_future_int, t_char_null);

t_sql := 'GRANT ALL ON '||in_tar_tbl_name||' TO maxdata, madmax, maxuser';
maxtemp.p_exec_temp_ddl(t_ignore_error, t_sql, t_char_null, t_char_null, t_char_null, t_char_null, t_future_int, t_future_int, t_char_null);

-- Insert data into it by aggregating data.
-- Use optimizer hints for select.

t_sql := 'INSERT INTO ' || in_tar_tbl_name;

IF in_debug_flag > 0 THEN
    INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum,t_sql);
END IF;

t_sqlnum := 10500;

t_sql2 := ' SELECT /*+ ORDERED PARALLEL(m1) PARALLEL_INDEX(m1) USE_NL(m1) */ ';

IF in_debug_flag > 0 THEN
    INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum,t_sql2);
END IF;

t_sql := t_sql || t_sql2;

-- Specify 6keys. Use parent id for the aggregating dimension.

t_sqlnum := 10700;

IF in_dim_type = 'L' THEN
    t_sql2 := in_tar_lev||' LOCATION_LEVEL,'||
              't.parent_id LOCATION_ID,'    ||
              't.m_lev MERCH_LEVEL,'        ||
              't.m_id MERCH_ID,'            ||
              't.t_lev TIME_LEVEL,'         ||
              't.t_id TIME_ID ';
ELSIF in_dim_type = 'M' THEN
    t_sql2 := 't.l_lev LOCATION_LEVEL,'     ||
              't.l_id LOCATION_ID,'         ||
              in_tar_lev||' MERCH_LEVEL,'   ||
              't.parent_id MERCH_ID,'       ||
              't.t_lev TIME_LEVEL,'         ||
              't.t_id TIME_ID ';
ELSIF in_dim_type = 'T' THEN
    t_sql2 := 't.l_lev LOCATION_LEVEL,'    ||
              't.l_id LOCATION_ID,'        ||
              't.m_lev MERCH_LEVEL,'       ||
              't.m_id MERCH_ID,'           ||
              in_tar_lev||' TIME_LEVEL,'   ||
              't.parent_id TIME_ID ';
ELSE
    RAISE_APPLICATION_ERROR(-20001,'Unsupported dim type: ' || in_dim_type);
END IF;

IF in_debug_flag > 0 THEN
    INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum,t_sql2);
END IF;

t_sql := t_sql || t_sql2;

-- Loop through columns and apply their aggr rules.

t_sqlnum := 11000;

DECLARE CURSOR c_colnam IS
    SELECT src_col_name,agg_rule_loc,agg_rule_merch,agg_rule_time
    FROM maxapp.aggr_hier_column
    ORDER BY aggr_hier_col_id;
BEGIN
    t_cnt := 0;

    FOR c1 IN c_colnam LOOP

        t_cnt := t_cnt + 1;

        IF in_dim_type = 'L' THEN
            t_agg_rule := UPPER(c1.agg_rule_loc);
        ELSIF in_dim_type = 'M' THEN
            t_agg_rule := UPPER(c1.agg_rule_merch);
        ELSE
            t_agg_rule := UPPER(c1.agg_rule_time);
        END IF;

        IF t_agg_rule='SYSDATE' THEN
            t_sql2 := ', SYSDATE '|| c1.src_col_name;
        ELSIF t_agg_rule='BOP' THEN
            t_sql2 := ',SUM(' || 'm1.' || c1.src_col_name ||
                '*t.bop_flag) ' || c1.src_col_name;
        ELSIF t_agg_rule='EOP' THEN
            t_sql2 := ',SUM(' || 'm1.' || c1.src_col_name ||
                '*t.eop_flag) ' || c1.src_col_name;
        ELSE
            t_sql2 := ',' || t_agg_rule || '(' || 'm1.' || c1.src_col_name || ') '
                || c1.src_col_name;
        END IF;

        IF in_debug_flag > 0 THEN
            INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum+t_cnt, t_sql2);
        END IF;

        t_sql := t_sql || t_sql2;

        --exit when t_cnt > 10;

    END LOOP;
END; -- cursor c_colnam

t_sql2 := ' FROM ' || in_key_tbl_name || ' t, '||in_src_tbl_name||' m1 ';

t_cnt := t_cnt + 1;
IF in_debug_flag > 0 THEN
    INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum+t_cnt, t_sql2);
END IF;

t_sql := t_sql || t_sql2;

-- If in_all_rows flag is off, THEN
-- use a selection criteria.
-- Otherwise, aggregate the entire rows.

t_sqlnum := 13000;

IF in_all_rows_flag = 1 THEN
    t_sql2 := ' WHERE t.l_lev=m1.location_level';
    t_sql2 := t_sql2 || ' AND t.l_id=m1.location_id';
    t_sql2 := t_sql2 || ' AND t.m_lev=m1.merch_level';
    t_sql2 := t_sql2 || ' AND t.m_id=m1.merch_id';
    t_sql2 := t_sql2 || ' AND t.t_lev=m1.time_level';
    t_sql2 := t_sql2 || ' AND t.t_id=m1.time_id';
ELSE
    t_sql2 := ' WHERE t.pw_id = m1.workplan_id';
    t_sql2 := t_sql2 || ' AND t.l_lev=m1.location_level';
    t_sql2 := t_sql2 || ' AND t.l_id=m1.location_id';
    t_sql2 := t_sql2 || ' AND t.m_lev=m1.merch_level';
    t_sql2 := t_sql2 || ' AND t.m_id=m1.merch_id';
    t_sql2 := t_sql2 || ' AND t.t_lev=m1.time_level';
    t_sql2 := t_sql2 || ' AND t.t_id=m1.time_id';
END IF;

IF in_debug_flag > 0 THEN
    INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum+t_cnt, t_sql2);
END IF;

t_sql := t_sql || t_sql2;

-- Compose a group by clause.

IF in_dim_type = 'L' THEN
    t_sql2 := ' group by '   ||
              --'t.l_lev,' ||
              't.parent_id,' ||
              't.m_lev,'     ||
              't.m_id,'      ||
              't.t_lev,'     ||
              't.t_id';
ELSIF in_dim_type = 'M' THEN
    t_sql2 := ' group by '   ||
              't.l_lev,'     ||
              't.l_id,'      ||
              --'t.m_lev,'   ||
              't.parent_id,' ||
              't.t_lev,'     ||
              't.t_id';
ELSE
    t_sql2 := ' group by ' ||
              't.l_lev,'   ||
              't.l_id,'    ||
              't.m_lev,'   ||
              't.m_id,'    ||
              --'t.t_lev,' ||
              't.parent_id';
END IF;

t_cnt := t_cnt + 1;
IF in_debug_flag > 0 THEN
    INSERT INTO maxdata.t_sc_log VALUES (t_sqlnum+t_cnt, t_sql2);
END IF;

t_sql := t_sql || t_sql2;

-- Execute join/create mplan table.
t_sqlnum := 14000;
EXECUTE IMMEDIATE t_sql;

COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        --rollback;
        COMMIT; -- no harm to commit on error in this proc.

        t_sql := SQLERRM || ' (' || t_call ||
                ', SQL#:' || t_sqlnum || ')';

        -- Log the error message.

        maxdata.ins_import_log ('p_aggr_hier','error', t_sql, NULL, NULL, NULL);
        COMMIT;

        raise_application_error (-20001,t_sql);

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_AGGR_HIER" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_AGGR_HIER" TO "MAXUSER";
