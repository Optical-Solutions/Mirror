--------------------------------------------------------
--  DDL for Procedure P_GET_CL_HIST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GET_CL_HIST" 
(
    in_cube_id          IN  NUMBER,
    in_pw_id            IN  NUMBER,
    in_kpi_dv_id        IN  NUMBER,
    in_fcast_ver_id     IN  NUMBER,  -- Default: -1 (history aggregation)
    in_future2          IN  NUMBER,
    in_debug_flag       IN  NUMBER,  --  See parameter description below
    out_table_nm        OUT VARCHAR2
)

AS

/* ----------------------------------------------------------------------

Change History

$Log: 2178_p_get_cl_hist.sql,v $
Revision 1.22.2.5.2.3  2009/05/15 15:27:35  anchan
FIXID S0565601: ROUND to prevent possible "number precision
too large" error

Revision 1.22.2.5.2.2  2009/05/06 18:34:56  makirk
Added extra debugging lines.  Minor formatting cleanup.

Revision 1.22.2.5.2.1  2009/04/24 20:03:34  anchan
FIXID S0565601: disallow two sessions from stepping on each other; keep checking until the other session either finishes or aborts.

Revision 1.22.2.5  2008/12/03 21:20:58  makirk
Fix for S0523347

Revision 1.22.2.4  2008/10/08 14:45:05  makirk
Added alias qualifier to create temp table statement where clause

Revision 1.22.2.3  2008/09/30 16:49:30  makirk
Fixes for S0538376 and S0530926

Revision 1.22.2.2  2008/09/26 20:25:18  makirk
Added fix to debugging section so that cube backups weren't endlessly saved

Revision 1.22.2.1  2008/06/11 20:25:41  makirk
Fix for S0504735

Revision 1.22  2008/02/29 16:10:49  makirk
Reviewed by J Cho

Revision 1.2  2008/02/29 16:10:05  makirk
Checked into DDL dir

Revision 1.1  2008/02/28 18:03:01  makirk
Initial checkin.  Changes made for forecast aggregation.


Revision 1.21  2007/10/05 15:14:08  clapper
FIXID AUTOPUSH: SOS 1256274

Revision 1.20.2.1  2007/06/19 18:50:15  anchan
S0441055: remove aliases and increase variable size to allow more column names

Revision 1.20  2007/06/19 14:39:30  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.16  2006/09/20 21:04:05  makirk
Changed calls from p_ins_long_import_log to p_log

Revision 1.15  2006/09/14 16:16:52  joscho
Fixed CLSET->CLSET worksheet missing data problem.
(S0378345, S0375809)

Revision 1.14  2006/05/05 12:58:18  joscho
Make cluster batch and cube cleanup mutually exclusive.

Revision 1.13  2006/03/06 16:10:05  makirk
Added default tablespace

Revision 1.12  2006/02/15 23:29:19  joscho
Synch up the parameter lists of p_get_cl_hist and p_custom_aggr.

Revision 1.11  2006/01/10 21:25:59  makirk
Removed t_call param from call to p_log and wrapped each call in a debug var check

Revision 1.10  2006/01/09 19:22:59  makirk
Modified for creating temp tables under maxtemp

Revision 1.9  2005/11/23 13:44:24  joscho
Enhance error message to reflect the possible causes

Revision 1.8  2005/10/21 20:07:54  raabuh
fix the $Log key substitution
reviewed by Joseph


V6.1
6.1.0-005 10/19/05 Rahman   Added code for replacing tablespace names with the ones set in userpref table
6.1.0-001 06/06/05 Diwakar  Re Written for 6.1

Usage : External and internal

Description : Creates temporary cluster history tables and aggregate store data to cluster/set.

Dependant on:

p_gen_time_inclause
p_insert_cl_status

Parameters  :

in_cube_id      : Cube ID of the worksheet.
in_pw_id        : Worksheet ID to build cluster history
in_kpi_dv_id    : Data version of a worksheet to build cluster history.
in_fcast_ver_id : History Aggregation, Default is -1
in_future2      : Placeholder.  Pass in -1.
in_debug_flag   : Debug flag.  Values are 1 or 0. Application always passes as 0.
out_table_nm    : Returns Temporary cluster history table name to application that
                  was generated for the given worksheet, cube and data version id.
-------------------------------------------------------------------------------- */

n_sqlnum               NUMBER(10,0);
t_proc_name            VARCHAR2(32)    := 'p_get_cl_hist';
t_error_level          VARCHAR2(6)     := 'info';
t_call                 VARCHAR2(1000);
v_sql                  VARCHAR2(8000)  := NULL;
t_cur_stmt             VARCHAR2(8000)  := NULL;
t_sql2                 VARCHAR2(255);
t_sql3                 VARCHAR2(255);
t_cnt                  NUMBER(10,0);
t_future_int           NUMBER(2)       := -1;
t_char_null            CHAR(1)         := NULL;
t_ignore_error         NUMBER          := 0; -- 0 is raise exception.  1 is to ignore

t_temp_joined_tbl_name VARCHAR2(64);
t_plancount            NUMBER(10);
t_delete_temp_table    VARCHAR2(255)   := 'Y';
t_status               VARCHAR2(2);
t_hist_ok              VARCHAR2(2);
t_cnt2                 NUMBER;
t_6key_tbl_name        VARCHAR2(80);
t_str_included         NUMBER;
t_cl_included          NUMBER;
t_clset_included       NUMBER;
t_hist_tbl_cnt         NUMBER;
t_counter              NUMBER;
t_table_list           VARCHAR2(255);
t_table_hint           VARCHAR2(255);
t_query_hint           VARCHAR2(64);
t_cl_row_cnt           NUMBER(10);
t_ignore_err_flg       NUMBER(1)       := 0;
t_loc_id               NUMBER(10);
t_min_t_id             NUMBER(10);
t_max_t_id             NUMBER(10);
t_max_t_lev            NUMBER(10);
t_start_date           DATE;
t_end_date             DATE;
t_insert_sql           VARCHAR2(8000);
t_select_sql           VARCHAR2(8000);
t_tbl                  VARCHAR2(40);
t_col_nam              VARCHAR2(30);
t_unique_count         NUMBER;
t_in_clause            VARCHAR2(3900);
t_tablespace_cl_hist   VARCHAR2(200);
t_clset_id             NUMBER(10);
t_backup_cube_data     VARCHAR2(255)   := 'Y';
t_agg_rule_loc         VARCHAR2(50);
t2_agg_rule_loc        VARCHAR2(50);
t_src_col_name         VARCHAR2(50);
t_src_tab_name         VARCHAR2(40);
t2_src_col_name        VARCHAR2(50);
t2_src_tab_name        VARCHAR2(40);
t_entity_id            NUMBER(10);
t_error_msg            VARCHAR2(1000);

t_timeout              NUMBER:= 0;
t_start_dttm           DATE;
t_check_sess_flg       NUMBER(2):=1;
t_sess_exists_flg      NUMBER(2);
t_session_id           VARCHAR2(30);
t_new_session_id       VARCHAR2(30);
t_elapsed_min          NUMBER(10);

TYPE TabCurTyp IS REF CURSOR;
c_tabname TabCurTyp;
c_colname TabCurTyp;

BEGIN
n_sqlnum := 1000;

-- Get the pref tablespace name from userpref table.
BEGIN
        SELECT value_1
          INTO t_tablespace_cl_hist
          FROM maxapp.userpref
         WHERE UPPER(key_1) = 'TABLESPACE_CL_HIST';

        EXCEPTION WHEN NO_DATA_FOUND THEN
                t_tablespace_cl_hist := 'MMAX_CL_HIST';
END;

-- Log the parameters of the procedure
t_call := t_proc_name            || '(' ||
    COALESCE(in_cube_id,     -1) || ',' ||   -- NVL(int, 'NULL') returns error because of diff datatype.
    COALESCE(in_pw_id,       -1) || ',' ||
    COALESCE(in_kpi_dv_id,   -1) || ',' ||
    COALESCE(in_fcast_ver_id,-1) || ',' ||
    COALESCE(in_future2,     -1) || ',' ||
    COALESCE(in_debug_flag,  -1) || ',' ||
                           'OUT' || ')';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
COMMIT;

t_ignore_error := 0;

-- Clean up the log table.
IF in_debug_flag > 0 THEN
BEGIN
    n_sqlnum := 1500;

    t_ignore_error := 1;
    v_sql := 'TRUNCATE TABLE maxdata.t_sc_log';

    IF in_debug_flag > 0 THEN
        maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
    END IF;

    maxdata.p_execute_ddl_sql(v_sql,t_ignore_error,t_future_int,t_char_null,t_char_null);
END;
END IF;

-- Check params.
n_sqlnum := 2000;

IF    in_cube_id   IN (0,-1)
   OR in_pw_id     IN (0,-1)
   OR in_kpi_dv_id IN (0,-1) THEN
BEGIN
        RAISE_APPLICATION_ERROR (-20001,'Invalid Cube_id/pw_id/dv_id');
END;
END IF;

n_sqlnum := 3000;

-- Get the flag for deleting temp tables.
BEGIN
        SELECT value_1
          INTO t_delete_temp_table
          FROM maxapp.userpref
         WHERE key_1 = 'DELETE_TEMP_TABLE';

        EXCEPTION WHEN OTHERS
                THEN t_delete_temp_table := 'Y';
END;

n_sqlnum := 3500;
-- Get the flag for backing up the cube data for debugging.
BEGIN
        SELECT COALESCE(value_1,'N')
          INTO t_backup_cube_data
          FROM maxapp.userpref
         WHERE key_1 = 'CL_HIST_SAVE_CUBE';

        EXCEPTION WHEN OTHERS
                THEN t_backup_cube_data := 'N';
END;

n_sqlnum := 3600;
-- If the t_backup_cube_data flag is set to Y then back up the cube data to -(in_pw_id)
-- This is for debugging purposes only
IF t_backup_cube_data = 'Y' THEN
BEGIN
        DELETE FROM maxdata.t_cube_loc   WHERE cube_id = -(in_pw_id);
        DELETE FROM maxdata.t_cube_merch WHERE cube_id = -(in_pw_id);
        DELETE FROM maxdata.t_cube_time  WHERE cube_id = -(in_pw_id);

        INSERT INTO maxdata.t_cube_loc   SELECT -(in_pw_id),l_lev,l_id           FROM maxdata.t_cube_loc   WHERE cube_id = in_cube_id;
        INSERT INTO maxdata.t_cube_merch SELECT -(in_pw_id),m_lev,m_id           FROM maxdata.t_cube_merch WHERE cube_id = in_cube_id;
        INSERT INTO maxdata.t_cube_time  SELECT -(in_pw_id),t_lev,t_id,kpi_dv_id FROM maxdata.t_cube_time  WHERE cube_id = in_cube_id;

        -- Turn the flag off so cube copies don't endlessly accumulate
        UPDATE maxapp.userpref SET value_1='N' WHERE key_1='CL_HIST_SAVE_CUBE';
        COMMIT;
END;
END IF;

n_sqlnum := 4000;
-- Check the status of the cl hist.

SELECT COUNT(*) INTO t_cnt
  FROM maxdata.cl_hist_status
 WHERE planworksheet_id = in_pw_id
   AND kpi_dv_id = in_kpi_dv_id;

n_sqlnum := 5000;

t_hist_ok := '--'; -- Assume that hist needs to be re-built.

IF in_debug_flag > 0 THEN
        v_sql := 't_cnt='||t_cnt;
        maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
END IF;

IF t_cnt > 1 THEN
    RAISE_APPLICATION_ERROR (-20001, 'More than one cluster history status entries for a planworksheet/gridsets');
ELSIF t_cnt = 0 THEN
    -- If the cl hist status entry is not found, then insert a new one.
    -- P_insert_cl_status will plug in LY/TY/LLY entries with start/end_date.

    maxdata.p_insert_cl_status(in_pw_id,in_kpi_dv_id,t_future_int,t_future_int,t_future_int);
END IF;

n_sqlnum := 6100;
SELECT TO_NUMBER(COALESCE(property_value,default_value))
  INTO t_timeout
  FROM maxdata.t_application_property
 WHERE property_key = 'clusterHistory.generation.timeout';

n_sqlnum := 6200;
-- S0565601: keep checking until another session either finishes or aborts
WHILE (t_check_sess_flg = 1) -- Should run at least ONE iteration
LOOP
        t_check_sess_flg  := 0;
        t_elapsed_min     := 0;
        t_sess_exists_flg := 1;

        n_sqlnum := 6210;
        -- Check if another session is already building this cluster history
        WHILE (t_elapsed_min<t_timeout) AND (t_sess_exists_flg>0)
        LOOP
                n_sqlnum := 6220;
                SELECT MIN(session_id),
                       MIN(build_start_dttm)
                  INTO t_session_id,
                       t_start_dttm
                  FROM maxdata.cl_hist_status
                 WHERE planworksheet_id = in_pw_id
                   AND kpi_dv_id        = in_kpi_dv_id
                   AND status           = 'IP';

                -- User must have been given "GRANT SELECT ON v$session TO..."
                n_sqlnum := 6230;
                SELECT COUNT(*)
                  INTO t_sess_exists_flg
                  FROM v$session
                 WHERE audsid = t_session_id;

                IF (t_sess_exists_flg>0) THEN
                        dbms_lock.sleep(60);
                        t_elapsed_min := ROUND( (SYSDATE - t_start_dttm) * 24 * 60 );
                END IF;
        END LOOP;

        n_sqlnum := 6240;
        IF (t_elapsed_min >= t_timeout) THEN
        BEGIN
                t_ignore_err_flg := 1;
                t_error_msg := 'Timed out while waiting for cluster history build by another session (AUDSID='||t_session_id
                        ||'), which has been running since '||TO_CHAR(t_start_dttm,'MM/DD/YY HH:MI AM')||'.';

                RAISE_APPLICATION_ERROR (-20001, t_error_msg);
        END;
        END IF;

        n_sqlnum := 6300;
        -- Get the name of the temp table to be dropped and recreated
        SELECT table_nm,status,
               build_start_dttm,
               session_id
          INTO out_table_nm,
               t_status,
               t_start_dttm,
               t_new_session_id
          FROM maxdata.cl_hist_status
         WHERE planworksheet_id = in_pw_id
           AND kpi_dv_id        = in_kpi_dv_id
        FOR UPDATE;
        -- Don't commit until ready to release update lock;

        -- Check if another session just sneaked in before this session
        IF (t_status='IP') AND (t_new_session_id!=t_session_id) THEN
                ROLLBACK;
                t_check_sess_flg := 1; -- Forces ANOTHER iteration
        END IF;
END LOOP;

n_sqlnum := 7000;
-- Check the status.
-- If status='IP', then the previous session must have aborted, so continue.
IF in_debug_flag > 0 THEN
        v_sql := 't_status='||t_status;
        maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
END IF;


n_sqlnum := 7200;
-- Only show error if timeout period has been exceeded.  Otherwise do a rebuild
IF t_status = 'ER' THEN
        t_ignore_err_flg := 1;
        RAISE_APPLICATION_ERROR (-20001, 'Prior cluster history build failed.  See MAXDATA.IMPORT_LOG.');
END IF;

n_sqlnum := 8000;
-- If the history is ok then check if there is data.
-- Exception will be thrown if the table doesn't exist (bad data in maxdata.cl_hist_status)

IF t_status = 'OK' THEN
BEGIN
        n_sqlnum := 8100;
        t_cnt    := 0;
        v_sql    := 'SELECT COUNT(*) FROM ' || out_table_nm || ' WHERE ROWNUM <= 1';

        IF in_debug_flag > 0 THEN
                maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
        END IF;

        BEGIN
        n_sqlnum := 8200;

        EXECUTE IMMEDIATE v_sql INTO t_cnt;

        t_hist_ok := 'OK';

        EXCEPTION
        WHEN others THEN
                t_hist_ok := 'NT';
        END;
END;
ELSE
BEGIN
    t_hist_ok := t_status;
END;
END IF;

-- If we don't have to re-build the history, then commit (release UPDATE lock) and return.

IF t_hist_ok = 'OK' THEN
BEGIN
        n_sqlnum := 9000;
        -- Set the last accessed date.

        UPDATE maxdata.cl_hist_status
           SET last_accessed = SYSDATE
         WHERE planworksheet_id = in_pw_id
           AND kpi_dv_id        = in_kpi_dv_id;

        COMMIT;

        RETURN;
END;
ELSE --'OB' or 'IP' w/o session--
BEGIN
        --Mark status as 'build In-Progress' and
        --commit. Ready to release update lock;
        n_sqlnum := 10000;

        UPDATE maxdata.cl_hist_status
           SET status           = 'IP',
               cube_id          = in_cube_id,
               build_start_dttm = SYSDATE,
               session_id       = SYS_CONTEXT('USERENV','SESSIONID')
         WHERE planworksheet_id = in_pw_id
           AND kpi_dv_id        = in_kpi_dv_id;

        COMMIT;

        IF in_debug_flag > 0 THEN
            v_sql := 'Rebuild reason: '||t_hist_ok;
            maxdata.p_log(t_proc_name,t_error_level,v_sql, t_char_null, n_sqlnum);
        END IF;
END;
END IF;


-- Build the history.
n_sqlnum := 10500;

IF in_fcast_ver_id != -1 THEN
        t_entity_id := 34;
ELSE
        t_entity_id := 21;
END IF;

-- MCK ,02/08/08
-- Check that the entity is an allowed value (21 or 34)
SELECT dv.entity
  INTO t_entity_id
  FROM maxdata.wlkd_kpi_dataversion wlkd
  JOIN maxapp.dataversion dv ON dv.dv_id = wlkd.dv_id
 WHERE wlkd.kpi_dv_id = in_kpi_dv_id;

IF t_entity_id NOT IN (21,34) THEN
BEGIN
    RAISE_APPLICATION_ERROR (-20001,'Invalid entity ('||t_cnt||').');
END;
END IF;

n_sqlnum := 11000;

-- Check if dynamic metadata table was loaded with table/column names.

SELECT COUNT(*) INTO t_cnt
FROM maxapp.multifact_column;

IF t_cnt = 0 THEN
BEGIN
    RAISE_APPLICATION_ERROR (-20001,'maxapp.multifact_column not loaded yet.');
END;
END IF;

n_sqlnum := 12000;

SELECT COUNT(*)
  INTO t_cnt2
  FROM (SELECT DISTINCT UPPER(LTRIM(RTRIM(src_col_name))),
                        UPPER(LTRIM(RTRIM(entity)))
          FROM maxapp.multifact_column);

IF t_cnt <> t_cnt2 then
BEGIN
    RAISE_APPLICATION_ERROR (-20001,'Duplicate column name found in maxapp.multifact_column.');
END;
END IF;

-- Check the source table names.
n_sqlnum := 13000;

SELECT COUNT(*)
  INTO t_cnt2
  FROM (SELECT DISTINCT UPPER(LTRIM(RTRIM(src_tab_name))),
                        UPPER(LTRIM(RTRIM(entity)))
          FROM maxapp.multifact_column);

n_sqlnum := 14000;

SELECT COUNT(*)
  INTO t_cnt
  FROM (SELECT DISTINCT src_tab_name,entity FROM maxapp.multifact_column);

IF t_cnt <> t_cnt2 THEN
BEGIN
        n_sqlnum := 14100;

        UPDATE maxapp.multifact_column
           SET src_tab_name = UPPER(LTRIM(RTRIM(src_tab_name)));

        COMMIT;

        n_sqlnum := 14200;

        SELECT COUNT(DISTINCT src_tab_name)
          INTO t_cnt
          FROM maxapp.multifact_column;

        IF t_cnt <> t_cnt2 THEN
                RAISE_APPLICATION_ERROR (-20001,'Table names in maxapp.multifact_column cannot have spaces in them.');
        END IF;
END;
END IF;

-- Check that entity is not null
n_sqlnum := 14500;
t_cnt := 0;

SELECT COUNT(*)
  INTO t_cnt
  FROM maxapp.multifact_column
 WHERE entity IS NULL;

IF t_cnt > 0 THEN
        RAISE_APPLICATION_ERROR (-20001,'NULL values found in maxapp.multifact_column.entity');
END IF;
-- End of checking inputs.


n_sqlnum := 15000;
-- Set store flag
BEGIN
    SELECT 1 INTO t_str_included
      FROM maxdata.t_cube_loc
     WHERE cube_id = in_cube_id
       AND l_lev = 4
       AND ROWNUM <= 1;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                t_str_included := 0;
END;

n_sqlnum := 15500;
-- set cluster flag
BEGIN
    SELECT 1 INTO t_cl_included
      FROM maxdata.t_cube_loc
     WHERE cube_id = in_cube_id
       AND l_lev = 1002
       AND ROWNUM <= 1;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                t_cl_included := 0;
END;

n_sqlnum := 16000;

-- Set cluster set flag
BEGIN
    SELECT 1, l_id
      INTO t_clset_included, t_clset_id
      FROM maxdata.t_cube_loc
     WHERE cube_id = in_cube_id
       AND l_lev = 1001
       AND ROWNUM <= 1;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                t_clset_included := 0;
END;

IF in_debug_flag > 0 THEN
    v_sql := ' clset:'||t_clset_included||', cl:'||t_cl_included||', str:'||t_str_included;
    maxdata.p_log(t_proc_name,t_error_level,v_sql, t_char_null, n_sqlnum);
END IF;

n_sqlnum := 17000;

-- Check if the flags are set to 0 or 1.

IF  t_clset_included NOT IN (0,1) OR
    t_cl_included NOT IN (0,1) OR
    (t_clset_included + t_cl_included) = 0 THEN
BEGIN
    RAISE_APPLICATION_ERROR(-20001,' Unexpected cluster levels or empty T_CUBE_LOC');
END;
END IF;

-- Fill store id into ... from the ids of maxdata.t_cube_loc for the supplied in_cube_id
-- If the store level id's are in the cube table, then use them.
-- Else if the cluster id's are there, then get all the store id's from the cluster id's.
-- Else get all the store id's from the cluster set id.


IF t_str_included = 1 THEN
        BEGIN
                n_sqlnum := 18000;
                INSERT INTO maxdata.t_cube_loc_cluster
                        (cube_id,
                         l_lev,
                         l_id,
                         clstr_spc_id)
                SELECT in_cube_id,
                       4,
                       str.lvnloc_id,
                       str.fnl_clstr_spc_id
                  FROM maxdata.t_cube_loc cube,
                       maxdata.clstr_str str
                 WHERE cube.cube_id = in_cube_id
                   AND cube.l_lev   = 4
                   AND cube.l_id    = str.lvnloc_id;
        END;
        ELSE
        IF t_cl_included = 1 THEN
        BEGIN
                n_sqlnum := 18100;
                INSERT INTO maxdata.t_cube_loc_cluster
                        (cube_id,
                         l_lev,
                         l_id,
                         clstr_spc_id)
                SELECT in_cube_id,
                       4,
                       str.lvnloc_id,
                       str.fnl_clstr_spc_id
                  FROM maxdata.t_cube_loc cube,
                       maxdata.clstr_str str
                 WHERE cube.cube_id = in_cube_id
                   AND cube.l_lev   = 1002
                   AND cube.l_id    = str.fnl_clstr_spc_id;

        END;
        ELSE
        BEGIN
                n_sqlnum := 18200;
                INSERT INTO maxdata.t_cube_loc_cluster
                        (cube_id,
                         l_lev,
                         l_id,
                         clstr_spc_id)
                SELECT DISTINCT in_cube_id,
                       4,
                       str.lvnloc_id,
                       str.fnl_clstr_spc_id
                  FROM maxdata.t_cube_loc cube,
                       maxdata.clstr_spc spc,
                       maxdata.clstr_str str
                 WHERE cube.cube_id     = in_cube_id
                   AND cube.l_lev       = 1001
                   AND cube.l_id        = spc.clstr_st_id
                   AND spc.clstr_spc_id = str.fnl_clstr_spc_id
                   AND spc.clstr_grp_id <> -1;

        END;
        END IF;
END IF;

-- End of filling cluster ids.

-- Fill 4key cluster table.

n_sqlnum := 19000;

INSERT INTO maxdata.t_cube_4key_cluster
       (cube_id,
        m_lev,
        m_id,
        l_lev,
        l_id,
        clstr_spc_id)
SELECT in_cube_id,
       m.m_lev,
       m.m_id,
       l.l_lev,
       l.l_id,
       l.clstr_spc_id
  FROM maxdata.t_cube_merch m,
       maxdata.t_cube_loc_cluster l
 WHERE m.cube_id = in_cube_id
   AND l.cube_id = in_cube_id;

-- end filling 4key cluster table

n_sqlnum := 20000;

-- Table names to define columns

t_6key_tbl_name := ' maxdata.t_cube_4key_cluster k, maxdata.t_cube_time t ';

-- Using a metadata table, dynamically build a SQL stmt that creates
-- multifact temp tables with dimension subcube table.

-- Drop the table. If not exists, ignore the error.
BEGIN
        n_sqlnum := 21000;

        t_ignore_error := 1;
        v_sql := 'DROP TABLE ' || out_table_nm;

        IF in_debug_flag > 0 THEN
                maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
        END IF;

        maxtemp.p_exec_temp_ddl(t_ignore_error, v_sql, t_char_null, t_char_null, t_char_null, t_char_null, t_future_int, t_future_int, t_char_null);

        EXCEPTION
                WHEN OTHERS THEN
                        NULL;
END;

-- Create a table first and then insert data into it.
-- It is to keep the datatypes of the original columns.

n_sqlnum := 22000;

v_sql :='CREATE TABLE ' || out_table_nm ||
    ' nologging pctfree 0 storage (next 10M) tablespace '||t_tablespace_cl_hist||' as ' ||
    'SELECT ' ||
    'k.l_lev LOCATION_LEVEL,'||  -- lv4loc_id is NOT loc_lev but just for datatype.
    'k.l_id LOCATION_ID,'||
    'k.m_lev MERCH_LEVEL,'||
    'k.m_id MERCH_ID,'||
    't.t_lev TIME_LEVEL,'||
    't.t_id TIME_ID';

IF in_debug_flag > 0 THEN
        INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum,SUBSTR(v_sql,1,255));
END IF;

n_sqlnum := 23000;

-- MCK, 02/08/08
-- Set cursor logic based on if forecasting is being used or not.
-- Dropped agg_rule_id from old cursor since it was not being used
-- GET FIELDS FOR TABLE CREATE SELECT STMT
IF in_fcast_ver_id != -1 THEN
    t_cur_stmt := 'SELECT src_tab_name,src_col_name ' ||
              '  FROM maxapp.multifact_column ' ||
              ' WHERE entity = 34 ' ||
              ' ORDER BY src_tab_name,multifact_column_id';
ELSE
    t_cur_stmt := 'SELECT src_tab_name,src_col_name ' ||
              '  FROM maxapp.multifact_column ' ||
              ' WHERE entity = 21 ' ||
              ' ORDER BY src_tab_name,multifact_column_id';
END IF;

t_cnt    := 0;
t_cnt2   := 0;
t_tbl    := 'xxxxxx';

n_sqlnum := 23001;
OPEN c_colname FOR t_cur_stmt;
LOOP
        FETCH c_colname INTO t_src_tab_name,t_src_col_name;
        EXIT WHEN c_colname%NOTFOUND;

        t_cnt := t_cnt + 1;

        IF t_src_tab_name <> t_tbl THEN
                t_cnt2 := t_cnt2 + 1;
                t_tbl  := t_src_tab_name;
        END IF;

        t_col_nam := t_src_col_name;

        t_sql2 := ', ' || 'm'||t_cnt2 || '.' || t_col_nam; -- || ' ' || t_col_nam;

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum+t_cnt, t_sql2);
        END IF;

        v_sql := v_sql || t_sql2;
END LOOP;  -- cursor c_colnam

CLOSE c_colname;

n_sqlnum := 23995;
t_sql2 := ' FROM ' || t_6key_tbl_name;

t_cnt := t_cnt + 1;


IF in_debug_flag > 0 THEN
        INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum+t_cnt, t_sql2);
END IF;

v_sql := v_sql || t_sql2;

n_sqlnum := 24000;
-- MCK, 02/07/08
-- Set cursor logic based on if forecasting is being used or not
-- GET FIELDS FOR CREATE TABLE FROM STMT
IF in_fcast_ver_id != -1 THEN
        t_cur_stmt := 'SELECT DISTINCT src_tab_name' ||
                      '  FROM maxapp.multifact_column' ||
                      ' WHERE entity = 34' ||
                      ' ORDER BY src_tab_name';
ELSE
        t_cur_stmt := 'SELECT DISTINCT src_tab_name' ||
                      '  FROM maxapp.multifact_column' ||
                      ' WHERE entity = 21' ||
                      ' ORDER BY src_tab_name';
END IF;

t_cnt := 0;

n_sqlnum := 24100;
OPEN c_tabname FOR t_cur_stmt;

LOOP
        FETCH c_tabname INTO t_src_tab_name;
        EXIT WHEN c_tabname%NOTFOUND;

        t_cnt  := t_cnt + 1;
        t_sql2 := ',' || 'maxdata.' || t_src_tab_name || ' m' || t_cnt;

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum + t_cnt, t_sql2);
        END IF;

        v_sql := v_sql || t_sql2;
END LOOP;  -- cursor c_tabname

n_sqlnum := 24600;
CLOSE c_tabname;

n_sqlnum := 25000;
t_sql2 := ' WHERE 1=0 AND m1.time_level=0 AND rownum<=0 ';    -- dummy expression just to create a table without data.

IF in_debug_flag > 0 THEN
        INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, SUBSTR(t_sql2,1,255));
        INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, SUBSTR(t_sql2,256,510));
END IF;

v_sql := v_sql || t_sql2;

-- Create multifact table.

n_sqlnum := 26000;

t_ignore_error := 0;

IF in_debug_flag > 0 THEN
        maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
END IF;

maxtemp.p_exec_temp_ddl(t_ignore_error,v_sql,t_char_null,t_char_null,t_char_null,t_char_null,t_future_int,t_future_int,t_char_null);

n_sqlnum := 26500;

-- We need to grant permissions on the table we just created in maxtemp
t_ignore_error := 0;
v_sql := 'GRANT ALL ON '||out_table_nm||' TO maxdata, madmax, maxuser';

IF in_debug_flag > 0 THEN
        maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
END IF;

maxtemp.p_exec_temp_ddl(t_ignore_error, v_sql, t_char_null, t_char_null, t_char_null, t_char_null, t_future_int, t_future_int, t_char_null);

IF in_debug_flag > 0 THEN
        INSERT INTO maxdata.t_sc_log values (n_sqlnum,SUBSTR(v_sql,1,255));
END IF;

-- Aggregate to cl or cl set.
-- Build a query for aggregation.

-- CHANGE t_joined_tbl_name to another temporary join table
n_sqlnum := 27000;

t_temp_joined_tbl_name := out_table_nm ||'_T';

-- Drop the other temporary join table. If not exists, ignore the error.
n_sqlnum := 28000;

v_sql := ' DROP TABLE ' || t_temp_joined_tbl_name;

t_ignore_error := 1;

IF in_debug_flag > 0 THEN
        maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
END IF;

maxtemp.p_exec_temp_ddl(t_ignore_error,v_sql,t_char_null,t_char_null,t_char_null,t_char_null,t_future_int,t_future_int,t_char_null);

n_sqlnum := 29000;

SELECT COUNT(*)
  INTO t_hist_tbl_cnt
  FROM (SELECT DISTINCT src_tab_name
          FROM maxapp.multifact_column
         WHERE entity = t_entity_id) A;

-- If there is only one fact table,
-- then do not create an intermediate temp table (*_T) but
-- use the final table for aggregation.

n_sqlnum := 30000;

IF t_hist_tbl_cnt = 1 THEN
BEGIN
        t_temp_joined_tbl_name := out_table_nm;
END;
ELSE
BEGIN
        -- Create temporary multifact table.
        n_sqlnum := 31000;
        v_sql := 'CREATE TABLE ' || t_temp_joined_tbl_name ||
                ' NOLOGGING PCTFREE 0 STORAGE (NEXT 10M) TABLESPACE '||t_tablespace_cl_hist||'  as ' ||
                ' SELECT * FROM '|| out_table_nm;

        n_sqlnum := 32000;

        t_ignore_error := 0;

        IF in_debug_flag > 0 THEN
                maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
        END IF;

        maxtemp.p_exec_temp_ddl(t_ignore_error,v_sql,t_char_null,t_char_null,t_char_null,t_char_null,t_future_int,t_future_int,t_char_null);

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, SUBSTR(v_sql,1,255));
        END IF;

        n_sqlnum := 32500;
        -- We need to grant permissions on the table we just created in maxtemp
        t_ignore_error := 0;
        v_sql := 'GRANT ALL ON '||t_temp_joined_tbl_name||' TO maxdata, madmax, maxuser';

        IF in_debug_flag > 0 THEN
                maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
        END IF;

        maxtemp.p_exec_temp_ddl(t_ignore_error, v_sql, t_char_null, t_char_null, t_char_null, t_char_null, t_future_int, t_future_int, t_char_null);

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log values (n_sqlnum,SUBSTR(v_sql,1,255));
        END IF;
END;
END IF; --t_hist_tbl_cnt = 1

-- table names for INSERT STATEMENT

t_6key_tbl_name := ' maxdata.t_cube_4key_cluster cl_cube ';

n_sqlnum := 33000;
-- MCK, 02/08/08
-- Set cursor logic based on if forecasting is being used or not
IF in_fcast_ver_id != -1 THEN
        t_cur_stmt := 'SELECT DISTINCT src_tab_name' ||
                      '  FROM maxapp.multifact_column' ||
                      ' WHERE entity = 34' ||
                      ' ORDER BY src_tab_name';
ELSE
        t_cur_stmt := 'SELECT DISTINCT src_tab_name' ||
                      '  FROM maxapp.multifact_column' ||
                      ' WHERE entity = 21' ||
                      ' ORDER BY src_tab_name';
END IF;

t_counter := 0;

n_sqlnum := 33005;
OPEN c_tabname FOR t_cur_stmt;

n_sqlnum := 33010;
LOOP
        FETCH c_tabname INTO t_src_tab_name;
        EXIT WHEN c_tabname%NOTFOUND;

        t_counter    := t_counter + 1;

        n_sqlnum     := n_sqlnum + 1;
        t_table_list := '['||t_src_tab_name||']';

        maxdata.p_get_query_hint('HISTORY',t_table_list,t_table_hint,t_query_hint);

        /* Strips off schema name, then REPLACE function substitutes tablenames with alias: */

        t_table_hint := REPLACE(t_table_hint,maxdata.f_object_part(t_src_tab_name),'m');

        t_insert_sql :='INSERT INTO ' || t_temp_joined_tbl_name || '(';

        t_select_sql := ' SELECT /*+ '||t_query_hint||' '||t_table_hint||' */ ';

        BEGIN
        IF in_debug_flag > 0 THEN
            INSERT INTO maxdata.t_sc_log values (n_sqlnum,t_insert_sql);
        END IF;
        END;

        n_sqlnum := n_sqlnum + 1;

        BEGIN
        IF in_debug_flag > 0 THEN
            INSERT INTO maxdata.t_sc_log values (n_sqlnum, t_select_sql);
        END IF;
        END;

        n_sqlnum := n_sqlnum + 1;


        IF (t_cl_included = 0 AND t_clset_included = 1) THEN -- aggregate directly to cl set
        BEGIN
            t_sql2 := ' 1001  LOCATION_LEVEL,'||
                t_clset_id ||' LOCATION_ID,'||
                'cl_cube.m_lev MERCH_LEVEL,'||
                'cl_cube.m_id MERCH_ID,'||
                'm.time_level TIME_LEVEL,'||
                'm.time_id TIME_ID';
        END;
        ELSE
        BEGIN
            t_sql2:= ' 1002  LOCATION_LEVEL,'||
                'cl_cube.clstr_spc_id LOCATION_ID,'||
                'cl_cube.m_lev MERCH_LEVEL,'||
                'cl_cube.m_id MERCH_ID,'||
                'm.time_level TIME_LEVEL,'||
                'm.time_id TIME_ID';
        END;
        END IF;

        IF in_debug_flag > 0 THEN
        BEGIN
            INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum,t_sql2);
        END;
        END IF;


        t_insert_sql := t_insert_sql || ' LOCATION_LEVEL,LOCATION_ID,MERCH_LEVEL,MERCH_ID,TIME_LEVEL,TIME_ID ';

        t_select_sql := t_select_sql || t_sql2;

        n_sqlnum := 33040;
        -- MCK, 02/08/08
        -- Set cursor logic based on if forecasting is being used or not
        -- GET FIELDS FOR INSERT SELECT STMT
        IF in_fcast_ver_id != -1 THEN
                t_cur_stmt := 'SELECT src_tab_name,src_col_name,agg_rule_loc ' ||
                              '  FROM maxapp.multifact_column ' ||
                              ' WHERE entity = 34 ' ||
                              ' ORDER BY src_tab_name,multifact_column_id';
        ELSE
                t_cur_stmt := 'SELECT src_tab_name,src_col_name,agg_rule_loc ' ||
                              '  FROM maxapp.multifact_column ' ||
                              ' WHERE entity = 21 ' ||
                              ' ORDER BY src_tab_name,multifact_column_id';
        END IF;

        t_cnt  := 0;
        t_cnt2 := 0;
        t_tbl  := 'xxxxxx';

        OPEN c_colname FOR t_cur_stmt;

        LOOP
                FETCH c_colname INTO t2_src_tab_name,t2_src_col_name,t2_agg_rule_loc;
                EXIT WHEN c_colname%NOTFOUND;

                t_cnt := t_cnt + 1;

                IF t2_src_tab_name <> t_tbl THEN
                        t_cnt2 := t_cnt2 + 1;
                        t_tbl  := t2_src_tab_name;
                END IF;

                IF t2_src_tab_name = t_src_tab_name THEN
                BEGIN
                        IF (INSTR(UPPER(t2_src_col_name),'DATE') > 0 OR
                            INSTR(UPPER(t2_src_col_name),'DT') > 0 OR
                            INSTR(UPPER(t2_src_col_name),'_DTTM') > 0) THEN
                        BEGIN
                            IF UPPER(t_agg_rule_loc) IN ('SYSDATE','SYSDATETIME') THEN
                            BEGIN
                                t_sql2 := ', SYSDATE ' || t2_src_col_name;
                            END;
                            ELSE
                            BEGIN
                                t_sql2 := ', ' || t2_agg_rule_loc || '(' || 'm.' || t2_src_col_name || ')';
                                    --|| t2_src_col_name;
                            END;
                            END IF;
                        END;
                        ELSE
                        BEGIN
                            t_sql2 := ', ' || t2_agg_rule_loc || '(' || 'm.' || t2_src_col_name || ')';
                                --|| t2_src_col_name;
                        END;
                        END IF;

                        t_insert_sql := t_insert_sql || ',' || t2_src_col_name;
                        t_select_sql := t_select_sql || t_sql2;
                END;
                ELSE
                BEGIN
                        IF (INSTR(UPPER(t2_src_col_name),'DATE') > 0 OR
                            INSTR(UPPER(t2_src_col_name),'DT') > 0 OR
                            INSTR(UPPER(t2_src_col_name),'_DTTM') > 0) THEN
                        BEGIN
                                IF UPPER(t2_agg_rule_loc) IN ('SYSDATE','SYSDATETIME') THEN
                                        t_sql2 := ', SYSDATE ' || t2_src_col_name;
                                ELSIF UPPER(t2_agg_rule_loc) = 'MIN' THEN
                                        t_sql2 := ', SYSDATE + 36500 '; -- 100 years ahead
                                ELSIF UPPER(t2_agg_rule_loc) = 'MAX' THEN
                                        t_sql2 := ', SYSDATE - 36500 '; --  100 years back
                                ELSE
                                        RAISE_APPLICATION_ERROR (-20001,'Unsupported agg_rule_loc type for Date data type. ' );
                                END IF;

                                t_insert_sql := t_insert_sql || ',' || t2_src_col_name;
                                t_select_sql := t_select_sql || t_sql2;
                        END;
                        END IF;
                END;
                END IF;

                n_sqlnum := n_sqlnum + 1;

                IF in_debug_flag > 0 THEN
                        INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
                END IF;
        END LOOP; -- cursor c_colnam

        CLOSE c_colname;

        t_insert_sql := t_insert_sql || ')';

        t_sql2 := ' FROM ' || t_6key_tbl_name;

        t_select_sql := t_select_sql || t_sql2;

        n_sqlnum := n_sqlnum + 1;


        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
        END IF;

        n_sqlnum := n_sqlnum + 1;

        -- Joined Fact table name.

        t_sql2 := ' INNER JOIN maxdata.' || t_src_tab_name || ' m ';

        t_select_sql := t_select_sql || t_sql2;

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
        END IF;

        n_sqlnum := n_sqlnum + 1;

        t_tbl  := 'm';

        t_sql2 := ' ON cl_cube.cube_id = ' || in_cube_id;

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
        END IF;

        -- to join with time ids call maxdata.p_gen_time_inclause procedure
        /*
        t_sql2 := t_sql2 || ' and t.t_lev = '   || t_tbl || '.time_level';
        t_sql2 := t_sql2 || ' and t.t_id = '    || t_tbl || '.time_id';
        */

        maxdata.p_gen_time_inclause(in_cube_id,in_kpi_dv_id, t_future_int, t_future_int, t_future_int,t_in_clause);

        -- due to 255 charcter length of t_sc_log column, for loop used to log t_in_clause variable.

        IF in_debug_flag > 0 THEN
        BEGIN
            FOR i IN 0..ROUND(LENGTH(t_in_clause)/255)+1 LOOP
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, SUBSTR(t_in_clause,t_cnt,255));
                t_cnt := i * 255;
            END LOOP;
        END;
        END IF;

        t_select_sql := t_select_sql || t_sql2 || ' AND ' || SUBSTR(t_in_clause,1,LENGTH(t_in_clause));

        t_sql2 :=  ' AND cl_cube.m_lev ='    || t_tbl || '.merch_level';
        t_sql2 := t_sql2 || ' AND cl_cube.m_id ='     || t_tbl || '.merch_id';
        t_sql2 := t_sql2 || ' AND cl_cube.l_lev = 4 ';
        t_sql2 := t_sql2 || ' AND cl_cube.l_lev ='    || t_tbl || '.location_level';
        t_sql2 := t_sql2 || ' AND cl_cube.l_id='      || t_tbl || '.location_id';

        IF in_debug_flag > 0 THEN
            INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
        END IF;

        t_select_sql := t_select_sql || t_sql2;

        n_sqlnum := n_sqlnum + 1;
        t_sql2   := ' GROUP BY ';

        IF (t_cl_included = 0 and t_clset_included = 1) THEN -- aggregate directly to cl set
                t_sql2 := t_sql2; -- nothing
        ELSE
                t_sql2 := t_sql2 || 'cl_cube.clstr_spc_id,';
        END IF;

        t_sql2:= t_sql2 ||
            'cl_cube.m_lev,' ||
            'cl_cube.m_id,' ||
            'm.time_level,' ||
            'm.time_id';

        t_cnt := t_cnt + 1;

        IF in_debug_flag > 0 THEN
        BEGIN
            INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
        END;
        END IF;

        t_select_sql := t_select_sql || t_sql2;

        v_sql := t_insert_sql || t_select_sql;

        -- Execute join/create temporary multifact table.
        n_sqlnum := n_sqlnum + 1;

        IF in_debug_flag > 0 THEN
                maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
        END IF;

        EXECUTE IMMEDIATE v_sql;

        COMMIT;
END LOOP;  -- cursor c_tabname

CLOSE c_tabname;

n_sqlnum := 34000;

-- If an intermediate aggregation table (T_CL*_T) was used,
-- then aggregate the data in the intermediate table into
-- the final cluster table (T_CL).

IF t_hist_tbl_cnt > 1 THEN
BEGIN
        n_sqlnum:=34100;

        v_sql := 'INSERT INTO ' || out_table_nm ||
                ' SELECT LOCATION_LEVEL,' ||
                'LOCATION_ID,'    ||
                'MERCH_LEVEL,'    ||
                'MERCH_ID,'       ||
                'TIME_LEVEL,'     ||
                'TIME_ID ';

        IF in_debug_flag > 0 THEN
        BEGIN
        INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, SUBSTR(v_sql,1,255));
        END;
        END IF;

        n_sqlnum:=34208;

        -- MCK, 02/08/08
        -- Set cursor logic based on if forecasting is being used or not
        IF in_fcast_ver_id != -1 THEN
                t_cur_stmt := 'SELECT src_col_name,agg_rule_loc ' ||
                '  FROM maxapp.multifact_column ' ||
                ' WHERE entity = 34 ' ||
                ' ORDER BY src_tab_name,multifact_column_id';
        ELSE
                t_cur_stmt := 'SELECT src_col_name,agg_rule_loc ' ||
                        '  FROM maxapp.multifact_column ' ||
                        ' WHERE entity = 21 ' ||
                        ' ORDER BY src_tab_name,multifact_column_id';
        END IF;

        t_cnt := 0;

        n_sqlnum:=34210;
        OPEN c_colname FOR t_cur_stmt;

        n_sqlnum:=34220;
        LOOP
                FETCH c_colname INTO t_src_col_name,t_agg_rule_loc;
                EXIT WHEN c_colname%NOTFOUND;

                t_cnt := t_cnt + 1;

                IF UPPER(t_agg_rule_loc) IN ('SYSDATE','SYSDATETIME') THEN
                    t_sql2 := ', SYSDATE ' || t_src_col_name;
                ELSE
                    t_sql2 := ', ' || t_agg_rule_loc || '(' || t_src_col_name || ')';
                                --|| t_src_col_name;
                END IF;

                IF in_debug_flag > 0 THEN
                BEGIN
                    INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum+t_cnt, t_sql2);
                END;
                END IF;

                v_sql := v_sql || t_sql2;
        END LOOP;  -- cursor c_colname

        CLOSE c_colname;

        n_sqlnum:= 34230;

        t_sql2 := ' FROM ' || t_temp_joined_tbl_name;
        v_sql  := v_sql || t_sql2;

        n_sqlnum := 34300;

        IF in_debug_flag > 0 THEN
        BEGIN
        INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
        END;
        END IF;

        t_sql2:= ' GROUP BY LOCATION_LEVEL,'||
                'LOCATION_ID,'   ||
                'MERCH_LEVEL,'   ||
                'MERCH_ID,'      ||
                'TIME_LEVEL,'    ||
                'TIME_ID ';

        v_sql := v_sql || t_sql2;

        n_sqlnum := 34400;

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
                maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
        END IF;

        EXECUTE IMMEDIATE v_sql;

        COMMIT;

        IF t_delete_temp_table = 'Y' THEN
        BEGIN
                n_sqlnum := 34500;
                v_sql := 'DROP TABLE '||t_temp_joined_tbl_name;

                IF in_debug_flag > 0 THEN
                        INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
                END IF;

                t_ignore_error := 0;

                IF in_debug_flag > 0 THEN
                        maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
                END IF;

                maxtemp.p_exec_temp_ddl(t_ignore_error,v_sql,t_char_null,t_char_null,t_char_null,t_char_null,t_future_int,t_future_int,t_char_null);
        END;
        END IF; -- if delete_temp=0

        COMMIT;
END;
END IF;  -- t_hist_tbl_cnt > 1


-- Now, if we aggregate from cl to cl set, then...
n_sqlnum := 35000;

IF t_cl_included = 1 AND t_clset_included = 1 THEN
BEGIN
        n_sqlnum := 35100;

        v_sql :='INSERT INTO ' || out_table_nm||
        ' SELECT /*+ APPEND PARALLEL(m1, 2) */ '; --No searching freespace. Avoid high parallel degree.

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum,SUBSTR(v_sql,1,255));
        END IF;

        n_sqlnum := 35200;

        SELECT l.l_id
          INTO t_loc_id
          FROM maxdata.t_cube_loc l
         WHERE l.cube_id = in_cube_id
           AND l.l_lev   = 1001;

        t_sql2:=1001 || ' LOCATION_LEVEL,'||
                t_loc_id  || ' LOCATION_ID,'||
                ' MERCH_LEVEL,' ||
                ' MERCH_ID,'    ||
                ' TIME_LEVEL,'  ||
                ' TIME_ID ';

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum,t_sql2);
        END IF;

        v_sql := v_sql || t_sql2;

        n_sqlnum := 35300;

        -- MCK, 02/08/08
        -- Set cursor logic based on if forecasting is being used or not
        IF in_fcast_ver_id != -1 THEN
                t_cur_stmt := 'SELECT src_col_name,agg_rule_loc ' ||
                        '  FROM maxapp.multifact_column ' ||
                        ' WHERE entity = 34 ' ||
                        ' ORDER BY src_tab_name,multifact_column_id';
        ELSE
                t_cur_stmt := 'SELECT src_col_name,agg_rule_loc ' ||
                        '  FROM maxapp.multifact_column ' ||
                        ' WHERE entity = 21 ' ||
                        ' ORDER BY src_tab_name,multifact_column_id';
        END IF;

        t_cnt  := 0;
        t_cnt2 := 0;

        n_sqlnum := 35310;
        OPEN c_colname FOR t_cur_stmt;

        n_sqlnum := 35320;
        LOOP
                FETCH c_colname INTO t_src_col_name,t_agg_rule_loc;
                EXIT WHEN c_colname%NOTFOUND;

                t_cnt := t_cnt + 1;

                IF UPPER(t_agg_rule_loc) IN ('SYSDATE','SYSDATETIME') THEN
                        t_sql2 := ', SYSDATE ' || t_src_col_name;
                ELSE
                        t_sql2 := ', ' || t_agg_rule_loc || '(' || t_src_col_name || ')';
                        --|| t_src_col_name;
                END IF;

                IF in_debug_flag > 0 THEN
                        INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum+t_cnt, t_sql2);
                END IF;

                v_sql := v_sql || t_sql2;
        END LOOP;  -- cursor c_colname

        CLOSE c_colname;

        n_sqlnum := 35400;
        t_sql2 := ' FROM ' || out_table_nm || ' m1 ';

        t_cnt := t_cnt + 1;

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
        END IF;

        v_sql := v_sql || t_sql2;

        n_sqlnum := 35500;

        t_sql2:= ' GROUP BY '   ||
                'merch_level,' ||
                'merch_id,'    ||
                'time_level,'  ||
                'time_id ';

        t_cnt := t_cnt + 1;

        IF in_debug_flag > 0 THEN
                INSERT INTO maxdata.t_sc_log VALUES (n_sqlnum, t_sql2);
        END IF;

        v_sql := v_sql || t_sql2;

        -- Execute join/create multifact table.

        n_sqlnum := 35600;

        IF in_debug_flag > 0 THEN
                maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
        END IF;

        EXECUTE IMMEDIATE v_sql;

        COMMIT;
END;
END IF; -- aggregate from cl to cl set.

-- Perform customized aggregations
n_sqlnum := 36000;

SELECT unique_count INTO t_unique_count
FROM maxapp.mmax_config;

IF t_unique_count = 1 THEN
        maxdata.p_custom_aggr (
                in_cube_id,
                in_pw_id,
                in_kpi_dv_id,
                in_fcast_ver_id,
                in_future2,
                in_debug_flag);
END IF;

-- Mark the status as 'OK'.
-- Don't do this for bridge/trend mgmt because CL HIST is deleted for them.

-- Get row count
v_sql := 'SELECT COUNT(*) FROM ' || out_table_nm;

IF in_debug_flag > 0 THEN
        maxdata.p_log(t_proc_name,t_error_level,v_sql,t_char_null,n_sqlnum);
END IF;

EXECUTE IMMEDIATE v_sql INTO t_cl_row_cnt;

-- Get start date and end date
n_sqlnum := 37000;

SELECT MIN(t_id),
       MAX(t_id),
       MAX(max_lev.t_lev)
  INTO t_min_t_id,
       t_max_t_id,
       t_max_t_lev
  FROM maxdata.t_cube_time t,
       (SELECT MAX(t_lev) t_lev
          FROM maxdata.t_cube_time
         WHERE cube_id = in_cube_id
           AND kpi_dv_id = in_kpi_dv_id ) max_lev
 WHERE t.cube_id   = in_cube_id
   AND t.kpi_dv_id = in_kpi_dv_id
   AND t.t_lev     = max_lev.t_lev;

n_sqlnum := 38000;

BEGIN
CASE t_max_t_lev
    WHEN 51 THEN -- WEEK
        SELECT MIN(lv5time_start_date),
               MAX(lv5time_end_date)
          INTO t_start_date,
               t_end_date
          FROM maxapp.lv5time lvx
         WHERE lvx.lv5time_lkup_id = t_min_t_id
            OR lvx.lv5time_lkup_id = t_max_t_id;
    WHEN 50 THEN -- MONTH
        SELECT MIN(lv4time_start_date),
               MAX(lv4time_end_date)
          INTO t_start_date,
               t_end_date
          FROM maxapp.lv4time lvx
         WHERE lvx.lv4time_lkup_id = t_min_t_id
            OR lvx.lv4time_lkup_id = t_max_t_id;
    WHEN 49 THEN -- QUARTER
        SELECT MIN(lv3time_start_date),
               MAX(lv3time_end_date)
          INTO t_start_date,
               t_end_date
          FROM maxapp.lv3time lvx
        WHERE lvx.lv3time_lkup_id = t_min_t_id
           OR lvx.lv3time_lkup_id = t_max_t_id;
    WHEN 48 THEN -- SEASON
        SELECT MIN(lv2time_start_date),
               MAX(lv2time_end_date)
          INTO t_start_date,
               t_end_date
          FROM maxapp.lv2time lvx
         WHERE lvx.lv2time_lkup_id = t_min_t_id
            OR lvx.lv2time_lkup_id = t_max_t_id;
    WHEN 47 THEN -- YEAR
        SELECT MIN(lv1time_start_date),
               MAX(lv1time_end_date)
          INTO t_start_date,
               t_end_date
          FROM maxapp.lv1time lvx
         WHERE lvx.lv1time_lkup_id = t_min_t_id
            OR lvx.lv1time_lkup_id = t_max_t_id;
    ELSE
        raise_application_error (-20001,'Invalid Time Level for given ID.');
END CASE;
END;

n_sqlnum := 39000;

UPDATE maxdata.cl_hist_status
   SET status        = 'OK',
       cube_id       = NULL,
       row_count     = t_cl_row_cnt,
       plan_count    = t_plancount,
       dv_start_date = t_start_date,
       dv_end_date   = t_end_date,
       table_nm      = out_table_nm,
       build_run_sec = ROUND((sysdate - build_start_dttm)*24*60*60)
 WHERE planworksheet_id = in_pw_id
   AND kpi_dv_id        = in_kpi_dv_id;

IF in_debug_flag <= 0 THEN
BEGIN
    n_sqlnum := 40000;
    DELETE FROM maxdata.t_cube_4key_cluster WHERE cube_id = in_cube_id;

    n_sqlnum := 41000;
    DELETE FROM maxdata.t_cube_loc_cluster WHERE cube_id = in_cube_id;
END;
END IF;

COMMIT;

EXCEPTION
    WHEN OTHERS THEN

        COMMIT; -- No harm to commit on error in this proc.

        -- If status is already 'IP' or 'ER' then just signal the condition
        -- but do not treat it as a real error.

        IF t_ignore_err_flg = 1 THEN
            RAISE; -- Display the error message and terminate the procedure without logging the error.
        END IF;

        IF v_sql IS NOT NULL THEN
            t_error_level := 'info';
            t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
            maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, NULL, n_sqlnum, NULL);
            maxdata.p_log (t_proc_name, t_error_level, v_sql, t_char_null, n_sqlnum);
        END IF;

        UPDATE maxdata.cl_hist_status
        SET status = 'ER'
        WHERE planworksheet_id = in_pw_id
        AND kpi_dv_id = in_kpi_dv_id;

        IF in_debug_flag <= 0 THEN
        BEGIN
            DELETE FROM maxdata.t_cube_4key_cluster WHERE cube_id = in_cube_id;
            DELETE FROM maxdata.t_cube_loc_cluster WHERE cube_id = in_cube_id;
        END;
        END IF;

        -- Log the error message
        t_error_level := 'error';

        v_sql := SQLERRM || ' (' || t_call ||
                ', SQL#:' || n_sqlnum || ')';

        maxdata.p_log (t_proc_name, t_error_level, v_sql, t_char_null, n_sqlnum);

        COMMIT;

        RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_GET_CL_HIST" TO "MADMAX";
