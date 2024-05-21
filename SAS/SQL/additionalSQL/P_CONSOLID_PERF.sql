--------------------------------------------------------
--  DDL for Procedure P_CONSOLID_PERF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_CONSOLID_PERF" 
(   in_cube_id       NUMBER,
    in_pw_id         NUMBER,
    in_tar_merch_lev NUMBER,    -- 1 based
    in_tar_loc_lev   NUMBER,
    in_tar_time_lev  NUMBER,
    in_merch_td_flag NUMBER,    -- 1 for TD, 0 for BU
    in_loc_td_flag   NUMBER,    -- 1 for TD, 0 for BU
    in_priority_hier NUMBER,    -- 1 for MERCH, 2 for LOC
    in_kpi_dv_id     NUMBER,    -- kpi dv id
    in_future2       NUMBER,    -- For future use.
    in_future3       NUMBER,    -- For future use.
    in_debug_flag    NUMBER,
    out_tbl_name     OUT VARCHAR2
) AS

/* ----------------------------------------------------------------------

Change History

$Log: 2144_p_consolid_perf.sql,v $
Revision 1.18  2007/06/19 14:39:41  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.14  2006/09/20 21:04:08  makirk
Changed calls from p_ins_long_import_log to p_log

Revision 1.13  2006/09/15 13:09:33  joscho
For intermediate table cleanup, check 'T_PC%', not 'T_PC_%'
because intermediate file name was changed.

Revision 1.12  2006/09/14 16:12:43  joscho
Fixed double-up value of time aggregation  (S0350623).
Added missing p_get_time_inclause

Revision 1.11  2006/03/31 16:43:49  joscho
Update handling loc lev for cluster

Revision 1.10  2006/03/24 14:51:41  joscho
Support cluster

Revision 1.9  2006/01/09 19:22:57  makirk
Modified for creating temp tables under maxtemp

Revision 1.8  2006/01/02 19:18:59  joscho
Fixed the logic to validate the merch level

Revision 1.7  2005/12/01 19:43:24  joscho
Implemented T_CUBE_* tables and p_compose_query.

Revision 1.5  2005/11/23 18:41:31  Dirapa
compose query change


V5.6
5.6.0-012 03/05/04 Joseph   enh#1234

Description:


Parameters:
in_cube_id          : Cube ID for the worksheet.
in_pw_id            : Worksheet ID
in_tar_merch_lev    : Target Merch level
in_tar_loc_lev      : Target Location Level
in_tar_time_lev     : Target Time Level
in_merch_td_flag    : Merch Top Down or Bottom Up. 1 For TD, 0 for BU
in_loc_td_flag      : Location Top Down or Bottom Up. 1 For TD, 0 for BU
in_priority_hier    : Consolidation Priority 1 for Merch, 2 for Location
in_kpi_dv_id        : kpi_dv_id for T_CUBE_TIME table.
in_future2          : placeholder. Pass in -1.
in_future3          : placeholder. Pass in -1.
in_debug_flag       : Debug Flag. Application always passes as 0.
out_tbl_name        : Return table name to application.

-------------------------------------------------------------------------------- */

n_sqlnum      NUMBER(10,0);
t_proc_name   VARCHAR2(32)    := 'p_consolid_perf';
t_error_level VARCHAR2(6)     := 'info';
t_call        VARCHAR2(1000);
v_sql         VARCHAR2(1000)  := NULL;
v_sql2        VARCHAR2(255);
v_sql3        VARCHAR2(255);

t_row_cnt         NUMBER;
t_l_path          NUMBER(10,0);
t_loc_src_lev     NUMBER;
t_merch_src_lev   NUMBER;
t_time_src_lev    NUMBER;
t_l_tmpl_id       NUMBER(10,0);
t_m_tmpl_id       NUMBER(10,0);
t_t_tmpl_id       NUMBER(10,0);
t_aggr_col        VARCHAR2(30);
t_src_lv          NUMBER(6);
t_tar_lv          NUMBER(6);
t_tmpl_id         NUMBER(10,0);
t_dim_type        CHAR(1);
t_child_col_nam   VARCHAR2(30);
t_parent_col_nam  VARCHAR2(30);
t_tmpl_lv         NUMBER(6);
t_child_tbl_nam   VARCHAR2(30);
t_src_tbl_name    VARCHAR2(64);
t_tar_tbl_name    VARCHAR2(64);
t_final_tbl_name  VARCHAR2(64);
t_all_rows_flag   NUMBER(6);
t_level_incl_flag NUMBER(1,0);
t_dynamic_flag    NUMBER(1,0);
t_partial_flag    NUMBER(1,0);
t_filtered        NUMBER(1,0);
t_aggr_loc_flag   NUMBER          := 0;    -- No aggregation
t_aggr_merch_flag NUMBER          := 0;    -- No aggregation
t_aggr_time_flag  NUMBER          := 0;    -- No aggregation
t_cnt             NUMBER;
t_loop_cnt        NUMBER          := 0;
t_query_type      VARCHAR2(64);
t_tbl_name        VARCHAR2(64);
t_out_dml         VARCHAR2(1000);
t_out_join        VARCHAR2(1000);
t_out_where       VARCHAR2(4000);
t_out_option      VARCHAR2(100);
t_out_future1     VARCHAR2(100);
t_char_null       CHAR(1)         := NULL;
t_int_null        NUMBER          := NULL;
t_future_int      NUMBER(10)      := -1;
t_ignore_error    NUMBER          := 0;   -- 0 is Raise exception. 1 to ignore when called p_execute_ddl_sql
t_sql_stmt        VARCHAR2(100);
t_sql_col_list    VARCHAR2(100);
t_time_inclause   VARCHAR2(1000);



BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call :=                t_proc_name || '(' ||
    COALESCE(in_cube_id,       -123) || ',' ||
    COALESCE(in_pw_id,         -123) || ',' ||
    COALESCE(in_tar_merch_lev, -123) || ',' ||
    COALESCE(in_tar_loc_lev,   -123) || ',' ||
    COALESCE(in_tar_time_lev,  -123) || ',' ||
    COALESCE(in_merch_td_flag, -123) || ',' ||
    COALESCE(in_loc_td_flag,   -123) || ',' ||
    COALESCE(in_priority_hier, -123) || ',' ||
    COALESCE(in_kpi_dv_id,     -123) || ',' ||
    COALESCE(In_future2,       -123) || ',' ||
    COALESCE(In_future3,       -123) || ',' ||
    COALESCE(in_debug_flag,    0)    || ',' ||
    ' OUT )';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
COMMIT;

t_final_tbl_name := 't_pc'||in_pw_id;
out_tbl_name := 'maxtemp.'||t_final_tbl_name;

n_sqlnum := 1500;
BEGIN
    v_sql := 'DROP TABLE '||out_tbl_name;
    t_ignore_error := 1;

    maxtemp.p_exec_temp_ddl(t_ignore_error,v_sql,t_char_null,t_char_null,t_char_null,t_char_null,t_future_int,t_future_int,t_char_null);

    EXCEPTION
        WHEN OTHERS THEN NULL;
END;

n_sqlnum := 2000;
-- None of the parameters should be null.
IF in_cube_id       IS NULL OR
   in_pw_id         IS NULL OR
   in_tar_merch_lev IS NULL OR
   in_tar_loc_lev   IS NULL OR
   in_tar_time_lev  IS NULL OR
   in_merch_td_flag IS NULL OR
   in_loc_td_flag   IS NULL OR
   in_priority_hier IS NULL
THEN
    RAISE_APPLICATION_ERROR (-20001, 'No Null Parameter(s) allowed');
END IF;

n_sqlnum := 2050;
-- Not all three target level can be null
IF COALESCE(in_tar_merch_lev,0) IN (0,-1)
    AND COALESCE(in_tar_loc_lev,0) IN (0,-1)
    AND COALESCE(in_tar_time_lev,0) IN (0,-1) THEN
BEGIN
    RAISE_APPLICATION_ERROR (-20001, 'Not all three target levels can be NULL or -1');
END;
END IF;

n_sqlnum := 2100;
-- Target merch level should be >= 1 and <= 10

IF (in_tar_merch_lev < 1 OR in_tar_merch_lev > 10) AND in_tar_merch_lev <> -1 THEN
    RAISE_APPLICATION_ERROR (-20001, 'Target merch level should be >= 1 and <= 10 OR -1');
END IF;

-- Target location level should be >= 1 and <= 10 for non-cluster
-- or 1001, 1002 for cluster

IF in_tar_loc_lev <> -1 AND
   NOT (in_tar_loc_lev >= 1 AND in_tar_loc_lev <= 10) AND
   NOT (in_tar_loc_lev = 1001 OR in_tar_loc_lev = 1002) THEN
    RAISE_APPLICATION_ERROR (-20001, 'Target location level should be >= 1 and <= 10, or 1001/1002');
END IF;

-- Target time level >=47 and <=51
IF (in_tar_time_lev < 47 OR in_tar_time_lev > 51) AND in_tar_time_lev <> -1 THEN
    RAISE_APPLICATION_ERROR (-20001, 'Target time level should be >= 47 and <= 51 OR -1');
END IF;

---- END OF INPUT CHECK


n_sqlnum := 2150;
-- Set Aggregation flag

IF in_tar_loc_lev IS NOT NULL AND in_tar_loc_lev != -1 THEN
BEGIN
    t_aggr_loc_flag := 1;
END;
END IF;

IF in_tar_merch_lev IS NOT NULL AND in_tar_merch_lev != -1 THEN
BEGIN
    t_aggr_merch_flag := 1;
END;
END IF;

IF in_tar_time_lev IS NOT NULL AND in_tar_time_lev != -1 THEN
BEGIN
    t_aggr_time_flag := 1;
END;
END IF;


n_sqlnum := 2200;
-- Exit if all aggregation flags are 0. At least one aggregation should exist.

IF t_aggr_loc_flag = 0 AND t_aggr_merch_flag = 0 AND t_aggr_time_flag = 0 THEN
BEGIN
    RAISE_APPLICATION_ERROR (-20001, 'Aggregation is not requested for any hierarchy ');
END;
END IF;

n_sqlnum := 3000;
SELECT COUNT(*) INTO t_cnt
FROM maxdata.t_cube_loc
WHERE cube_id = in_cube_id;

IF t_cnt = 0 THEN
BEGIN
    -- Source level is wrong
    RAISE_APPLICATION_ERROR (-20001, 'No rows found in T_CUBE_LOC for the specified cube id');
END;
END IF;

n_sqlnum := 3050;
SELECT COUNT(*) INTO t_cnt
FROM maxdata.t_cube_merch
WHERE cube_id = in_cube_id;

IF t_cnt = 0 THEN
BEGIN
    -- Source level is wrong
    RAISE_APPLICATION_ERROR (-20001, 'No rows found in T_CUBE_MERCH for the specified cube id');
END;
END IF;

n_sqlnum := 3100;
SELECT COUNT(*) INTO t_cnt
FROM maxdata.t_cube_time
WHERE cube_id = in_cube_id;

IF t_cnt = 0 THEN
BEGIN
    -- Source level is wrong
    RAISE_APPLICATION_ERROR (-20001, 'No rows found in T_CUBE_TIME for the specified cube id');
END;
END IF;

-- If aggregation is requested for this hierarchy, then
-- there should be one and only one source level and
-- the source level should be lower than the target level.

IF t_aggr_loc_flag = 1 THEN
BEGIN

    -- distinct level from t_cube_loc should return only 1 level

    n_sqlnum := 3150;
    SELECT COUNT(DISTINCT l_lev) INTO t_cnt
    FROM maxdata.t_cube_loc
    WHERE cube_id = in_cube_id;

    IF t_cnt != 1 THEN
    BEGIN
        -- Source level is wrong
        RAISE_APPLICATION_ERROR (-20001, 'Given location source level is wrong');
    END;
    END IF;

    -- source level should be lower than the target level

    n_sqlnum := 3200;
    SELECT DISTINCT l_lev INTO t_loc_src_lev
    FROM maxdata.t_cube_loc
    WHERE cube_id = in_cube_id;

    -- Select loc_path_id from worksheet to determine cluster worksheet

    n_sqlnum := 3250;
    SELECT loc_path_id INTO t_l_path
    FROM maxdata.planworksheet
    WHERE planworksheet_id = in_pw_id;

    -- For cluster worksheet, comparing levels is not that simple like
    -- other hierarchy because cl_set=1001, cl=1002, store=4, etc.

    IF t_l_path > 1000 THEN
    BEGIN
        -- For store level, add 1000 so that we may compare with 1001 or 1002.

        IF t_loc_src_lev = 4 THEN
            t_loc_src_lev := t_loc_src_lev + 1000;
        END IF;
    END;
    END IF;

    IF in_tar_loc_lev >= t_loc_src_lev THEN
    BEGIN
        -- Target location level aggregation is not higher than source level
        RAISE_APPLICATION_ERROR (-20001,
            'Target location level aggregation is not higher than source level');
    END;
    END IF;

    -- Now, set the store level back to 4.

    IF t_l_path > 1000 THEN
    BEGIN
        IF t_loc_src_lev = 1004 THEN
            t_loc_src_lev := t_loc_src_lev - 1000;
        END IF;
    END;
    END IF;

END;
END IF;

IF t_aggr_merch_flag = 1 THEN
BEGIN
    -- Distinct level from t_cube_merch should return only 1 level

    n_sqlnum := 3300;
    SELECT COUNT(DISTINCT m_lev) INTO t_cnt
    FROM maxdata.t_cube_merch
    WHERE cube_id = in_cube_id;

    IF t_cnt != 1 THEN
    BEGIN
        -- Source level is wrong
        RAISE_APPLICATION_ERROR (-20001, 'Given merch source level is wrong');
    END;
    END IF;

    -- Source level should be lower than the target level

    n_sqlnum := 3350;
    SELECT DISTINCT m_lev INTO t_merch_src_lev
    FROM maxdata.t_cube_merch
    WHERE cube_id = in_cube_id;

    IF in_tar_merch_lev >= t_merch_src_lev THEN
    BEGIN
        -- Target merch level aggregation is not higher than source level
        RAISE_APPLICATION_ERROR (-20001,
            'Target merch level aggregation is not higher than source level');
    END;
    END IF;
END;
END IF; -- IF t_aggr_merch_flag = 1 THEN

IF t_aggr_time_flag = 1 THEN
BEGIN
    -- distinct level from t_cube_time should return only 1 level

    n_sqlnum := 3400;
    SELECT COUNT(DISTINCT t_lev) INTO t_cnt
    FROM maxdata.t_cube_time
    WHERE cube_id = in_cube_id;

    IF t_cnt != 1 THEN
    BEGIN
        -- Source level is wrong
        RAISE_APPLICATION_ERROR (-20001, 'Given time source level is wrong');
    END;
    END IF;

    -- source level should be lower than the target level

    n_sqlnum := 3450;
    SELECT DISTINCT t_lev INTO t_time_src_lev
    FROM maxdata.t_cube_time
    WHERE cube_id = in_cube_id;

    IF in_tar_time_lev >= t_time_src_lev THEN
    BEGIN
        -- Target time level aggregation is not higher than source level
        RAISE_APPLICATION_ERROR (-20001,
            'Target time level aggregation is not higher than source level');
    END;
    END IF;
END;
END IF; -- IF t_aggr_time_flag = 1 THEN


-- Get the source planworksheet ids.

v_sql:='TRUNCATE TABLE maxdata.t_pc_pw' ;

n_sqlnum := 3500;
EXECUTE IMMEDIATE v_sql;


-- Loop L,M,T and aggregate data IF necessary.

t_src_tbl_name := NULL;
t_tar_tbl_name := NULL;

FOR t_loop_cnt IN 1..3 LOOP

    n_sqlnum        := 3800;
    t_tmpl_id       := NULL;
    t_child_col_nam := NULL;
    t_cnt           := 0;

    IF t_loop_cnt = 1 THEN
    BEGIN
        t_dim_type := 'L';
        t_aggr_col := 'l_id';
        t_src_lv   := t_loc_src_lev;
        t_tar_lv   := in_tar_loc_lev;
        t_tmpl_id  := t_l_tmpl_id;
    END;
    ELSIF t_loop_cnt = 2 THEN
    BEGIN
        t_dim_type := 'M';
        t_aggr_col := 'm_id';
        t_src_lv   := t_merch_src_lev;
        t_tar_lv   := in_tar_merch_lev;
        t_tmpl_id  := t_m_tmpl_id;
    END;
    ELSE
    BEGIN
        t_dim_type := 'T';
        t_aggr_col := 't_id';
        t_src_lv   := t_time_src_lev;
        t_tar_lv   := in_tar_time_lev;
        t_tmpl_id  := t_t_tmpl_id;
    END;
    END IF;


    IF t_dim_type = 'L' AND t_aggr_loc_flag = 1 THEN
        t_tmpl_lv        := in_tar_loc_lev;
        IF t_l_path = 1 THEN -- regular loc hier.
            t_child_col_nam  := 'lv'|| t_src_lv ||'loc_id';
            t_parent_col_nam := 'lv'||in_tar_loc_lev||'loc_id';
            t_child_tbl_nam  := 'maxdata.lv'||t_src_lv ||'loc';
        ELSIF t_l_path > 1000 THEN -- cluster
            IF t_src_lv = 4 THEN
                t_child_tbl_nam := 'maxdata.clstr_str';
                t_child_col_nam := 'lvnloc_id';

                IF in_tar_loc_lev = 1001 THEN -- target is cluster set
                    t_parent_col_nam := 'clstr_st_id';
                ELSIF in_tar_loc_lev = 1002 then -- target is cluster level.
                    t_parent_col_nam := 'fnl_clstr_spc_id';
                ELSE
                    RAISE_APPLICATION_ERROR (-20001,'Unsupported location target level: '||in_tar_loc_lev);
                END IF;
            ELSIF t_src_lv = 1002 THEN
                t_child_tbl_nam := 'maxdata.clstr_spc';
                t_child_col_nam := 'clstr_spc_id';

                IF in_tar_loc_lev = 1001 THEN -- target is cluster set
                    t_parent_col_nam := 'clstr_st_id';
                ELSE
                    RAISE_APPLICATION_ERROR (-20001, 'Unsupported location target level: '||in_tar_loc_lev);
                END IF;
            ELSE
                -- Source level for clustering must be store or cluster level.
                RAISE_APPLICATION_ERROR (-20001,'Unsupported location source level for clustering: '||t_src_lv);
            END IF;
        ELSE -- t_l_path
             -- Alternate loc hier is not supported yet.
            RAISE_APPLICATION_ERROR (-20001, 'Unsupported hierarchy path: '||t_l_path);
            END IF; -- t_l_path  loc_flag
    ELSIF t_dim_type = 'M' AND t_aggr_merch_flag = 1 THEN
        t_child_col_nam := 'lv'||t_src_lv||'ctree_id';

        IF in_tar_merch_lev = 1 THEN
        BEGIN
            t_parent_col_nam := 'lv'||in_tar_merch_lev||'cmast_id';
        END;
        ELSE
        BEGIN
            t_parent_col_nam := 'lv'||in_tar_merch_lev||'ctree_id';
        END;
        END IF;

        -- in_src_merch_lev >= 2, so we may always use ctree.
        t_child_tbl_nam := 'maxdata.lv'||t_src_lv||'ctree';
        t_tmpl_lv       := in_tar_merch_lev + 10; -- template table has 10-based merch level.

    ELSIF t_dim_type = 'T' AND t_aggr_time_flag = 1 THEN

        t_child_col_nam  := 'lv'||(t_src_lv - 46)||'time_lkup_id';
        t_parent_col_nam := 'lv'||(in_tar_time_lev - 46) ||'time_lkup_id';
        t_child_tbl_nam  := 'maxapp.lv'||(t_src_lv - 46)||'time';
        t_tmpl_lv        := in_tar_time_lev;

    END IF; -- dim_type = T'

    IF t_child_col_nam IS NOT NULL THEN -- check IF aggr flag is on.

        v_sql:='TRUNCATE TABLE maxdata.t_pc_src_mem' ;
        n_sqlnum := 4100;
        EXECUTE IMMEDIATE v_sql;

        v_sql:='TRUNCATE TABLE maxdata.t_pc_all_children' ;
        n_sqlnum := 4200;
        EXECUTE IMMEDIATE v_sql;

        v_sql:='TRUNCATE TABLE maxdata.t_pc_parent_child' ;
        n_sqlnum := 4300;
        EXECUTE IMMEDIATE v_sql;

        v_sql:='TRUNCATE TABLE maxdata.t_pc_f_prnt_child' ;
        n_sqlnum := 4400;
        EXECUTE IMMEDIATE v_sql;

        v_sql:='TRUNCATE TABLE maxdata.t_pc_bop_eop_date' ;
        n_sqlnum := 4500;
        EXECUTE IMMEDIATE v_sql;

        v_sql:='TRUNCATE TABLE maxdata.t_pc_bop_tid' ;
        n_sqlnum := 4600;
        EXECUTE IMMEDIATE v_sql;

        v_sql:='TRUNCATE TABLE maxdata.t_pc_eop_tid' ;
        n_sqlnum := 4700;
        EXECUTE IMMEDIATE v_sql;

        v_sql:='TRUNCATE TABLE maxdata.t_pc_7key' ;
        n_sqlnum := 4800;
        EXECUTE IMMEDIATE v_sql;


        -- Set the input/output table names for aggregation.
        -- Make the prior loop's target table as the source table for this loop.

        IF t_src_tbl_name IS NULL THEN
            t_src_tbl_name  := 'maxdata.mplan_submit';
            t_all_rows_flag := 0;
        ELSE
            t_src_tbl_name  := t_tar_tbl_name;
            t_all_rows_flag := 1;
        END IF;

        -- Set a temporary intermediate table name for each aggregation.

        t_tar_tbl_name := 'maxtemp.t_pc'||in_pw_id||'aggr'||t_dim_type;

        -- Pull all source members from mplan.

        n_sqlnum := 4850;

        IF t_src_tbl_name = 'maxdata.mplan_submit' THEN
        BEGIN
            t_query_type  := 'MPLAN_CUBE_MANY';
            t_tbl_name    := 'MPLAN_SUBMIT';
            t_out_dml     := NULL;
            t_out_join    := NULL;
            t_out_where   := NULL;
            t_out_option  := NULL;
            t_out_future1 := NULL;

            maxdata.p_compose_query (
                t_query_type,
                in_cube_id,     -- cube id ,(-1) for NULL
                in_kpi_dv_id,       -- kpi dataversion id for time ,(-1) for NULL
                in_pw_id,       -- worksheet ID, (-1) for NULL
                t_tbl_name,     -- A FACT or MPLAN-split tablename.
                t_future_int,       -- (-1)
                t_future_int,       -- (-1)
                t_future_int,       -- (-1)
                t_future_int,       -- (-1)
                in_debug_flag,      -- (0)
                t_out_dml,
                t_out_join,
                t_out_where,
                t_out_option,       -- Reserved for SS...
                t_out_future1);

            -- Currently, time IN-clause is not generated by p_compose_query because of the app requirement.
            -- When this limitation is lifted, delete the following procedure call.

            n_sqlnum := 4855;

            maxdata.p_gen_time_inclause(
                                in_cube_id,
                                in_kpi_dv_id,
                                -1,
                                -1,
                                -1,
                                t_time_inclause);

            t_time_inclause := ' AND ' || t_time_inclause;

            -- Now, compose the complete SQL stmt

            n_sqlnum := 4860;

            t_sql_stmt := ' INSERT INTO maxdata.t_pc_src_mem ';
            t_sql_col_list := ' workplan_id, merch_level, merch_id, location_level, location_id, time_level, time_id, 0, 0  ';

            v_sql :=
                                t_sql_stmt||
                                t_out_dml ||
                                t_sql_col_list ||
                                t_out_join||
                                t_out_where||
                                t_time_inclause ||
                                t_out_option;

            IF in_debug_flag = 1 THEN
                n_sqlnum := 4865;
                maxdata.p_log (t_proc_name,t_error_level,t_sql_stmt,t_char_null,n_sqlnum);
                maxdata.p_log (t_proc_name,t_error_level,t_out_dml,t_char_null,n_sqlnum);
                maxdata.p_log (t_proc_name,t_error_level,t_sql_col_list,t_char_null,n_sqlnum);
                maxdata.p_log (t_proc_name,t_error_level,t_out_join,t_char_null,n_sqlnum);
                maxdata.p_log (t_proc_name,t_error_level,t_out_where,t_char_null,n_sqlnum);
                maxdata.p_log (t_proc_name,t_error_level,t_time_inclause,t_char_null,n_sqlnum);
                maxdata.p_log (t_proc_name,t_error_level,t_out_option,t_char_null,n_sqlnum);
            END IF;

            EXECUTE IMMEDIATE v_sql;
            COMMIT;

            -- Filter source members for TD/BU worksheet.

            n_sqlnum := 4870;

            maxdata.p_filter_td_bu (
                in_merch_td_flag,   -- Merch TD/BU flag. 1 for TD, 2 for BU.
                in_loc_td_flag,     -- Loc TD/BU flag. 1 for TD, 2 for BU.
                in_priority_hier,   -- 1 for LOC, 2 for MERCH
                in_debug_flag
            );
        END;
        ELSE
        BEGIN
            n_sqlnum := 4880;

            v_sql := ' INSERT INTO maxdata.t_pc_src_mem '||
                 ' SELECT NULL, merch_level, merch_id, location_level, location_id, time_level, time_id, 0, 0 ' ||
                 ' FROM '  || t_src_tbl_name ;

            IF in_debug_flag = 1 THEN
                v_sql2 := SUBSTR(v_sql,1,255);
                v_sql3 := SUBSTR(v_sql,256,255);
                maxdata.ins_import_log ('p_consolid_perf','info', v_sql2, v_sql3, n_sqlnum, NULL);
            END IF;

            EXECUTE IMMEDIATE v_sql;
            COMMIT;
        END;
        END IF;  --t_src_tbl_name = 'maxdata.mplan'

        -- If there is no rows, then something is wrong.

        n_sqlnum := 4890;

        SELECT COUNT(*) INTO t_row_cnt
        FROM maxdata.t_pc_src_mem
        WHERE rownum<=1;

        IF t_row_cnt = 0 THEN
            RAISE_APPLICATION_ERROR(-20001,'No source mplan data found. Loop count: '||t_cnt);
        END IF;

        n_sqlnum := 4900;
        v_sql := ' INSERT INTO maxdata.t_pc_all_children ' ||
             ' SELECT DISTINCT '|| t_aggr_col ||
             ' FROM maxdata.t_pc_src_mem';    -- t_tar_tbl_name

        IF in_debug_flag = 1 THEN
            v_sql2 := SUBSTR(v_sql,1,255);
            v_sql3 := SUBSTR(v_sql,256,255);
            maxdata.ins_import_log ('p_consolid_perf','info', v_sql2, v_sql3, n_sqlnum, NULL);
        END IF;

        EXECUTE IMMEDIATE v_sql;

        -- IF there is no rows, THEN something is wrong.

        SELECT COUNT(*) INTO t_row_cnt
        FROM maxdata.t_pc_all_children
        WHERE ROWNUM<=1;

        IF t_row_cnt = 0 THEN
            RAISE_APPLICATION_ERROR(-20001,'No source plan data found');
        END IF;

        v_sql := ' INSERT INTO maxdata.t_pc_parent_child (child_id, parent_id) ' ||
             ' SELECT ' ||  t_child_col_nam || ', ' || t_parent_col_nam ||
             ' FROM '|| t_child_tbl_nam || '  c, maxdata.t_pc_all_children a ' ||
             ' WHERE c.' || t_child_col_nam || ' = a.child_id';

        n_sqlnum := 5000;

        IF in_debug_flag = 1 THEN
            v_sql2 := SUBSTR(v_sql,1,255);
            v_sql3 := SUBSTR(v_sql,256,255);
            maxdata.ins_import_log ('p_consolid_perf','info', t_child_col_nam, t_parent_col_nam, n_sqlnum, NULL);
        END IF;

        EXECUTE IMMEDIATE v_sql;

        t_filtered := 0;

        IF t_tmpl_id <> 0 AND t_tmpl_id IS NOT NULL THEN

            n_sqlnum := 6000;
            v_sql := ' SELECT level_incl_flag, dynamic_flag, partial_flag ' ||
                 ' FROM maxdata.dimset_template_lev ' ||
                 ' WHERE template_id = :t_tmpl_id ' ||
                 ' AND level_number= :t_tmpl_lv';

            IF in_debug_flag = 1 THEN
                v_sql2 := SUBSTR(v_sql,1,255);
                v_sql3 := SUBSTR(v_sql,256,255);
                maxdata.ins_import_log ('p_consolid_perf','info', v_sql2, v_sql3, n_sqlnum , NULL);
                maxdata.ins_import_log ('p_consolid_perf','info', t_tmpl_id, t_tmpl_lv, n_sqlnum , NULL);
            END IF;

            EXECUTE IMMEDIATE v_sql INTO t_level_incl_flag, t_dynamic_flag, t_partial_flag
            USING t_tmpl_id, t_tmpl_lv;

            IF t_level_incl_flag = 1 AND (t_dynamic_flag = 1 or t_partial_flag = 1) THEN

                n_sqlnum := 6500;

                v_sql :=' INSERT INTO maxdata.t_pc_f_prnt_child  ' ||
                    ' SELECT pc.child_id, pc.parent_id, 0, 0 , 0, 0 ' || -- for eop/bop for time.
                    ' FROM maxdata.t_pc_parent_child pc,  maxdata.dimset_template_mem t ' ||
                    ' WHERE pc.parent_id = t.member_id ' ||
                    ' AND t.template_id = :t_tmpl_id ' ||
                    ' AND t.level_number= :t_tmpl_lv';

                IF in_debug_flag = 1 THEN
                    v_sql2 := SUBSTR(v_sql,1,255);
                    v_sql3 := SUBSTR(v_sql,256,255);
                    maxdata.ins_import_log ('p_consolid_perf','info', v_sql2, v_sql3, n_sqlnum, NULL);
                END IF;

                EXECUTE IMMEDIATE v_sql USING t_tmpl_id, t_tmpl_lv;

                t_filtered := 1;
            END IF;
        END IF;

        -- IF not filtered yet,
        -- THEN put all the rows to the filtered table.

        IF t_filtered = 0 THEN
            n_sqlnum:= 7000;
            v_sql := 'Unassigned';

            IF in_debug_flag = 1 THEN
                v_sql2 := SUBSTR(v_sql,1,255);
                v_sql3 := SUBSTR(v_sql,256,255);
                maxdata.ins_import_log ('p_consolid_perf','info', v_sql2, v_sql3, n_sqlnum, NULL);
            END IF;

            INSERT INTO maxdata.t_pc_f_prnt_child
            SELECT child_id,parent_id, 0, 0, 0 , 0
            FROM maxdata.t_pc_parent_child;

        END IF;

        -- For time aggregation, find eop/bop periods and set flags.

        IF t_dim_type = 'T' AND t_aggr_time_flag = 1 THEN

            -- Find the max/min (eop/bop) dates for each parent.

            v_sql := ' INSERT INTO maxdata.t_pc_bop_eop_date '||
                 ' SELECT min(lv'||(t_src_lv - 46)||'time_start_date),' ||
                    'max(lv'||(t_src_lv - 46)||'time_end_date)' ||
                 ' FROM maxapp.lv'||(t_src_lv - 46)||'time t, maxdata.t_pc_f_prnt_child f ' ||
                 ' WHERE t.lv'||(t_src_lv - 46)||'time_lkup_id = f.child_id ' ||
                 ' GROUP BY t.lv'||(in_tar_time_lev - 46)||'time_lkup_id';

            n_sqlnum:= 8000;

            IF in_debug_flag = 1 THEN
                v_sql2 := SUBSTR(v_sql,1,255);
                v_sql3 := SUBSTR(v_sql,256,255);
                maxdata.ins_import_log ('p_consolid_perf','info', v_sql2, v_sql3, n_sqlnum, NULL);
            END IF;

            EXECUTE IMMEDIATE v_sql;

            -- Find the corresponding time_lkup_id for eop/bop dates.

            v_sql := 'INSERT INTO maxdata.t_pc_bop_tid ' ||
                 ' SELECT lv'||(t_src_lv - 46)||'time_lkup_id ' ||
                 ' FROM maxapp.lv'||(t_src_lv - 46)||'time t, maxdata.t_pc_bop_eop_date d ' ||
                 ' WHERE t.lv'||(t_src_lv - 46)||'time_start_date = d.start_date';

            n_sqlnum:= 9000;

            IF in_debug_flag = 1 THEN
                v_sql2 := SUBSTR(v_sql,1,255);
                v_sql3 := SUBSTR(v_sql,256,255);
                maxdata.ins_import_log ('p_consolid_perf','info', v_sql2, v_sql3, n_sqlnum, NULL);
            END IF;

            EXECUTE IMMEDIATE v_sql;

            v_sql := 'INSERT INTO maxdata.t_pc_eop_tid '||
                ' SELECT lv'||(t_src_lv - 46)||'time_lkup_id ' ||
                ' FROM maxapp.lv'||(t_src_lv - 46)||'time t, maxdata.t_pc_bop_eop_date d ' ||
                ' WHERE t.lv'||(t_src_lv - 46)||'time_end_date = d.end_date';

            n_sqlnum:= 10000;

            IF in_debug_flag = 1 THEN
                v_sql2 := SUBSTR(v_sql,1,255);
                v_sql3 := SUBSTR(v_sql,256,255);
                maxdata.ins_import_log ('p_consolid_perf','info', v_sql2, v_sql3, n_sqlnum, NULL);
            END IF;

            EXECUTE IMMEDIATE v_sql;

            -- Set the bop/eop flags
            n_sqlnum:= 10500;

            UPDATE maxdata.t_pc_f_prnt_child f
            SET bop_flag = 1
            WHERE EXISTS (
                SELECT * FROM maxdata.t_pc_bop_tid b
                WHERE f.child_id = b.t_id);

            UPDATE maxdata.t_pc_f_prnt_child f
            SET eop_flag = 1
            WHERE EXISTS (
                SELECT * FROM maxdata.t_pc_eop_tid b
                WHERE f.child_id = b.t_id);
        END IF; -- aggr time.


        -- Build 7keys with parent id. For time,
        -- set period/cycle and eop/bop flags (0 is default).
        IF t_dim_type = 'T' THEN
            v_sql := 'INSERT INTO maxdata.t_pc_7key ' ||
                ' SELECT s.pw_id, s.m_lev, s.m_id, s.l_lev, s.l_id, s.t_lev, s.t_id, f.parent_id, ' ||
                        'f.bop_flag, f.eop_flag, 0, 0 ' ||
                ' FROM maxdata.t_pc_f_prnt_child f, ' ||
                     ' maxdata.t_pc_src_mem s, ' ||
                     ' maxapp.lv'||(in_tar_time_lev - 46)||'time t '||
                ' WHERE f.child_id = s.'||t_aggr_col||
                ' AND f.parent_id = t.lv'||(in_tar_time_lev - 46)||'time_lkup_id' ;
        ELSE
            v_sql := 'INSERT INTO maxdata.t_pc_7key ' ||
                ' SELECT s.pw_id, s.m_lev, s.m_id, s.l_lev, s.l_id, s.t_lev, s.t_id, f.parent_id, ' ||
                    'f.bop_flag, f.eop_flag, 0, 0 ' ||
                ' FROM maxdata.t_pc_f_prnt_child f, ' ||
                    ' maxdata.t_pc_src_mem s  ' ||
                ' WHERE f.child_id = s.'||t_aggr_col ;
        END IF;

        n_sqlnum:= 11000;

        IF in_debug_flag = 1 THEN
            v_sql2 := SUBSTR(v_sql,1,255);
            v_sql3 := SUBSTR(v_sql,256,255);
            maxdata.ins_import_log ('p_consolid_perf','info', v_sql2, v_sql3, n_sqlnum, NULL);
        END IF;

        EXECUTE IMMEDIATE v_sql;

        -- Aggregate data for each hierarchy.
        n_sqlnum:= 12000;

        maxdata.p_aggr_hier (
            'maxdata.t_pc_7key',
             t_src_tbl_name,
             t_tar_tbl_name,
             t_dim_type,    --'L', 'M', 'T'
             t_src_lv,
             t_tar_lv,
             t_all_rows_flag,
             in_debug_flag);

        -- Clean up prior source table.
        -- If the source table name is 't_pc%', then delete it. NOTE: it's not 't_pc_%'. See t_src_tbl_name setting.

        n_sqlnum:= 12100;
        IF in_debug_flag = 0 AND SUBSTR(t_src_tbl_name,1,12) = 'maxtemp.t_pc' THEN
            v_sql := ' DROP TABLE '||t_src_tbl_name;
            t_ignore_error := 1;
            maxtemp.p_exec_temp_ddl(t_ignore_error,v_sql,t_char_null,t_char_null,t_char_null,t_char_null,t_future_int,t_future_int,t_char_null);
        END IF;
    END IF; -- chil_col_name<>'' (this means the current dim should be aggregated)
END LOOP;

v_sql := ' RENAME ' ||SUBSTR(t_tar_tbl_name, 9) ||' TO ' || t_final_tbl_name; -- Ora-specific: do not qualify the source table (t_tar_tbl).

n_sqlnum:= 13000;
t_ignore_error := 0;
maxtemp.p_exec_temp_ddl(t_ignore_error,v_sql,t_char_null,t_char_null,t_char_null,t_char_null,t_future_int,t_future_int,t_char_null);

IF in_debug_flag=0 THEN
        n_sqlnum := 14000;
        v_sql:='TRUNCATE TABLE maxdata.t_pc_pw';
        EXECUTE IMMEDIATE v_sql;
        n_sqlnum := 14000;
        v_sql:='TRUNCATE TABLE maxdata.t_pc_src_mem';
        EXECUTE IMMEDIATE v_sql;
        n_sqlnum := 14100;
        v_sql:='TRUNCATE TABLE maxdata.t_pc_all_children';
        EXECUTE IMMEDIATE v_sql;
        n_sqlnum := 14200;
        v_sql:='TRUNCATE TABLE maxdata.t_pc_parent_child';
        EXECUTE IMMEDIATE v_sql;
        n_sqlnum := 14300;
        v_sql:='TRUNCATE TABLE maxdata.t_pc_f_prnt_child';
        EXECUTE IMMEDIATE v_sql;
        n_sqlnum := 14400;
        v_sql:='TRUNCATE TABLE maxdata.t_pc_bop_eop_date';
        EXECUTE IMMEDIATE v_sql;
        n_sqlnum := 14500;
        v_sql:='TRUNCATE TABLE maxdata.t_pc_bop_tid';
        EXECUTE IMMEDIATE v_sql;
        n_sqlnum := 14600;
        v_sql:='TRUNCATE TABLE maxdata.t_pc_eop_tid';
        EXECUTE IMMEDIATE v_sql;
        n_sqlnum := 14700;
        v_sql:='TRUNCATE TABLE maxdata.t_pc_7key';
        EXECUTE IMMEDIATE v_sql;
END IF;

EXCEPTION
   /* IF an exception is raised, close cursor before exiting. */
   WHEN OTHERS THEN
        --rollback;
        COMMIT; -- no harm.
        v_sql2 := SUBSTR(v_sql,1,255);
        v_sql3 := SUBSTR(v_sql,256,255);
        maxdata.ins_import_log ('p_consolid_perf','error' , v_sql2, v_sql3, n_sqlnum, NULL);
        COMMIT;

        v_sql := SQLERRM || ' ( ' || t_call  ||
                ', SQL#:' || n_sqlnum || ' )';
        -- Log the error message.
        v_sql2 := SUBSTR(v_sql,1,255);
        v_sql3 := SUBSTR(v_sql,256,255);
        maxdata.ins_import_log ('p_consolid_perf','error' , v_sql2, v_sql3, n_sqlnum, NULL);
        COMMIT;

        RAISE_APPLICATION_ERROR (-20001,v_sql);

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_CONSOLID_PERF" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_CONSOLID_PERF" TO "MAXUSER";
