--------------------------------------------------------
--  DDL for Procedure P_POP_T_CUBE_TBL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_POP_T_CUBE_TBL" 
        (in_planworksheet_id NUMBER,
         out_cube_id     OUT NOCOPY NUMBER,
         out_kpi_dv_id   OUT NOCOPY VARCHAR2,
         out_cell_cnt    OUT NOCOPY NUMBER)
AS
n_sqlnum                 NUMBER(10)      := 100;
t_call                   VARCHAR2(1000);
t_error_level            VARCHAR2(6)     := 'info';
t_proc_name              VARCHAR2(30)    := 'p_pop_t_cube_tbl';
t_sql2                   VARCHAR2(255);
t_sql3                   VARCHAR2(255);
v_sql                    VARCHAR2(4000)  := NULL;

v_53_week_adj_flg        NUMBER;
v_alternate_calendar_flg NUMBER;
v_cal_member_id          NUMBER;
v_cell_cnt               NUMBER;
v_cl_hist_status_cnt     NUMBER(6)       := 0;
v_cluster_set_id         NUMBER;
v_cnt                    NUMBER(6)       := 0;
v_conv_dim_lvl_num       NUMBER;
v_cube_id                NUMBER(10);
v_curr_level             NUMBER(6);
v_curr_offset            NUMBER(6)       := 0;
v_curr_tbl_id            VARCHAR2(30);
v_curr_time_id           NUMBER;
v_days_in_period         NUMBER(6);
v_dim_lvl_num            NUMBER;
v_dim_member_id          NUMBER;
v_dv_id                  NUMBER(6);
v_errmsg                 VARCHAR2(1000);
v_from_loc_id            NUMBER(10);
v_from_merch_id          NUMBER(10);
v_from_time_id           NUMBER(10);
v_kpi_cnt                NUMBER(6)       := 0;
v_kpi_dv_id              NUMBER(6);
v_kpi_sql                VARCHAR2(1000);
v_lev_cal_member_id      NUMBER;
v_loc_level_from         NUMBER(6);
v_loc_level_to           NUMBER(6);
v_loc_path_id            NUMBER(10);
v_loc_template_id        NUMBER(10);
v_merch_level_from       NUMBER(6);
v_merch_level_to         NUMBER(6);
v_merch_path_id          NUMBER(10);
v_merch_template_id      NUMBER(10);
v_out_error_code         NUMBER(10);
v_out_error_msg          VARCHAR2(1000);
v_out_resolved_id        NUMBER(10);
v_partial_flag           NUMBER          := 0;
v_partial_loc_flag       NUMBER(1);
v_partial_merch_flag     NUMBER(1);
v_planworksheet_id       NUMBER(10);
v_row_cnt                NUMBER(6)       := 0;
v_start_date_53week      NUMBER;
v_tbl_nm                 VARCHAR2(30);
v_time_level_from        NUMBER(6);
v_time_level_to          NUMBER(6);
v_time_path              NUMBER;
v_time_path_id           NUMBER(10);
v_time_template_id       NUMBER(10);
v_tmp_convert            NUMBER(6);
v_tmp_convert2           NUMBER(6);
v_worksheet_template_id  NUMBER(10);

stmt                     VARCHAR2(1000);

TYPE t_crs               IS REF CURSOR;
c_kpi                    t_crs;
c_time_id                t_crs;
c_dimset_mem             t_crs;
c_dim_level              t_crs;
c_lvl51_id               t_crs;

BEGIN
n_sqlnum := 100;
t_call := t_proc_name||'('||in_planworksheet_id||',out_cube_id,out_kpi_dv_id,out_cell_cnt)';
maxdata.ins_import_log (t_proc_name,t_error_level, t_call, null, null, null);

BEGIN
        n_sqlnum := 110;
        SELECT planworksheet_id,
               from_time_id,
               from_merch_id,
               from_loc_id,
               from_time_level,
               to_time_level,
               from_merch_level,
               to_merch_level,
               from_loc_level,
               to_loc_level,
               time_path_id,
               merch_path_id,
               loc_path_id,
               worksheet_template_id,
               time_template_id,
               merch_template_id,
               loc_template_id,
               partial_loc_flag,
               partial_merch_flag
          INTO v_planworksheet_id,
               v_from_time_id,
               v_from_merch_id,
               v_from_loc_id,
               v_time_level_from,
               v_time_level_to,
               v_merch_level_from,
               v_merch_level_to,
               v_loc_level_from,
               v_loc_level_to,
               v_time_path_id,
               v_merch_path_id,
               v_loc_path_id,
               v_worksheet_template_id,
               v_time_template_id,
               v_merch_template_id,
               v_loc_template_id,
               v_partial_loc_flag,
               v_partial_merch_flag
          FROM maxdata.planworksheet
         WHERE planworksheet_id = in_planworksheet_id;
         EXCEPTION
                WHEN NO_DATA_FOUND THEN
                        RAISE_APPLICATION_ERROR(-20001,'Planworksheet ID, '||in_planworksheet_id||', was not found in maxdata.planworksheet');
END;

v_cluster_set_id := v_loc_path_id - 1000;

/*
dbms_output.put_line('v_planworksheet_id='||v_planworksheet_id);
dbms_output.put_line('v_from_time_id='||v_from_time_id);
dbms_output.put_line('v_from_merch_id='||v_from_merch_id);
dbms_output.put_line('v_from_loc_id='||v_from_loc_id);
dbms_output.put_line('v_time_level_from='||v_time_level_from);
dbms_output.put_line('v_time_level_to='||v_time_level_to);
dbms_output.put_line('v_merch_level_from='||v_merch_level_from);
dbms_output.put_line('v_merch_level_to='||v_merch_level_to);
dbms_output.put_line('v_loc_level_from='||v_loc_level_from);
dbms_output.put_line('v_loc_level_to='||v_loc_level_to);
dbms_output.put_line('v_time_path_id='||v_time_path_id);
dbms_output.put_line('v_merch_path_id='||v_merch_path_id);
dbms_output.put_line('v_loc_path_id='||v_loc_path_id);
dbms_output.put_line('v_worksheet_template_id='||v_worksheet_template_id);
dbms_output.put_line('v_time_template_id='||v_time_template_id);
dbms_output.put_line('v_merch_template_id='||v_merch_template_id);
dbms_output.put_line('v_loc_template_id='||v_loc_template_id);
dbms_output.put_line('v_partial_loc_flag='||v_partial_loc_flag);
dbms_output.put_line('v_partial_merch_flag='||v_partial_merch_flag);
dbms_output.put_line('v_cluster_set_id='||v_cluster_set_id);
dbms_output.new_line;
*/

n_sqlnum := 120;
SELECT COUNT(*) INTO v_row_cnt FROM maxdata.clstr_st WHERE clstr_st_id = v_cluster_set_id;
IF v_row_cnt = 0 THEN
        n_sqlnum := 130;
        -- dbms_output.put_line('Cluster set, '||v_cluster_set_id||' not found in maxdata.clstr_set');
        RAISE_APPLICATION_ERROR(-20001,'Cluster set, '||v_cluster_set_id||' not found in maxdata.clstr_set');
END IF;

-- Found nothing, stop
IF v_planworksheet_id IS NULL THEN
        n_sqlnum := 140;
        v_sql := 'The planworksheet_id was not found in maxdata.planworksheet: '|| t_call ||', SQL#:' || n_sqlnum || ')';
        RAISE_APPLICATION_ERROR(-20001,v_sql);
END IF;

-- Set the 53'd week adjustment flag
-- True will mean keep the adj, false set it to 0
n_sqlnum := 150;
SELECT CASE NVL(TO_NUMBER(property_value),0) WHEN 1 THEN 0 ELSE 1 END
  INTO v_53_week_adj_flg
  FROM maxdata.t_application_property
 WHERE property_key='SkipFirstWeekOf53WeekCycle';

n_sqlnum := 160;
maxapp.p_get_next_key (94,1000,1,v_cube_id,v_errmsg);
-- dbms_output.put_line('v_cube_id='||v_cube_id);
n_sqlnum := 170;
v_tbl_nm := 'maxdata.t_cube_merch';
SELECT COUNT(*) INTO v_row_cnt FROM maxdata.t_cube_merch WHERE cube_id = v_cube_id;
IF v_row_cnt != 0 THEN RAISE_APPLICATION_ERROR(-20001,'The cube_id ('||v_cube_id||') already exists in '||v_tbl_nm||': '|| t_call ||', SQL#:' || n_sqlnum || ')');
END IF;

n_sqlnum := 180;
v_tbl_nm := 'maxdata.t_cube_loc';
SELECT COUNT(*) INTO v_row_cnt FROM maxdata.t_cube_loc WHERE cube_id = v_cube_id;
IF v_row_cnt != 0 THEN RAISE_APPLICATION_ERROR(-20001,'The cube_id ('||v_cube_id||') already exists in '||v_tbl_nm||': '|| t_call ||', SQL#:' || n_sqlnum || ')');
END IF;

n_sqlnum := 190;
v_tbl_nm := 'maxdata.t_cube_time';
SELECT COUNT(*) INTO v_row_cnt FROM maxdata.t_cube_time WHERE cube_id = v_cube_id;
IF v_row_cnt != 0 THEN RAISE_APPLICATION_ERROR(-20001,'The cube_id ('||v_cube_id||') already exists in '||v_tbl_nm||': '|| t_call ||', SQL#:' || n_sqlnum || ')');
END IF;


-- ------------------------- MERCH -------------------------
-- Set partial flag for this dimension
SELECT COUNT(*) INTO v_partial_flag FROM maxdata.dimset_template_lev WHERE template_id = v_merch_template_id AND partial_flag=1;

n_sqlnum := 200;
FOR v_curr_level IN REVERSE v_merch_level_from..v_merch_level_to
LOOP
BEGIN
        n_sqlnum := 210;
        -- dbms_output.put_line('MERCH loop, curr merch_level='||v_curr_level);
        IF v_curr_level = v_merch_level_to THEN -- BOTTOM LEVEL ONLY
                n_sqlnum := 220;
                IF v_partial_merch_flag = 0 AND v_partial_flag = 0 THEN  -- FULL
                        n_sqlnum := 230;
                        v_tmp_convert := v_merch_level_from-10;

                        IF v_merch_level_from = 11 THEN
                                n_sqlnum := 240;
                                -- str.Format(lvCMastTable, rootLevelId -10);
                                v_curr_tbl_id := 'lv'||v_tmp_convert||'cmast_id';
                        ELSE    -- str.Format(lvCTreeTable, rootLevelId -10);
                                n_sqlnum := 250;
                                v_curr_tbl_id := 'lv'||v_tmp_convert||'ctree_id';
                        END IF;

                        -- dbms_output.put_line('MERCH loop, tmp_convert='||v_tmp_convert||', v_curr_tbl_id='||v_curr_tbl_id);
                        n_sqlnum := 260;
                        IF v_curr_level = v_merch_level_from THEN
                                n_sqlnum := 270;
                                INSERT INTO maxdata.t_cube_merch (cube_id, m_lev, m_id)
                                VALUES (v_cube_id, v_merch_level_from-10, v_from_merch_id);
                        ELSIF v_curr_level = 20 THEN
                                n_sqlnum := 280;
                                v_sql :=
                                'INSERT INTO maxdata.t_cube_merch (cube_id, m_lev, m_id) '||
                                'SELECT '||v_cube_id||', 10, tree.lv10ctree_id '||
                                '  FROM maxdata.lv10ctree tree, '||
                                '       maxdata.lv10mast  mast '||
                                ' WHERE tree.lv10ctree_id = mast.lv10mast_id '||
                                '   AND mast.active_lkup=1 '||
                                '   AND mast.record_type IN (''M'',''L'') '||
                                '   AND tree.'||v_curr_tbl_id||' = '||v_from_merch_id;

                                -- dbms_output.put_line(n_sqlnum||','||v_sql);
                                n_sqlnum := 290;
                                EXECUTE IMMEDIATE v_sql;
                        ELSE
                                n_sqlnum := 300;
                                v_tmp_convert := v_curr_level-10;
                                IF v_tmp_convert=1 THEN
                                        v_sql :=
                                        'INSERT INTO maxdata.t_cube_merch (cube_id, m_lev, m_id) '||
                                        'SELECT '||v_cube_id||', '||v_tmp_convert||', lv'||v_tmp_convert||'cmast_id '||
                                        '  FROM maxdata.lv'||v_tmp_convert||'cmast '||
                                        ' WHERE '||v_curr_tbl_id||' = '||v_from_merch_id||
                                        '   AND record_type IN (''M'',''L'')';
                                ELSE
                                        v_sql :=
                                        'INSERT INTO maxdata.t_cube_merch (cube_id, m_lev, m_id) '||
                                        'SELECT '||v_cube_id||', '||v_tmp_convert||', lv'||v_tmp_convert||'ctree_id '||
                                        '  FROM maxdata.lv'||v_tmp_convert||'ctree t, '||
                                        '       maxdata.lv'||v_tmp_convert||'cmast m '||
                                        ' WHERE t.lv'||v_tmp_convert||'cmast_id=m.lv'||v_tmp_convert||'cmast_id '||
                                        '   AND t.'||v_curr_tbl_id||' = '||v_from_merch_id||
                                        '   AND m.record_type IN (''M'',''L'')';
                                END IF;

                                -- dbms_output.put_line(n_sqlnum||','||v_sql);
                                n_sqlnum := 310;
                                EXECUTE IMMEDIATE v_sql;
                        END IF;
                ELSE -- PARTIAL
                        n_sqlnum := 320;
                        INSERT INTO maxdata.t_cube_merch
                               (cube_id,
                                m_lev,
                                m_id)
                        SELECT v_cube_id,
                               v_curr_level-10,
                               member_id
                          FROM maxdata.dimset_template_mem
                         WHERE template_id  = v_merch_template_id
                           AND level_number = v_curr_level;
                END IF;
        ELSE -- ALL OTHER LEVELS (NOT BOTTOM)
                n_sqlnum := 330;
                IF v_curr_level = 11 THEN -- MCK, could this ever be true?
                        n_sqlnum := 340;
                        v_tmp_convert := v_curr_level-10;
                        v_curr_tbl_id := 'lv'||v_tmp_convert||'cmast_id';
                ELSE
                        n_sqlnum := 350;
                        v_tmp_convert := v_curr_level-10;
                        v_curr_tbl_id := 'lv'||v_tmp_convert||'ctree_id';
                END IF;

                n_sqlnum := 360;
                v_tmp_convert2 := v_tmp_convert+1;
                v_sql :=
                'INSERT INTO maxdata.t_cube_merch (cube_id, m_lev, m_id) '||
                'SELECT DISTINCT '||v_cube_id||','||v_tmp_convert||',a.'||v_curr_tbl_id||
                '  FROM maxdata.lv'||v_tmp_convert2||'ctree a, '||
                '       maxdata.t_cube_merch b '||
                ' WHERE a.lv'||v_tmp_convert2||'ctree_id = b.m_id '||
                '   AND b.m_lev='||v_tmp_convert2||
                '   AND b.cube_id = '||v_cube_id||
                '   AND a.'||v_curr_tbl_id||' IS NOT NULL';
                -- dbms_output.put_line(n_sqlnum||','||v_sql);

                n_sqlnum := 370;
                EXECUTE IMMEDIATE v_sql;
        END IF;
END;
END LOOP;


-- ------------------------- LOC -------------------------
-- Set partial flag for this dimension
SELECT count(*) INTO v_partial_flag FROM maxdata.dimset_template_lev WHERE template_id = v_loc_template_id AND partial_flag=1;
-- Since this is always used for clusters we can always use clstr_str
n_sqlnum := 380;
FOR v_curr_level IN REVERSE v_loc_level_from..v_loc_level_to
LOOP
        n_sqlnum := 390;
        -- dbms_output.put_line('LOC loop, loc_level='||v_curr_level);
        CASE v_curr_level
        WHEN 1001 THEN
                n_sqlnum := 400;
                INSERT INTO maxdata.t_cube_loc
                        (cube_id, l_lev, l_id)
                VALUES (v_cube_id, 1001, v_cluster_set_id);

                --v_row_cnt := SQL%ROWCOUNT;
                -- dbms_output.put_line('LOC, '||n_sqlnum||', count='||v_row_cnt);
                -- dbms_output.put_line('LOC, '||n_sqlnum||', v_cube_id='||v_cube_id||', v_cluster_set_id='||v_cluster_set_id);
        WHEN 1002 THEN
                n_sqlnum := 410;
                IF v_loc_level_from = 1001 THEN
                        n_sqlnum := 420;
                        IF v_partial_loc_flag != 0 AND v_partial_flag != 0 THEN -- PARTIAL
                                n_sqlnum := 430;

                                IF v_curr_level=v_loc_level_to THEN
                                        n_sqlnum := 440;
                                        INSERT INTO maxdata.t_cube_loc (cube_id, l_lev, l_id)
                                        SELECT v_cube_id, 1002, member_id
                                          FROM maxdata.dimset_template_mem
                                         WHERE template_id  = v_loc_template_id
                                           AND level_number = 1002;

                                         --v_row_cnt := SQL%ROWCOUNT;
                                         -- dbms_output.put_line('LOC, '||n_sqlnum||', count='||v_row_cnt);
                                ELSE
                                        n_sqlnum := 450;
                                        INSERT INTO maxdata.t_cube_loc (cube_id, l_lev, l_id)
                                        SELECT DISTINCT v_cube_id, 1002, fnl_clstr_spc_id
                                          FROM maxdata.clstr_str
                                         WHERE clstr_st_id = v_cluster_set_id
                                           AND fnl_clstr_spc_id != (SELECT clstr_spc_id
                                                                      FROM maxdata.clstr_spc
                                                                     WHERE clstr_grp_id = -1
                                                                       AND clstr_st_id  = v_cluster_set_id)
                                           AND lvnloc_id IN (SELECT l_id
                                                               FROM maxdata.t_cube_loc
                                                              WHERE cube_id = v_cube_id
                                                                AND l_lev=4);

                                        --v_row_cnt := SQL%ROWCOUNT;
                                        -- dbms_output.put_line('LOC, '||n_sqlnum||', count='||v_row_cnt);
                                END IF;
                        ELSE -- FULL
                                -- sql.Format(m_Sql_Cluster_1, cubeId, clusterSetId, clusterSetId);
                                n_sqlnum := 460;
                                INSERT INTO maxdata.t_cube_loc (cube_id, l_lev, l_id)
                                SELECT DISTINCT v_cube_id, 1002, fnl_clstr_spc_id
                                  FROM maxdata.clstr_str
                                 WHERE clstr_st_id = v_cluster_set_id
                                   AND fnl_clstr_spc_id != (SELECT clstr_spc_id
                                                              FROM maxdata.clstr_spc
                                                             WHERE clstr_grp_id = -1
                                                               AND clstr_st_id  = v_cluster_set_id);

                                 --v_row_cnt := SQL%ROWCOUNT;
                                 -- dbms_output.put_line('LOC, '||n_sqlnum||', count='||v_row_cnt);
                        END IF;
                ELSIF v_loc_level_from = 1002 THEN
                        n_sqlnum := 470;
                        INSERT INTO maxdata.t_cube_loc (cube_id, l_lev, l_id)
                        VALUES (v_cube_id, 1002, v_from_loc_id);

                        --v_row_cnt := SQL%ROWCOUNT;
                        -- dbms_output.put_line('LOC, '||n_sqlnum||', count='||v_row_cnt);
                END IF;
        ELSE -- 4, STORE
                n_sqlnum := 480;
                IF v_partial_loc_flag != 0 THEN -- PARTIAL
                        n_sqlnum := 490;
                        INSERT INTO maxdata.t_cube_loc (cube_id, l_lev, l_id)
                        SELECT v_cube_id, 1002, member_id
                          FROM maxdata.dimset_template_mem
                         WHERE template_id = v_loc_template_id
                           AND level_number = 1002;

                        --v_row_cnt := SQL%ROWCOUNT;
                        -- dbms_output.put_line('LOC, '||n_sqlnum||', count='||v_row_cnt);
                ELSE
                        CASE v_loc_level_from
                        WHEN 1001 THEN
                                n_sqlnum := 500;
                                IF v_from_loc_id = -1 THEN
                                        INSERT INTO maxdata.t_cube_loc (cube_id, l_lev, l_id)
                                        SELECT DISTINCT v_cube_id, 4, lvnloc_id
                                          FROM maxdata.clstr_str
                                         WHERE clstr_st_id = v_cluster_set_id
                                           AND fnl_clstr_spc_id IN (SELECT DISTINCT fnl_clstr_spc_id
                                                                      FROM maxdata.clstr_str
                                                                     WHERE clstr_st_id = v_cluster_set_id
                                                                       AND fnl_clstr_spc_id != (SELECT clstr_spc_id
                                                                                                  FROM maxdata.clstr_spc
                                                                                                 WHERE clstr_grp_id = -1
                                                                                                   AND clstr_st_id  = v_cluster_set_id));
                                        --v_row_cnt := SQL%ROWCOUNT;
                                        -- dbms_output.put_line('LOC, '||n_sqlnum||', count='||v_row_cnt);
                                ELSE
                                        n_sqlnum := 510;
                                        INSERT INTO maxdata.t_cube_loc (cube_id, l_lev, l_id)
                                        SELECT DISTINCT v_cube_id, 4, lvnloc_id
                                          FROM maxdata.clstr_str
                                         WHERE clstr_st_id = v_cluster_set_id
                                           AND fnl_clstr_spc_id IN (SELECT DISTINCT fnl_clstr_spc_id
                                                                      FROM maxdata.clstr_str
                                                                     WHERE clstr_st_id = v_cluster_set_id
                                                                       AND lvnloc_id   = v_from_loc_id);
                                         --v_row_cnt := SQL%ROWCOUNT;
                                         -- dbms_output.put_line('LOC, '||n_sqlnum||', count='||v_row_cnt);
                                END IF;
                        WHEN 1002 THEN
                                n_sqlnum := 520;
                                INSERT INTO maxdata.t_cube_loc (cube_id, l_lev, l_id)
                                SELECT DISTINCT v_cube_id, 4, lvnloc_id
                                  FROM maxdata.clstr_str
                                 WHERE clstr_st_id = v_cluster_set_id
                                   AND fnl_clstr_spc_id = v_from_loc_id;
                                --v_row_cnt := SQL%ROWCOUNT;
                                -- dbms_output.put_line('LOC, '||n_sqlnum||', count='||v_row_cnt);
                        WHEN 4 THEN
                                n_sqlnum := 530;
                                INSERT INTO maxdata.t_cube_loc (cube_id, l_lev, l_id)
                                VALUES (v_cube_id, 4, v_from_loc_id);
                                --v_row_cnt := SQL%ROWCOUNT;
                                -- dbms_output.put_line('LOC, '||n_sqlnum||', count='||v_row_cnt);
                        ELSE
                                NULL;
                        END CASE;
                END IF;
        END CASE;
END LOOP;


-- ------------------------- TIME -------------------------
n_sqlnum := 540;
out_cube_id := v_cube_id;

n_sqlnum := 550;
SELECT COUNT(*)
  INTO v_cl_hist_status_cnt
  FROM maxdata.cl_hist_status
 WHERE planworksheet_id = in_planworksheet_id;

n_sqlnum := 560;
IF v_cl_hist_status_cnt = 0 THEN
        -- CURSOR QUERY
        n_sqlnum := 570;
        DECLARE
        CURSOR c_kpi IS
                SELECT kpi_dv_id
                  FROM maxdata.wlkd_kpi_dataversion
                 WHERE kpi_dv_id IN (SELECT kpi_dv_id
                                       FROM maxdata.wltd_template_dataversion
                                      WHERE worksheet_template_id = v_worksheet_template_id
                                      UNION
                                     SELECT dv_id AS kpi_dv_id
                                       FROM maxapp.seeding_rules
                                      WHERE seeding_rule_id =
                                        (SELECT (CASE planwork_stat_id WHEN 0 THEN seeding_id ELSE reseed_seeding_id END) AS seeding_id
                                           FROM maxdata.planworksheet
                                          WHERE planworksheet_id=in_planworksheet_id))
                  AND dv_id IN (SELECT dv_id FROM maxapp.dataversion dv WHERE dv.entity IN (21,34))
                ORDER BY kpi_dv_id;
        BEGIN
                OPEN c_kpi;
                LOOP
                        FETCH c_kpi INTO v_kpi_dv_id;
                        EXIT WHEN c_kpi%NOTFOUND;

                        n_sqlnum := 580;
                        -- We know kpi isn't in cl_hist so insert it
                        -- dbms_output.put_line(n_sqlnum||', '||in_planworksheet_id||', '||v_dv_id);
                        maxdata.p_insert_cl_status (in_planworksheet_id,v_kpi_dv_id,-1,-1,-1);
                END LOOP;
        END;
        IF c_kpi%ISOPEN THEN  -- cursor is open
                CLOSE c_kpi;
        END IF;
ELSE
        n_sqlnum := 590;
        -- dbms_output.put_line(n_sqlnum||', '||in_planworksheet_id||', '||v_worksheet_template_id);
        -- CURSOR QUERY, FOR LOADING MISSING KPIS ONLY
        DECLARE
        CURSOR c_kpi IS
                SELECT kpi_dv_id
                  FROM maxdata.wlkd_kpi_dataversion
                 WHERE kpi_dv_id IN (SELECT kpi_dv_id
                                       FROM maxdata.wltd_template_dataversion
                                      WHERE worksheet_template_id = v_worksheet_template_id
                                      UNION
                                     SELECT dv_id AS kpi_dv_id
                                       FROM maxapp.seeding_rules
                                      WHERE seeding_rule_id =
                                        (SELECT (CASE planwork_stat_id WHEN 0 THEN seeding_id ELSE reseed_seeding_id END) AS seeding_id
                                           FROM maxdata.planworksheet
                                          WHERE planworksheet_id=in_planworksheet_id))
                   AND dv_id IN (SELECT dv_id FROM maxapp.dataversion dv WHERE dv.entity IN (21,34))
                   AND kpi_dv_id NOT IN (SELECT kpi_dv_id FROM maxdata.cl_hist_status WHERE planworksheet_id = in_planworksheet_id)
                 ORDER BY kpi_dv_id;
        BEGIN
                OPEN c_kpi;
                LOOP
                        FETCH c_kpi INTO v_kpi_dv_id;
                        EXIT WHEN c_kpi%notfound;
                        n_sqlnum := 600;
                        -- We know kpi isn't in cl_hist so insert it
                        maxdata.p_insert_cl_status (in_planworksheet_id,v_kpi_dv_id,-1,-1,-1);
                END LOOP;
        END;
        IF c_kpi%ISOPEN THEN  -- cursor is open
                CLOSE c_kpi;
        END IF;
END IF;

n_sqlnum := 610;
-- CURSOR QUERY
v_kpi_sql :=
'SELECT kpi.dv_id,dv.kpi_dv_id '||
'  FROM maxdata.cl_hist_status dv '||
'  JOIN maxdata.wlkd_kpi_dataversion kpi ON dv.kpi_dv_id=kpi.kpi_dv_id'||
' WHERE dv.planworksheet_id = '||in_planworksheet_id||
'   AND dv.status IN (''OB'',''NB'')'||
' ORDER BY kpi.dv_id ASC';

-- dbms_output.put_line(n_sqlnum||', '||v_kpi_sql);
-- Determine if this uses an alternate calendar
n_sqlnum := 620;
SELECT alternate_calendar_flg
  INTO v_alternate_calendar_flg
  FROM maxdata.planworksheet pw
  JOIN maxdata.wlwt_worksheet_template wt ON wt.worksheet_template_id=pw.worksheet_template_id
 WHERE pw.planworksheet_id = in_planworksheet_id;

-- If v_time_path_id = 50 then it follows normal processing regardless of flag
n_sqlnum := 630;
IF v_alternate_calendar_flg = 1 THEN
-- ===================================== ALTERNATE CALENDAR =====================================
--dbms_output.put_line(n_sqlnum||': TIME ALT CAL - v_time_path_id='||v_time_path_id||', worksheet_template_id='||v_worksheet_template_id);
n_sqlnum := 640;

        -- Loop on dv_ids
        stmt := v_kpi_sql;
        OPEN c_kpi FOR stmt;
        LOOP
                FETCH c_kpi INTO v_dv_id,v_kpi_dv_id;
                EXIT WHEN c_kpi%notfound;

                --dbms_output.put_line(n_sqlnum||': TIME ALT CAL - kpi_dv_id='||v_kpi_dv_id);
                -- Loop on all week levels and use these to obtain the other members

                n_sqlnum := 650;
                INSERT INTO maxdata.t_cube_time (cube_id, t_lev, t_id, kpi_dv_id)
                SELECT v_cube_id, level_number, member_id, v_kpi_dv_id
                  FROM maxdata.dimset_template_mem
                 WHERE level_number = 45
                   AND template_id = v_time_template_id;

                DECLARE
                CURSOR c_alt_lvl51_mem IS
                SELECT member_id
                  FROM maxdata.dimset_template_mem
                 WHERE template_id  = v_time_template_id
                   AND level_number = 51;

                BEGIN
                        OPEN c_alt_lvl51_mem;
                        LOOP
                                -- Grab the members from each lvlXtime table for each member_id
                                FETCH c_alt_lvl51_mem INTO v_cal_member_id;
                                EXIT WHEN c_alt_lvl51_mem%notfound;

                                --dbms_output.put_line(n_sqlnum||': TIME ALT CAL - calendar_member_id='||v_cal_member_id);
                                n_sqlnum := 660;
                                DECLARE
                                CURSOR c_dim_level IS
                                        SELECT DISTINCT(dimension_level_no)
                                          FROM wlla_level_assignment
                                         WHERE worksheet_template_id = v_worksheet_template_id
                                           AND dimension_type_cd     = 'T'
                                           AND dimension_level_no    < 52;
                                BEGIN
                                        OPEN c_dim_level;
                                        LOOP
                                        -- Grab the current level
                                        FETCH c_dim_level INTO v_curr_level;
                                        EXIT WHEN c_dim_level%notfound;

                                        --dbms_output.put_line(n_sqlnum||': TIME ALT CAL - dimension_level_no='||v_curr_level);
                                        IF v_curr_level <> 45 THEN
                                                n_sqlnum := 670;
                                                v_conv_dim_lvl_num := v_curr_level -46;

                                                n_sqlnum := 680;
                                                IF v_kpi_dv_id = 12 THEN -- TY, only need to use data in lv5time.  No time shifting needed
                                                        n_sqlnum := 690;
                                                        v_sql :=
                                                        ' INSERT INTO maxdata.t_cube_time (cube_id, t_lev, t_id, kpi_dv_id) '||
                                                        ' SELECT '||v_cube_id||','||v_curr_level||',t1.lv'||v_conv_dim_lvl_num||'time_lkup_id,'||v_kpi_dv_id||
                                                        '  FROM maxapp.lv5time t1 '||
                                                        ' WHERE t1.lv5time_lkup_id = '||v_cal_member_id ||
                                                        ' and 0 = (select count(*) from t_cube_time where t_lev =' || v_curr_level || ' and t_id = t1.lv'||v_conv_dim_lvl_num||'time_lkup_id '||
                                                        ' and kpi_dv_id=' || v_kpi_dv_id || 'and cube_id = '||v_cube_id||')';

                                                        --dbms_output.put_line(n_sqlnum||': '||v_sql);
                                                ELSE -- Calculate LLY, NY, etc
                                                        n_sqlnum := 700;
                                                        v_sql := 'SELECT lv'||v_conv_dim_lvl_num||'time_lkup_id from maxapp.lv5time where lv5time_lkup_id = ' || v_cal_member_id;
                                                        EXECUTE IMMEDIATE v_sql into v_lev_cal_member_id;

                                                        --dbms_output.put_line(n_sqlnum||': Input: '||v_kpi_dv_id||','||v_curr_level||','||v_lev_cal_member_id);
                                                        maxdata.p_calc_days_offset(v_kpi_dv_id,v_curr_level,v_lev_cal_member_id,v_days_in_period,v_start_date_53week);
                                                        --dbms_output.put_line(n_sqlnum||': Output: '||v_days_in_period||','||v_start_date_53week);

                                                        n_sqlnum := 710;
                                                        IF v_start_date_53week > 0 THEN
                                                                v_start_date_53week := 1;
                                                        END IF;

                                                        n_sqlnum := 720;
                                                        v_sql :=
                                                        'INSERT INTO maxdata.t_cube_time (cube_id, t_lev, t_id, kpi_dv_id) '||
                                                        'SELECT '||v_cube_id||','||v_curr_level||',t1.lv'||v_conv_dim_lvl_num||'time_lkup_id,'||v_kpi_dv_id||
                                                        '  FROM maxapp.lv'||v_conv_dim_lvl_num||'time t1'||
                                                        ' WHERE lv'||v_conv_dim_lvl_num||'time_start_date = '||
                                                        '      (SELECT lvX.lv'||v_conv_dim_lvl_num||'time_start_date+('||v_days_in_period||
                                                                        '-('||v_53_week_adj_flg||'*'||v_start_date_53week||'*7))   '||
                                                        '         FROM maxapp.lv'||v_conv_dim_lvl_num||'time  lvX '||
                                                        '         JOIN maxapp.lv5time lv5 ON lv5.lv'||v_conv_dim_lvl_num||'time_lkup_id=lvX.lv'||v_conv_dim_lvl_num||'time_lkup_id '||
                                                        '        WHERE lv5.lv5time_lkup_id='||v_cal_member_id||')'||
                                                        '   AND (SELECT COUNT(*) '||
                                                                 ' FROM t_cube_time '||
                                                                ' WHERE t_lev ='|| v_curr_level ||
                                                                  ' AND t_id = t1.lv'||v_conv_dim_lvl_num||'time_lkup_id '||
                                                                  ' AND kpi_dv_id='|| v_kpi_dv_id ||
                                                                  ' AND cube_id = '||v_cube_id||') = 0';

                                                        --dbms_output.put_line(n_sqlnum||': '||v_sql);
                                                END IF;
                                                EXECUTE IMMEDIATE v_sql;
                                        END IF;
                                        END LOOP; -- levels
                                END;

                                IF c_dim_level%ISOPEN THEN  -- cursor is open
                                        CLOSE c_dim_level;
                                END IF;
                        END LOOP; -- members

                        IF c_alt_lvl51_mem%ISOPEN THEN  -- cursor is open
                                CLOSE c_alt_lvl51_mem;
                        END IF;
                END;
        END LOOP; -- kpi_dv_ids

        IF c_kpi%ISOPEN THEN  -- cursor is open
                CLOSE c_kpi;
        END IF;
ELSE    -- ===================================== NORMAL CALENDAR =====================================
        n_sqlnum := 730;
        -- Set partial flag for this dimension
        SELECT COUNT(*)
          INTO v_partial_flag
          FROM maxdata.dimset_template_lev
         WHERE template_id  = v_time_template_id
           AND partial_flag = 1;

        IF v_time_template_id IS NOT NULL AND v_partial_flag != 0 THEN
        -- ===================================== PARTIAL =====================================
                DECLARE
                CURSOR c_dim_level IS
                        SELECT DISTINCT(dimension_level_no)
                          FROM maxdata.wlla_level_assignment
                         WHERE worksheet_template_id = v_worksheet_template_id
                           AND dimension_type_cd     = 'T'
                           AND dimension_level_no    < 52;
                BEGIN
                        OPEN c_dim_level;
                        LOOP
                        -- Grab the current level
                        FETCH c_dim_level INTO v_curr_level;
                        EXIT WHEN c_dim_level%notfound;

                        IF v_curr_level = v_time_level_to THEN -- At level 1
                        -- ===================================== TOP LEVEL - PARTIAL =====================================
                                -- KPI_DV_ID LOOP START
                                n_sqlnum := 740;
                                stmt := v_kpi_sql;
                                OPEN c_kpi FOR stmt;
                                LOOP
                                        FETCH c_kpi INTO v_dv_id,v_kpi_dv_id;
                                        EXIT WHEN c_kpi%notfound;
                                        --dbms_output.put_line('('||n_sqlnum||') ------------------- (1a) KPI='||v_dv_id||','||v_kpi_dv_id||' -------------------');

                                        IF v_dv_id = 12 THEN
                                                n_sqlnum := 750;
                                                -- dbms_output.put_line('(1a) TIME: '||n_sqlnum||', kpi='||v_dv_id||', time_template_id='||v_time_template_id||', curr_level='||v_curr_level);
                                                INSERT INTO maxdata.t_cube_time (cube_id, t_lev, t_id, kpi_dv_id)
                                                SELECT v_cube_id, v_curr_level, member_id, v_kpi_dv_id
                                                  FROM maxdata.dimset_template_mem
                                                 WHERE template_id  = v_time_template_id
                                                   AND level_number = v_curr_level;

                                                --v_row_cnt := SQL%ROWCOUNT;
                                                --dbms_output.put_line('('||n_sqlnum||') (1a) TIME: count='||v_row_cnt||', t_lev='||v_curr_level||', dv_id='||v_dv_id);
                                        ELSE
                                                n_sqlnum := 760;
                                                -- MEMBER_ID LOOP START
                                                DECLARE
                                                CURSOR c_dimset_mem IS
                                                        SELECT member_id,level_number
                                                          FROM maxdata.dimset_template_mem
                                                         WHERE template_id = v_time_template_id
                                                           AND level_number != 45
                                                           AND level_number < 52;
                                                BEGIN
                                                        OPEN c_dimset_mem;
                                                        LOOP
                                                                FETCH c_dimset_mem INTO v_dim_member_id,v_dim_lvl_num;
                                                                EXIT WHEN c_dimset_mem%notfound;

                                                                --dbms_output.put_line('('||n_sqlnum||') (1a) TIME: v_dim_lvl_num='||v_dim_lvl_num);

                                                                n_sqlnum := 770;
                                                                maxdata.p_calc_days_offset(v_kpi_dv_id,v_dim_lvl_num,v_dim_member_id,v_days_in_period,v_start_date_53week);
                                                                v_conv_dim_lvl_num := v_dim_lvl_num -46;

                                                                n_sqlnum := 780;
                                                                v_sql :=
                                                                'INSERT INTO maxdata.t_cube_time (cube_id, t_lev, t_id, kpi_dv_id) '||
                                                                'SELECT '||v_cube_id||','||v_dim_lvl_num||',lv'||v_conv_dim_lvl_num||'time_lkup_id,'||v_kpi_dv_id||
                                                                '  FROM maxapp.lv'||v_conv_dim_lvl_num||'time '||
                                                                ' WHERE lv'||v_conv_dim_lvl_num||'time_start_date = '||
                                                                '      (SELECT lvX.lv'||v_conv_dim_lvl_num||'time_start_date+('||v_days_in_period||
                                                                                '-('||v_53_week_adj_flg||'*'||v_start_date_53week||'*7))   '||
                                                                '         FROM maxapp.lv'||v_conv_dim_lvl_num||'time  lvX '||
                                                                '         JOIN maxapp.lv1time lv1 ON lv1.lv1time_lkup_id=lvX.lv1time_lkup_id '||
                                                                '        WHERE lvX.lv'||v_conv_dim_lvl_num||'time_lkup_id='||v_dim_member_id||')';
                                                                --dbms_output.put_line('(1a) TIME: '||n_sqlnum||', '||v_sql);

                                                                EXECUTE IMMEDIATE v_sql;
                                                                v_row_cnt := SQL%ROWCOUNT;

                                                                --dbms_output.put_line('('||n_sqlnum||') (1a) TIME: count='||v_row_cnt||', t_lev='||v_conv_dim_lvl_num);
                                                        END LOOP; -- MEMBERS
                                                END;

                                                IF c_dimset_mem%ISOPEN THEN  -- cursor is open
                                                        CLOSE c_dimset_mem;
                                                END IF;
                                        END IF;
                                END LOOP; -- KPIs

                                CLOSE c_kpi;

                                n_sqlnum:= 790;
                                v_row_cnt := SQL%ROWCOUNT;
                                --dbms_output.put_line('('||n_sqlnum||') (1a) TIME: '||n_sqlnum||', count='||v_row_cnt);
                        ELSE -- PARTIAL, LEVELS 2,3+
                                -- ===================================== OTHER LEVELS - PARTIAL =====================================
                                IF v_time_template_id IS NOT NULL THEN
                                        --dbms_output.put_line('('||n_sqlnum||') ------------------- (2) LEVEL '||v_curr_level||', PARTIAL ----------------------');
                                        n_sqlnum := 800;
                                        --dbms_output.put_line('('||n_sqlnum||') TIME, '||n_sqlnum||', v_sql='||v_kpi_sql);
                                        stmt := v_kpi_sql;
                                        OPEN c_kpi FOR stmt;
                                        LOOP
                                                FETCH c_kpi INTO v_dv_id,v_kpi_dv_id;
                                                EXIT WHEN c_kpi%notfound;

                                                --dbms_output.put_line('('||n_sqlnum||') ------------------- (2) DV_ID/KPI='||v_dv_id||'/'||v_kpi_dv_id||' -------------------');
                                                n_sqlnum := 810;
                                                IF v_time_path_id > 50 AND v_curr_level NOT IN (45,51) THEN
                                                        v_conv_dim_lvl_num := v_curr_level -46;
                                                        -- All parents will be calculated off of lvl51 here
                                                        DECLARE
                                                        CURSOR c_lvl51_id IS
                                                                SELECT member_id
                                                                  FROM maxdata.dimset_template_mem
                                                                 WHERE template_id  = v_time_template_id
                                                                   AND level_number = 51;
                                                        BEGIN
                                                                OPEN c_lvl51_id;
                                                                LOOP
                                                                        FETCH c_lvl51_id INTO v_curr_time_id;
                                                                        EXIT WHEN c_lvl51_id%NOTFOUND;

                                                                        n_sqlnum := 820;
                                                                        --dbms_output.put_line(n_sqlnum||': Input: '||v_kpi_dv_id||','||v_curr_level||','||v_cal_member_id);
                                                                        maxdata.p_calc_days_offset(v_kpi_dv_id,51,v_curr_time_id,v_days_in_period,v_start_date_53week);
                                                                        --dbms_output.put_line(n_sqlnum||': Output: '||v_days_in_period||','||v_start_date_53week);

                                                                        n_sqlnum := 830;
                                                                        IF v_start_date_53week > 0 THEN
                                                                                v_start_date_53week := 1;
                                                                        END IF;

                                                                        n_sqlnum := 840;
                                                                        -- Because children can share a parent it is ok to toss out duplicate parent_id by checking
                                                                        -- the t_cube_time table for that id
                                                                        v_sql :=
                                                                        'INSERT INTO maxdata.t_cube_time (cube_id, t_lev, t_id, kpi_dv_id) '||
                                                                        'SELECT '||v_cube_id||','||v_curr_level||',lv'||v_conv_dim_lvl_num||'time_lkup_id,'||v_kpi_dv_id||
                                                                        '  FROM maxapp.lv'||v_conv_dim_lvl_num||'time '||
                                                                        ' WHERE lv'||v_conv_dim_lvl_num||'time_lkup_id = '||
                                                                             ' (SELECT lv'||v_conv_dim_lvl_num||'time_lkup_id '||
                                                                                ' FROM maxapp.lv5time '||
                                                                                ' WHERE lv5time_start_date = (SELECT lv5time_start_date+('||v_days_in_period||'-('||v_53_week_adj_flg||'*'||v_start_date_53week||'*7))'||
                                                                                                              ' FROM maxapp.lv5time '||
                                                                                                            ' WHERE lv5time_lkup_id ='||v_curr_time_id||'))'||
                                                                        '   AND lv'||v_conv_dim_lvl_num||'time_lkup_id NOT IN (SELECT t_id FROM maxdata.t_cube_time WHERE cube_id ='||v_cube_id||')';
                                                                        --dbms_output.put_line('('||n_sqlnum||')  '||v_sql);

                                                                        EXECUTE IMMEDIATE v_sql;
                                                                END LOOP; -- MEMBERS
                                                        END;

                                                        IF c_lvl51_id%ISOPEN THEN  -- cursor is open
                                                                CLOSE c_lvl51_id;
                                                        END IF;
                                                ELSE
                                                        n_sqlnum := 850;
                                                        INSERT INTO maxdata.t_cube_time (cube_id, t_lev, t_id, kpi_dv_id)
                                                        SELECT v_cube_id, v_curr_level, member_id, v_kpi_dv_id
                                                          FROM maxdata.dimset_template_mem
                                                         WHERE template_id  = v_time_template_id
                                                           AND level_number = v_curr_level;
                                                END IF;

                                                --v_row_cnt := SQL%ROWCOUNT;
                                                --dbms_output.put_line('('||n_sqlnum||') (2) TIME: count='||v_row_cnt||', t_lev='||v_curr_level||', dv_id='||v_dv_id);
                                        END LOOP; -- KPIs

                                        CLOSE c_kpi;
                                        --v_row_cnt := SQL%ROWCOUNT;
                                        -- dbms_output.put_line('TIME, '||n_sqlnum||', count='||v_row_cnt);
                                END IF;
                        END IF;
                        END LOOP; -- PARTIAL LEVELS LOOP
                END;
                        IF c_dim_level%ISOPEN THEN  -- cursor is open
                                CLOSE c_dim_level;
                        END IF;
        ELSE -- FULL
        -- ===================================== FULL =====================================
                FOR v_curr_level IN REVERSE v_time_level_from..v_time_level_to
                LOOP
                        IF v_curr_level = v_time_level_to THEN -- At level 1
                                -- ===================================== TOP LEVEL - FULL =====================================
                                --dbms_output.put_line('('||n_sqlnum||') ------------------- (1b) LEVEL '||v_curr_level||', FULL -------------------');
                                -- dbms_output.put_line('CURRENT KPI SQL: '||n_sqlnum||', '||v_kpi_sql);

                                n_sqlnum := 860;
                                IF v_time_level_from= 45 and  v_time_template_id IS NULL THEN
                                        n_sqlnum := 870;
                                        RAISE_APPLICATION_ERROR(-20001,'Like Exception - Cannot have event in standard time dimension');
                                ELSE
                                        -- dbms_output.put_line('('||n_sqlnum||') ------------------- NOT 45, FULL -------------------');
                                        n_sqlnum := 880;
                                        v_tmp_convert  := v_curr_level-46;
                                        v_tmp_convert2 := v_time_level_from-46;

                                        n_sqlnum := 890;
                                        v_cnt := 0;
                                        stmt := v_kpi_sql;
                                        -- dbms_output.put_line('(1b) '||n_sqlnum||', '||v_kpi_sql);
                                        -- KPI_DV_ID LOOP START
                                        OPEN c_kpi FOR stmt;
                                        LOOP
                                                FETCH c_kpi INTO v_dv_id,v_kpi_dv_id;
                                                EXIT WHEN c_kpi%notfound;

                                                --dbms_output.put_line('('||n_sqlnum||') ------------------- (1c) KPI='||v_dv_id||','||v_kpi_dv_id||' -------------------');
                                                --dbms_output.put_line('('||n_sqlnum||') (1c) TIME: '||n_sqlnum||': v_cube_id='||v_cube_id||', v_curr_level='||v_curr_level||', v_time_level_from='||v_time_level_from||', v_dv_id='||v_dv_id);

                                                n_sqlnum := 900;
                                                v_sql :=
                                                'SELECT lv'||v_tmp_convert||'time_lkup_id'||
                                                '  FROM maxapp.lv'||v_tmp_convert||'time '||
                                                ' WHERE lv'||v_tmp_convert2||'time_lkup_id = '||v_from_time_id;
                                                --dbms_output.put_line('(1c) TIME: '||n_sqlnum||', '||v_sql);

                                                n_sqlnum := 910;
                                                stmt := v_sql;

                                                -- TIME_ID LOOP START
                                                OPEN c_time_id FOR stmt;
                                                LOOP
                                                        FETCH c_time_id INTO v_curr_time_id;
                                                        EXIT WHEN c_time_id%notfound;
                                                        BEGIN

                                                        n_sqlnum := 920;
                                                        maxdata.p_calc_days_offset(v_kpi_dv_id,v_curr_level,v_curr_time_id,v_days_in_period,v_start_date_53week);

                                                        n_sqlnum := 930;
                                                        v_sql :=
                                                        'SELECT lv'||v_tmp_convert||'time_lkup_id  '||
                                                        '  FROM maxapp.lv'||v_tmp_convert||'time   '||
                                                        ' WHERE lv'||v_tmp_convert||'time_start_date = '||
                                                        '      (SELECT lvX.lv'||v_tmp_convert||'time_start_date+('||v_days_in_period||
                                                                        '-('||v_53_week_adj_flg||'*'||v_start_date_53week||'*7))   '||
                                                        '         FROM maxapp.lv'||v_tmp_convert||'time  lvX '||
                                                        '         JOIN maxapp.lv1time lv1 ON lv1.lv1time_lkup_id=lvX.lv1time_lkup_id '||
                                                        '        WHERE lvX.lv'||v_tmp_convert||'time_lkup_id='||v_curr_time_id||')';

                                                        --dbms_output.put_line('(1d) TIME: '||n_sqlnum||', '||v_sql);

                                                        EXECUTE IMMEDIATE v_sql INTO v_out_resolved_id;

                                                        --v_row_cnt := SQL%ROWCOUNT;
                                                        -- dbms_output.put_line('(1d) TIME, '||n_sqlnum||', count='||v_row_cnt);
                                                        -- dbms_output.put_line('(1d) '||n_sqlnum||', v_curr_time_id='||v_curr_time_id||',v_curr_offset='||v_curr_offset||', v_53_week_adj_flg='||v_53_week_adj_flg);
                                                        -- dbms_output.put_line('(1d) '||n_sqlnum||', v_cube_id='||v_cube_id||', v_curr_level='||v_curr_level||', v_out_resolved_id='||v_out_resolved_id||', v_dv_id='||v_dv_id);

                                                        n_sqlnum := 940;
                                                        INSERT INTO maxdata.t_cube_time VALUES (v_cube_id, v_curr_level, v_out_resolved_id, v_kpi_dv_id);
                                                        --v_row_cnt := SQL%ROWCOUNT;
                                                        -- dbms_output.put_line('(1d) TIME, '||n_sqlnum||', count='||v_row_cnt);

                                                        n_sqlnum := 950;
                                                        v_curr_time_id := v_out_resolved_id;
                                                        END;
                                                END LOOP;
                                                CLOSE c_time_id;
                                        END LOOP;
                                        CLOSE c_kpi;
                                END IF;
                        ELSE -- ALL OTHER LEVELS, FULL
                                --dbms_output.put_line('('||n_sqlnum||') -------------------- (2) LEVEL '||v_curr_level||', FULL ------------------------');
                                n_sqlnum := 960;
                                v_tmp_convert  := v_curr_level-46;
                                v_tmp_convert2 := v_time_level_to-46;

                                IF v_tmp_convert > 0 THEN
                                        v_sql :=
                                        'INSERT INTO maxdata.t_cube_time (cube_id, t_lev, t_id, kpi_dv_id) '||
                                        'SELECT DISTINCT '||v_cube_id||','||
                                                         v_curr_level||','||
                                                         'a.lv'||v_tmp_convert||'time_lkup_id, '||
                                                         'b.kpi_dv_id '||
                                        '  FROM maxapp.lv'||v_tmp_convert2||'time a,'||
                                                'maxdata.t_cube_time b '||
                                        ' WHERE a.lv'||v_tmp_convert2||'time_lkup_id = b.t_id '||
                                        '   AND b.t_lev = '||v_time_level_to||
                                        '   AND b.cube_id = '||v_cube_id;
                                        --dbms_output.put_line('('||n_sqlnum||') TIME: '||n_sqlnum||': v_cube_id='||v_cube_id||', v_curr_level='||v_curr_level||', v_dv_id='||v_dv_id||', count='||v_row_cnt);
                                        --dbms_output.put_line(n_sqlnum||', '||v_sql);

                                        n_sqlnum := 970;
                                        EXECUTE IMMEDIATE v_sql;

                                       -- v_row_cnt := SQL%ROWCOUNT;
                                       --dbms_output.put_line('TIME, '||n_sqlnum||', count='||v_row_cnt);
                                END IF;
                        END IF; -- TOP CHECK
                END LOOP; -- FULL LEVELS LOOP
        END IF; -- PARTIAL VS FULL CHECK
END IF; -- ALTERNATE VS NORMAL CAL CHECK

COMMIT;
-- Get kpis for output param
n_sqlnum := 1000;
out_kpi_dv_id := NULL;
DECLARE CURSOR c_kpi_dv_id IS
        SELECT kpi_dv_id
          FROM maxdata.cl_hist_status
         WHERE planworksheet_id = in_planworksheet_id
           AND status IN ('OB','NB');
BEGIN
        n_sqlnum := 1010;
        FOR c1 IN c_kpi_dv_id LOOP
                n_sqlnum := 1020;
                out_kpi_dv_id := out_kpi_dv_id || c1.kpi_dv_id || ',';
        END LOOP;
END;

-- Drop trailing ","
n_sqlnum := 1030;
out_kpi_dv_id := SUBSTR(out_kpi_dv_id,1,LENGTH(out_kpi_dv_id)-1);

-- Return total cell count
n_sqlnum := 1040;
SELECT COUNT(*) INTO v_cell_cnt FROM maxdata.t_cube_merch WHERE cube_id = out_cube_id;
out_cell_cnt := v_cell_cnt;

n_sqlnum := 1050;
SELECT COUNT(*) INTO v_cell_cnt FROM maxdata.t_cube_loc WHERE cube_id = out_cube_id;
out_cell_cnt := out_cell_cnt * v_cell_cnt;

n_sqlnum := 1060;
-- Only want count for 1 kpi_dv_id out of all of the possible dv_ids
-- Use TY if available else just use the lowest dv_id
SELECT COUNT(*) INTO v_cell_cnt FROM maxdata.t_cube_time WHERE cube_id = out_cube_id AND kpi_dv_id = 12;

IF v_cell_cnt = 0 THEN
        n_sqlnum := 1070;
        SELECT COUNT(*)
          INTO v_cell_cnt
          FROM maxdata.t_cube_time
         WHERE cube_id = out_cube_id
           AND kpi_dv_id = (SELECT MIN(kpi_dv_id) FROM maxdata.t_cube_time WHERE cube_id = out_cube_id);
END IF;

n_sqlnum := 1080;
out_cell_cnt := out_cell_cnt * v_cell_cnt;

EXCEPTION
        WHEN OTHERS THEN
                ROLLBACK;
                IF v_sql IS NOT NULL THEN
                        t_error_level := 'info';
                        t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
                        t_sql3 := SUBSTR(v_sql,1,255);
                        maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
                END IF;
                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM||' ('||t_call||', SQL#:'||n_sqlnum||')';
                t_sql2 := SUBSTR(v_sql,1,255);
                t_sql3 := SUBSTR(v_sql,256,255);
                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
                COMMIT;
                RAISE_APPLICATION_ERROR(-20001,v_sql);
END p_pop_t_cube_tbl;

/

  GRANT EXECUTE ON "MAXDATA"."P_POP_T_CUBE_TBL" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_POP_T_CUBE_TBL" TO "MAXUSER";
