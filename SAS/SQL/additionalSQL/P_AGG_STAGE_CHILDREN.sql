--------------------------------------------------------
--  DDL for Procedure P_AGG_STAGE_CHILDREN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_STAGE_CHILDREN" (
    in_target_tbl   VARCHAR2,
    in_parent_level NUMBER,
    in_parent_id    NUMBER,
    in_dim_ord_cd   VARCHAR2, --'ML','-M','LM','-L'
    in_analyze_pct  NUMBER
)
AS    
/*
------------------------------------------------------------------------------
$Log: 5240_IDA_p_agg_stage_children.sql,v $
Revision 1.1.2.1.2.3  2009/05/13 20:02:15  anchan
PERFORMANCE: use separate work tables for each of the M-L-T aggregation dimensions.

Revision 1.1.2.1.2.2  2009/03/26 19:40:09  anchan
FIXID S0567466: expanded string size 8000x2=16000

Revision 1.1.2.1.2.1  2009/03/25 16:13:01  anchan
FIXID S0567466: BOP/EOP now handled by agg_rule proc.

Revision 1.1.2.1  2008/11/26 17:30:14  anchan
FIXID : BASELINE check-in

------------------------------------------------------------------------------
*/
    t_source_tbl   VARCHAR2(30);
--  t_work_tbl_into VARCHAR2(30); --either _merch or _loc--
    t_work_tbl_merch VARCHAR2(30);
    t_work_tbl_loc VARCHAR2(30);
    t_work_tbl_time VARCHAR2(30);
    t_active_cmd   VARCHAR2(10);
    t_base_m_level NUMBER(2); --relative level 1~10--
    t_base_l_level NUMBER(1); --relative level 1~4--
    t_base_t_level NUMBER(2);
    t_simulate_sec NUMBER(10);
    t_sampling_level NUMBER(2); --absolute level of LVxCTREE(11~20)/LVxLOC(1~4)--
    t_sampling_factor NUMBER(2);
    t_partition_nm VARCHAR2(30);
    t_base_row_cnt NUMBER(10):=0;
    t_col_list VARCHAR2(8000);
    t_agg_list VARCHAR2(8000);
    t_query_hint VARCHAR2(100);
    t_table_hint VARCHAR2(500);
    t_hint_list VARCHAR2(1000);
    t_exists_flg NUMBER(1);
    t_groupby_clause VARCHAR2(100);
    t_simulate_clause VARCHAR2(100):=' ';
    t_sampling_clause VARCHAR2(100):=' ';
--    t_long_txt LONG;
    t_proc_name VARCHAR2(30):='p_agg_stage_children';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(16000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg     VARCHAR2(1000) := NULL;
 
    CURSOR c_children_id IS
        SELECT * FROM maxdata.v_time_children
        WHERE table_nm=in_target_tbl 
        AND parent_level=in_parent_level AND parent_id=in_parent_id
        AND base_loaded_flg IN(-1,1) --doesn't matter whether or not aggregated_flg=1--
        ORDER BY time_id;

BEGIN
n_sqlnum:=10000;
t_call := t_proc_name || ' ( ' ||
    in_target_tbl || ',' ||
    in_parent_level || ',' ||
    in_parent_id || ',' ||
    in_dim_ord_cd || ',' ||
    in_analyze_pct || 
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, NULL, n_sqlnum);

n_sqlnum:=10100;
maxdata.p_agg_logger(in_target_tbl,in_parent_level,in_parent_id,'STAGE-C','START');

n_sqlnum:=11000;
SELECT source_nm,active_cmd,simulate_sec,sampling_level,sampling_factor,
    (base_merch_level-10),base_location_level,base_time_level
INTO t_source_tbl,t_active_cmd,t_simulate_sec,t_sampling_level,t_sampling_factor,
    t_base_m_level,t_base_l_level,t_base_t_level
FROM maxdata.agac_aggregate_control,maxdata.v_base_level
WHERE table_nm=in_target_tbl;

n_sqlnum:=11100;
t_work_tbl_merch :=t_source_tbl||'#MERCH';
t_work_tbl_loc :=t_source_tbl||'#LOC';
t_work_tbl_time :=t_source_tbl||'#TIME';

n_sqlnum:=11200;
IF(t_active_cmd='SIMULATE')THEN
    dbms_lock.sleep(t_simulate_sec);
    t_simulate_clause:=' AND 1=0 AND m.time_level=0 /*SIMULATE MODE, DO NOTHING*/ ';
END IF;

n_sqlnum:=12000;
--Check to make sure there are no rows from a previously failed run--
v_sql:='SELECT COUNT(*) FROM '||t_work_tbl_merch||' WHERE ROWNUM=1';
EXECUTE IMMEDIATE v_sql INTO t_exists_flg;
IF(t_exists_flg>0)THEN
    t_error_msg:='The WORK table '||t_work_tbl_merch||' must be empty. A previous run may have failed.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=12100;
--Check to make sure there are no rows from a previously failed run--
v_sql:='SELECT COUNT(*) FROM '||t_work_tbl_loc||' WHERE ROWNUM=1';
EXECUTE IMMEDIATE v_sql INTO t_exists_flg;
IF(t_exists_flg>0)THEN
    t_error_msg:='The WORK table '||t_work_tbl_loc||' must be empty. A previous run may have failed.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=13000;
v_sql:='ALTER SESSION ENABLE PARALLEL DML';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=14000;
v_sql:='TRUNCATE TABLE '||t_work_tbl_time||' REUSE STORAGE';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=16000;
FOR r_child IN c_children_id  
LOOP
    IF(r_child.base_loaded_flg=-1)THEN 
        t_error_msg:='Required child partition not loaded yet:('||+(r_child.time_level)||','||+(r_child.time_id)||')';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;
END LOOP;

n_sqlnum:=17000;
IF(t_active_cmd='SAMPLE')THEN
    n_sqlnum:=17100;
    IF(t_sampling_level BETWEEN 11 AND 20)THEN --MERCH--
        n_sqlnum:=17200;
           t_sampling_clause:=' JOIN lv'||+(t_base_m_level)||'ctree dx '
       ||' ON(m.merch_id=dx.lv'||+(t_base_m_level)||'ctree_id '
       ||' AND MOD(dx.lv'||+(t_sampling_level-10)||'ctree_id,' ||+(t_sampling_factor)||')=0 ) '; 
    ELSE--(t_sampling_level BETWEEN  1 AND 4) --LOC--
        n_sqlnum:=17300;
        t_sampling_clause:=' JOIN lv'||+(t_base_l_level)||'loc dx '
       ||' ON(m.location_id=dx.lv'||+(t_base_l_level)||'loc_id '
       ||' AND MOD(dx.lv'||+(t_sampling_level)||'loc_id,' ||+(t_sampling_factor)||')=0 ) '; 
    END IF;
    maxdata.p_get_query_hint('AGG_LOAD_S','[M][DX]',t_table_hint,t_query_hint);
ELSE
    n_sqlnum:=17400;
    maxdata.p_get_query_hint('AGG_LOAD_S','[M]',t_table_hint,t_query_hint);
END IF;
t_hint_list:=t_query_hint||' '||t_table_hint;

n_sqlnum:=18000;
FOR r_child IN c_children_id  
LOOP
    n_sqlnum:=18100;
    --source and partition names must be the same--
    t_partition_nm:= f_partition_nm(in_target_tbl,r_child.time_level,r_child.time_id);    

    n_sqlnum:=18200;
    SELECT SIGN(num_rows) INTO t_exists_flg --0 if not yet analyzed-- 
    FROM user_tab_partitions
    WHERE table_name=in_target_tbl 
    AND partition_name=t_partition_nm;

    --All source partitions must already have been analyzed for proper execution plan--    
    IF(t_exists_flg=0)AND(t_active_cmd!='SIMULATE')THEN --stats don't exist if not analyzed yet--   
        n_sqlnum:=18300;
        --Do a quick-and-dirty analyze, to generate proper execution plan--  
        dbms_stats.gather_table_stats(ownname=>'MAXDATA',tabname=>in_target_tbl,partname=>t_partition_nm,
                  granularity=>'PARTITION',block_sample=>TRUE,estimate_percent =>1,method_opt=>'FOR ALL COLUMNS SIZE 1');
    END IF;

    n_sqlnum:=18400;
    --copy each child partition to merch-partioned temp table--
    v_sql:='INSERT /*+APPEND {STAGE}*/ INTO '||t_work_tbl_time
        ||' SELECT /*+'||t_hint_list||'*/  m.*  '
        ||' FROM '||in_target_tbl||' m'
        ||t_sampling_clause
        ||' WHERE (merch_level='||+(t_base_m_level)||' AND location_level='||+(t_base_l_level)||')'
        ||' AND location_id>0 AND merch_id>0' --filter out bogus rows--
        ||' AND (time_level='||+(r_child.time_level)||' AND time_id='||+(r_child.time_id)||')'
        ||t_simulate_clause;
    EXECUTE IMMEDIATE v_sql;
    t_base_row_cnt:=t_base_row_cnt+SQL%ROWCOUNT;
    IF(SQL%ROWCOUNT=0)AND(t_active_cmd!='SIMULATE')THEN
        t_error_msg:='No base data found in partition for ('||+(r_child.time_level)|| ','||+(r_child.time_id)||').';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;
    COMMIT;

END LOOP;

n_sqlnum:=20000;
maxdata.p_agg_logger(in_target_tbl,in_parent_level,in_parent_id,'STAGE-C','FINISH',
    f_parallel_deg(t_table_hint,'M'),t_base_row_cnt,-1);

EXCEPTION
WHEN OTHERS THEN
    t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
    ROLLBACK;
    maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
