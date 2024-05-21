--------------------------------------------------------
--  DDL for Procedure P_AGG_TIME
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_TIME" (
    in_target_tbl   VARCHAR2,
    in_parent_level NUMBER,
    in_parent_id    NUMBER,
    in_dim_ord_cd   VARCHAR2, --'ML','-M','LM','-L'
    in_analyze_pct  NUMBER
)
AS    
/*
------------------------------------------------------------------------------
$Log: 5240_IDA_p_agg_time.sql,v $
Revision 1.1.2.1  2009/05/13 17:56:08  anchan
PERFORMANCE: use separate tables for each of the M-L-T aggregation dimensions.

------------------------------------------------------------------------------
*/
    t_source_tbl   VARCHAR2(30);
    t_work_tbl_into VARCHAR2(30); --either _merch or _loc--
    t_work_tbl_merch VARCHAR2(30);
    t_work_tbl_loc VARCHAR2(30);
    t_work_tbl_time VARCHAR2(30);
    t_active_cmd   VARCHAR2(10);
    t_base_m_level NUMBER(2);
    t_base_l_level NUMBER(1);
    t_base_t_level NUMBER(2);
    t_simulate_sec NUMBER(10);
    t_sampling_level NUMBER(2);  --of LVxCTREE--
    t_sampling_factor NUMBER(2);
    t_first_id NUMBER(10):=0;
    t_last_id  NUMBER(10):=0;
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
    t_proc_name VARCHAR2(30):='p_agg_time';
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

    CURSOR c_temp_part IS
        SELECT partition_name 
        FROM user_tab_partitions 
        WHERE table_name=t_work_tbl_time;

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
maxdata.p_agg_logger(in_target_tbl,in_parent_level,in_parent_id,'TIME','START');

n_sqlnum:=11000;
SELECT source_nm,active_cmd,simulate_sec,(sampling_level-10),sampling_factor,
    (base_merch_level-10),base_location_level,base_time_level
INTO t_source_tbl,t_active_cmd,t_simulate_sec,t_sampling_level,t_sampling_factor,
    t_base_m_level,t_base_l_level,t_base_t_level
FROM maxdata.agac_aggregate_control,maxdata.v_base_level
WHERE table_nm=in_target_tbl;

n_sqlnum:=11100;
t_work_tbl_merch :=t_source_tbl||'#MERCH';
t_work_tbl_loc :=t_source_tbl||'#LOC';
t_work_tbl_time :=t_source_tbl||'#TIME';
IF in_dim_ord_cd IN('ML','-L') THEN
    t_work_tbl_into:=t_work_tbl_merch;
ELSE--IN('LM','-M')
    t_work_tbl_into:=t_work_tbl_loc;
END IF;

n_sqlnum:=11100;
IF(t_active_cmd='SIMULATE')THEN
    dbms_lock.sleep(t_simulate_sec);
    t_simulate_clause:=' AND 1=0 AND m.time_level=0 /*SIMULATE MODE, DO NOTHING*/ ';
END IF;

n_sqlnum:=13000;
v_sql:='ALTER SESSION ENABLE PARALLEL DML';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=16000;
--Find the first and last children time_id's:--
FOR r_child IN c_children_id  
LOOP
    IF (t_first_id=0)OR(r_child.time_id<t_first_id) THEN
        t_first_id:=r_child.time_id;
    END IF;
    IF (r_child.time_id>t_last_id)THEN
        t_last_id:=r_child.time_id;
    END IF;
END LOOP;

n_sqlnum:=17000;
maxdata.p_agg_col_rule (t_source_tbl,'T',t_base_m_level,t_base_l_level,
    in_parent_level,in_parent_id,0,t_col_list,t_agg_list,t_first_id,t_last_id);

n_sqlnum:=19000;
IF(in_analyze_pct>0)AND(t_active_cmd!='SIMULATE')THEN
    --All temp partitions must be analyzed for proper execution plan--    
    dbms_stats.gather_table_stats(ownname=>'MAXDATA',tabname=>t_work_tbl_time,
        granularity=>'ALL',block_sample=>TRUE,estimate_percent =>in_analyze_pct,
        method_opt=>'FOR COLUMNS SIZE 1 merch_level,merch_id,location_level,location_id');
END IF;

n_sqlnum:=19100;
maxdata.p_get_query_hint('AGG_LOAD_T','[M]',t_table_hint,t_query_hint);
t_hint_list:=t_query_hint||' '||t_table_hint;

n_sqlnum:=20000;
FOR r_part IN c_temp_part  
LOOP
    n_sqlnum:=20100;
    --perform GROUP-BY on each of the time-partitions, and copy to work table(LOC or MERCH)--
    --faster to do GROUP-BY on each of the small partitions; rather than one monolithic table--
    v_sql:='INSERT /*+APPEND {TIME}*/ INTO '||t_work_tbl_into
        ||'(merch_level,merch_id,location_level,location_id,time_level,time_id,'||t_col_list||')'
        ||' SELECT /*+'||t_hint_list||'*/ '
        ||+(t_base_m_level)||',m.merch_id,'||+(t_base_l_level)||',m.location_id,'
        ||+(in_parent_level)||','||+(in_parent_id)||','||t_agg_list
        ||' FROM '||t_work_tbl_time||' PARTITION('||r_part.partition_name||') m '
        ||' WHERE(merch_level='||+(t_base_m_level)||' AND location_level='||+(t_base_l_level)||')'
        ||' AND location_id>0 AND merch_id>0' --filter out bogus rows--
        ||t_simulate_clause
        ||' GROUP BY merch_id,location_id ';
--        t_long_txt:=SUBSTR(v_sql,1,4000);
--        INSERT INTO T_DEBUG_SQL(proc_nm,long_msg) VALUES(t_proc_name,t_long_txt);
--        t_long_txt:=SUBSTR(v_sql,4001,4000);
--        INSERT INTO T_DEBUG_SQL(proc_nm,long_msg) VALUES(t_proc_name,t_long_txt);
--        COMMIT;
    EXECUTE IMMEDIATE v_sql;
    t_base_row_cnt:=t_base_row_cnt+SQL%ROWCOUNT;
    COMMIT;
  
END LOOP;

n_sqlnum:=21000;
IF(t_base_row_cnt=0)AND(t_active_cmd!='SIMULATE')THEN
    t_error_msg:='No base data found in partition for ('||in_parent_level|| ','||in_parent_id||').';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=22000;
--clean up the hash table, since contents no longer needed--
v_sql:='TRUNCATE TABLE '||t_work_tbl_time||' REUSE STORAGE';
EXECUTE IMMEDIATE v_sql;

maxdata.p_agg_logger(in_target_tbl,in_parent_level,in_parent_id,'TIME','FINISH',
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
