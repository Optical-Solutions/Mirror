--------------------------------------------------------
--  DDL for Procedure P_AGG_MERCH
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_MERCH" (
    in_target_tbl   VARCHAR2,--informational only--
    in_time_level   NUMBER,  
    in_time_id      NUMBER,  
    in_dim_ord_cd   VARCHAR2, --'ML','-M','LM','-L'
    in_analyze_pct  NUMBER
) AS
/*
------------------------------------------------------------------------------
$Log: 5240_IDA_p_agg_merch.sql,v $
Revision 1.1.2.1.2.2  2009/05/13 20:02:12  anchan
PERFORMANCE: use separate work tables for each of the M-L-T aggregation dimensions.

Revision 1.1.2.1.2.1  2009/03/25 16:12:00  anchan
FIXID S0567466: move the call to agg_rule inside the main loop.

Revision 1.1.2.1  2008/11/26 17:30:11  anchan
FIXID : BASELINE check-in

------------------------------------------------------------------------------
*/
    t_source_tbl   VARCHAR2(30);
    t_active_cmd   VARCHAR2(10);
    t_base_m_level NUMBER(2);
    t_base_l_level NUMBER(1);
    t_base_t_level NUMBER(2);
    t_simulate_sec NUMBER(10);
    t_work_tbl VARCHAR2(30);
    t_work_tbl_from VARCHAR2(30);
    t_col_list VARCHAR2(8000);
    t_agg_list VARCHAR2(8000);
    t_agg_row_cnt NUMBER(10):=0;
    t_rollup_level NUMBER(2);
    t_this_m_level   NUMBER(2);
    t_base_dim_tbl VARCHAR2(30);
    t_base_dim_col VARCHAR2(30);
    t_rollup_dim_col VARCHAR2(30);
    t_part_name VARCHAR2(30);
    t_query_hint VARCHAR2(100);
    t_table_hint VARCHAR2(500);
    t_hint_list VARCHAR2(1000);
    t_row_count NUMBER(1);
    t_simulate_clause VARCHAR2(100):=' ';
--    t_long_txt LONG;
    t_proc_name VARCHAR2(30):='p_agg_merch';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(16000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg     VARCHAR2(1000) := NULL;

    CURSOR c_higher_lower_level IS
        SELECT *
        FROM maxdata.path_seg
        WHERE path_id=11 --MERCH dimension--
        ORDER BY lowerlevel_id DESC; --order is *IMPORTANT*.

BEGIN
n_sqlnum:=10000;
t_call := t_proc_name || ' ( ' ||
    in_target_tbl || ',' ||
    in_time_level|| ',' ||
    in_time_id || ',' ||
    in_dim_ord_cd || ',' ||
    in_analyze_pct || 
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, NULL, n_sqlnum);

n_sqlnum:=10100;
maxdata.p_agg_logger(in_target_tbl,in_time_level,in_time_id,'MERCH','START');

n_sqlnum:=10200;
SELECT source_nm,active_cmd,simulate_sec,(base_merch_level-10),base_location_level,base_time_level
INTO t_source_tbl,t_active_cmd,t_simulate_sec,t_base_m_level,t_base_l_level,t_base_t_level
FROM maxdata.agac_aggregate_control,maxdata.v_base_level
WHERE table_nm=in_target_tbl;

n_sqlnum:=10300;
t_work_tbl:=t_source_tbl||'#MERCH';
IF in_dim_ord_cd IN('ML','-L') THEN
    t_work_tbl_from:=t_source_tbl||'#MERCH';
ELSE --'LM','-M'
    t_work_tbl_from:=t_source_tbl||'#LOC';
END IF;

n_sqlnum:=11100;
IF(t_active_cmd='SIMULATE')THEN
    dbms_lock.sleep(t_simulate_sec);
    t_simulate_clause:=' AND 1=0 AND m.time_level=0 /*SIMULATE MODE, DO NOTHING*/ ';
END IF;

n_sqlnum:=12000;
v_sql:='ALTER SESSION ENABLE PARALLEL DML';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=13000;
IF(in_analyze_pct>0) THEN --Wipe out previous tablestats--
/*--*/dbms_stats.unlock_table_stats('MAXDATA',t_work_tbl);--*10g--
END IF;

n_sqlnum:=14000;
maxdata.p_get_query_hint('AGG_LOAD_M','[DX][M]',t_table_hint,t_query_hint);
t_hint_list:=t_query_hint||' '||t_table_hint;

n_sqlnum:=15000;
FOR r_hier IN c_higher_lower_level
LOOP 
        
    n_sqlnum:=15100;
    t_this_m_level:=r_hier.lowerlevel_id-10; --since absolute values are 11~20--
    --analyze the newly populated partition--
    t_part_name :='M'||+(t_this_m_level);
    IF(in_analyze_pct>0)AND(t_work_tbl=t_work_tbl_from)AND(t_this_m_level>=4) THEN
        n_sqlnum:=15110;
        dbms_stats.gather_table_stats(ownname=>'MAXDATA',tabname=>t_work_tbl,partname=>t_part_name,
            granularity=>'PARTITION',block_sample=>TRUE,estimate_percent =>in_analyze_pct,
            method_opt=>'FOR COLUMNS SIZE 1 merch_level,merch_id,location_level,location_id');
        n_sqlnum:=15120;      
        dbms_stats.gather_table_stats(ownname=>'MAXDATA',tabname=>t_work_tbl,
            granularity=>'GLOBAL',block_sample=>TRUE,estimate_percent =>in_analyze_pct,
            method_opt=>'FOR COLUMNS SIZE 1 merch_level,merch_id,location_level,location_id');
    END IF;

    t_base_dim_tbl:='lv'||+(t_this_m_level)||'ctree';
    t_base_dim_col:='lv'||+(t_this_m_level)||'ctree'||'_id';

    t_rollup_level := r_hier.higherlevel_id-10; --since absolute values are 11~20--

    n_sqlnum:=15200;
    maxdata.p_agg_col_rule (t_source_tbl,'M',t_rollup_level,t_base_l_level,in_time_level,in_time_id,0,t_col_list,t_agg_list);

    n_sqlnum:=15300;
    IF t_rollup_level=1 THEN
        t_rollup_dim_col:='lv1cmast_id';
    ELSE
        t_rollup_dim_col:='lv'||+(t_rollup_level)||'ctree_id';
    END IF;
    
    n_sqlnum:=15400;
    v_sql:='INSERT /*+APPEND {MERCH}*/ INTO '||t_work_tbl
        ||'(merch_level,merch_id,location_level,location_id,time_level,time_id,'||t_col_list||')'
        ||' SELECT  /*+'||t_hint_list||' */'
        ||+(t_rollup_level)||','||t_rollup_dim_col||',m.location_level,m.location_id,'||+(in_time_level)||','||+(in_time_id)||','||t_agg_list
        ||' FROM maxdata.'||t_base_dim_tbl||' dx '
        || 'JOIN maxdata.'||t_work_tbl_from||' m ON(m.merch_id=dx.'||t_base_dim_col||')'
        ||' WHERE m.merch_level='||+(t_this_m_level)
        ||' AND m.time_level='||+(in_time_level)||' AND m.time_id='||+(in_time_id)
        ||t_simulate_clause
        ||' GROUP BY '||t_rollup_dim_col||',m.location_level,m.location_id';
--        t_long_txt:=SUBSTR(v_sql,1,4000);
--        INSERT INTO T_DEBUG_SQL(proc_nm,long_msg) VALUES(t_proc_name,t_long_txt);
--        t_long_txt:=SUBSTR(v_sql,4001,4000);
--        INSERT INTO T_DEBUG_SQL(proc_nm,long_msg) VALUES(t_proc_name,t_long_txt);
--        COMMIT;
    
    EXECUTE IMMEDIATE v_sql;
    t_agg_row_cnt:=t_agg_row_cnt+SQL%ROWCOUNT;
    IF(SQL%ROWCOUNT=0)AND(t_active_cmd!='SIMULATE')THEN
        t_error_msg:='No data found in table='||t_work_tbl
            ||' for MERCH_LEVEL='||+(t_this_m_level)||'.';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;
    COMMIT;
    
    --For the first iteration, the "from" work table could be different--
    t_work_tbl_from:=t_work_tbl; --ONLY after the first iteration--

END LOOP;--INNER: along merch--


maxdata.p_agg_logger(in_target_tbl,in_time_level,in_time_id,'MERCH','FINISH',
    f_parallel_deg(t_table_hint,'M'),-1,t_agg_row_cnt);

EXCEPTION
WHEN OTHERS THEN
    t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
    ROLLBACK;
    maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
