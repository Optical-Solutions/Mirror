--------------------------------------------------------
--  DDL for Procedure P_AGG_COUNT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_COUNT" (
    in_target_tbl   VARCHAR2,
    in_holding_tbl  VARCHAR2,
    in_parent_level NUMBER, --must be higher than the base_time_level--
    in_parent_id    NUMBER,
    out_agg_row_cnt  OUT NUMBER 
)
AS    
/*
------------------------------------------------------------------------------
$Log: 5230_IDA_p_agg_count.sql,v $
Revision 1.1.2.1.2.3  2009/05/13 20:02:11  anchan
PERFORMANCE: use separate work tables for each of the M-L-T aggregation dimensions.

Revision 1.1.2.1.2.2  2009/05/13 17:49:11  anchan
ENHANCEMENT: allow metadata-driven COUNT columns and aggrules, instead of fixed,harcoded columns.

Revision 1.1.2.1.2.1  2009/03/26 19:38:39  anchan
FIXID S0567466: expanded long strings to 8000x2=16000

Revision 1.1.2.1  2008/11/26 17:30:09  anchan
FIXID : BASELINE check-in

------------------------------------------------------------------------------
*/
    t_source_tbl  VARCHAR2(30);
    t_work_tbl_count  VARCHAR2(30);
    t_work_tbl_merch  VARCHAR2(30);
    t_work_tbl_loc    VARCHAR2(30);
    t_active_cmd  VARCHAR2(10);
    t_child_level  NUMBER(2);
    t_base_m_level NUMBER(2);
    t_base_l_level NUMBER(1);
    t_base_t_level NUMBER(2);
    t_simulate_sec NUMBER(10);
    t_col_list VARCHAR2(8000);
    t_agg_list VARCHAR2(8000);
    t_query_hint VARCHAR2(100);
    t_table_hint VARCHAR2(500);
    t_hint_list VARCHAR2(1000);
    t_exists_flg NUMBER(1);
    t_time_list VARCHAR2(1000):=' ';
    t_simulate_clause VARCHAR2(100):=' ';
    t_child_first_id NUMBER(10):=0;
    t_child_last_id NUMBER(10):=0;
--    t_long_txt LONG;
    t_proc_name VARCHAR2(30):='p_agg_count';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(16000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg     VARCHAR2(1000) := NULL;
              
    --same cursor definition as in p_agg_post--
    CURSOR c_children_id IS
        SELECT * FROM maxdata.v_time_children
        WHERE table_nm=in_target_tbl 
        AND parent_level=in_parent_level AND parent_id=in_parent_id
        ORDER BY time_id;

    CURSOR c_count_col IS
        SELECT column_name,column_id
        FROM user_tab_columns
        WHERE table_name=t_work_tbl_count
        AND column_name NOT IN('MERCH_LEVEL','MERCH_ID','LOCATION_LEVEL','LOCATION_ID')
        ORDER BY column_id; 

BEGIN
n_sqlnum:=10000;
t_call := t_proc_name || ' ( ' ||
    in_target_tbl || ',' ||
    in_holding_tbl || ',' ||
    in_parent_level || ',' ||
    in_parent_id || ',' ||
    'out' ||
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, 'START', n_sqlnum);

n_sqlnum:=11000;
SELECT source_nm,active_cmd,simulate_sec,(base_merch_level-10),base_location_level,base_time_level
INTO t_source_tbl,t_active_cmd,t_simulate_sec,t_base_m_level,t_base_l_level,t_base_t_level
FROM maxdata.agac_aggregate_control,maxdata.v_base_level
WHERE table_nm=in_target_tbl;

n_sqlnum:=11100;
t_work_tbl_count:=t_source_tbl||'#COUNT';
t_work_tbl_merch:=t_source_tbl||'#MERCH';
t_work_tbl_loc:=t_source_tbl||'#LOC';

n_sqlnum:=12000;
IF(t_active_cmd='SIMULATE')THEN
    dbms_lock.sleep(t_simulate_sec);
    t_simulate_clause:=' AND 1=0 AND m.time_level=0 /*SIMULATE MODE, DOES NOTHING*/ ';
END IF;

n_sqlnum:=13000;
v_sql:='ALTER SESSION ENABLE PARALLEL DML';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=14000;
v_sql:='TRUNCATE TABLE '||t_work_tbl_count;
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=17000;
FOR r_child IN c_children_id  
LOOP
    n_sqlnum:=17100;
    IF(r_child.base_loaded_flg=-1)OR(r_child.aggregated_flg=-1)THEN
        t_error_msg:='A required child partition not loaded/aggregated yet:'
            ||'('||+(r_child.time_level)||','||+(r_child.time_id)||')';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;
    
    IF (t_child_first_id=0)OR(r_child.time_id<t_child_first_id) THEN
        t_child_first_id:=r_child.time_id;
    END IF;
    IF (r_child.time_id>t_child_last_id)THEN
        t_child_last_id:=r_child.time_id;
    END IF;
    
    t_time_list:=t_time_list||r_child.time_id||',';
    t_child_level:=r_child.time_level;

END LOOP;
t_time_list:=RTRIM(t_time_list,',');
IF(t_time_list=' ')THEN
    t_error_msg:='No aggregated children partitions found for: '
        ||'('||+(in_parent_level)||','||+(in_parent_id)||')';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;  

n_sqlnum:=18000;
--just get the list of COUNT columns and agg_rules--
maxdata.p_agg_col_rule (t_source_tbl,'C',0,0,in_parent_level,in_parent_id,0,t_col_list,t_agg_list,
                        t_child_first_id,t_child_last_id);

n_sqlnum:=19000;
maxdata.p_get_query_hint('AGG_LOAD_C','[C][M]',t_table_hint,t_query_hint);
t_hint_list:=t_query_hint||' '||t_table_hint;

n_sqlnum:=20000;
--upper-levl rows only--
v_sql:='INSERT /*+APPEND {COUNT}*/ INTO '||t_work_tbl_count
    ||'(merch_level,merch_id,location_level,location_id,'||t_col_list||')'
    ||' SELECT /*+'||t_hint_list||'*/'
    ||' merch_level,merch_id,location_level,location_id,'||t_agg_list
    ||' FROM '||in_target_tbl||' m '
    ||' WHERE time_level='||+(t_child_level)||' AND time_id IN('||t_time_list||')'
    ||' AND(m.merch_level<'||+(t_base_m_level)||' OR m.location_level<'||+(t_base_l_level)||')'
    ||t_simulate_clause
    ||' GROUP BY merch_level,merch_id,location_level,location_id';
--        t_long_txt:=SUBSTR(v_sql,1,4000);
--        INSERT INTO T_DEBUG_SQL(proc_nm,long_msg) VALUES(t_proc_name,t_long_txt);
--        t_long_txt:=SUBSTR(v_sql,4001,4000);
--        INSERT INTO T_DEBUG_SQL(proc_nm,long_msg) VALUES(t_proc_name,t_long_txt);
--        COMMIT;
EXECUTE IMMEDIATE v_sql;
COMMIT;


n_sqlnum:=21000;
--just get the list of ALL columns--
maxdata.p_agg_col_rule (t_source_tbl,'T',0,0,in_parent_level,in_parent_id,0,t_col_list,t_agg_list);
t_agg_list:=','||t_col_list||','; --extra COMMAs added temporarily so that REPLACE function works correctly--

n_sqlnum:=21100;
--add the "c." prefix to COUNT columns--
FOR r_col IN c_count_col
LOOP
    t_agg_list:=REPLACE(t_agg_list, ','||r_col.column_name||',', ',c.'||r_col.column_name||',');
END LOOP;
t_agg_list:=TRIM(BOTH ',' FROM t_agg_list);--remove the extra COMMAs added previously--

n_sqlnum:=22000;
maxdata.p_log (t_proc_name, t_error_level, t_call, 'COUNT-JOIN', n_sqlnum);

n_sqlnum:=23000;
v_sql:='INSERT /*+APPEND {COUNT}*/ INTO '||in_holding_tbl
    ||'(merch_level,merch_id,location_level,location_id,time_level,time_id,'||t_col_list||')'
    ||' SELECT  /*+'||t_hint_list||'*/' 
    ||' m.merch_level,m.merch_id,m.location_level,m.location_id,time_level,time_id,'||t_agg_list 
    ||' FROM '||t_work_tbl_count||' c ' --LEFT or RIGHT join uses fast "HASH JOIN RIGHT OUTER BUFFERED"...?--
    ||' JOIN '||t_work_tbl_merch||' m ' --RIGHT join not absolutely necessary here, but for performance...--
    ||'   ON(    m.merch_level=c.merch_level and m.merch_id=c.merch_id '
    ||'      AND m.location_level=c.location_level AND m.location_id=c.location_id)'
    ||' WHERE(m.merch_level<'||+(t_base_m_level)||')' --optional; for performance???
    ||'   AND(c.merch_level<'||+(t_base_m_level)||')' --optional; for performance???
    ||t_simulate_clause;
--        t_long_txt:=SUBSTR(v_sql,1,4000);
--        INSERT INTO T_DEBUG_SQL(proc_nm,long_msg) VALUES(t_proc_name,t_long_txt);
--        t_long_txt:=SUBSTR(v_sql,4001,4000);
--        INSERT INTO T_DEBUG_SQL(proc_nm,long_msg) VALUES(t_proc_name,t_long_txt);
--        COMMIT;
EXECUTE IMMEDIATE v_sql;
out_agg_row_cnt:=SQL%ROWCOUNT;
COMMIT;

n_sqlnum:=24000;
v_sql:='INSERT /*+APPEND {COUNT}*/ INTO '||in_holding_tbl
    ||'(merch_level,merch_id,location_level,location_id,time_level,time_id,'||t_col_list||')'
    ||' SELECT  /*+'||t_hint_list||'*/'
    ||' m.merch_level,m.merch_id,m.location_level,m.location_id,time_level,time_id,'||t_agg_list
    ||' FROM '||t_work_tbl_count||' c ' --LEFT or RIGHT join uses fast "HASH JOIN RIGHT OUTER BUFFERED"...?--
    ||' JOIN '||t_work_tbl_loc||' m '  --RIGHT join not absolutely necessary here, but for performance...--
    ||'   ON(    m.merch_level=c.merch_level and m.merch_id=c.merch_id '
    ||'      AND m.location_level=c.location_level AND m.location_id=c.location_id)'
    ||' WHERE(m.location_level<'||+(t_base_l_level)||')' --optional; for performance???
    ||'   AND(c.location_level<'||+(t_base_l_level)||')' --optional; for performance???
    ||t_simulate_clause;
--        t_long_txt:=SUBSTR(v_sql,1,4000);
--        INSERT INTO T_DEBUG_SQL(proc_nm,long_msg) VALUES(t_proc_name,t_long_txt);
--        t_long_txt:=SUBSTR(v_sql,4001,4000);
--        INSERT INTO T_DEBUG_SQL(proc_nm,long_msg) VALUES(t_proc_name,t_long_txt);
--        COMMIT;
EXECUTE IMMEDIATE v_sql;
out_agg_row_cnt:=out_agg_row_cnt+SQL%ROWCOUNT;
COMMIT;

n_sqlnum:=25000;
IF(out_agg_row_cnt=0)AND(t_active_cmd!='SIMULATE')THEN
    t_error_msg:='No COUNT rows found for ('||in_parent_level|| ','||in_parent_id||').';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=30000;
maxdata.p_log (t_proc_name, t_error_level, t_call, 'FINISH', n_sqlnum);

EXCEPTION
WHEN OTHERS THEN
    t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
    ROLLBACK;
    maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
