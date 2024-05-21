--------------------------------------------------------
--  DDL for Procedure P_AGG_POST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_POST" (
    in_target_tbl   VARCHAR2,
    in_time_level   NUMBER,
    in_time_id      NUMBER,
    in_dim_ord_cd   VARCHAR2, --'ML','-M','LM','-L'
    in_recovery_flg NUMBER
)
/*
------------------------------------------------------------------------------
$Log: 5240_IDA_p_agg_post.sql,v $
Revision 1.1.2.1.2.1  2009/05/13 20:02:13  anchan
PERFORMANCE: use separate work tables for each of the M-L-T aggregation dimensions.

Revision 1.1.2.1  2008/11/26 17:30:11  anchan
FIXID : BASELINE check-in

====================
NOTE: Though source and target TABLE names can be different, 
      source and target PARTITION names must be the same. 
------------------------------------------------------------------------------
*/ 
AS    
    t_source_tbl  VARCHAR2(30);
    t_work_tbl_merch  VARCHAR2(30);
    t_work_tbl_loc    VARCHAR2(30);
    t_work_tbl_base   VARCHAR2(30);
    t_count_tbl   VARCHAR2(30);
    t_active_cmd  VARCHAR2(10);
    t_base_m_level NUMBER(2);
    t_base_l_level NUMBER(1);
    t_base_t_level NUMBER(2);
    t_simulate_sec NUMBER(10);
    t_count_flg NUMBER(1);
    t_source_part VARCHAR2(30) ;
    t_target_part VARCHAR2(30) ;
    t_target_ind  VARCHAR2(30) ;
    t_target_tblspace VARCHAR2(30);
    t_target_indspace VARCHAR2(30);
    t_holding_tbl VARCHAR2(30);
    t_golden_tbl  VARCHAR2(30);
    t_recover_tbl VARCHAR2(30);
    t_base_row_cnt NUMBER(10):=-1;
    t_agg_row_cnt NUMBER(10):=-1;
    t_exists_flg NUMBER(1);
    t_holding_ind VARCHAR2(30);
    t_time_level NUMBER(6);
    t_time_id NUMBER(10);
    t_query_hint VARCHAR2(100);
    t_table_hint VARCHAR2(500);
    t_hint_list VARCHAR2(1000);
    t_parallel_deg NUMBER(2);
    t_compress_opt VARCHAR2(30);
    t_simulate_clause VARCHAR2(100):=' ';
    t_index_col_list VARCHAR2(100):=' ';
    t_step_nm VARCHAR2(10):='POST';
    
    t_proc_name VARCHAR2(30):='p_agg_post';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(8000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg     VARCHAR2(1000) := NULL;
    
    --same cursor definition as in p_agg_count--
    CURSOR c_children_id IS
        SELECT * FROM maxdata.v_time_children
        WHERE table_nm=in_target_tbl 
        AND parent_level=in_time_level AND parent_id=in_time_id
        ORDER BY time_id;
               
BEGIN
n_sqlnum:=10000;
t_call := t_proc_name || ' ( ' ||
    in_target_tbl|| ',' ||
    in_time_level|| ',' ||
    in_time_id|| ',' ||
    in_dim_ord_cd|| ',' ||
    in_recovery_flg|| 
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, NULL, n_sqlnum);

n_sqlnum:=11000;
SELECT source_nm,active_cmd,simulate_sec,(base_merch_level-10),base_location_level,base_time_level
INTO t_source_tbl,t_active_cmd,t_simulate_sec,t_base_m_level,t_base_l_level,t_base_t_level
FROM maxdata.agac_aggregate_control,maxdata.v_base_level
WHERE table_nm=in_target_tbl;

n_sqlnum:=11100;
t_count_tbl:=t_source_tbl||'#COUNT';
t_work_tbl_merch:=t_source_tbl||'#MERCH';
t_work_tbl_loc:=t_source_tbl||'#LOC';
IF in_dim_ord_cd IN('ML','-L') THEN
    t_work_tbl_base:=t_work_tbl_merch;
ELSE--IN('LM','-M')
    t_work_tbl_base:=t_work_tbl_loc;
END IF;

n_sqlnum:=12000;
--If more than the 4key columns exist in the count table, set the count flag--
SELECT SIGN(COUNT(*)) INTO t_count_flg
FROM user_tab_columns 
WHERE table_name=t_count_tbl
AND column_name NOT IN('MERCH_LEVEL','MERCH_ID','LOCATION_LEVEL','LOCATION_ID');

n_sqlnum:=12100;
IF(in_time_level<t_base_t_level)AND(t_count_flg=1)THEN
    t_step_nm:=t_step_nm||'+C';
END IF;

n_sqlnum:=12200;
IF(in_recovery_flg=1)THEN
    maxdata.p_agg_logger(in_target_tbl,in_time_level,in_time_id,t_step_nm,'RESTART');
ELSE
    maxdata.p_agg_logger(in_target_tbl,in_time_level,in_time_id,t_step_nm,'START');
END IF;

n_sqlnum:=12300;
IF(t_active_cmd='SIMULATE')THEN
    dbms_lock.sleep(t_simulate_sec);
    t_simulate_clause:=' AND 1=0 AND m.time_level=0 /*SIMULATE MODE, DO NOTHING*/ ';
END IF;

n_sqlnum:=13000;
v_sql:='ALTER SESSION ENABLE PARALLEL DML';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=14000;
IF(in_time_level<t_base_t_level)THEN
    t_exists_flg:=0;  
    FOR r_child IN c_children_id  
    LOOP
        n_sqlnum:=14100;
        IF(r_child.base_loaded_flg=-1)OR(r_child.aggregated_flg=-1)THEN
            t_error_msg:='A required child partition not loaded/aggregated yet:'
                ||'('||+(r_child.time_level)||','||+(r_child.time_id)||')';
            RAISE_APPLICATION_ERROR(-20001,t_error_msg);
        END IF;
        t_exists_flg:=1;  
    END LOOP;

    n_sqlnum:=14200;
    IF(t_exists_flg=0)THEN
        t_error_msg:='No aggregated children partitions found for: '
            ||'('||+(in_time_level)||','||+(in_time_id)||')';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;
END IF;


n_sqlnum:=15000;
--The table must have one and only one UNIQUE index:--
SELECT MIN(index_name) INTO t_target_ind
FROM user_indexes 
WHERE table_name=in_target_tbl AND uniqueness='UNIQUE';
IF (t_target_ind IS NULL) THEN
    t_error_msg:='The table '||in_target_tbl||' must exist, and with one unique index.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=16000;
t_target_part :=f_partition_nm(in_target_tbl,in_time_level,in_time_id);
t_source_part:=t_target_part; --source and target partition names must be same--
t_holding_tbl:=t_target_part||'#';
t_golden_tbl :=t_target_part||'$';
t_holding_ind:=t_target_part||'_';

SELECT tablespace_name,CASE compression WHEN 'ENABLED' THEN 'COMPRESS' ELSE ' ' END
INTO t_target_tblspace,t_compress_opt
FROM user_tab_partitions
WHERE table_name=in_target_tbl AND partition_name=t_target_part;

n_sqlnum:=18000;
IF(in_recovery_flg=1)THEN --called from p_agg_recover--
BEGIN
    n_sqlnum:=18100;
    SELECT MIN(table_name) INTO t_recover_tbl
    FROM user_tables WHERE table_name IN(t_holding_tbl,t_golden_tbl);
    CASE 
        WHEN(t_recover_tbl=t_holding_tbl)THEN
            n_sqlnum:=18200;
            v_sql:='TRUNCATE TABLE '||t_holding_tbl;--avoid DROP TABLE...PURGE of a big table--
            EXECUTE IMMEDIATE v_sql;

            n_sqlnum:=18300;
            v_sql:='DROP TABLE '||t_holding_tbl;--avoid DROP TABLE...PURGE of a big table--
            EXECUTE IMMEDIATE v_sql;
        WHEN(t_recover_tbl=t_golden_tbl)THEN
            GOTO swap_partition;
        ELSE
            NULL;
    END CASE;
END;
END IF;

n_sqlnum:=19000;
maxdata.p_get_query_hint('AGG_LOAD_P','[M]',t_table_hint,t_query_hint);
t_hint_list:=t_query_hint||' '||t_table_hint;
t_parallel_deg:=f_parallel_deg(t_table_hint,'M');

n_sqlnum:=19100;
v_sql:='CREATE TABLE '||t_holding_tbl
    ||' '||t_compress_opt
    ||' TABLESPACE '||t_target_tblspace
    ||' NOLOGGING  PCTFREE 0  PARALLEL '||t_parallel_deg
    ||' AS SELECT * FROM '||t_source_tbl
    ||' WHERE 0=1 AND time_level=0';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=20000;
--Truncate target partition before populating the stage table, 
--  so that total tablespace size does not increase: 
v_sql:='ALTER TABLE '||in_target_tbl||' TRUNCATE PARTITION '||t_target_part;
IF(t_active_cmd!='SIMULATE')THEN
    maxdata.p_execute_ddl_wait(v_sql,100,10);--Do not error if p_agg_reindex job is running--
END IF;

n_sqlnum:=21000;
--base-level rows--
v_sql:='INSERT /*+APPEND {POST}*/ INTO '||t_holding_tbl
    ||' SELECT /*+'||t_hint_list||'*/  * FROM '||t_work_tbl_base||' m'
    ||' WHERE(merch_level='||+(t_base_m_level)||' AND location_level='||+(t_base_l_level)||')'
    ||t_simulate_clause;
EXECUTE IMMEDIATE v_sql;
t_base_row_cnt:=SQL%ROWCOUNT;
COMMIT;

n_sqlnum:=22000;
--upper-level aggregated rows--
IF(in_time_level<t_base_t_level)AND(t_count_flg=1)THEN
    n_sqlnum:=22100;
    maxdata.p_agg_count(in_target_tbl,t_holding_tbl,in_time_level,in_time_id,t_agg_row_cnt);
ELSE
    n_sqlnum:=22200;
    --upper-level rows--
    v_sql:='INSERT /*+APPEND {POST}*/ INTO '||t_holding_tbl
        ||' SELECT /*+'||t_hint_list||'*/  * FROM '||t_work_tbl_merch||' m'
        ||' WHERE(merch_level<'||+(t_base_m_level)||')'
        ||t_simulate_clause;
    EXECUTE IMMEDIATE v_sql;
    t_agg_row_cnt:=SQL%ROWCOUNT;
    COMMIT;

    n_sqlnum:=22300;
    --upper-level rows--
    v_sql:='INSERT /*+APPEND {POST}*/ INTO '||t_holding_tbl
        ||' SELECT /*+'||t_hint_list||'*/  * FROM '||t_work_tbl_loc||' m'
        ||' WHERE(location_level<'||+(t_base_l_level)||')'
        ||t_simulate_clause;
    EXECUTE IMMEDIATE v_sql;
    t_agg_row_cnt:=t_agg_row_cnt+SQL%ROWCOUNT;

END IF;


n_sqlnum:=23000;
DECLARE CURSOR c_index_cols IS 
    SELECT column_name FROM user_ind_columns WHERE index_name=t_target_ind
    ORDER BY column_position;
BEGIN
--retrieve the 6key columns in order--
    FOR r_col IN c_index_cols
    LOOP
        t_index_col_list:=t_index_col_list||r_col.column_name||',';
    END LOOP;
    t_index_col_list:=RTRIM(t_index_col_list,',');
END;

n_sqlnum:=24000;
--so that the new index will occupy the same space as the old index:--
SELECT tablespace_name,CASE compression WHEN 'ENABLED' THEN 'COMPRESS' ELSE ' ' END
INTO t_target_indspace,t_compress_opt
FROM user_ind_partitions
WHERE index_name=t_target_ind AND partition_name=t_source_part;

n_sqlnum:=25000;
v_sql:='CREATE UNIQUE INDEX '||t_holding_ind||' ON '||t_holding_tbl
    ||'('||t_index_col_list||')'
    ||' '||t_compress_opt
    ||' UNUSABLE NOLOGGING PARALLEL '||t_parallel_deg
    ||' TABLESPACE '||t_target_indspace;
EXECUTE IMMEDIATE v_sql;


n_sqlnum:=26000;
--rename to indicate that it is ready to be swapped--
v_sql:='RENAME '||t_holding_tbl||' TO '||t_golden_tbl;
EXECUTE IMMEDIATE v_sql;

<<swap_partition>>
n_sqlnum:=27000;
v_sql:='ALTER TABLE '||in_target_tbl
    ||' EXCHANGE PARTITION '||t_target_part||' WITH TABLE '||t_golden_tbl
    ||' INCLUDING INDEXES WITHOUT VALIDATION';
IF(t_active_cmd!='SIMULATE')THEN
    maxdata.p_execute_ddl_wait(v_sql,180,20);--wait up to 1 hour if "p_agg_reindex" is running--
END IF;

n_sqlnum:=28000;
v_sql:='DROP TABLE '||t_golden_tbl;--empty table, since partition was truncated earlier--
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=29000;
--If the tablestats are locked, they're retained even after a TRUNCATE--
/*--*/dbms_stats.lock_table_stats('MAXDATA',t_work_tbl_merch);--*10g--
/*--*/dbms_stats.lock_table_stats('MAXDATA',t_work_tbl_loc);--*10g--

n_sqlnum:=30000;
--Only clean up the work tables after a successful posting--
v_sql:='TRUNCATE TABLE '||t_work_tbl_merch||' REUSE STORAGE';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=30100;
v_sql:='TRUNCATE TABLE '||t_work_tbl_loc||' REUSE STORAGE';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=32000;
maxdata.p_agg_logger(in_target_tbl,in_time_level,in_time_id,t_step_nm,'FINISH',
    f_parallel_deg(t_table_hint,'M'),t_base_row_cnt,t_agg_row_cnt);

EXCEPTION
WHEN OTHERS THEN
    t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
    ROLLBACK;
    maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
