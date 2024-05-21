--------------------------------------------------------
--  DDL for Procedure P_AGG_REINDEX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_REINDEX" (
    in_target_tbl       VARCHAR2,
    in_partition_set    VARCHAR2:='%',
    in_timeout_minutes  NUMBER:=60
) 
AS    
/*
------------------------------------------------------------------------------
$Log: 5250_IDA_p_agg_reindex.sql,v $
Revision 1.1.2.1.2.1  2009/05/13 20:02:18  anchan
PERFORMANCE: use separate work tables for each of the M-L-T aggregation dimensions.

Revision 1.1.2.1  2008/11/26 17:30:16  anchan
FIXID : BASELINE check-in

------------------------------------------------------------------------------
*/
    t_target_ind  VARCHAR2(30) ;
    t_target_part VARCHAR2(30) ;
    t_target_tblspace VARCHAR2(30);
    t_target_indspace VARCHAR2(30);
    t_holding_tbl VARCHAR2(30);
    t_inactive_minutes NUMBER(10):=0;
    t_query_hint VARCHAR2(100);
    t_table_hint VARCHAR2(500);
    t_parallel_deg NUMBER(2);
    
    t_proc_name VARCHAR2(30):='p_agg_reindex';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(3000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg     VARCHAR2(1000) := NULL;

BEGIN
n_sqlnum:=10000;
t_call := t_proc_name || ' ( ' ||
    in_target_tbl||',' ||
    in_partition_set||',' ||
    in_timeout_minutes||
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, 'START', n_sqlnum);

n_sqlnum:=11000;
v_sql:='ALTER SESSION ENABLE PARALLEL DML';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=12000;
SELECT index_name INTO t_target_ind
FROM user_indexes 
WHERE table_name=in_target_tbl AND uniqueness='UNIQUE';

n_sqlnum:=19000;
maxdata.p_get_query_hint('AGG_LOAD_I','[M]',t_table_hint,t_query_hint);
t_parallel_deg:=f_parallel_deg(t_table_hint,'M');

n_sqlnum:=13000;
WHILE(t_inactive_minutes<=in_timeout_minutes)
LOOP
    n_sqlnum:=13100;
    SELECT MIN(partition_name) INTO t_target_part
    FROM user_ind_partitions
    WHERE index_name=t_target_ind
    AND partition_name LIKE in_partition_set
    AND status='UNUSABLE';
       
    n_sqlnum:=13200;
    IF(t_target_part IS NOT NULL)THEN
        t_call:='index partition='||t_target_part;
        maxdata.p_log (t_proc_name, t_error_level, t_call, 'START', n_sqlnum);

        n_sqlnum:=13300;
        v_sql:='ALTER INDEX '||t_target_ind||' REBUILD PARTITION '||t_target_part
            ||' PARALLEL '||t_parallel_deg;
        maxdata.p_execute_ddl_wait(v_sql,10,30);--Do not error if "p_agg_post" is doing EXCANGE PARTITION--

        maxdata.p_log (t_proc_name, t_error_level, t_call, 'FINISH', n_sqlnum);
        t_inactive_minutes :=0;
        
        IF (in_timeout_minutes>0) THEN
           dbms_lock.sleep(30); --In case "p_agg_post" is waiting to do an EXCHANGE PARTITION--
        END IF;        
--      n_sqlnum:=13400;
        --not necessary to analyze index, since it is done as part of rebuilding index process-- 
        --dbms_stats.gather_index_stats(ownname=>'MAXDATA',indname=>t_target_ind,partname=>t_target_part,
        --       granularity=>'PARTITION',degree=>in_parallel_cnt,estimate_percent =>in_analyze_pct);

    ELSE
        n_sqlnum:=13500;
        t_inactive_minutes :=t_inactive_minutes+1;
        dbms_lock.sleep(60);
    END IF;
    
END LOOP;

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
