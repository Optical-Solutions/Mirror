--------------------------------------------------------
--  DDL for Procedure P_AGG_LOGGER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_LOGGER" (
    in_target_tbl   VARCHAR2,
    in_time_level   NUMBER,
    in_time_id        NUMBER,
    in_step_nm      VARCHAR2, 
    in_marker_cmd   VARCHAR2, --START,RESTART,FINISH,RECOVER, or ROLLBACK
    in_parallel_deg NUMBER:=0,
    in_base_row_cnt NUMBER:=-1,
    in_agg_row_cnt  NUMBER:=-1
) 
/*
------------------------------------------------------------------------------
$Log: 5220_IDA_p_agg_logger.sql,v $
Revision 1.1.2.1.2.1  2009/05/13 20:02:10  anchan
PERFORMANCE: use separate work tables for each of the M-L-T aggregation dimensions.

Revision 1.1.2.1  2008/11/26 17:30:09  anchan
FIXID : BASELINE check-in

------------------------------------------------------------------------------
*/
AS PRAGMA AUTONOMOUS_TRANSACTION;
        
    t_sampling_factor NUMBER(10);
    t_run_seq_no    NUMBER(10);
    t_method_nm    VARCHAR2(10);
    
    t_proc_name     VARCHAR2(30):='p_agg_logger';
    t_call          VARCHAR2(255);
    n_sqlnum        NUMBER(6);
    v_sql           VARCHAR2(3000);
    t_error_level   VARCHAR2(6):= 'info';
    t_error_msg        VARCHAR2(1000) := NULL;
       
BEGIN
n_sqlnum:=1000;
t_call := t_proc_name || ' ( ' ||
    in_target_tbl || ',' ||
    in_time_level || ',' ||
    in_time_id || ',' ||
    in_step_nm ||',' ||
    in_marker_cmd ||  
    ' ) ';
--maxdata.p_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum);

n_sqlnum:=2000;
SELECT run_seq_no,method_nm,
     (CASE active_cmd  WHEN 'SAMPLE' THEN sampling_factor WHEN 'SIMULATE' THEN 0 ELSE NULL END)
INTO t_run_seq_no,t_method_nm,t_sampling_factor
FROM maxdata.agac_aggregate_control
WHERE table_nm=in_target_tbl;

n_sqlnum:=3000;
CASE in_marker_cmd
    WHEN 'START' THEN
        n_sqlnum:=3100;
        INSERT INTO maxdata.agas_aggregate_step 
            (table_nm,time_level,time_id,step_nm,method_nm,run_seq_no,sampling_factor,start_dttm) 
        SELECT table_nm,time_level,time_id,in_step_nm,method_nm,run_seq_no,t_sampling_factor,SYSDATE
        FROM maxdata.agah_aggregate_header
        WHERE table_nm=in_target_tbl 
        AND time_level=in_time_level
        AND time_id=in_time_id;
    WHEN 'RESTART' THEN
        n_sqlnum:=3150;
        UPDATE maxdata.agas_aggregate_step
            SET start_dttm=SYSDATE --just reset the start_dttm--
        WHERE table_nm=in_target_tbl 
        AND time_level=in_time_level AND time_id=in_time_id
        AND step_nm=in_step_nm
        AND run_seq_no=t_run_seq_no;
    ELSE --'FINISH','RECOVER','ROLLBACK'
        n_sqlnum:=3200;
        UPDATE maxdata.agas_aggregate_step
            SET finish_dttm=SYSDATE,
                parallel_cnt=in_parallel_deg,
                base_row_cnt=in_base_row_cnt,
                agg_row_cnt=in_agg_row_cnt
        WHERE table_nm=in_target_tbl 
        AND time_level=in_time_level AND time_id=in_time_id
        AND step_nm=in_step_nm
        AND run_seq_no=t_run_seq_no;
END CASE;

IF(SQL%ROWCOUNT=0)THEN
    t_error_msg:='No STEP rows inserted/updated: '
        ||in_target_tbl||' ('||+(in_time_level)||','||+(in_time_id)||') '||in_step_nm||'.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

COMMIT;

dbms_lock.sleep(1);

EXCEPTION
WHEN OTHERS THEN
    t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
    ROLLBACK;
    maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
