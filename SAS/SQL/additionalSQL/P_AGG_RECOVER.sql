--------------------------------------------------------
--  DDL for Procedure P_AGG_RECOVER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_RECOVER" (
    in_target_tbl   VARCHAR2,
    in_dim_order_cd VARCHAR2,
    in_special_cmd  VARCHAR2:=NULL
) 
AS    
/*
------------------------------------------------------------------------------
$Log: 5245_IDA_p_agg_recover.sql,v $
Revision 1.1.2.1.2.1  2009/05/13 20:02:16  anchan
PERFORMANCE: use separate work tables for each of the M-L-T aggregation dimensions.

Revision 1.1.2.1  2008/11/26 17:30:15  anchan
FIXID : BASELINE check-in

===============
If ABORT command is specified, waits for 60 seconds before proceeding to give 
a last chance for the user to change mind...
------------------------------------------------------------------------------
*/
    t_source_tbl VARCHAR2(30);
    t_work_tbl_merch  VARCHAR2(30);
    t_work_tbl_loc    VARCHAR2(30);
    t_time_level NUMBER(2);
    t_time_id    NUMBER(10);
    t_source_part VARCHAR2(30) ;
    t_target_part VARCHAR2(30) ;
    t_holding_tbl VARCHAR2(30);
    t_golden_tbl  VARCHAR2(30);
    t_exists_flg NUMBER(1);
    t_run_seq_no NUMBER(10);
    t_simulated_flg NUMBER(1);            
    t_failed_post_flg NUMBER(1);
    t_table_nm     VARCHAR2(30);
    t_start_dttm   DATE;
    t_finish_dttm  DATE;
               
    t_proc_name VARCHAR2(30):='p_agg_recover';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(3000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg     VARCHAR2(1000) := NULL;

BEGIN
n_sqlnum:=10000;
t_call := t_proc_name || ' ( ' ||
    in_target_tbl||','||
    in_dim_order_cd||','|| 
    COALESCE(in_special_cmd,'NULL')|| 
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, NULL, n_sqlnum);

n_sqlnum:=10100;
IF(USER!='MAXDATA') THEN
    t_error_msg:='Must be logged in as MAXDATA user.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=10200;
IF(in_special_cmd IS NOT NULL)AND(in_special_cmd!='ABORT')THEN
    t_error_msg:='Invalid SPECIAL command specified. It must be ABORT or not specified.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=11000;
v_sql:='ALTER SESSION ENABLE PARALLEL DML';
EXECUTE IMMEDIATE v_sql;


n_sqlnum:=12000;
SELECT source_nm,table_nm,start_dttm,finish_dttm
    INTO t_source_tbl,t_table_nm,t_start_dttm,t_finish_dttm
FROM maxdata.agac_aggregate_control 
WHERE table_nm=in_target_tbl;

n_sqlnum:=12100;
t_work_tbl_merch :=t_source_tbl||'#MERCH';
t_work_tbl_loc :=t_source_tbl||'#LOC';

n_sqlnum:=13000;
SELECT MAX(run_seq_no) INTO t_run_seq_no
FROM maxdata.agah_aggregate_header
WHERE table_nm=in_target_tbl;

BEGIN
    n_sqlnum:=14000;
    SELECT time_level,time_id
    INTO t_time_level,t_time_id
    FROM maxdata.agah_aggregate_header
    WHERE table_nm=in_target_tbl
    AND run_seq_no=t_run_seq_no
    AND(start_dttm IS NOT NULL)AND(finish_dttm IS NULL); --started but never finished--
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        t_error_msg:='Nothing to recover for '||in_target_tbl||'.';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    WHEN TOO_MANY_ROWS THEN
        t_error_msg:='More than one aborted partition found for '||in_target_tbl||'.';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

n_sqlnum:=15000;
SELECT (CASE active_cmd WHEN 'SIMULATE' THEN 1 ELSE 0 END)
INTO t_simulated_flg
FROM maxdata.agac_aggregate_control
WHERE table_nm=in_target_tbl;

n_sqlnum:=16000;
t_target_part :=f_partition_nm(in_target_tbl,t_time_level,t_time_id);
t_holding_tbl:=t_target_part||'#';
t_golden_tbl :=t_target_part||'$';


n_sqlnum:=20000;
--determine if it had aborted at POST step--
SELECT SIGN(COUNT(*)) INTO t_failed_post_flg
FROM maxdata.agas_aggregate_step
WHERE run_seq_no=t_run_seq_no
AND table_nm=in_target_tbl
AND(time_level=t_time_level AND time_id=t_time_id)
AND step_nm IN('POST','POST+C')
AND finish_dttm IS NULL;

IF(in_special_cmd='ABORT')AND(t_failed_post_flg=1)THEN
    n_sqlnum:=20100;
    dbms_lock.sleep(60);--last chance to change mind--
END IF;

n_sqlnum:=21000;
--delete rows from any previous recovery/abort attempts--
DELETE FROM maxdata.agas_aggregate_step
WHERE run_seq_no=t_run_seq_no
AND table_nm=in_target_tbl AND step_nm IN('RECOVER','ROLLBACK','ABORT')
AND(time_level=t_time_level AND time_id=t_time_id);
COMMIT;

n_sqlnum:=22000;
IF(in_special_cmd='ABORT')THEN
    n_sqlnum:=22300;
    maxdata.p_agg_logger(in_target_tbl,t_time_level,t_time_id,'ABORT','START');
    
    IF(t_failed_post_flg=1)THEN
        n_sqlnum:=22200;
        v_sql:='TRUNCATE TABLE '||t_holding_tbl;--avoid DROP TABLE...PURGE of a big table--
        maxdata.p_execute_ddl_sql(v_sql,1,-1,-1,-1);

        n_sqlnum:=22210;
        v_sql:='DROP TABLE '||t_holding_tbl;
        maxdata.p_execute_ddl_sql(v_sql,1,-1,-1,-1);

        n_sqlnum:=22300;
        v_sql:='TRUNCATE TABLE '||t_golden_tbl;--avoid DROP TABLE...PURGE of a big table--
        maxdata.p_execute_ddl_sql(v_sql,1,-1,-1,-1);

        n_sqlnum:=22310;
        v_sql:='DROP TABLE '||t_golden_tbl;
        maxdata.p_execute_ddl_sql(v_sql,1,-1,-1,-1);
    END IF;

    n_sqlnum:=22400;
    --update start_dttm to indicate rollback of a load:--
    UPDATE maxdata.agah_aggregate_header
    SET start_dttm=NULL
    WHERE table_nm=in_target_tbl
    AND time_level=t_time_level AND time_id=t_time_id; 
    COMMIT; 
END IF;


n_sqlnum:=23000;
IF(in_special_cmd IS NULL)THEN
    IF(t_failed_post_flg=1)THEN
        n_sqlnum:=23100;
        maxdata.p_agg_logger(in_target_tbl,t_time_level,t_time_id,'RECOVER','START');

        n_sqlnum:=23300;
        maxdata.p_agg_post(in_target_tbl,t_time_level,t_time_id,in_dim_order_cd,1);

        n_sqlnum:=23400;
        --update status to indicate completion of a load:--
        UPDATE maxdata.agah_aggregate_header
        SET base_loaded_flg=1,aggregated_flg=1,scheduled_flg=0,do_agg_flg=NULL,finish_dttm=SYSDATE
        WHERE table_nm=in_target_tbl
        AND time_level=t_time_level AND time_id=t_time_id;
        COMMIT;
    ELSE --must've failed before POST step; it's safe to simply repeat all steps-- 
        n_sqlnum:=23500;
        DELETE FROM maxdata.agas_aggregate_step
        WHERE run_seq_no=t_run_seq_no
        AND table_nm=in_target_tbl
        AND(time_level=t_time_level AND time_id=t_time_id);
        COMMIT;
        
        n_sqlnum:=23600;
        maxdata.p_agg_logger(in_target_tbl,t_time_level,t_time_id,'ROLLBACK','START');
           
        n_sqlnum:=23700;
        --update start_dttm to indicate rollback of a load:--
        UPDATE maxdata.agah_aggregate_header
        SET start_dttm=NULL
        WHERE table_nm=in_target_tbl
        AND time_level=t_time_level AND time_id=t_time_id; 
        COMMIT;
    END IF;
END IF;

n_sqlnum:=25000;
v_sql:='TRUNCATE TABLE '||t_work_tbl_merch;
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=25100;
v_sql:='TRUNCATE TABLE '||t_work_tbl_loc;
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=30000;
UPDATE maxdata.agac_aggregate_control
SET do_stop_flg=0,
    finish_dttm=SYSDATE --finish_dttm must be non-null value in order to CONTINUE--
WHERE table_nm=in_target_tbl;
COMMIT;

EXCEPTION
WHEN OTHERS THEN
    t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
    ROLLBACK;
    maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
