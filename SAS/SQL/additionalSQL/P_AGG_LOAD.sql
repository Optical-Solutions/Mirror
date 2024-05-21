--------------------------------------------------------
--  DDL for Procedure P_AGG_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_LOAD" (
    in_action_cmd    VARCHAR2, --must be one of:ACTUAL,SCHEDULE,SIMULATE,SAMPLE,STOP,RECOVER,CONTINUE.
    in_method_nm     VARCHAR2, --RECLASS,REALIGN,NEWLOAD,BACKLOAD,FULLLOAD--
    in_target_tbl    VARCHAR2,
    in_time_level    NUMBER,
    in_time_id         NUMBER,
    in_special_opt   VARCHAR2:='YEAR' --specify DEBUG to use time_level other than 47(year)--
)  
/*
$Log: 5250_IDA_p_agg_load.sql,v $
Revision 1.1.2.1.2.2  2009/06/11 15:38:14  anchan
FIXID S0580991: correct variable name

Revision 1.1.2.1.2.1  2009/05/13 20:02:17  anchan
PERFORMANCE: use separate work tables for each of the M-L-T aggregation dimensions.

Revision 1.1.2.1  2008/11/26 17:30:16  anchan
FIXID : BASELINE check-in

================================================================================
NOTE:
--------------------------------------------------------------------------------
*/
AS
    t_source_tbl   VARCHAR2(30);
    t_exists_flg   NUMBER(1);
    t_run_next_no  NUMBER(10);
    t_analyze_pct  NUMBER(4,1):=0.0;
    t_time_level   NUMBER(6);
    t_time_id      NUMBER(10);
    t_base_t_level NUMBER(2);
    t_sampling_level NUMBER(2);
    t_dim_order_cd VARCHAR2(3);
    t_stop_flg     NUMBER(1);
    t_table_nm     VARCHAR2(30);
    t_start_dttm   DATE;
    t_finish_dttm  DATE;
    t_active_cmd   VARCHAR2(10);
    t_simulate_flg NUMBER(1);
    t_method_nm    VARCHAR2(10);
    t_agg_chk_flg  VARCHAR2(10);
                                
    t_proc_name VARCHAR2(30):='p_agg_load';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(3000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg     VARCHAR2(1000) := NULL;

    CURSOR c_scheduled_time IS
        SELECT *
        FROM maxdata.agah_aggregate_header 
        WHERE table_nm=in_target_tbl
        AND(  (scheduled_flg=1 AND run_seq_no=t_run_next_no)
            OR(time_level=in_time_level AND time_id=in_time_id) )
        ORDER BY time_level DESC,time_id ASC;--order is important--

BEGIN
n_sqlnum:=10000;
t_call := t_proc_name || ' ( ' ||
    in_action_cmd || ',' ||
    in_method_nm || ',' ||
    in_target_tbl|| ',' ||
    in_time_level|| ',' ||
    in_time_id || ',' ||   
    in_special_opt ||
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, NULL, n_sqlnum);

n_sqlnum:=10001;
IF in_method_nm IN('NEWLOAD','BACKLOAD','FULLLOAD')AND(in_special_opt!='DEBUG')THEN
    t_error_msg:=in_method_nm||' process not supported yet.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=10100;
IF(USER!='MAXDATA') THEN
    t_error_msg:='Must be logged in as MAXDATA user.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=10200;
IF(in_action_cmd IS NULL)OR(in_method_nm IS NULL)OR(in_target_tbl IS NULL)
OR(in_time_level IS NULL)OR(in_time_id IS NULL)THEN
    t_error_msg:='Every parameter must be specified.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=10300;
SELECT MAX(value_1) INTO t_dim_order_cd 
FROM maxapp.userpref
WHERE max_user_id=-1 AND key_1='AGG_DIMENSION_ORDER';

n_sqlnum:=10310;
IF COALESCE(t_dim_order_cd,'?') NOT IN('ML','LM') THEN
    t_error_msg:='Invalid AGG_DIMENSION_ORDER configured or missing; it must be one of: ML,LM.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=10320;
CASE
WHEN (in_method_nm='REALIGN')AND(t_dim_order_cd='ML')THEN
    t_dim_order_cd:='-L';
WHEN (in_method_nm='RECLASS')AND(t_dim_order_cd='LM')THEN
    t_dim_order_cd:='-M';
ELSE
    t_dim_order_cd:=t_dim_order_cd;--do nothing--
END CASE;

n_sqlnum:=10400;
SELECT MAX(table_nm),MAX(source_nm),MAX(start_dttm),MAX(finish_dttm),MAX(base_time_level),MAX(sampling_level)
    INTO t_table_nm,t_source_tbl,t_start_dttm,t_finish_dttm,t_base_t_level,t_sampling_level
FROM maxdata.agac_aggregate_control,maxdata.v_base_level 
WHERE table_nm=in_target_tbl;


n_sqlnum:=11000;
v_sql:='ALTER SESSION ENABLE PARALLEL DML';
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=12800;
IF(in_action_cmd='STOP')THEN
    UPDATE maxdata.agac_aggregate_control
    SET do_stop_flg=1,
        finish_dttm=SYSDATE 
    WHERE table_nm=in_target_tbl;
    COMMIT;
    RETURN;
END IF;

n_sqlnum:=13000;
IF in_action_cmd NOT IN('ACTUAL','SCHEDULE','SIMULATE','SAMPLE','STOP','RECOVER','CONTINUE') THEN
    t_error_msg:='Invalid ACTION specified; it must be one of: ACTUAL,SCHEDULE,SIMULATE,SAMPLE,STOP,RECOVER,CONTINUE.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=13400;
IF in_method_nm NOT IN('RECLASS','REALIGN','NEWLOAD','BACKLOAD','FULLLOAD') THEN
    t_error_msg:='Invalid PROCESS specified; it must be one of: RECLASS,REALIGN,NEWLOAD,BACKLOAD,FULLLOAD.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=13500;
IF(in_time_level!=47)AND(in_special_opt!='DEBUG')THEN
    t_error_msg:='Must specify DEBUG when time-level is not 47. Use DEBUG with caution.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=13600;
IF(in_action_cmd='SAMPLE')AND(t_source_tbl=in_target_tbl)THEN
    t_error_msg:='For sampling, source and target tables must be different.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=13610;
IF(in_action_cmd='SAMPLE')AND(in_method_nm='REALIGN')AND(t_dim_order_cd='ML')
    AND NOT(t_sampling_level BETWEEN 2 AND 4) THEN
    t_error_msg:='For SAMPLE of REALIGN, the SAMPLING_LEVEL must be between 2 AND 4 (LOCATION).';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=13620;
IF(in_action_cmd='SAMPLE')AND(in_method_nm='RECLASS')AND(t_dim_order_cd='LM')
    AND NOT(t_sampling_level BETWEEN 2 AND 4) THEN
    t_error_msg:='For SAMPLE of RECLASS, the SAMPLING_LEVEL must be between 12 AND 20 (MERCH).';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=14100;
IF(t_table_nm IS NULL)THEN
    t_error_msg:='The target table has not been defined in the CONTROL table.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=14200;
IF(t_start_dttm IS NOT NULL)AND(t_finish_dttm IS NULL)THEN
    t_error_msg:='The CONTROL record indicates that a LOAD is still in progress.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=14300;
SELECT COUNT(*) INTO t_exists_flg
FROM maxdata.agac_aggregate_control
WHERE source_nm=t_source_tbl AND table_nm<>in_target_tbl
AND(start_dttm IS NOT NULL) AND (finish_dttm IS NULL);
IF(t_exists_flg>0)THEN
    t_error_msg:='The CONTROL record indicates that another LOAD is already using the source table.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=14350;
IF(in_action_cmd='RECOVER')THEN
    maxdata.p_agg_recover(in_target_tbl,t_dim_order_cd);
    RETURN;
END IF;

n_sqlnum:=14400;
--check if any start_dttm w/o finish_dttm exists--
SELECT MAX(time_level),MAX(time_id) INTO t_time_level,t_time_id
FROM maxdata.agah_aggregate_header 
WHERE table_nm=in_target_tbl
AND(start_dttm IS NOT NULL)
AND(finish_dttm IS NULL); 
IF(t_time_level IS NOT NULL)THEN
    t_error_msg:='A shutdown is still in progress or a LOAD has aborted:'
        ||'('||t_time_level||','||t_time_id||').';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=15000;
IF(in_action_cmd!='CONTINUE')THEN
    maxdata.p_agg_recreate_worktbl(t_source_tbl,in_target_tbl);
END IF;

n_sqlnum:=16000;
IF(in_action_cmd IN('ACTUAL','SIMULATE','SAMPLE','SCHEDULE'))THEN
    n_sqlnum:=16100;
    
    UPDATE maxdata.agac_aggregate_control
    SET method_nm=in_method_nm,
        do_stop_flg=0,    
        active_cmd=in_action_cmd,
        start_dttm=SYSDATE,
        finish_dttm=SYSDATE --in case of SCHEDULE--
    WHERE table_nm=in_target_tbl;
    COMMIT;
    
    n_sqlnum:=16200;
    maxdata.p_agg_sched(in_method_nm,in_target_tbl,in_time_level,in_time_id,t_base_t_level);
        
END IF;

n_sqlnum:=17000;
SELECT MAX(run_seq_no) INTO t_run_next_no
FROM maxdata.agah_aggregate_header 
WHERE table_nm=in_target_tbl 
AND scheduled_flg=1;
IF(t_run_next_no IS NULL)THEN
    t_error_msg:='No partitions selected/scheduled.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=18000;
--IF 'CONTINUE', these values remain unchanged from the previous run:--
SELECT method_nm,active_cmd 
   INTO t_method_nm,t_active_cmd
FROM maxdata.agac_aggregate_control 
WHERE table_nm=in_target_tbl;

n_sqlnum:=18100;
IF(t_active_cmd='SCHEDULE')THEN
    RETURN;
END IF;

n_sqlnum:=18200;
IF(t_active_cmd='SIMULATE')THEN 
    t_simulate_flg:=1;
END IF;

n_sqlnum:=18300;
IF(t_method_nm!=in_method_nm)THEN --in case of CONTINUE--
    t_error_msg:='Specified process '||in_method_nm
        ||', but it was already in the middle of another process:'||t_method_nm||'.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;


n_sqlnum:=19200;
SELECT MAX(value_1) INTO t_agg_chk_flg 
FROM maxapp.userpref
WHERE max_user_id=-1 AND key_1='AGG_RULE_CHECK';

n_sqlnum:=19210;
IF COALESCE(t_agg_chk_flg,'OFF') NOT IN('ON','OFF') THEN
    t_error_msg:='Invalid AGG_RULE_CHECK value configured; it must be either ON or OFF.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=19220;
IF t_agg_chk_flg='ON' THEN
    maxdata.p_agg_chk_col_rule(t_source_tbl,0);
END IF;


dbms_lock.sleep(1);

--BEGIN LOAD--
n_sqlnum:=20000;
UPDATE maxdata.agac_aggregate_control
SET finish_dttm=NULL, --esp. in case of CONTINUE--
    do_stop_flg=0
WHERE table_nm=in_target_tbl;
COMMIT;

n_sqlnum:=21000;
t_time_level:=0;
FOR r_time IN c_scheduled_time 
LOOP
    n_sqlnum:=21010;
    IF(t_time_level!=r_time.time_level)THEN
        t_analyze_pct:=1.0;--*10g*:Analyze WORK table only for the first partition of each time_level--
    ELSE
        t_analyze_pct:=0.0;--No need to analyze, since stats can be reused--
    END IF;
    t_time_level:=r_time.time_level;--needed for the next iteration--

    n_sqlnum:=21100;
    UPDATE maxdata.agac_aggregate_control
    SET do_stop_flg=0,
        finish_dttm=SYSDATE
    WHERE table_nm=in_target_tbl
    AND do_stop_flg=1;
    t_stop_flg:=SQL%ROWCOUNT;
    COMMIT;
    
    IF(t_stop_flg=1)THEN       
        t_error_msg:='A STOP was requested. Stopping...';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;
    
    n_sqlnum:=21200;
    --update start_dttm to indicate start of a load:--
    UPDATE maxdata.agah_aggregate_header
    SET start_dttm=SYSDATE,finish_dttm=NULL,method_nm=t_method_nm
    WHERE table_nm=in_target_tbl
    AND time_level=r_time.time_level AND time_id=r_time.time_id; 
    COMMIT;

    n_sqlnum:=21300;
    IF(in_method_nm IN('RECLASS','REALIGN'))
    OR(r_time.time_level=t_base_t_level)THEN
        maxdata.p_agg_stage(in_target_tbl,r_time.time_level,r_time.time_id,t_dim_order_cd,t_analyze_pct);
    ELSE --IN('FULLLOAD','NEWLOAD','BACKLOAD') AND r_time.time_level<t_base_t_level
        maxdata.p_agg_stage_children(in_target_tbl,r_time.time_level,r_time.time_id,t_dim_order_cd,
                            t_analyze_pct);
        maxdata.p_agg_time(in_target_tbl,r_time.time_level,r_time.time_id,t_dim_order_cd,
                            t_analyze_pct);
    END IF;
    
    n_sqlnum:=21400;
    IF t_dim_order_cd IN('ML') THEN 
        maxdata.p_agg_merch(in_target_tbl,r_time.time_level,r_time.time_id,t_dim_order_cd,t_analyze_pct);
    END IF;
    
    n_sqlnum:=21500;
    IF t_dim_order_cd IN('ML','-L','LM') THEN 
        maxdata.p_agg_location(in_target_tbl,r_time.time_level,r_time.time_id,t_dim_order_cd,t_analyze_pct);
    END IF;

    n_sqlnum:=21600;
    IF t_dim_order_cd IN('LM','-M') THEN 
        maxdata.p_agg_merch(in_target_tbl,r_time.time_level,r_time.time_id,t_dim_order_cd,t_analyze_pct);
    END IF;

    n_sqlnum:=21700;
    maxdata.p_agg_post(in_target_tbl,r_time.time_level,r_time.time_id,t_dim_order_cd,0);

    n_sqlnum:=21800;
    --update status to indicate completion of a load:--
    UPDATE maxdata.agah_aggregate_header
    SET base_loaded_flg=1,aggregated_flg=1,scheduled_flg=0,do_agg_flg=NULL,finish_dttm=SYSDATE
    WHERE table_nm=in_target_tbl
    AND time_level=r_time.time_level AND time_id=r_time.time_id; 
    COMMIT;
    
    n_sqlnum:=22000;
    IF(in_action_cmd='ACTUAL')AND(t_source_tbl=in_target_tbl)THEN
        maxdata.p_set_cl_status ('N',r_time.time_id,r_time.time_level,0,0,0); --mark affected clusters as OBsolete--
    END IF;

END LOOP;
--END LOAD--

n_sqlnum:=22000;
--This LOAD is now finished--
UPDATE maxdata.agac_aggregate_control
SET finish_dttm=SYSDATE,
    do_stop_flg=0
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
