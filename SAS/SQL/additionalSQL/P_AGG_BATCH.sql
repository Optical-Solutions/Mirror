--------------------------------------------------------
--  DDL for Procedure P_AGG_BATCH
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_BATCH" (
    in_action_cmd   VARCHAR2, --must be one of:ACTUAL,SIMULATE,SAMPLE,STOP,RECOVER,CONTINUE.--
    in_method_nm    VARCHAR2, --RECLASS,REALIGN,FULLLOAD,NEWLOAD=BACKLOAD--
    in_target_tbl   VARCHAR2
)
/*
$Log: 5260_IDA_p_agg_batch.sql,v $
Revision 1.1.2.1  2008/11/26 17:30:17  anchan
FIXID : BASELINE check-in

================================================================================
NOTE: Only those years(time_level=47) in the _HEADER table with BASE_LOADED_FLG=1 are processed.
--------------------------------------------------------------------------------
*/
AS
    t_active_cmd    VARCHAR2(10):=in_action_cmd;
    t_method_nm     VARCHAR2(10):=in_method_nm;
    t_dim_order     VARCHAR2(3);
    t_dimension_cd  VARCHAR2(2);
    t_start_dttm    DATE;
    t_finish_dttm   DATE;
    t_exists_flg    NUMBER(1);
    t_base_t_level  NUMBER(2);

    t_proc_name     VARCHAR2(30):='p_agg_batch';
    t_call          VARCHAR2(255);
    n_sqlnum        NUMBER(6);
    v_sql           VARCHAR2(3000);
    t_error_level   VARCHAR2(6):= 'info';
    t_error_msg	    VARCHAR2(1000) := NULL;

    CURSOR c_queued_year IS
        SELECT *
        FROM maxdata.agah_aggregate_header
        WHERE table_nm=in_target_tbl
        AND do_agg_flg=1
        AND time_level=47
        ORDER BY time_id;

BEGIN
n_sqlnum:=10000;
t_call := t_proc_name || ' ( ' ||
    in_action_cmd || ',' ||
    in_method_nm || ',' ||
    in_target_tbl||
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, NULL, n_sqlnum);

n_sqlnum:=10100;
IF(USER!='MAXDATA') THEN
    t_error_msg:='Must be logged in as MAXDATA user.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=10200;
IF(in_action_cmd IS NULL)OR(in_method_nm IS NULL)OR(in_target_tbl IS NULL)THEN
    t_error_msg:='Every parameter must be specified.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=10300;
IF in_action_cmd NOT IN('ACTUAL','SIMULATE','SAMPLE','STOP','RECOVER','CONTINUE') THEN
    t_error_msg:='Invalid ACTION specified; it must be one of: ACTUAL,SIMULATE,SAMPLE,STOP,RECOVER,CONTINUE.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=10400;
IF in_method_nm NOT IN('RECLASS','REALIGN','NEWLOAD','BACKLOAD','FULLLOAD') THEN
    t_error_msg:='Invalid PROCESS specified; it must be one of: RECLASS,REALIGN,NEWLOAD,BACKLOAD,FULLLOAD.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=11300;
SELECT base_time_level INTO t_base_t_level
FROM maxdata.v_base_level;

n_sqlnum:=11400;
SELECT MAX(start_dttm),MAX(finish_dttm)
    INTO t_start_dttm,t_finish_dttm
FROM maxdata.agac_aggregate_control
WHERE table_nm=in_target_tbl;

n_sqlnum:=12000;
IF(in_action_cmd IN('ACTUAL','SIMULATE','SAMPLE'))THEN
BEGIN
    n_sqlnum:=12100;
    IF(t_start_dttm IS NOT NULL)AND(t_finish_dttm IS NULL)THEN
        t_error_msg:='The CONTROL record indicates that a LOAD is still in progress.';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;

    n_sqlnum:=12200;
    --first, clear do_agg_flg from previous runs:--
    UPDATE maxdata.agah_aggregate_header
        SET do_agg_flg=NULL
    WHERE table_nm=in_target_tbl
    AND time_level=47
    AND do_agg_flg=1;

    IF(in_method_nm IN ('RECLASS','REALIGN','FULLLOAD'))THEN
        n_sqlnum:=12300;
        UPDATE maxdata.agah_aggregate_header
            SET do_agg_flg=1
        WHERE table_nm=in_target_tbl
        AND time_level=47
        AND base_loaded_flg=1; --skip if BASE_LOADED_FLG IN(0,-1) --
    ELSE--IF(in_method_nm IN ('NEWLOAD','BACKLOAD'))THEN
        n_sqlnum:=12400;
        UPDATE maxdata.agah_aggregate_header
            SET do_agg_flg=1
        WHERE table_nm=in_target_tbl
        AND time_level=47
        AND time_id IN
           (SELECT DISTINCT tl.parent_id
            FROM maxdata.agah_aggregate_header ah
            JOIN maxdata.v_time_lkup tl ON(ah.time_id=tl.time_id)
            WHERE ah.table_nm=in_target_tbl
          --AND ah.time_level=t_base_t_level
            AND ah.do_agg_flg=1
            AND tl.parent_level=47);
        IF (SQL%ROWCOUNT=0)THEN
            t_error_msg:='No time partitions have been specified(with DO_AGG_FLG=1).';
            RAISE_APPLICATION_ERROR(-20001,t_error_msg);
        END IF;
    END IF;

    n_sqlnum:=12500;
    UPDATE agac_aggregate_control
        SET method_nm=in_method_nm,
        active_cmd=in_action_cmd
    WHERE table_nm=in_target_tbl;

    COMMIT;
END;
END IF;

n_sqlnum:=13000;
IF(in_action_cmd='CONTINUE')THEN
BEGIN
    n_sqlnum:=13100;
    SELECT COUNT(*) INTO t_exists_flg
    FROM agah_aggregate_header
    WHERE table_nm=in_target_tbl
    AND time_level=47
    AND scheduled_flg=1;

    n_sqlnum:=13200;
    --if the previous year partition had completed, there would be nothing to CONTINUE--
    IF(t_exists_flg=0)THEN
        --therefore, need to retrieve original parms to process the following year--
        SELECT active_cmd,method_nm
        INTO t_active_cmd,t_method_nm
        FROM maxdata.agac_aggregate_control
        WHERE table_nm=in_target_tbl;
    END IF;
END;
END IF;

n_sqlnum:=14000;
IF(in_action_cmd IN('ACTUAL','SIMULATE','SAMPLE','CONTINUE'))THEN
BEGIN
    n_sqlnum:=14100;

    t_exists_flg:=0;
    FOR r_time IN c_queued_year
    LOOP
        n_sqlnum:=14110;
        --p_agg_load clears the do_agg_flg as each year is completed--
        p_agg_load(t_active_cmd,t_method_nm,in_target_tbl,47,r_time.time_id);

        n_sqlnum:=14120;
        --in case of CONTINUE, need to retrieve original parms to process the following year--
        SELECT active_cmd,method_nm
        INTO t_active_cmd,t_method_nm
        FROM maxdata.agac_aggregate_control
        WHERE table_nm=in_target_tbl;

        t_exists_flg:=1;
    END LOOP;

    n_sqlnum:=14130;
    IF(t_exists_flg=0)THEN
        t_error_msg:='No partitions selected/scheduled.';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;

END;
ELSE --STOP,RECOVER--
    n_sqlnum:=14200;
    p_agg_load(in_action_cmd,t_method_nm,in_target_tbl,47,0000);

END IF;

n_sqlnum:=20000;

EXCEPTION
WHEN OTHERS THEN
	t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
	ROLLBACK;
	maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
	RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
