--------------------------------------------------------
--  DDL for Procedure P_AGG_SCHED
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_SCHED" (
    in_method_nm    VARCHAR2,
    in_target_tbl    VARCHAR2,
    in_time_level    NUMBER,
    in_time_id	     NUMBER,
    in_base_t_level  NUMBER
)
/*
$Log: 5240_IDA_p_agg_sched.sql,v $
Revision 1.1.2.1  2008/11/26 17:30:13  anchan
FIXID : BASELINE check-in

================================================================================
NOTE:
--------------------------------------------------------------------------------
*/
AS
    t_source_tbl  VARCHAR2(30);
    t_recover_tbl VARCHAR2(30);
    t_exists_flg    NUMBER(1);
    t_count_tbl_flg NUMBER(1);
    t_run_next_no NUMBER(10);
    t_analyze_pct NUMBER(4,1):=0.0;
    t_time_level NUMBER(6);
    t_time_id    NUMBER(10);
    t_base_loaded_flg NUMBER(1);
    t_do_agg_cnt NUMBER(2):=0;
    t_method_nm VARCHAR2(10);

    t_proc_name VARCHAR2(30):='p_agg_sched';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(3000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg	 VARCHAR2(1000) := NULL;

    CURSOR c_scheduled_time IS
        SELECT *
        FROM agah_aggregate_header
        WHERE table_nm=in_target_tbl
        AND scheduled_flg=1
        ORDER BY time_level DESC,time_id;

BEGIN
n_sqlnum:=10000;
t_call := t_proc_name || ' ( ' ||
    in_method_nm || ',' ||
    in_target_tbl|| ',' ||
    in_time_level|| ',' ||
    in_time_id || ',' ||
    in_base_t_level ||
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, NULL, n_sqlnum);

n_sqlnum:=11000;
--get the next run number--
SELECT source_nm,run_seq_no,method_nm
INTO t_source_tbl,t_run_next_no,t_method_nm
FROM agac_aggregate_control
WHERE table_nm=in_target_tbl;

n_sqlnum:=11100;
t_run_next_no:=COALESCE(t_run_next_no,100)+1;

n_sqlnum:=12000;
--Make sure no recovery tables exist--
SELECT MIN(table_name) INTO t_recover_tbl
FROM user_tables
WHERE table_name like '%'||t_source_tbl||'%'
AND RTRIM(table_name,'#$') IN
    (SELECT partition_name
    FROM user_tab_partitions
    WHERE table_name=in_target_tbl);

IF(t_recover_tbl IS NOT NULL)THEN
    t_error_msg:='Cannot schedule. A recovery table exists:'||t_recover_tbl;
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=12220;
UPDATE agac_aggregate_control
SET run_seq_no=t_run_next_no
WHERE table_nm=in_target_tbl;

n_sqlnum:=12300;
--unflag any outstanding scheduled_flg from a previous run--
UPDATE agah_aggregate_header
SET scheduled_flg=0
WHERE table_nm=in_target_tbl
AND scheduled_flg=1;


n_sqlnum:=12400;
--first, set scheduled_flg for bottom-level(can be base_time_level or above) partitions--
UPDATE agah_aggregate_header ag
SET scheduled_flg=1
WHERE ag.table_nm=in_target_tbl
AND(ag.do_agg_flg=1 OR in_method_nm IN('RECLASS','REALIGN','FULLLOAD') )
AND ag.base_loaded_flg IN(-1,1)
AND(ag.time_level IN(SELECT lowerlevel_id FROM maxdata.path_seg WHERE path_id=50) )--child_level's--
AND(  (ag.time_level=in_time_level AND ag.time_id=in_time_id)
    OR EXISTS(SELECT 1 FROM maxdata.v_time_lkup vt
             WHERE vt.time_level=ag.time_level AND vt.time_id=ag.time_id
             AND parent_level=in_time_level AND parent_id=in_time_id)
   );


n_sqlnum:=12500;
--next, set scheduled_flg for parents of the bottom-level partitions--
UPDATE agah_aggregate_header ag
SET scheduled_flg=1
WHERE ag.table_nm=in_target_tbl
AND(ag.base_loaded_flg IN(-1,1) OR in_method_nm IN('FULLLOAD','NEWLOAD','BACKLOAD') )
AND ag.time_level>=in_time_level --only upto the specified level--
AND(ag.time_level IN(SELECT higherlevel_id FROM maxdata.path_seg WHERE path_id=50) )--parent_level's--
AND EXISTS
    (SELECT 1 FROM
        (SELECT parent_level,parent_id
        FROM maxdata.v_time_lkup vt
        JOIN agah_aggregate_header ag ON(vt.time_level=ag.time_level AND vt.time_id=ag.time_id)
        WHERE ag.table_nm=in_target_tbl
        AND ag.scheduled_flg=1) vx
    WHERE vx.parent_level=ag.time_level AND vx.parent_id=ag.time_id);


n_sqlnum:=12600;
IF(in_method_nm IN('FULLLOAD','NEWLOAD','BACKLOAD'))THEN
    t_base_loaded_flg:=-1;
ELSE
    t_base_loaded_flg:=NULL;
END IF;

n_sqlnum:=13000;
FOR r_time IN c_scheduled_time
LOOP
    n_sqlnum:=13100;
    IF(r_time.base_loaded_flg=-1)
    AND( (r_time.time_level=in_base_t_level)OR(in_method_nm IN('RECLASS','REALIGN')) )THEN
        t_error_msg:='A required time partition not BASE-LOADED yet:'
            ||'('||r_time.time_level||','||r_time.time_id||')';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;

    n_sqlnum:=13200;
    t_do_agg_cnt:=t_do_agg_cnt+r_time.do_agg_flg;

    n_sqlnum:=13300;
    UPDATE agah_aggregate_header
    SET method_nm=t_method_nm,
        base_loaded_flg=(CASE WHEN time_level=in_base_t_level
                                THEN base_loaded_flg
                                ELSE COALESCE(t_base_loaded_flg,base_loaded_flg) END),
        aggregated_flg=-1,
        run_seq_no=t_run_next_no,
        start_dttm=NULL,finish_dttm=NULL
    WHERE table_nm=in_target_tbl
    AND time_level=r_time.time_level AND time_id=r_time.time_id;
END LOOP;

n_sqlnum:=13400;
IF (in_method_nm IN('NEWLOAD','BACKLOAD'))AND(t_do_agg_cnt=0) THEN
    t_error_msg:='No time partitions specified(with DO_AGG_FLG).';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

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
