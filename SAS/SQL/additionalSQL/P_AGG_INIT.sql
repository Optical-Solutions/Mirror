--------------------------------------------------------
--  DDL for Procedure P_AGG_INIT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_INIT" (
    in_table_set VARCHAR2, --Specify a table name or '%' for ALL tables--
    in_checksum  NUMBER, --Sanity check; VALUE/SUM of RUN_SEQ_NO(s) from the CONTROL table-- 
                        --To override, specify 999999999 (not recommended);
    in_nowarn_flg NUMBER:=0 --To disable warning about dependency/order of partitions--                     
) 
AS    
/*
------------------------------------------------------------------------------
$Log: 5250_IDA_p_agg_init.sql,v $
Revision 1.1.2.1.2.1  2009/06/23 17:57:20  anchan
FIXID S0588851: USE_NL(M) hint inside subquery can return wrong result.  USE_NL(AH,M) always returns correct result.

Revision 1.1.2.1  2008/12/02 15:33:21  anchan
No comment given.
Added for rename from 5270_IDA_p_agg_init.sql.
See originally named file for history prior to the rename.

Revision 1.1.2.1  2008/11/26 17:30:06  anchan
BASELINE check-in

=========================
DESCRIPTION:
   Initializes the BASE_LOADED_FLG by scanning the fact tables to determine if 
   any rows are found for each of the partitions.  Also sets AGGREGATED_FLG to 
   unknown (-1) status, which means an aggregation needs to be performed.
WARNING:  This procedure should be run once immediately before starting an 
    aggregation. DO NOT RUN THIS WHILE AGGREGATION IS IN PROGRESS.  
   It should not be run again until the start of the next aggregation
   (weeks or months later).   

------------------------------------------------------------------------------
*/
    t_exists_flg NUMBER(1):=0;
    t_checksum   NUMBER(10):=0;

    t_proc_name VARCHAR2(30):='p_agg_init';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(8000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg     VARCHAR2(1000) := NULL;
    
    CURSOR c_table_list IS
        SELECT *
        FROM maxdata.agac_aggregate_control 
        WHERE table_nm LIKE in_table_set
        ORDER BY table_nm;

BEGIN
n_sqlnum:=10000;

t_call := t_proc_name || ' ( ' ||
    in_table_set || 
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum);
----------------------

n_sqlnum:=11000;
FOR r_table IN c_table_list 
LOOP
    t_exists_flg:=1;
    t_checksum:=t_checksum + COALESCE(r_table.run_seq_no,0);
    
    n_sqlnum:=11100;
    IF(r_table.start_dttm IS NOT NULL)AND(r_table.finish_dttm IS NULL)THEN
        t_error_msg:='The CONTROL record indicates that a LOAD is still in progress.';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;
END LOOP;

n_sqlnum:=12000;
IF(t_exists_flg=0)THEN
    t_error_msg:='No matching table names found in the CONTROL table.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=13000;
IF NOT((t_checksum=in_checksum)OR(in_checksum=999999999)) THEN
    t_error_msg:='Safety check: specified checksum value does not match RUN_SEQ_NO value(s) in the CONTROL table.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;


n_sqlnum:=20000;
FOR r_table IN c_table_list 
LOOP

    n_sqlnum:=21000;
    INSERT INTO maxdata.agah_aggregate_header(table_nm,time_level,time_id,aggregated_flg,base_loaded_flg)
    SELECT DISTINCT r_table.table_nm,time_level,time_id,0,0
    FROM maxdata.v_time_lkup tl
    WHERE NOT EXISTS(SELECT 1 FROM maxdata.agah_aggregate_header
                        WHERE table_nm=r_table.table_nm 
                        AND tl.time_level=time_level AND tl.time_id=time_id)
    ORDER BY time_level,time_id; 
    COMMIT;

    n_sqlnum:=22000;
    --first, clear any existing flags, but skipping any with DO_AGG_FLG=0--
    UPDATE maxdata.agah_aggregate_header ah
    SET method_nm=NULL,aggregated_flg=NULL,base_loaded_flg=NULL,
        do_agg_flg=NULL,scheduled_flg=NULL,start_dttm=NULL,finish_dttm=NULL
    WHERE table_nm=r_table.table_nm
    AND COALESCE(do_agg_flg,-1)!=0;
                      
    n_sqlnum:=22100;
    --second,set the flags for each base_loaded/indexed partition--
    v_sql:='UPDATE /*+LEADING(ah)*/ maxdata.agah_aggregate_header ah'
        ||' SET base_loaded_flg=1'
        ||' WHERE table_nm='''||r_table.table_nm||''''
        ||' AND COALESCE(do_agg_flg,-1)!=0'
        ||' AND EXISTS(SELECT /*+FIRST_ROWS(1) INDEX(m) USE_NL(ah,m)*/ 1'
        ||'     FROM maxdata.'||r_table.source_nm||' m'
        ||'     WHERE time_level=ah.time_level AND time_id=ah.time_id)';
    EXECUTE IMMEDIATE v_sql;

    n_sqlnum:=22200;
    --third,set the flags for each aggregated/indexed partition--
    v_sql:='UPDATE /*+LEADING(ah)*/ maxdata.agah_aggregate_header ah'
        ||' SET aggregated_flg=1'
        ||' WHERE table_nm='''||r_table.table_nm||''''
        ||' AND COALESCE(do_agg_flg,-1)!=0'
        ||' AND EXISTS(SELECT /*+FIRST_ROWS(1) INDEX(m) USE_NL(ah,m)*/ 1'
        ||'     FROM maxdata.'||r_table.source_nm||' m'
        ||'     WHERE time_level=ah.time_level AND time_id=ah.time_id'
        ||'     AND merch_level=1 AND location_level=1)';
    EXECUTE IMMEDIATE v_sql;

    COMMIT;

    n_sqlnum:=22300;
    DECLARE
    CURSOR c_header_list IS
        SELECT time_level,MIN(time_id) lower_id,MAX(time_id) upper_id 
        FROM maxdata.agah_aggregate_header
        WHERE table_nm=r_table.table_nm
        AND base_loaded_flg=1
        GROUP BY time_level;
    BEGIN
    FOR r_partition IN c_header_list
    LOOP
        --Flag any partitions between lower and upper bounds of each time_level
        --which are empty or not aggregated--
        n_sqlnum:=22310;
        UPDATE maxdata.agah_aggregate_header
            SET method_nm='WARNING'
            WHERE table_nm=r_table.table_nm
            AND time_level=r_partition.time_level
            AND(time_id BETWEEN r_partition.lower_id AND r_partition.upper_id)
            AND( (base_loaded_flg IS NULL) OR (aggregated_flg IS NULL) )
            AND COALESCE(do_agg_flg,-1)!=0;
    END LOOP;
    COMMIT;
    END;
END LOOP; 

n_sqlnum:=30000;
SELECT SIGN(COUNT(*)) INTO t_exists_flg
FROM maxdata.agah_aggregate_header
WHERE table_nm LIKE in_table_set
AND method_nm='WARNING';

n_sqlnum:=30100;
IF ( (t_exists_flg=1) AND (in_nowarn_flg!=1)) THEN
    t_error_msg:='WARNING: empty or unaggregated partitions found. To skip, set DO_AGG_FLG=0.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;


EXCEPTION
WHEN OTHERS THEN
    t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
    ROLLBACK;
    maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
