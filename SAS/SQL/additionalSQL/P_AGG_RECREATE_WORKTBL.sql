--------------------------------------------------------
--  DDL for Procedure P_AGG_RECREATE_WORKTBL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_RECREATE_WORKTBL" (
    in_source_tbl    VARCHAR2,
    in_target_tbl    VARCHAR2
) 
AS    
/*
------------------------------------------------------------------------------
$Log: 5240_IDA_p_agg_recreate_worktbl.sql,v $
Revision 1.1.2.1.2.2  2009/05/14 20:56:02  anchan
Check for UNUSED columns

Revision 1.1.2.1.2.1  2009/05/13 20:02:14  anchan
PERFORMANCE: use separate work tables for each of the M-L-T aggregation dimensions.

Revision 1.1.2.1  2008/11/26 17:30:12  anchan
FIXID : BASELINE check-in

==========
The #COUNT table must already exist and is checked for existence only.
The #TIME,#MERCH, and #LOC tables will be created if they don't exist
or if they're even slightly different than the source/base table.
------------------------------------------------------------------------------
*/
    t_count_tbl VARCHAR2(30):=in_source_tbl||'#COUNT';
    t_target_tblspace VARCHAR2(30);
    t_exists_flg NUMBER(1);

    t_mismatch_cnt NUMBER(4);
    t_query_hint VARCHAR2(100);
    t_table_hint VARCHAR2(500);
    t_parallel_deg NUMBER(2);

    t_proc_name VARCHAR2(30):='p_agg_recreate_worktbl';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(8000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg     VARCHAR2(1000) := NULL;

    CURSOR c_work_tables IS
        SELECT table_name
        FROM user_tables 
        WHERE table_name IN(in_source_tbl||'#MERCH',in_source_tbl||'#LOC',in_source_tbl||'#TIME');
 
BEGIN
n_sqlnum:=10000;
t_call := t_proc_name || ' ( ' ||
    in_source_tbl|| 
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, NULL, n_sqlnum);

----------------------------------------------
n_sqlnum:=11000;
SELECT MIN(tablespace_name)
INTO t_target_tblspace
FROM user_tab_partitions
WHERE table_name=t_count_tbl;

IF (t_target_tblspace IS NULL) THEN
    t_error_msg:='The table '||t_count_tbl||' does not exist, or not partitioned(required even if no COUNT kpis are used).';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=12000;
--check if any #COUNT columns are not found in the base table--
SELECT SIGN(COUNT(*)) INTO t_exists_flg
FROM user_tab_columns
WHERE table_name=t_count_tbl
AND column_name NOT IN
    (SELECT column_name FROM user_tab_columns WHERE table_name=in_source_tbl);
IF (t_exists_flg=1) THEN
    t_error_msg:='Column names in '||t_count_tbl||' must be the same as the base table '||in_source_tbl;
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum:=13000;
maxdata.p_get_query_hint('AGG_LOAD_C','[C]',t_table_hint,t_query_hint);
t_parallel_deg:=f_parallel_deg(t_table_hint,'C');

n_sqlnum:=13100;
v_sql:='ALTER TABLE '||t_count_tbl||' NOLOGGING PARALLEL '||t_parallel_deg;
EXECUTE IMMEDIATE v_sql;

--Any UNUSED columns will prevent EXCHANGE PARTITION later--
n_sqlnum:=14000;
SELECT COUNT(*) INTO t_exists_flg
FROM user_unused_col_tabs
WHERE table_name=in_target_tbl;
IF(t_exists_flg>0)THEN
    t_error_msg:='Any hidden "UNUSED" columns in the target table '||in_target_tbl||' must be dropped.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

----------------------------------------------
--Check if the source and target tables' column definitions are exactly the same--
n_sqlnum:=20000;
IF(in_source_tbl!=in_target_tbl)THEN
BEGIN
    n_sqlnum:=20100;
    SELECT COUNT(*) INTO t_mismatch_cnt
    FROM(
        SELECT column_id,column_name,data_type,data_precision,data_scale
        FROM user_tab_columns
        WHERE table_name IN(in_source_tbl,in_target_tbl)
        GROUP BY column_id,column_name,data_type,data_precision,data_scale
        HAVING COUNT(*)<2); --if exactly the same, has COUNT(*)=2--
        
    n_sqlnum:=20200;
    IF(t_mismatch_cnt>0)THEN
        t_error_msg:='ALL column order/definition must be exactly the same between source table '||in_source_tbl
                    ||' and target table '||in_target_tbl||'.';
        RAISE_APPLICATION_ERROR(-20001,t_error_msg);
    END IF;
END;
END IF;

----------------------------------------------
--Check if the source/base and work tables' column definitions are exactly the same--
n_sqlnum:=21000;
SELECT COUNT(*) INTO t_mismatch_cnt
FROM(
    SELECT column_id,column_name,data_type,data_precision,data_scale
    FROM user_tab_columns
    WHERE table_name IN(in_source_tbl,in_source_tbl||'#MERCH',in_source_tbl||'#LOC',in_source_tbl||'#TIME')
    GROUP BY column_id,column_name,data_type,data_precision,data_scale
    HAVING COUNT(*)<4);--if exactly the same, has COUNT(*)=4--
    

IF(t_mismatch_cnt>0)THEN
BEGIN
    n_sqlnum:=21100;
    FOR r_table IN c_work_tables
    LOOP
        v_sql:='TRUNCATE TABLE '||r_table.table_name;
        EXECUTE IMMEDIATE v_sql;    
        v_sql:='DROP TABLE '||r_table.table_name;
        EXECUTE IMMEDIATE v_sql;    
    END LOOP;
    
    BEGIN--MERCH--
    n_sqlnum:=21300;
    v_sql:='CREATE TABLE '||in_source_tbl||'#MERCH'
        ||' TABLESPACE '||t_target_tblspace
        ||' NOLOGGING NOCOMPRESS PCTFREE 0'
        ||' PARTITION BY RANGE (MERCH_LEVEL)'
        ||' SUBPARTITION BY HASH(merch_id)'
        ||' SUBPARTITION TEMPLATE('
        ||'     SUBPARTITION sp1,'
        ||'     SUBPARTITION sp2,'
        ||'     SUBPARTITION sp3,'
        ||'     SUBPARTITION sp4,'
        ||'     SUBPARTITION sp5,'
        ||'     SUBPARTITION sp6,'
        ||'     SUBPARTITION sp7,'
        ||'     SUBPARTITION sp8'
        ||'     )'
        ||' (  '
        ||'   PARTITION M1 VALUES LESS THAN (2),'
        ||'   PARTITION M2 VALUES LESS THAN (3),'
        ||'   PARTITION M3 VALUES LESS THAN (4),'
        ||'   PARTITION M4 VALUES LESS THAN (5),'
        ||'   PARTITION M5 VALUES LESS THAN (6),'
        ||'   PARTITION M6 VALUES LESS THAN (7),'
        ||'   PARTITION M7 VALUES LESS THAN (8),'
        ||'   PARTITION M8 VALUES LESS THAN (9),'
        ||'   PARTITION M9 VALUES LESS THAN (10),'
        ||'   PARTITION M10 VALUES LESS THAN (11)'
        ||' )'
        ||' AS SELECT * FROM '||in_source_tbl||' WHERE 1=0 AND time_level=0';
    EXECUTE IMMEDIATE v_sql;
    END;--MERCH--

    BEGIN--LOC--
    n_sqlnum:=21400;
    v_sql:='CREATE TABLE '||in_source_tbl||'#LOC'
        ||' TABLESPACE '||t_target_tblspace
        ||' NOLOGGING NOCOMPRESS PCTFREE 0'
        ||' PARTITION BY RANGE (LOCATION_LEVEL)'
        ||' SUBPARTITION BY HASH(location_id)'
        ||' SUBPARTITION TEMPLATE('
        ||'     SUBPARTITION sp1,'
        ||'     SUBPARTITION sp2,'
        ||'     SUBPARTITION sp3,'
        ||'     SUBPARTITION sp4,'
        ||'     SUBPARTITION sp5,'
        ||'     SUBPARTITION sp6,'
        ||'     SUBPARTITION sp7,'
        ||'     SUBPARTITION sp8'
        ||'     )'
        ||' (  '
        ||'   PARTITION L1 VALUES LESS THAN (2),'
        ||'   PARTITION L2 VALUES LESS THAN (3),'
        ||'   PARTITION L3 VALUES LESS THAN (4),'
        ||'   PARTITION L4 VALUES LESS THAN (5)'
        ||' )'
        ||' AS SELECT * FROM '||in_source_tbl||' WHERE 1=0 AND time_level=0';
    EXECUTE IMMEDIATE v_sql;
    END;--LOC--

    BEGIN--TIME--
    n_sqlnum:=21500;
    v_sql:='CREATE TABLE '||in_source_tbl||'#TIME'
        ||' TABLESPACE '||t_target_tblspace
        ||' NOLOGGING NOCOMPRESS PCTFREE 0'
        ||' PARTITION BY HASH (MERCH_ID)'
        ||'   PARTITIONS 8'
        ||' AS SELECT * FROM '||in_source_tbl||' WHERE 1=0 AND time_level=0';
    EXECUTE IMMEDIATE v_sql;
    END;--TIME--

END;    
END IF;

n_sqlnum:=30000;
maxdata.p_get_query_hint('AGG_LOAD_M','[M]',t_table_hint,t_query_hint);
t_parallel_deg:=f_parallel_deg(t_table_hint,'M');

n_sqlnum:=30100;
v_sql:='ALTER TABLE '||in_source_tbl||'#MERCH NOLOGGING PARALLEL '||t_parallel_deg;
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=30200;
maxdata.p_get_query_hint('AGG_LOAD_L','[M]',t_table_hint,t_query_hint);
t_parallel_deg:=f_parallel_deg(t_table_hint,'M');

n_sqlnum:=30300;
v_sql:='ALTER TABLE '||in_source_tbl||'#LOC NOLOGGING PARALLEL '||t_parallel_deg;
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=30400;
maxdata.p_get_query_hint('AGG_LOAD_T','[M]',t_table_hint,t_query_hint);
t_parallel_deg:=f_parallel_deg(t_table_hint,'M');

n_sqlnum:=30500;
v_sql:='ALTER TABLE '||in_source_tbl||'#TIME NOLOGGING PARALLEL '||t_parallel_deg;
EXECUTE IMMEDIATE v_sql;

n_sqlnum:=90000;

EXCEPTION
WHEN OTHERS THEN
    t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
    ROLLBACK;
    maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
