--------------------------------------------------------
--  DDL for Procedure P_DB_MAKE_TABLESPACE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DB_MAKE_TABLESPACE" (
	in_year_id	    INTEGER,  -- lv1time_lkup_id from LV1TIME table; specify -1 for ALL years --
	in_table_nm		VARCHAR2,
	in_table_path	VARCHAR2,
	in_index_path	VARCHAR2,
	in_action_cd	VARCHAR2 := 'PREVIEW', -- Specify 'EXECUTE' to actually create.
	in_extent_size	INTEGER   := 32 -- Recommeded size for TABLE extents(MB); minimum is 2 MB.--
	-- INDEX extents will be built with half the size of TABLE extents.
)
/*--------------------------------------------------------------------------------
$Log: 2080_ORA_p_db_make_tablespace.sql,v $
Revision 1.5.8.4  2008/12/11 18:17:30  anchan
FIXID S0548536: accomodate ASM in 10g

Revision 1.5.8.2  2008/09/29 19:34:11  anchan
No comment given.

Revision 1.5.8.1  2008/09/29 15:18:22  anchan
Allow ASM in filepath

Revision 1.5  2007/06/19 14:40:03  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.1  2006/11/28 17:51:15  anchan
FIXID : For DBA/PS folks when setting up a new database.

=====DESCRIPTION=====
For use with a RANGE-partitioned table only.

Automatically creates tablespaces for each of the partitions and indexes.

NOTE: Extents larger than 32MB may cause SQLLDR to occupy too much wasted space.

----------------------------------------------------------------------------------*/
AS
t_row_count			    NUMBER(10);
t_period_unit			CHAR(1);
t_table_datafile		VARCHAR2(255) :='';
t_index_datafile		VARCHAR2(255) :='';
t_table_extent_spec		VARCHAR2(255);
t_index_extent_spec		VARCHAR2(255);
t_seq_no			    NUMBER(6);
q				        CHAR(1)			:=''''; --single quotation mark
t_period_cd				VARCHAR2(2);
t_maxval_name           VARCHAR2(30);

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_db_make_tablespace';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				    VARCHAR2(255);
t_sql3				    VARCHAR2(255);
t_error_msg	            VARCHAR2(1000) := NULL;

--Oracle only--

CURSOR c_time_level_id IS
SELECT 47 time_level,lv1time_lkup_id time_id
FROM maxapp.lv1time
WHERE lv1time_lkup_id=in_year_id OR in_year_id=-1
UNION
SELECT 48 time_level,lv2time_lkup_id time_id
FROM maxapp.lv2time
WHERE lv1time_lkup_id=in_year_id OR in_year_id=-1
UNION
SELECT 49 time_level,lv3time_lkup_id time_id
FROM maxapp.lv3time
WHERE lv1time_lkup_id=in_year_id OR in_year_id=-1
UNION
SELECT 50 time_level,lv4time_lkup_id time_id
FROM maxapp.lv4time
WHERE lv1time_lkup_id=in_year_id OR in_year_id=-1
UNION
SELECT 51 time_level,lv5time_lkup_id time_id
FROM maxapp.lv5time
WHERE lv1time_lkup_id=in_year_id OR in_year_id=-1
ORDER BY time_level,time_id;


BEGIN
n_sqlnum := 1000;
t_call := t_proc_name || ' ( ' ||
        COALESCE(in_year_id, -123)     || ',' ||   -- NOTE: COALESCE(int, 'NULL') returns error because of diff datatype.
        COALESCE(in_table_nm, 'NULL')  || ',' ||
        COALESCE(in_table_path, 'NULL')  || ',' ||
        COALESCE(in_index_path, 'NULL')  || ',' ||
        COALESCE(in_action_cd, 'NULL')  || ',' ||
        COALESCE(in_extent_size, -123) ||
	' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum);

IF in_action_cd NOT IN('PREVIEW','EXECUTE') THEN
    t_error_msg:='Invalid action command. Must be one of: PREVIEW,EXECUTE.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

t_table_extent_spec :=
	' SIZE '||TO_CHAR(in_extent_size*2)||'M'
	||' AUTOEXTEND ON NEXT '||TO_CHAR(GREATEST(in_extent_size,2))||'M'
	||' MAXSIZE UNLIMITED EXTENT MANAGEMENT LOCAL UNIFORM SIZE '||TO_CHAR(in_extent_size)||'M';


t_index_extent_spec :=
	' SIZE '||TO_CHAR(in_extent_size)||'M'
	||' AUTOEXTEND ON NEXT '||TO_CHAR(GREATEST(TRUNC(in_extent_size/2),1))||'M'
	||' MAXSIZE UNLIMITED EXTENT MANAGEMENT LOCAL UNIFORM SIZE '||TO_CHAR(in_extent_size/2)||'M';


n_sqlnum := 2000;
v_sql := 'TRUNCATE TABLE maxdata.DBMS_make_sql';
EXECUTE IMMEDIATE v_sql;
-------------------

SELECT MIN(partition_name) INTO  t_maxval_name
FROM dba_tab_partitions
WHERE table_owner='MAXDATA' AND table_name=in_table_nm
AND partition_name IN(in_table_nm||'_47_MAXVAL',in_table_nm||'_Y_MAXVAL');

IF (t_maxval_name IS NULL)THEN
    t_error_msg:='MAXVAL partition does not exist or incorrectly named.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

t_seq_no :=0;

n_sqlnum := 3000;
FOR one IN c_time_level_id
LOOP --MAIN--

IF (t_maxval_name=in_table_nm||'_Y_MAXVAL')THEN
    SELECT DECODE(one.time_level,47,'y',48,'s',49,'q',50,'m',51,'w','?')
    INTO t_period_cd FROM dual WHERE ROWNUM=1;
ELSE
    t_period_cd:=TO_CHAR(one.time_level);
END IF;

n_sqlnum := 3100;
t_seq_no := t_seq_no + 1;

IF (SUBSTR(in_table_path,1,1)<>'+') THEN --if not ASM--
   t_table_datafile:=LOWER(in_table_nm)||'_'||t_period_cd||'_'||one.time_id||'_01.dbf';
END IF;

v_sql :=  ' CREATE TABLESPACE '||UPPER(in_table_nm)||'_'||t_period_cd||'_'||one.time_id
	||' DATAFILE '||q||in_table_path||t_table_datafile||q
	||t_table_extent_spec;

INSERT INTO maxdata.DBMS_make_sql(table_nm,run_seq_no,sql_string)
VALUES(in_table_nm,t_seq_no,v_sql);

IF in_action_cd='EXECUTE' THEN
	EXECUTE IMMEDIATE v_sql;
END IF;

n_sqlnum := 3200;
t_seq_no := t_seq_no + 1;

IF (SUBSTR(in_index_path,1,1)<>'+') THEN --if not ASM--
   t_index_datafile:=LOWER(in_table_nm)||'_'||t_period_cd||'_'||one.time_id||'_idx_01.dbf';
END IF;

v_sql :=  ' CREATE TABLESPACE '||UPPER(in_table_nm)||'_'||t_period_cd||'_'||one.time_id||'_IDX'
	||' DATAFILE '||q||in_index_path||t_index_datafile||q
	||t_index_extent_spec;

INSERT INTO maxdata.DBMS_make_sql(table_nm,run_seq_no,sql_string)
VALUES(in_table_nm,t_seq_no,v_sql);

IF in_action_cd='EXECUTE' THEN
	EXECUTE IMMEDIATE v_sql;
END IF;

END LOOP; --MAIN--

COMMIT;

EXCEPTION
        WHEN OTHERS THEN
                ROLLBACK;

                IF v_sql IS NOT NULL THEN
                        t_error_level := 'info';
                        t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
                        maxdata.p_log (t_proc_name, t_error_level, v_sql, t_sql2, n_sqlnum);
                END IF;

                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM || ' (' || t_call ||
                                ', SQL#:' || n_sqlnum || ')';
                maxdata.p_log (t_proc_name, t_error_level, v_sql, NULL, n_sqlnum);
                --COMMIT;

                RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/
