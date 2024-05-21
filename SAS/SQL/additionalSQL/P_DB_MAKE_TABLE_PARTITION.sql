--------------------------------------------------------
--  DDL for Procedure P_DB_MAKE_TABLE_PARTITION
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DB_MAKE_TABLE_PARTITION" (
	in_year_id	     INTEGER,  -- lv1time_lkup_id from LV1TIME table; specify -1 for ALL years --
	in_table_nm	     VARCHAR2,
	in_action_cd	 VARCHAR2 :='PREVIEW', -- Specify 'EXECUTE' to actually create.
	in_tablespace_nm VARCHAR2 :='EACH',
		-- If EACH, each partition is assigned its own tablespace (must already exist).
		-- If MAXVAL, puts ALL table partitions in same tablespace as _MAXVAL partitions.
		-- If <tablespace> is specified, ALL table partitions will be placed there;
	in_indexspace_nm VARCHAR2 :='EACH'
		-- If EACH, each partition is assigned its own tablespace (must already exist).
		-- If MAXVAL, puts ALL index partitions in same tablespace as _MAXVAL partitions.
		-- If <tablespace> is specified, ALL index partitions will be placed there;
)
/*--------------------------------------------------------------------------------
$Log: 2082_ORA_p_db_make_table_partition.sql,v $
Revision 1.6.8.5  2008/12/11 18:17:29  anchan
FIXID S0548536: accomodate ASM in 10g

Revision 1.6.8.3  2008/10/06 15:19:14  anchan
No comment given.

Revision 1.6.8.2  2008/09/29 19:33:59  anchan
No comment given.

Revision 1.6.8.1  2008/09/29 15:19:27  anchan
Added two optional parameters for specifying table and index tablespaces.
Accept EACH,MAXVAL as tablespace name.


Revision 1.2  2006/12/02 22:09:52  anchan
Removed "mmax_" prefix

Revision 1.1  2006/11/28 17:51:14  anchan
For DBA/PS folks when setting up a new database.

=====DESCRIPTION=====
Automatically creates partitions for each of the partitions and indexes.

For use with a RANGE-partitioned table only.

The "p_db_make_tablespace" procedure must have been run prior to running this proc
Make sure that the necessary tablespaces have been created beforehand for each of
the table and index partitions.

Table must have been setup with the 5 _MAXVAL partitions.

***NOTE: Partitions may only be created for later years than any existing ones.***

----------------------------------------------------------------------------------*/
AS
t_row_count				NUMBER(10);
t_period_unit			CHAR(1);
t_seq_no				NUMBER(6);
t_period_cd				VARCHAR2(2);
t_maxval_name         	VARCHAR2(30);
t_maxval_partition_nm	VARCHAR2(30);
t_target_partition_nm	VARCHAR2(30);
t_target_tablespace_nm	VARCHAR2(30) :=in_tablespace_nm;
t_target_indexspace_nm	VARCHAR2(30) :=in_indexspace_nm;

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_db_make_table_partition';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(4000) 		:= NULL;
t_sql2				    VARCHAR2(255);
t_sql3				    VARCHAR2(255);
t_error_msg    			VARCHAR2(1000);

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
        COALESCE(in_action_cd, 'NULL')  || ',' ||
        COALESCE(in_tablespace_nm, 'NULL')  || ',' ||
        COALESCE(in_indexspace_nm, 'NULL')  ||
	' ) ';

maxdata.p_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum);

IF in_action_cd NOT IN('PREVIEW','EXECUTE') THEN
    t_error_msg:='Invalid action command. Must be one of: PREVIEW,EXECUTE.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;

n_sqlnum := 2000;
v_sql := 'TRUNCATE TABLE maxdata.DBMS_make_sql';
EXECUTE IMMEDIATE v_sql;

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
LOOP

IF (t_maxval_name=in_table_nm||'_Y_MAXVAL')THEN
    SELECT DECODE(one.time_level,47,'y',48,'s',49,'q',50,'m',51,'w','?')
    INTO t_period_cd FROM dual WHERE ROWNUM=1;
ELSE
    t_period_cd:=TO_CHAR(one.time_level);
END IF;

t_maxval_partition_nm := in_table_nm||'_'||t_period_cd||'_maxval';
t_target_partition_nm := in_table_nm||'_'||t_period_cd||'_'||one.time_id;


IF UPPER(in_indexspace_nm)='EACH' OR (UPPER(in_indexspace_nm)<>'MAXVAL') THEN
BEGIN

	n_sqlnum := 3010;
	t_seq_no :=t_seq_no+1;

    IF UPPER(in_indexspace_nm)='EACH' THEN
        t_target_indexspace_nm := t_target_partition_nm||'_idx';
    END IF;
	v_sql :=  ' ALTER INDEX maxdata.ui_'||in_table_nm
			||' MODIFY DEFAULT ATTRIBUTES TABLESPACE '||t_target_indexspace_nm;

	n_sqlnum := 3020;
	INSERT INTO maxdata.DBMS_make_sql(table_nm,run_seq_no,sql_string)
	VALUES(in_table_nm,t_seq_no,v_sql);

	n_sqlnum := 3030;
	IF in_action_cd='EXECUTE' THEN
		EXECUTE IMMEDIATE v_sql;
	END IF;
END;
END IF;

-----
n_sqlnum := 3100;
t_seq_no :=t_seq_no+1;

v_sql :=  ' ALTER TABLE '||in_table_nm
	||' SPLIT PARTITION '||t_maxval_partition_nm||' AT ('||one.time_level||','||(one.time_id+1)||')'
	||' INTO (PARTITION '||t_target_partition_nm||',PARTITION '||t_maxval_partition_nm||')';
INSERT INTO maxdata.DBMS_make_sql(table_nm,run_seq_no,sql_string)
VALUES(in_table_nm,t_seq_no,v_sql);

IF in_action_cd='EXECUTE' THEN
	EXECUTE IMMEDIATE v_sql;
END IF;
-----

IF UPPER(in_tablespace_nm)='EACH' OR (UPPER(in_tablespace_nm)<>'MAXVAL') THEN
BEGIN
	n_sqlnum := 3200;
	t_seq_no :=t_seq_no+1;

    IF UPPER(in_indexspace_nm)='EACH' THEN
        t_target_tablespace_nm := t_target_partition_nm;
    END IF;

	v_sql :=  ' ALTER TABLE '||in_table_nm
		||' MOVE PARTITION '||t_target_partition_nm
		||' TABLESPACE '||t_target_tablespace_nm;

	n_sqlnum := 3210;
	INSERT INTO maxdata.DBMS_make_sql(table_nm,run_seq_no,sql_string)
	VALUES(in_table_nm,t_seq_no,v_sql);

	n_sqlnum := 3220;
	IF in_action_cd='EXECUTE' THEN
		EXECUTE IMMEDIATE v_sql;
	END IF;
END;
END IF;


END LOOP; --c_time_level_id--


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
