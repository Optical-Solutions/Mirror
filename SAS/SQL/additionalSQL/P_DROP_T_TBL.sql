--------------------------------------------------------
--  DDL for Procedure P_DROP_T_TBL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DROP_T_TBL" 
AS

/*------------------------------------------------------------------------
$Log: 2208_p_drop_t_tbl.sql,v $
Revision 1.12.2.1  2008/03/12 20:52:39  vejang
613 : Just change the files datetime

Revision 1.12  2008/03/11 13:37:38  dirapa
--MMMR66156, MMMR65824

Revision 1.11  2008/02/14 15:51:42  dirapa
Fixid: S0460437

Revision 1.10  2007/06/19 14:39:19  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.6  2006/07/28 19:02:12  makirk
Removed "created" from cursor select (unneeded)

Revision 1.5  2006/07/28 15:41:43  makirk
Added new logic for handling T_GPF and T_BI_SAS temp tables

Revision 1.4  2006/03/02 22:30:57  makirk
Modified wildcard search expressions and added T_BI and T_GPF checks

Revision 1.3  2006/02/17 22:18:57  healja
Replace $id with $Log
 2208_p_drop_t_tbl.sql,v 1.2 2005/08/01 18:18:11 dirapa Exp $

-- Change History:
-- V5.6
-- 5.6.0-029-18  05/17/04   Sachin  Added support for 'T_SEED_%' tables
-- V5.3.4
-- 05/07/03  Sachin  Added support for 'T_PWID_%' tables
-- 03/21/03  Sachin  Added support for 'T_AL%'
-- V5.4
-- 5.4.0-015 09/16/2002 Sachin Ghaisas  Initial entry.

-- Description:
-- Drop the temporary tables if the lock_id's are not present
-- in maxdata.mmax_locks table.
------------------------------------------------------------------------*/

t_lock_id      VARCHAR2(100);
t_sql          VARCHAR2(1000)  := '';
t_check        NUMBER          := 0;
t_sqlnum       NUMBER;
t_str_null     VARCHAR2(255)   :=  NULL;
t_int_null     NUMBER(10)      :=  NULL;

-- Ignore error flag set to on (object doesn't exist, etc)
t_ignore_error NUMBER(1)       := 1;

t_valid_days       NUMBER;
t_last_accessed    DATE;
t_drop_table	NUMBER;
t_row_count        NUMBER;
t_transaction_id	NUMBER;

BEGIN

-- DROP BASED ON LOCKS CHECK
-- If cube_id in table name is not in mmax_locks then drop it
DECLARE CURSOR c_get_temp_tables IS
SELECT table_name,
	DECODE(SUBSTR(table_name,1,10),'T_AL_6KEY_',SUBSTR(table_name,11,INSTR(table_name,'_',11)-11),
    	DECODE(SUBSTR(table_name,1,7),'T_6KEY_',    SUBSTR(table_name,8, INSTR(table_name,'_',8)-8),
        DECODE(SUBSTR(table_name,1,7),'T_SEED_',    SUBSTR(table_name,8, INSTR(table_name,'_',8)-8),
        DECODE(SUBSTR(table_name,1,7),'T_PWID_',    SUBSTR(table_name,8, INSTR(table_name,'_',8)-8))))) AS t_6key_name,
 	DECODE(SUBSTR(table_name,1,4),'T_PC',       SUBSTR(table_name,5)) AS t_pc_name
FROM all_tables
WHERE owner = 'MAXTEMP'
AND UPPER(table_name)  LIKE 'T\_AL\_6KEY_%\_%\_%' ESCAPE '\'
OR  UPPER(table_name)  LIKE 'T\_6KEY\_%\_%\_%'    ESCAPE '\'
OR  UPPER(table_name)  LIKE 'T\_SEED\_%\_%\_%'    ESCAPE '\'
OR  UPPER(table_name)  LIKE 'T\_PWID\_%\_%\_%'    ESCAPE '\'
OR  (UPPER(table_name) LIKE 'T\_PC%'              ESCAPE '\'
     AND UPPER(table_name) NOT LIKE 'T\_PC%\_%'   ESCAPE '\');

BEGIN
FOR c1 IN c_get_temp_tables
LOOP
    t_sqlnum := 500;
    IF c1.t_6key_name IS NOT NULL THEN
        t_lock_id := c1.t_6key_name;
    END IF;

    IF c1.t_pc_name IS NOT NULL THEN
        t_lock_id := c1.t_pc_name;
    END IF;

    t_sqlnum := 1000;
    SELECT MAX(lock_id) INTO t_check
    FROM maxdata.mmax_locks
    WHERE TO_CHAR(lock_id) = t_lock_id;

    IF t_check IS NULL THEN
        t_sqlnum := 2000;
        t_sql := 'DROP TABLE MAXTEMP.'||c1.table_name;
        maxtemp.p_exec_temp_ddl(1,t_sql,t_str_null,t_str_null,t_str_null,t_str_null,t_int_null,t_int_null,t_str_null);
        COMMIT;
    END IF;
    t_check := 0;
    t_sql := '';
END LOOP;
END;

-- DROP GPFs BASED ON CREATE DATE
-- If table is > 2 days since create date then drop it
t_sqlnum := 3000;
DECLARE CURSOR c_get_gpf_tables IS
SELECT 'MAXTEMP.'||object_name tabname
FROM all_objects
WHERE object_name LIKE 'T\_GPF\_%' ESCAPE '\'
AND object_type = 'TABLE'
AND TRUNC(SYSDATE) - TRUNC(created) > 2
AND owner = 'MAXTEMP';

BEGIN
FOR c2 IN c_get_gpf_tables
LOOP
    t_sqlnum := 4000;
    t_sql := 'DROP TABLE '||c2.tabname;
    maxtemp.p_exec_temp_ddl(t_ignore_error,t_sql,t_str_null,t_str_null,t_str_null,t_str_null,t_int_null,t_int_null,t_str_null);
END LOOP;
END;

-- DROP BIs BASED ON BIWT_WORKFLOW_TRANSACTION table and Valid days in t_application_property

t_sqlnum := 4500;
SELECT COALESCE(PROPERTY_VALUE,DEFAULT_VALUE) INTO t_valid_days
FROM maxdata.t_application_property
WHERE property_id = 1210;


t_sqlnum := 5000;
DECLARE CURSOR c_get_bi_tables IS
SELECT 'MAXTEMP.'||table_name tabname
FROM all_tables
WHERE table_name LIKE 'T\_BI\_SAS\_%' ESCAPE '\'
AND owner = 'MAXTEMP';

BEGIN
FOR c3 IN c_get_bi_tables
LOOP
	t_sqlnum := 6000;
	t_sql   := NULL;
	t_drop_table := 0;
	t_row_count := 0;
	t_transaction_id := NULL;


	BEGIN
	t_sqlnum := 6500;

	SELECT 1, create_dttm, transaction_id
	INTO   t_row_count, t_last_accessed, t_transaction_id
	FROM
		(SELECT transaction_id, metadata_nm table_nm,create_dttm from maxdata.biwt_workflow_transaction
			WHERE  UPPER(metadata_nm) = UPPER(c3.tabname)
		UNION
		SELECT transaction_id,fact_nm table_nm,create_dttm from maxdata.biwt_workflow_transaction
			WHERE  UPPER(fact_nm) = UPPER(c3.tabname)
		UNION
		SELECT transaction_id,dimension_nm table_nm,create_dttm from maxdata.biwt_workflow_transaction
			WHERE  UPPER(dimension_nm) = UPPER(c3.tabname)
		UNION
		SELECT transaction_id, property_nm table_nm,create_dttm from maxdata.biwt_workflow_transaction
			WHERE  UPPER(property_nm) = UPPER(c3.tabname)
		)  tabx
	WHERE upper(tabx.table_nm)=UPPER(c3.tabname);

	EXCEPTION
		WHEN NO_DATA_FOUND THEN
		    t_row_count := 0;
	END;

	IF t_row_count = 1 THEN
	BEGIN
		t_sqlnum := 6550;

		IF  TO_CHAR(SYSDATE,'J')- TO_CHAR(t_last_accessed,'J') > t_valid_days THEN
			t_drop_table := 1;
		END IF;
	END;
	ELSE
		t_drop_table := 1;
	END IF;

	IF t_drop_table = 1 THEN

		t_sqlnum := 7000;

		t_sql := 'DROP TABLE '||c3.tabname;
		maxtemp.p_exec_temp_ddl(t_ignore_error,t_sql,t_str_null,t_str_null,t_str_null,t_str_null,t_int_null,t_int_null,t_str_null);

		BEGIN
			DELETE FROM maxdata.BIWT_WORKFLOW_TRANSACTION WHERE transaction_id = t_transaction_id;
		EXCEPTION
		WHEN NO_DATA_FOUND THEN
			NULL;
		END;

	END IF; -- if t_drop_table = 1;
END LOOP;
END;

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_DROP_T_TBL" TO "MADMAX";
