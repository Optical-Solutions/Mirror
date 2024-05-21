--------------------------------------------------------
--  DDL for Procedure P_DROP_T_TMPL_TBL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DROP_T_TMPL_TBL" 
AS
------------------------------------------------------------------------
-- Change History:
-- V5.3.4
-- Backported from 5.4
-- V5.4
-- 5.4.0-015 09/16/2002 Sachin	Removed Exception Block so that error is shown if any.
-- 5.4.0-000 08/14/2002 Sachin Ghaisas	Initial entry.

-- V5.3.4
-- 03/21/03 Sachin  Support for 'T_AL%' tables
-- 03/11/03 Sachin	Added NVL function

-- Description:
-- Drop the tmpl tables if the lock_id's are not present
-- in maxdata.mmax_locks table.
------------------------------------------------------------------------

t_lock_id	VARCHAR2(100);
t_sql 		VARCHAR2(1000):= '';
t_check 	NUMBER := 0;
t_sqlnum 	NUMBER;

BEGIN

DECLARE CURSOR c_get_tmpl_tables
IS
SELECT table_name,
DECODE (SUBSTR(table_name,1,4), 'T_AL', SUBSTR(table_name,6,3),SUBSTR(table_name,8,3)) AS memlev,
DECODE (SUBSTR(table_name,1,4),
'T_AL', DECODE(SUBSTR(table_name,6,3),'MEM',SUBSTR(table_name,9),'LEV',SUBSTR(table_name,9),SUBSTR(table_name,5)),
DECODE(SUBSTR(table_name,8,3),'MEM',SUBSTR(table_name,11),'LEV',SUBSTR(table_name,11),SUBSTR(table_name,7))) AS name
FROM all_tables
WHERE owner = 'MAXDATA'
AND (UPPER(table_name) LIKE 'T_TMPL%'
OR UPPER(table_name) LIKE 'T_AL%')
AND UPPER(table_name) NOT LIKE 'T_AL_6KEY%'
ORDER BY
DECODE(SUBSTR(table_name,1,4),'T_AL', NVL(SUBSTR(table_name,6,3),'0'),NVL(SUBSTR(table_name,8,3),'0')) DESC ;

BEGIN
FOR c1 IN c_get_tmpl_tables
LOOP
	IF c1.memlev IN ('MEM','LEV') THEN
		t_lock_id := c1.name;
	ELSE
		IF 	SUBSTR(c1.name,1,1) = '_'  AND
			(INSTR(UPPER(c1.name),'_PVS',1) > 0 OR
			INSTR(UPPER(c1.name),'_WRKSHTS',1) > 0 OR
			INSTR(UPPER(c1.name),'_REF',1) > 0 OR
			INSTR(UPPER(c1.name),'_CHILDREN',1) > 0 OR
			INSTR(UPPER(c1.name),'_PARENTS',1) > 0 OR
			INSTR(UPPER(c1.name),'_TREES',1) > 0 ) THEN

			t_lock_id := SUBSTR(c1.name,2,INSTR(c1.name,'_',2)-2);
		ELSE
			IF SUBSTR(c1.name,1,1) <> '_' AND INSTR(UPPER(c1.name),'_LIST',1) > 0 THEN
				t_lock_id := SUBSTR(c1.name,1,INSTR(c1.name,'_',1)-1);
			ELSE
				t_lock_id := c1.name;
			END IF;
		END IF;
	END IF;

	t_sqlnum := 1000;
	SELECT MAX(lock_id) INTO t_check FROM maxdata.mmax_locks
	WHERE TO_CHAR(lock_id) = t_lock_id;

	IF t_check IS NULL THEN
		t_sqlnum := 2000;
		t_sql := 'DROP TABLE MAXDATA.'|| c1.table_name;
		EXECUTE IMMEDIATE t_sql;
		COMMIT;
	END IF;
	t_check := 0;
	t_sql := '';
END LOOP;
END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_DROP_T_TMPL_TBL" TO "MADMAX";
