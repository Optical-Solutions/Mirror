--------------------------------------------------------
--  DDL for Procedure P_GEN_LOC_TIME_2D
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GEN_LOC_TIME_2D" 
-- $Id: 005_12_p_gen_loc_time_2d.sql,v 1.3 2005/09/14 18:01:01 cbarr Exp $
-- Copyright (c) 2005 SAS Institute Inc., Cary, NC USA
--
-- Populates range between maxdata.client_config 'First Year', 'Last Year'
--
-- Date      By      Comment
-- 07/22/05  TS/kg   Adapted.
-- 07/31/05  cbarr   fixed iMin/MaxTimeId exec immediate
-- 09/14/05  kgall   Fixed issues with active, bop, cum eop
-- ----------------------------------------------------------------------------
AS

---  This process populates the two dimension table of location and time hierarchy information
---  It reads information from lv4loc table and populates data for location /time hierarchy
---
iCtr		INTEGER;
iTimeID		NUMBER(10,0);
iPeriod		NUMBER(6,0);
iMinTimeID	NUMBER(10,0);
iMaxTimeID	NUMBER(10,0);
iFirstCycle   NUMBER(10,0);
iLastCycle   NUMBER(10,0);
iKeyValue   VARCHAR2(50);
iSql		LONG;

iEOPStore	INTEGER;
iNewStore	INTEGER;
iCloseStore	INTEGER;
iActive		INTEGER;

isid 		INT;
iserial 	INT;
iTabName	VARCHAR2(80);
iRtn    	NUMBER;

CURSOR c_time IS
	SELECT lv5time_id period, cycle_id CYCLE, lv5time_lkup_id time_id, lv4time_lkup_id,
		lv5time_start_date start_date, lv5time_end_date end_date, days_in_period, loaded_flag, time_name
	FROM maxapp.lv5time WHERE lv5time_lkup_id BETWEEN iMinTimeID AND iMaxTimeID;

CURSOR c_loc IS
	SELECT lv4loc_id, start_date, close_date FROM maxdata.LV4LOC
		WHERE lv1loc_id IN (SELECT lv1loc_id FROM maxdata.LV1LOC WHERE num_user1 = 1 );

BEGIN
	--- Get the future week information
	iKeyValue := 'First Year';
	SELECT count(value_1) INTO iCtr FROM maxdata.CLIENT_CONFIG WHERE key_1 = iKeyValue;
	if iCtr != 1 then
	  dbms_output.put_line('Error need a KEY_1 row for '||iKeyValue);
	  iRtn := 1;
	  goto set_return;
	end if;
	SELECT value_1 INTO iFirstCycle FROM maxdata.CLIENT_CONFIG WHERE key_1 = iKeyValue;

	iKeyValue := 'Last Year';
	SELECT count(value_1) INTO iCtr FROM maxdata.CLIENT_CONFIG WHERE key_1 = iKeyValue;
	if iCtr != 1 then
	  dbms_output.put_line('Error need a KEY_1 row for '||iKeyValue);
	  iRtn := 2;
	  goto set_return;
	end if;
	SELECT value_1 INTO iLastCycle FROM maxdata.CLIENT_CONFIG WHERE key_1 = iKeyValue;

	SELECT MIN(lv5time_lkup_id) INTO iMinTimeID FROM maxapp.lv5time WHERE cycle_id = iFirstCycle;

	SELECT MAX(lv5time_lkup_id) INTO iMaxTimeID FROM maxapp.lv5time WHERE cycle_id = iLastCycle;

	maxdata.Ins_Import_Log('p_gen_loc_time_2D','Information', 'Start populate loc/time table', NULL,NULL,NULL) ;

	iSql := 'Truncate table maxdata.loc_time_2D';

	EXECUTE IMMEDIATE iSql;

	-- Get unique session information
	--- iSql := 'SELECT DISTINCT b.SID, b.serial#  FROM v$mystat a, v$session b WHERE a.SID=b.SID';
	isid := USERENV('sessionid');
	iserial :=0;

	--- EXECUTE IMMEDIATE iSql INTO isid, iserial;

	iTabName := 't_lt'||isid||iserial;

FOR c0 IN c_time LOOP

  FOR c1 IN c_loc LOOP

	iCloseStore := 0;
	iActive := 0;
	iNewStore := 0;
	iEOPStore	:= 0;

	IF ((c1.start_date IS NOT NULL AND c1.start_date <= c0.start_date)
	   AND (c1.close_date IS NULL OR c1.close_date > c0.end_date)) THEN
		iActive := 1;
	END IF;

	IF ((c1.start_date IS NOT NULL AND c1.start_date <= c0.end_date)
	   AND (c1.close_date IS NULL OR c1.close_date > c0.end_date)) THEN
		iEOPStore := 1;
	END IF;

	IF (c1.start_date BETWEEN c0.start_date AND c0.end_date) THEN
		iNewStore := 1;
	END IF;

	IF (c1.close_date BETWEEN c0.start_date AND c0.end_date) THEN
		iCloseStore := 1;
	END IF;

	INSERT INTO maxdata.LOC_TIME_2D(
		time_level,
		time_ID,
		location_level,
		location_id,
		LT_period,
		LT_cycle,
		LT_Days_in_period,
		LT_store_count,
		LT_period_count,
		LT_loaded_period_count,
		LT_new_store_count,
		LT_closed_store_count,
		LT_comp_store_count,
		LT_active_store_count,
		LT_BOP_active_store_count,
		LT_EOP_active_store_count,
		LT_Cum_EOP,
		LT_Char_Attr_1,
		LT_Char_Attr_2,
		LT_Char_Attr_3,
		LT_Char_Attr_4,
		LT_Char_Attr_5,
		LT_Num_Attr_6,
		LT_Num_Attr_7,
		LT_Num_Attr_8,
		LT_Num_Attr_9,
		LT_Num_Attr_10,
		LT_init_date)
	VALUES(
		51,
		c0.time_id,
		4,
		c1.lv4loc_id,
		c0.Period,
		c0.CYCLE,
		c0.days_in_period,
		1,
		1,
		c0.loaded_flag,
		iNewStore,
		iCloseStore,
		--- Not support
		0,   --- Comp store
		iActive,
		iActive,  --- BOP
		iEopStore,  --- EOP
		iEopStore,  --- Cum EOP
		NULL,  -- LT_Char_Attr_1,
		NULL,  -- LT_Char_Attr_2,
		NULL,  -- LT_Char_Attr_3,
		NULL,  -- LT_Char_Attr_4,
		NULL,  -- LT_Char_Attr_5,
		NULL,  -- LT_Num_Attr_6,
		NULL,  -- LT_Num_Attr_7,
		NULL,  -- LT_Num_Attr_8,
		NULL,  -- LT_Num_Attr_9,
		NULL,  -- LT_Num_Attr_10,
		SYSDATE
		);
	END LOOP;


END LOOP;

--- Aggregate to location

FOR iCtr IN REVERSE 1..3 LOOP
	iSql := 'insert into maxdata.loc_time_2D('||
		'time_level,'||
		'time_ID,'||
		'location_level,'||
		'location_id,'||
		'LT_period,'||
		'LT_cycle,'||
		'LT_Days_in_period,'||
		'LT_store_count,'||
		'LT_period_count,'||
		'LT_loaded_period_count,'||
		'LT_new_store_count,'||
		'LT_closed_store_count,'||
		'LT_comp_store_count,'||
		'LT_active_store_count,'||
		'LT_BOP_active_store_count,'||
		'LT_EOP_active_store_count,'||
		'LT_Cum_EOP,'||
		'LT_Char_Attr_1,'||
		'LT_Char_Attr_2,'||
		'LT_Char_Attr_3,'||
		'LT_Char_Attr_4,'||
		'LT_Char_Attr_5,'||
		'LT_Num_Attr_6,'||
		'LT_Num_Attr_7,'||
		'LT_Num_Attr_8,'||
		'LT_Num_Attr_9,'||
		'LT_Num_Attr_10,'||
		'LT_init_date) '||
	'select	'||
		'a.time_level,'||
		'a.time_ID,'||
		iCtr||' location_level,'||
		'b.lv'||iCtr||'loc_id location_id,'||
		'min(LT_period),'||
		'min(LT_cycle),'||
		'sum(LT_Days_in_period),'||
		'sum(LT_store_count),'||
		'1 LT_period_count,'||
		'max(LT_loaded_period_count),'||
		'sum(LT_new_store_count),'||
		'sum(LT_closed_store_count),'||
		'0 LT_comp_store_count,'||
		'sum(LT_active_store_count),'||
		'sum(LT_BOP_active_store_count),'||
		'sum(LT_EOP_active_store_count),'||
		'sum(LT_Cum_EOP),'||
		'Null LT_Char_Attr_1,'||
	    'Null LT_Char_Attr_2,'||
		'Null LT_Char_Attr_3,'||
		'Null LT_Char_Attr_4,'||
		'Null LT_Char_Attr_5,'||
		'Null LT_Num_Attr_6,'||
		'Null LT_Num_Attr_7,'||
		'Null LT_Num_Attr_8,'||
		'Null LT_Num_Attr_9,'||
		'Null LT_Num_Attr_10,'||
		'sysdate LT_init_date '||
	'from maxdata.loc_time_2D a, maxdata.lv4loc b '||
		'where a.location_level=4 and a.location_id=b.lv4loc_id '||
		'group by a.time_level, a.time_id, b.lv'||iCtr||'loc_id';
	EXECUTE IMMEDIATE iSql;
END LOOP;

--- Aggregate to time


FOR iCtr IN REVERSE 1..4 LOOP

  	  BEGIN
    		iSql := 'drop table maxdata.'||iTabName;
    		EXECUTE IMMEDIATE iSql;

    		EXCEPTION
    		WHEN OTHERS THEN
      		NULL;
   	 END;

	iSql := 'Create table maxdata.'||iTabName||' nologging pctfree 0 storage (next 10M) as '||
		'select distinct time_level, time_id, 0 bop, 0 eop from maxdata.loc_time_2d '||
		'where time_level= '||(47+iCtr)||' and location_level=4';
    		EXECUTE IMMEDIATE iSql;

	iSql := 'update maxdata.'||iTabName||' a set bop = 1 where time_id = ( '||
		'select min(lv'||(iCtr+1)||'time_lkup_id) from maxapp.lv'||(iCtr+1)||'time '||
		'where lv'||iCtr||'time_lkup_id=( '||
		'select lv'||iCtr||'time_lkup_id from '||
		'maxapp.lv'||(iCtr+1)||'time b where a.time_id=b.lv'||(iCtr+1)||'time_lkup_id))';
		EXECUTE IMMEDIATE iSql;

	iSql := 'update maxdata.'||iTabName||' a set bop = 1 where time_id = ( '||
		'select min(time_id) from maxdata.'||iTabName||')';

	iSql := 'update maxdata.'||iTabName||' a set eop = 1 where time_id = ( '||
		'select max(lv'||(iCtr+1)||'time_lkup_id) from maxapp.lv'||(iCtr+1)||'time '||
		'where lv'||iCtr||'time_lkup_id=( '||
		'select lv'||iCtr||'time_lkup_id from '||
		'maxapp.lv'||(iCtr+1)||'time b where a.time_id=b.lv'||(iCtr+1)||'time_lkup_id))';
		EXECUTE IMMEDIATE iSql;

	iSql := 'update maxdata.'||iTabName||' a set eop = 1 where time_id = ( '||
		'select max(time_id) from maxdata.'||iTabName||')';
		EXECUTE IMMEDIATE iSql;

	iSql := 'insert into maxdata.loc_time_2D('||
		'time_level,'||
		'time_ID,'||
		'location_level,'||
		'location_id,'||
		'LT_period,'||
		'LT_cycle,'||
		'LT_Days_in_period,'||
		'LT_store_count,'||
		'LT_period_count,'||
		'LT_loaded_period_count,'||
		'LT_new_store_count,'||
		'LT_closed_store_count,'||
		'LT_comp_store_count,'||
		'LT_active_store_count,'||
		'LT_BOP_active_store_count,'||
		'LT_EOP_active_store_count,'||
		'LT_Cum_EOP,'||
		'LT_Char_Attr_1,'||
		'LT_Char_Attr_2,'||
		'LT_Char_Attr_3,'||
		'LT_Char_Attr_4,'||
		'LT_Char_Attr_5,'||
		'LT_Num_Attr_6,'||
		'LT_Num_Attr_7,'||
		'LT_Num_Attr_8,'||
		'LT_Num_Attr_9,'||
		'LT_Num_Attr_10,'||
		'LT_init_date) '||
	'select	'||
		(iCtr+46)||','||
		'b.lv'||iCtr||'time_lkup_id,'||
		'location_level,'||
		'location_id,'||
		'0,'||
		'0,'||
		'sum(LT_Days_in_period),'||
		'sum(LT_store_count),'||
		'sum(LT_period_count),'||
		'sum(LT_loaded_period_count),'||
		'sum(LT_new_store_count),'||
		'sum(LT_closed_store_count),'||
		'0 LT_comp_store_count,'||
		'sum(LT_active_store_count),'||
		'sum(LT_BOP_active_store_count * c.bop),'||
		'sum(LT_EOP_active_store_count * c.eop),'||
		'sum(LT_Cum_EOP),'||
		'Null LT_Char_Attr_1,'||
		'Null LT_Char_Attr_2,'||
		'Null LT_Char_Attr_3,'||
		'Null LT_Char_Attr_4,'||
		'Null LT_Char_Attr_5,'||
		'Null LT_Num_Attr_6,'||
		'Null LT_Num_Attr_7,'||
		'Null LT_Num_Attr_8,'||
		'Null LT_Num_Attr_9,'||
		'Null LT_Num_Attr_10,'||
		'sysdate LT_init_date '||
	'from maxdata.loc_time_2D a, maxapp.lv'||(iCtr+1)||'time b, maxdata.'||iTabName||' c '||
		'where a.time_level='||(iCtr+47)||' and a.time_id=b.lv'||(iCtr+1)||'time_lkup_id '||
		'and a.time_id=c.time_id '||
		'group by a.location_level, a.location_id, b.lv'||iCtr||'time_lkup_id';

	EXECUTE IMMEDIATE iSql;
END LOOP;

--- Update period and cycle

FOR iCtr IN 1..4 LOOP

	iSql := 'Update maxdata.loc_time_2D a '||
		' set LT_period = (select lv'||iCtr||'time_id from maxapp.lv'||iCtr||'time b '||
				'where a.time_id=b.lv'||iCtr||'time_lkup_id ), '||
	    	'LT_cycle = (select cycle_id from maxapp.lv'||iCtr||'time b '||
				'where a.time_id=b.lv'||iCtr||'time_lkup_id ) '||
		'where a.time_level='||(iCtr+46);

	EXECUTE IMMEDIATE iSql;
END LOOP;

<<set_return>>
BEGIN
    iSql := 'drop table maxdata.'||iTabName;
    EXECUTE IMMEDIATE iSql;

    EXCEPTION
    WHEN OTHERS THEN
      NULL;
END;


END;

/

  GRANT EXECUTE ON "MAXDATA"."P_GEN_LOC_TIME_2D" TO "DATAMGR";
