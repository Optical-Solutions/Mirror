--------------------------------------------------------
--  DDL for Procedure P_DB_GET_CONCURRENCY_SLOT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DB_GET_CONCURRENCY_SLOT" (
	   in_slot_nm  VARCHAR2,
	   in_action_cd NUMBER, -- +1 to obtain; -1 to return.
	   in_future1  NUMBER)
AS
--
--------------------------------------------------------
-- $Log: 2422_p_db_get_concurrency_slot.sql,v $
-- Revision 1.7  2007/06/19 14:38:43  clapper
-- FIXID AUTOPUSH: SOS 1238247
--
-- Revision 1.3  2006/11/09 14:55:13  anchan
-- Do nothing if specified slot_nm is not found.
--
-- Revision 1.1  2006/11/08 19:39:08  anchan
-- S0389870: procedure to control the number of concurrent execution of resource-intensive queries
--
-- DESCRIPTION --
-- For controlling the number of resource-intensive queries.
-- Each such query is assigned a unique SLOT_NM, as specified in the maxdata.DBCC_CONCURRENCY_CONTROL table.
--    SLOT_MAX_CNT: Maximum number of configured slots available for concurrent queries.
--    WAITFOR_SEC_CNT: number of seconds this procedure will wait to obtain a slot.
--        Will seize a slot, even if exceeds maximum number of configured slots.
--    EXPIRE_SEC_CNT: Number of seconds before a slot expires, after which the slot is forcibly reclaimed.
--         Used for recovering slots left hanging after a session/server aborts or a runaway query.
--------------------------------------------------------
--
n_sqlnum 	        NUMBER(10,0);
t_proc_name 		VARCHAR2(64) := 'p_db_get_concurrency_slot';
t_error_level      	VARCHAR2(6) 		:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(1000) 		:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);
t_error_msg		VARCHAR2(1000);

t_interval NUMBER(10,2) := 5.0; -- 5.0 sec
t_sec NUMBER (10,2) := 0;

t_slot_max_cnt NUMBER(6);
t_slot_curr_cnt NUMBER(6);
t_expired_cnt NUMBER(6);
t_expire_sec NUMBER(6);
t_waitfor_sec NUMBER(6);
t_continue_flg NUMBER(1);

BEGIN

n_sqlnum := 10000;
IF in_action_cd=0 THEN
	maxdata.ins_import_log (t_proc_name,t_error_level,in_slot_nm||': No slot requested.',NULL ,NULL,NULL);
	RETURN;
END IF;


n_sqlnum := 20000;
BEGIN
SELECT slot_max_cnt,expire_sec_cnt,waitfor_sec_cnt,continue_flg
INTO t_slot_max_cnt,t_expire_sec,t_waitfor_sec,t_continue_flg
FROM maxdata.DBCC_CONCURRENCY_CONTROL
WHERE  slot_nm=in_slot_nm;
IF t_slot_max_cnt <= 0 THEN
	maxdata.ins_import_log (t_proc_name,t_error_level,in_slot_nm||': Slots disabled.',NULL ,NULL,NULL);
	RETURN;
END IF;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
	maxdata.ins_import_log (t_proc_name,t_error_level,in_slot_nm||': No slot configured.',NULL ,NULL,NULL);
	RETURN;
END;

IF in_action_cd<0 THEN
	-- Return the slot --
	n_sqlnum := 31000;
	UPDATE maxdata.DBCC_CONCURRENCY_CONTROL
	SET slot_curr_cnt=slot_curr_cnt-1
	WHERE slot_nm=in_slot_nm;
	n_sqlnum := 32000;
	DELETE FROM maxdata.DBCS_CONCURRENCY_SLOT
	WHERE  SLOT_NM=in_slot_nm
	AND expire_dttm=(SELECT MAX(expire_dttm)
			FROM maxdata.DBCS_CONCURRENCY_SLOT
			WHERE slot_nm=in_slot_nm)
	AND ROWNUM=1;
	COMMIT;
	maxdata.ins_import_log (t_proc_name,t_error_level,in_slot_nm||': Slot returned.',NULL,NULL,NULL);
	RETURN;
END IF;

n_sqlnum := 40000;
-- Delete any expired slots --
DELETE FROM maxdata.DBCS_CONCURRENCY_SLOT
WHERE  SLOT_NM=in_slot_nm
AND expire_dttm<SYSDATE;

n_sqlnum := 50000;
-- Sync the counter with number of slots, if different:--
SELECT COUNT(*) INTO t_slot_curr_cnt
FROM maxdata.DBCS_CONCURRENCY_SLOT
WHERE  slot_nm=in_slot_nm;
n_sqlnum := 51000;
UPDATE maxdata.DBCC_CONCURRENCY_CONTROL
SET slot_curr_cnt=t_slot_curr_cnt
WHERE slot_nm=in_slot_nm
AND slot_curr_cnt<>t_slot_curr_cnt;

COMMIT;


WHILE (t_sec < t_waitfor_sec)
LOOP
	n_sqlnum := 61000;
	-- Make an attempt to obtain a slot:--
	UPDATE maxdata.DBCC_CONCURRENCY_CONTROL
	SET slot_curr_cnt=slot_curr_cnt+1
	WHERE slot_nm=in_slot_nm
	AND slot_curr_cnt<slot_max_cnt;

	IF SQL%ROWCOUNT=1 THEN
		n_sqlnum := 62000;
		INSERT INTO maxdata.DBCS_CONCURRENCY_SLOT(slot_nm,expire_dttm)
		VALUES(in_slot_nm,SYSDATE+(t_expire_sec/86400));
		COMMIT;
		maxdata.Ins_Import_Log (t_proc_name, t_error_level,in_slot_nm||': Slot obtained, after waiting '||t_sec||' sec.' ,  NULL ,    NULL,   NULL);
		RETURN;
	END IF;

	-- No slots were available. Wait and try again:--
	n_sqlnum := 62000;
	DBMS_LOCK.SLEEP (t_interval);
	t_sec := t_sec + t_interval;

END LOOP;


--If we got here, then we waited more than the configured waitfor time --
IF t_continue_flg=1 THEN
	-- Seize an extra slot anyway, even if it exceeds the configured number of slots:--
	n_sqlnum := 70000;
	UPDATE maxdata.DBCC_CONCURRENCY_CONTROL
	SET slot_curr_cnt=slot_curr_cnt+1
	WHERE slot_nm=in_slot_nm;
	n_sqlnum := 71000;
	INSERT INTO maxdata.DBCS_CONCURRENCY_SLOT(slot_nm,expire_dttm)
	VALUES(in_slot_nm,SYSDATE+(t_expire_sec/86400));
	maxdata.Ins_Import_Log (t_proc_name, t_error_level,in_slot_nm||': Slot seized. Waitfor time expired after '||t_sec||' sec.' ,  NULL ,    NULL,   NULL);
ELSE
	RAISE_APPLICATION_ERROR (-20001, in_slot_nm||': Unable to obtain a slot. Waitfor time expired after '||t_sec||' sec.');

END IF;

COMMIT;

EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := SUBSTR(v_sql,1,255);
		t_sql3 := SUBSTR(v_sql,256,255);
		maxdata.Ins_Import_Log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, -1);
		--COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_DB_GET_CONCURRENCY_SLOT" TO "MADMAX";
