--------------------------------------------------------
--  DDL for Procedure P_HIGHLIGHT_LOCATIONS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_HIGHLIGHT_LOCATIONS" 
AS

-- Change History
--V5.6
-- 04/30/04	Sachin	#16676 Added approval_status checking and modifying sub select query.
-- 04/05/04	Diwakar	Web Enhancements
--V 4.7
-- 7/30/03  Rashmi   Refer to Enhancement ID 1941 and detailed design doc "IMPS 4_7 Web Distribution gaps".
-- 11/20/02 Helmi .. initial entry. 6/18/03 Porting to Oracle.

t_web_distrib_prep_time number(4);

BEGIN

UPDATE maxdata.lv7loc set reset_necessary = 0 where reset_necessary = 1;
UPDATE maxdata.lv6loc set reset_necessary = 0 where reset_necessary = 1;
UPDATE maxdata.lv5loc set reset_necessary = 0 where reset_necessary = 1;

--get the WEB_DISTRIB_PREP_TIME from userpref
BEGIN
SELECT  TO_NUMBER(value_1) INTO t_web_distrib_prep_time
FROM maxapp.userpref WHERE key_1 = 'WEB_DISTRIB_PREP_TIME';
END;

UPDATE maxdata.lv7loc lv7
SET reset_necessary = (	SELECT 1 FROM  maxdata.pogmaster pm
			WHERE pm.live_lv7loc_id = lv7.lv7loc_id
			AND (pm.pog_actual_start - SYSDATE) > 0
			AND (pm.pog_actual_start - SYSDATE) <= (NVL(pm.pog_set_days, 0) + t_web_distrib_prep_time)
			AND pm.approval_status = 1);

UPDATE maxdata.lv6loc lv6
SET reset_necessary = (SELECT 1 FROM DUAL
			WHERE EXISTS (SELECT 1 FROM maxdata.pogmaster pm,maxdata.lv7loc lv7
			WHERE lv6.lv6loc_id = lv7.lv6loc_id
			AND pm.live_lv7loc_id = lv7.lv7loc_id
			AND (pm.pog_actual_start - SYSDATE) > 0
			AND (pm.pog_actual_start - SYSDATE)  <= (NVL(pm.pog_set_days, 0) + t_web_distrib_prep_time)
			AND pm.approval_status = 1));

UPDATE maxdata.lv5loc  lv5
SET reset_necessary = (SELECT 1 FROM DUAL
			WHERE EXISTS (SELECT 1 FROM maxdata.pogmaster pm,maxdata.lv7loc lv7
					WHERE lv5.lv5loc_id = lv7.lv5loc_id
					AND pm.live_lv7loc_id = lv7.lv7loc_id
					AND (pm.pog_actual_start - SYSDATE) > 0
					AND (pm.pog_actual_start - SYSDATE)  <= (NVL(pm.pog_set_days, 0) + t_web_distrib_prep_time)
					AND pm.approval_status = 1));

COMMIT;

EXCEPTION
WHEN OTHERS THEN
	ROLLBACK;
	NULL;
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_HIGHLIGHT_LOCATIONS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_HIGHLIGHT_LOCATIONS" TO "MAXUSER";
