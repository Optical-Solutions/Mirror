--------------------------------------------------------
--  DDL for Procedure P_GEN_MERCH_1D
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GEN_MERCH_1D" 
-- $id: p_gen_merch_1d.sql.sql,v 1.0 2005/07/22 18:01:01 cbarr Exp $
-- Copyright (c) 2005 SAS Institute Inc., Cary, NC USA
-- Date      By      Comment
-- 07/22/05  TS/kg   Adapted.
-- 07/28/05  cbarr   add grant
-- ----------------------------------------------------------------------------
AS

---  This process populate the one dimension table of merchandise hierarchy infomation
---  It shall be executed whenever new products are added either through Datamanager Product
---      load or through applocation.
---
iCtr		INTEGER;
CtreeTab	VARCHAR2(80);
iSql		VARCHAR2(2000);

BEGIN

maxdata.Ins_Import_Log('p_gen_Merch_1D','Information',
            'Start populate Merch 1D table', NULL
            ,NULL,NULL) ;

iSql := 'Truncate table maxdata.Merch_1D';

EXECUTE IMMEDIATE iSql;

iCtr := 1;

---
WHILE iCtr < 11 LOOP

	IF iCtr = 1 THEN
		CtreeTab := 'lv1cmast';
	ELSE
		CtreeTab := 'lv'||iCtr||'ctree';
	END IF;

	iSql := 'insert into maxdata.Merch_1D (Merch_level,Merch_ID,Hier_Type) '||
		'select '||iCtr||', '||CtreeTab||'_id, hier_type from maxdata.'||CtreeTab;

	EXECUTE IMMEDIATE iSql;

	COMMIT;

	iCtr := iCtr+1;

END LOOP;

isql := 'analyze table maxdata.merch_1d compute statistics';
EXECUTE IMMEDIATE iSql;

maxdata.Ins_Import_Log('p_gen_Merch_1D','Information',
            'End populate Merch 1D table', NULL
            ,NULL,NULL) ;

end;

/

  GRANT EXECUTE ON "MAXDATA"."P_GEN_MERCH_1D" TO "DATAMGR";
