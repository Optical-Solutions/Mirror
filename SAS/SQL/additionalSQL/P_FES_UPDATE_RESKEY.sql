--------------------------------------------------------
--  DDL for Procedure P_FES_UPDATE_RESKEY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_FES_UPDATE_RESKEY" 
--- 11/05/13	Created for MCX: To update the model class with the mapping of live
as
iSql        	long;
iLev		number(2);
iCol1		varchar2(20);
iCol2		varchar2(20);
iCol3		varchar2(20);

Begin

	select hier_level into iLev from datamgr.Loadpref where upper(key_1)='MODEL_RES';
	select value_1    into iCol1 from datamgr.Loadpref where upper(key_1)='MODEL_RESKEY' and hier_level=iLev;
	select value_1    into iCol2 from datamgr.Loadpref where upper(key_1)='MODEL_RES_MOD_USERID' and hier_level=iLev;
	select value_1    into iCol3 from datamgr.Loadpref where upper(key_1)='MODEL_RES_MOD_STATUS' and hier_level=iLev;

	--- clear the message field first
	iSql := 'update maxdata.lv7cmast a set '||iCol3||'= NULL'||
		 ' where record_type=''L'' and '||iCol2||' is not null and '||iCol3||' is not null and substr('||iCol3||',1,5) = ''ERROR''';
	execute immediate iSql;
	commit;

  --- check none one to one mapping

  	iSql:='update maxdata.lv7cmast set '||iCol3||'=''ERROR: Live Member Has Assigned to Multiple Models'''||
        	' where '||iCol2||' in (select '||iCol2||' from maxdata.lv7cmast where '||iCol2||' is not null and ('||iCol3||' is null or substr('||iCol3||',1,5) = ''ERROR'')'||
        	' group by '||iCol2||' having count(*) > 1) and '||iCol2||' is not null and ('||iCol3||' is null or substr('||iCol3||',1,5) = ''ERROR'')';

  	execute immediate iSql;
  	commit;

	--- Flag if fact data exist
	iSql := 'update maxdata.lv7cmast a set '||iCol3||'= ''ERROR(11): Live record has Fact data loaded'''||
		 ' where record_type=''L'' and '||iCol2||' is not null and '||iCol3||' is null'||
		 ' and exists (select 1 from maxdata.merch_ids7_with_mfinc b, maxdata.lv7ctree c'||
		 ' where a.lv7cmast_id=c.lv7cmast_id and b.merch_id=c.lv7ctree_id)';
	execute immediate iSql;
	commit;

	iSql := 'update maxdata.lv7cmast a set '||iCol3||'= ''ERROR(11): Live record has Fact data loaded'''||
		 ' where record_type=''L'' and '||iCol2||' is not null and '||iCol3||' is null'||
		 ' and exists (select 1 from maxdata.merch_ids7_with_minv b, maxdata.lv7ctree c'||
		 ' where a.lv7cmast_id=c.lv7cmast_id and b.merch_id=c.lv7ctree_id)';
	execute immediate iSql;
	commit;

	--- update matched model with the live lv7cmast_id
	iSql := 'update maxdata.lv7cmast a set '||iCol1||'= ('||
		 ' select lv7cmast_id from maxdata.lv7cmast b where a.lv7cmast_userid = b.'||iCol2||' and b.record_type=''L'' and b.'||iCol3||' is null)'||
		 ' where record_type=''M'''||
		 ' and exists (select 1 from maxdata.lv7cmast c where a.lv7cmast_userid = c.'||iCol2||' and c.record_type=''L'' and c.'||iCol3||' is null)';
	execute immediate iSql;

	--- record the model's lv7cmast_userid in the matched live lv7cmast
	iSql := 'update maxdata.lv7cmast a set '||iCol3||'= ('||
		 ' select lv7cmast_id from maxdata.lv7cmast b where b.lv7cmast_userid = a.'||iCol2||' and b.record_type=''M'')'||
		 ' where record_type=''L'' and '||iCol3||' is null'||
		 ' and exists (select 1 from maxdata.lv7cmast c where c.lv7cmast_userid = a.'||iCol2||' and c.record_type=''M'') and '||iCol3||' is null';
	execute immediate iSql;

	--- Set error if can't find matching Model record
	iSql := 'update maxdata.lv7cmast set '||iCol3||'= ''ERROR(12): Model record not found'' where record_type=''L'''||
		' and (('||iCol3||' is null and '||iCol2||' is not null))';
	execute immediate iSql;
	commit;

commit;

End;

/
