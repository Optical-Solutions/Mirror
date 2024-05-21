--------------------------------------------------------
--  DDL for Procedure P_UPDATE_RES_STATUS_ATTRIB
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_UPDATE_RES_STATUS_ATTRIB" 
-- 9/26/01  modified to use loadpref for the reskey variables
as
iSql 			  long;
X				  number(2);
live_userid		  varchar2(50);
model_userid	  varchar2(50);
iCol			  varchar2(20);

cursor res_stat is
	select merch_level,live_userid,model_userid,res_status from maxdata.model_res_status;
Begin

--iCol:='char_user11';

For c1 in res_stat loop
	select value_1 into iCol from datamgr.Loadpref where upper(key_1)='MODEL_RES_STATUS' and hier_level=c1.merch_level;

	X:=c1.merch_level;
	live_userid:=c1.live_userid;
	model_userid:=c1.model_userid;

	if c1.res_status = 3
	then
	iSql:='update maxdata.lv'||X||'cmast set '||iCol||' ='||
	' (select merch_res_desc from model_res_status_lkup where merch_res_lkup_id = '||c1.res_status||')'||
	' where lv'||X||'cmast_userid = '''||live_userid||'''';
	else
	iSql:='update maxdata.lv'||X||'cmast set '||iCol||' ='||
	' (select merch_res_desc from model_res_status_lkup where merch_res_lkup_id = '||c1.res_status||')'||
	' where lv'||X||'cmast_userid = '''||model_userid||'''';
	end if;

	execute immediate iSql;
	commit;

end loop;

End;

/
