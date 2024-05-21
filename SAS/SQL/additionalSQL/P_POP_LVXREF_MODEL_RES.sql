--------------------------------------------------------
--  DDL for Procedure P_POP_LVXREF_MODEL_RES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_POP_LVXREF_MODEL_RES" (ID number,attr varchar)
as
iSql 			    long;
iSql_select 		long;
iSql_insert 		long;
iSql_update 		long;
iCtr 			number(5);

begin

for iCtr in 2..(ID-1) loop
	iSql_insert:=iSql_insert||',MODEL_PARENT_LV'||iCtr;
	iSql_select:=iSql_select||',a.lv'||iCtr||'ctree_id';
end loop;

-- populate lvxref_model_res table with model records whose reskey is not null and not 0 --
iSql:='Insert into maxdata.lvxref_model_res (MERCH_LEVEL,LAST_UPDATE,MODEL_USERID,MODEL_NAME,MODEL_ID'||iSql_insert||',ATTR_ITEM_USERID)'||
	' select '||ID||',sysdate,b.lv'||ID||'cmast_userid,b.name,a.lv'||ID||'ctree_id'||iSql_select||',b.'||attr||
	' from maxdata.lv'||ID||'ctree a, maxdata.lv'||ID||'cmast b'||
	' where a.lv'||ID||'cmast_id=b.lv'||ID||'cmast_id and a.record_type=''M'' and nvl(b.'||attr||',''0'') <> ''0''';
execute immediate iSql;
commit;

iSql_select:='';
iSql_update:='';

for iCtr in 2..(ID-1) loop
	iSql_update:=iSql_update||',LIVE_PARENT_LV'||iCtr;
	iSql_select:=iSql_select||',b.lv'||iCtr||'ctree_id';
end loop;

iSql:='update maxdata.lvxref_model_res a set (a.live_item_userid,a.live_name) = '||
	'(select b.lv'||ID||'cmast_userid,b.name from maxdata.lv'||ID||'cmast b where b.lv'||ID||'cmast_userid=a.ATTR_ITEM_USERID'||
    ' and a.model_userid<>b.lv'||ID||'cmast_userid)';
execute immediate iSql;
commit;

update maxdata.lvxref_model_res set comment_1='3';
commit;

-- if live item has multiple ctree ids do no resolve --
iSql:='update maxdata.lvxref_model_res a set comment_1=''6'''||
	'where a.live_item_userid in (select lv'||ID||'cmast_userid from lv'||ID||'cmast where lv'||ID||'cmast_id in '||
	'(select lv'||ID||'cmast_id from lv'||ID||'ctree group by lv'||ID||'cmast_id having count(*) > 1))';
execute immediate iSql;
commit;

iSql:='update maxdata.lvxref_model_res a set (a.live_item_id'||iSql_update||')='||
	'(select b.lv'||ID||'ctree_id'||iSql_select||
	' from maxdata.lv'||ID||'ctree b,lv'||ID||'cmast c where c.lv'||ID||'cmast_userid=a.live_item_userid'||
	' and b.lv'||ID||'cmast_id=c.lv'||ID||'cmast_id)'||
	' where comment_1=''3''';
execute immediate iSql;
commit;

---- Validate the match replacements

update maxdata.lvxref_model_res set comment_1='1' where nvl(attr_item_userid,0)<>nvl(live_item_userid,1) and comment_1='3';

update maxdata.lvxref_model_res set comment_1 = '2'
	where comment_1='3' and attr_item_userid = live_item_userid and (nvl(model_parent_lv2,0) <> nvl(live_parent_lv2,0)
			or nvl(model_parent_lv3,0) <> nvl(live_parent_lv3,0)
			or nvl(model_parent_lv4,0) <> nvl(live_parent_lv4,0)
			or nvl(model_parent_lv5,0) <> nvl(live_parent_lv5,0)
			or nvl(model_parent_lv6,0) <> nvl(live_parent_lv6,0)
			or nvl(model_parent_lv7,0) <> nvl(live_parent_lv7,0)
			or nvl(model_parent_lv8,0) <> nvl(live_parent_lv8,0)
			or nvl(model_parent_lv9,0) <> nvl(live_parent_lv9,0));

update maxdata.lvxref_model_res set comment_1='5' where comment_1='3' and attr_item_userid in (
	select attr_item_userid from maxdata.lvxref_model_res group by merch_level, attr_item_userid
		having count(1) > 1);

commit;

end;

/
