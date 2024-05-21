--------------------------------------------------------
--  DDL for Procedure P_DO_MODEL_RES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DO_MODEL_RES" 
as
iSql   	  		  long;
iSql_col          long;
cnt 			  number(5);
Ctr 			  number(5);
lvl 			  number(5);
ilv10cat 		  number(10);
max_hier_lvl	  number(5);
lvxcmast 		  number(10);
cmast_userid	  varchar2(30);
cmast_id		  number(10);
Mod_cmast_id	  number(10);
OldCtreeID		  number(10);
NewName			  varchar2(80);
ColName			  varchar2(80);
IsUpdate		  varchar2(2);
ColID			  number(5);
iCount			  number(2);
cursor c_model is
	select * from maxdata.lvxref_model_res where comment_1='3' order by merch_level desc;

begin

select max(higherlevel_id)-10 into max_hier_lvl from path_seg where path_id=11;

for c1 in c_model loop

	lvl:=c1.merch_level;

	iSql:='select count(1) from datamgr.hier_table where hier_type=11 and hier_level='||lvl;
	execute immediate iSql into cnt;

	iSql_col:='';

	for Ctr in 1..cnt loop

		iSql:='select column_name, isupdate from datamgr.hier_table where hier_type=11 and hier_level='||lvl||
				' and column_id='||Ctr;
		execute immediate iSql into ColName, IsUpdate;

		-- copy only columns where isUpdate=N and also do NOT copy Last_update and lvXcmast_id --
		if (substr(upper(ColName),4,8) <> 'CMAST_ID'
		   	and upper(ColName) <> 'LAST_UPDATE'
			and upper(IsUpdate)='Y')
		then
		    iSql_col:=iSql_col||ColName||',';
		end if;

	end loop;

	-- trim last comma off end of column list -
	iSql_col:=substr(iSql_col,1,length(iSql_col)-1);

	-- get userid, cmast_id, name of New Live item -
	iSql:='select lv'||lvl||'cmast_userid,lv'||lvl||'cmast_id,name from maxdata.lv'||lvl||'cmast'||
		' where lv'||lvl||'cmast_userid='''||c1.live_item_userid||'''';
	execute immediate iSql into cmast_userid, cmast_id, NewName;

	-- get ctree_id of New Live item -
	iSql:='select lv'||lvl||'ctree_id from maxdata.lv'||lvl||'ctree where lv'||lvl||'cmast_id='||cmast_id;
	execute immediate iSql into OldCtreeID;

	-- get cmast_id, name of Model item -
	-- iSql:='select lv'||lvl||'cmast_id from maxdata.lv'||lvl||'cmast where lv'||lvl||'cmast_userid='''||c1.model_userid||'''';
    -- mls 8-25-11: changed code to accept model_userids with a single quote in them
    iSql:='select lv'||lvl||'cmast_id from maxdata.lv'||lvl||'cmast where lv'||lvl||'cmast_userid=q''<'||c1.model_userid||'>''';
	execute immediate iSql into Mod_cmast_id;

	-- update userid of New Live item to "user_id-cmast_id" -
	iSql:='Update maxdata.lv'||lvl||'cmast set lv'||lvl||'cmast_userid='||
		'lv'||lvl||'cmast_userid'||'||''-''||'||'lv'||lvl||'cmast_id'||
		' where lv'||lvl||'cmast_userid='''||c1.live_item_userid||'''';
	execute immediate iSql;

	-- Update model item with userid and last_update cols of New Live item -
	iSql:= 'update lv'||lvl||'cmast set (lv'||lvl||'cmast_userid, last_update,'||iSql_col||')  = '||
        '(select '''||cmast_userid||''',sysdate,'||iSql_col||
	  	' from maxdata.lv'||lvl||'cmast where lv'||lvl||'cmast_id='||cmast_id||')'||
	  	' where lv'||lvl||'cmast_id='||Mod_cmast_id;
    execute immediate iSql;

	--- Update master name in dimset and planworksheet
	select count(1) into cnt from maxdata.dimset_template where dimension_type=2 and lvl between (from_level-10) and (to_level-10);

	if cnt > 0 then

	  	update maxdata.dimset_template_mem set member_name=NewName
	  		where template_id in (select template_id from maxdata.dimset_template
	  				where dimension_type=2 and lvl between (from_level-10) and (to_level-10))
	  			and level_number=(lvl+10) and member_id=Mod_cmast_id;
	end if;

	update  maxdata.planworksheet set from_merch_name=NewName where lvl = (from_merch_level-10) and from_merch_id=Mod_cmast_id;


	maxdata.p_change_merch_parent(OldCtreeID,c1.model_id,lvl);

	iSql:='update maxdata.lv'||lvl||'ctree set record_type=''L'' where lv'||lvl||'ctree_id='||c1.model_id;
	execute immediate iSql;

	iSql:='delete from maxdata.lv'||lvl||'ctree where lv'||lvl||'ctree_id='||OldCtreeID;
	execute immediate iSql;
	iSql:='delete from maxdata.lv'||lvl||'cmast where lv'||lvl||'cmast_id='||cmast_id;
	execute immediate iSql;

	commit;

	if lvl = max_hier_lvl then -- delete model sku

	  	iSql:='delete from maxdata.lv10merch where lv10mast_id in (select lv10mast_id from maxdata.lv10mast'||
	  		' where lv'||lvl||'ctree_id='||c1.model_id||' and record_type=''M'')';
	  	execute immediate iSql;

	  	iSql:='delete from maxdata.lv10ctree where lv10ctree_id in (select lv10mast_id from maxdata.lv10mast'||
	  		' where lv'||lvl||'ctree_id='||c1.model_id||' and record_type=''M'')';
	  	execute immediate iSql;

	  	iSql:='delete from maxdata.lv10mast where lv'||lvl||'ctree_id='||c1.model_id||' and record_type=''M''';
	  	execute immediate iSql;

	  	commit;

	end if;

end loop;

end;

/
