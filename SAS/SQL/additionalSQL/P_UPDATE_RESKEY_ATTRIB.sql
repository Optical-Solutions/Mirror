--------------------------------------------------------
--  DDL for Procedure P_UPDATE_RESKEY_ATTRIB
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_UPDATE_RESKEY_ATTRIB" 
--- 9/26/11	Modified for MCX: change to get reskey from loadpref
as
iSql        long;
iCol		varchar2(20);
cursor c_reskey is
	select hier_level,value_1 from datamgr.Loadpref where upper(key_1)='MODEL_RES' order by hier_level asc;
Begin

--- iCol:='num_user2';

For c1 in c_reskey loop

if c1.hier_level = 8 then

	select value_1 into iCol from datamgr.Loadpref where upper(key_1)='MODEL_RESKEY' and hier_level=c1.hier_level;

	iSql := 'update maxdata.lv'||c1.hier_level||'cmast set '||c1.value_1||'= ('||
		 ' select new_color_sku from new_colors_lkup where new_color_id = '||iCol||')'||
		 ' where record_type=''M'' and '||iCol||' is not null';
	execute immediate iSql;

elsif c1.hier_level = 7 then

	select value_1 into iCol from datamgr.Loadpref where upper(key_1)='MODEL_RESKEY' and hier_level=c1.hier_level;

	iSql := 'update maxdata.lv'||c1.hier_level||'cmast set '||c1.value_1||'= ('||
		 ' select new_style_sku from new_styles_lkup where new_style_id = '||iCol||')'||
		 ' where record_type=''M'' and '||iCol||' is not null';
	execute immediate iSql;

elsif c1.hier_level = 6 then


	select value_1 into iCol from datamgr.Loadpref where upper(key_1)='MODEL_RESKEY' and hier_level=c1.hier_level;

	iSql := 'update maxdata.lv'||c1.hier_level||'cmast set '||c1.value_1||'= ('||
		 ' select new_vendor_num from new_vendors_lkup where new_vendor_id = '||iCol||')'||
		 ' where record_type=''M'' and '||iCol||' is not null';
	execute immediate iSql;

end if;

end loop;

commit;

End;

/
