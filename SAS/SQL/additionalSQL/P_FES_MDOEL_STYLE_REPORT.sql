--------------------------------------------------------
--  DDL for Procedure P_FES_MDOEL_STYLE_REPORT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_FES_MDOEL_STYLE_REPORT" 
/*------------------------------------------------------------------------------------------------
Date Created: 11/21/2013
Description:
Parameter Description: None
Note: This process list the model style by template/worksheets
Date		Who  Desc
10/21/2013	ec   Created
------------------------------------------------------------------------------------------------*/
as

--- Local Temp tables
iModelTempl		varchar2(30) :='t_fes_model_template';
iModelPlan		varchar2(30) :='t_fes_model_plan';
iModelProd		varchar2(30) :='t_fes_model_prod';
iModelStyle		varchar2(30) :='t_fes_model_style';


iMerchTab		varchar2(30);
iCtreeTab		varchar2(30);
iMerchUserID		varchar2(30);

iCount			number(5);
iCtr			number(5);

iSql  			long;
iShopLvl		number(2);

iTimeLevel 		int;
iTimeID 		int;
iLocLevel 		int;
iLocID  		int;
iLocPath  		int;

iTimeTempl 		int;
iLocTempl 		int;
iMerchTempl  		int;
iWkShtID  		int;
iMerchID  		int;
iName			varchar2(50);

begin

select to_number(nvl(value_1,'10'))  into iShopLvl from maxdata.client_config where upper(key_1)='SHOP MERCH LEVEL';

iCtreeTab:='lv'||iShopLvl||'ctree';

if iShopLvl = 10 then
	iMerchTab:='lv10mast';
	iMerchUserID:='order_code';
else
	iMerchTab:='lv'||iShopLvl||'cmast';
	iMerchUserID:='lv'||iShopLvl||'cmast_userid';
end if;


--iModelTempl:='t_fes_model_template';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iModelTempl);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iModelTempl;
	execute immediate iSql;
end if;

iSql:='create table maxdata.'||iModelTempl||' nologging pctfree 0 as'||
	' select nvl(c.planworksheet_id,0) planworksheet_id, a.template_id,a.member_id merch_id,a.exclude_flag,a.removed_flag'||
	' from maxdata.dimset_template_mem a, maxdata.'||iCtreeTab||' b, maxdata.dimset_template c'||
	' where a.level_number='||(iShopLvl+10)||' and a.member_id=b.'||iCtreeTab||'_id'||
	' and b.record_type=''M'' and a.template_id=c.template_id';
execute immediate iSql;

iSql:='update maxdata.'||iModelTempl||' a set planworksheet_id=0'||
	' where not exists (select 1 from maxdata.planworksheet b where a.planworksheet_id=b.planworksheet_id)'||
	' and planworksheet_id > 0';
execute immediate iSql;
commit;

--iModelPlan:='t_fes_model_plan';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iModelPlan);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iModelPlan;
	execute immediate iSql;
end if;

iSql:='create table maxdata.'||iModelPlan||' nologging pctfree 0 as'||
	' select distinct a.workplan_id planworksheet_id, b.merch_template_id template_id, a.merch_id merch_id,0 exclude_flag,0 removed_flag'||
	' from maxdata.mplan_working a, maxdata.planworksheet b'||
	' where a.workplan_id=b.planworksheet_id'||
	' and not exists (select 1 from maxdata.'||iModelTempl||' e where e.merch_id=a.merch_id)'||
	' and b.to_merch_level='||(iShopLvl+10)||' and a.merch_level='||iShopLvl||
	' and exists (select 1 from maxdata.'||iCtreeTab||' d where a.merch_id=d.'||iCtreeTab||'_id and d.record_type=''M'')';

execute immediate iSql;

--iModelProd:='t_fes_model_prod';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iModelProd);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iModelProd;
	execute immediate iSql;
end if;

iSql:='create table maxdata.'||iModelProd||' nologging pctfree 0 as'||
	' select distinct 0 planworksheet_id, 0 template_id, lv7ctree_id merch_id,0 exclude_flag,0 removed_flag'||
	' from maxdata.'||iCtreeTab||' a'||
	' where not exists (select 1 from maxdata.'||iModelTempl||' b where b.merch_id=a.'||iCtreeTab||'_id)'||
	' and not exists (select * from maxdata.'||iModelPlan||' c where c.merch_id=a.'||iCtreeTab||'_id)'||
	' and a.record_type=''M''';
execute immediate iSql;


--iModelStyle:='t_fes_model_style';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iModelStyle);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iModelStyle;
	execute immediate iSql;
end if;

iSql:='create table maxdata.'||iModelStyle||' nologging pctfree 0 as select * from maxdata.fes_model_style where 1=2';
execute immediate iSql;

iSql:='insert into maxdata.'||iModelStyle||
	'(create_date,last_update, planworksheet_id,template_id,merch_id,exclude_flag,removed_flag, process_status)'||
	' select sysdate, sysdate, planworksheet_id,template_id,merch_id,exclude_flag,removed_flag, 1 from maxdata.'||iModelTempl||
	' union all'||
	' select sysdate, sysdate, planworksheet_id,template_id,merch_id,exclude_flag,removed_flag, 1 from maxdata.'||iModelPlan||
	' union all'||
	' select sysdate, sysdate, planworksheet_id,template_id,merch_id,exclude_flag,removed_flag, 1 from maxdata.'||iModelProd;
execute immediate iSql;
commit;

--- drop the local temp tables
--iModelTempl:='t_fes_model_template';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iModelTempl);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iModelTempl;
	execute immediate iSql;
end if;

--iModelPlan:='t_fes_model_plan';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iModelPlan);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iModelPlan;
	execute immediate iSql;
end if;

--iModelProd:='t_fes_model_prod';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iModelProd);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iModelProd;
	execute immediate iSql;
end if;

iSql:='create table maxdata.'||iModelProd||' nologging pctfree 0 as'||
	' select distinct planworksheet_id'||
	' from maxdata.'||iModelStyle||' where planworksheet_id > 0';
execute immediate iSql;

iSql:='select count(1)from maxdata.'||iModelProd;
execute immediate iSql into iCount;

while iCount > 0 loop

	iSql:='select planworksheet_id from maxdata.'||iModelProd||' where rownum=1';
	execute immediate iSql into iWkShtID;

	select count(1) into iCtr from  maxdata.planworksheet where planworksheet_id=iWkShtID and planversion_id > 0;

	if iCtr > 0 then

		select name, time_template_id, loc_template_id, merch_template_id into iName, iTimeTempl, iLocTempl, iMerchTempl
			from maxdata.planworksheet where planworksheet_id=iWkShtID and planversion_id > 0;

		if iTimeTempl is not null then
			select from_level, from_id into iTimeLevel, iTimeID from maxdata.dimset_template where template_id=iTimeTempl;
		else
			select from_time_level, from_time_id into iTimeLevel, iTimeID from maxdata.planworksheet
				where planworksheet_id=iWkShtID;

		end if;

		if iLocTempl is not null then

			select from_level, from_id into iLocLevel, iLocID from maxdata.dimset_template where template_id=iLocTempl;
		else
			select from_loc_level, from_loc_id into iLocLevel, iLocID from maxdata.planworksheet
				where planworksheet_id=iWkShtID;

		end if;

		iSql:='update maxdata.'||iModelStyle||' a set (tot_sales,tot_item,Sales_aur)=(select WP_NUM_048,WP_NUM_049, WP_NUM_098'||
			' from maxdata.mplan_working b'||
			' where a.merch_id=b.merch_id and b.merch_level='||iShopLvl||' and b.time_level='||iTimeLevel||' and b.time_id='||iTimeId||
			' and b.location_level='||iLocLevel||' and b.location_id='||iLocId||
			' and b.workplan_id='||iWkShtID||' and a.planworksheet_id=b.workplan_id)'||
			' where a.planworksheet_id='||iWkShtID;
		execute immediate iSql;
		commit;

	end if;

	iSql:='delete from maxdata.'||iModelProd||' where planworksheet_id='||iWkShtID;
	execute immediate iSql;
	commit;

	iSql:='select count(1)from maxdata.'||iModelProd;
	execute immediate iSql into iCount;

end loop;

---
iSql:='update maxdata.'||iModelStyle||' a set (Style_last_update, style_id, style_desc, Brand, Merch_type, shopper_comment_1, shopper_comment_2)=('||
	' select b.last_update, b.lv7cmast_userid, b.name, b.User_attrib12, decode(b.User_attrib44,0,''Basic'',1,''Fashion''), b.char_user9, b.bigchar_user15'||
	' from maxdata.lv7cmast b, maxdata.lv7ctree c '||
	' where a.merch_id=c.lv7ctree_id and b.lv7cmast_id=c.lv7cmast_id)';
execute immediate iSql;

iSql:='update maxdata.'||iModelStyle||' a set (dept_id, class_id,subclass_id)=(select b.lv4cmast_userid,c.lv5cmast_userid, d.lv6cmast_userid '||
	' from maxdata.lv4cmast b, maxdata.lv5cmast c, maxdata.lv6cmast d, maxdata.lv7ctree e'||
	' where a.merch_id=e.lv7ctree_id and e.lv4cmast_id=b.lv4cmast_id and e.lv5cmast_id=c.lv5cmast_id and e.lv6cmast_id=d.lv6cmast_id)';
execute immediate iSql;

iSql:='update maxdata.'||iModelStyle||' a set worksheet_desc=(select name from maxdata.planworksheet b '||
	' where a.planworksheet_id=b.planworksheet_id) where planworksheet_id > 0';
execute immediate iSql;

iSql:='update maxdata.'||iModelStyle||' a set template_desc=(select b.name from maxdata.dimset_template b '||
	' where a.template_id=b.template_id) where template_id > 0';
execute immediate iSql;

---- Merge with the perment table
----
iSql:='delete from maxdata.'||iModelStyle||' a where exists (select 1 from maxdata.fes_model_style b'||
	' where a.planworksheet_id=b.planworksheet_id and a.template_id=b.template_id and a.merch_id=b.merch_id and a.exclude_flag=b.exclude_flag'||
	' and a.removed_flag=b.removed_flag and a.Brand=b.Brand and a.Merch_type=b.Merch_type and a.shopper_comment_1=b.shopper_comment_1'||
	' and a.shopper_comment_2=b.shopper_comment_2 and a.tot_sales=b.tot_sales and a.tot_item=b.tot_item and a.sales_aur=b.sales_aur)';
execute immediate iSql;

iSql:='Update maxdata.'||iModelStyle||' a set create_date = (select create_date from maxdata.fes_model_style b'||
	' where a.planworksheet_id=b.planworksheet_id and a.template_id=b.template_id and a.merch_id=b.merch_id)'||
	' where exists (select 1 from maxdata.fes_model_style c where a.planworksheet_id=c.planworksheet_id'||
	' and a.template_id=c.template_id and a.merch_id=c.merch_id)';
execute immediate iSql;

iSql:='delete from maxdata.fes_model_style a where exists (select 1 from maxdata.'||iModelStyle||' b'||
	' where a.planworksheet_id=b.planworksheet_id and a.template_id=b.template_id and a.merch_id=b.merch_id)';
execute immediate iSql;

iSql:=' Insert into maxdata.fes_model_style select * from maxdata.'||iModelStyle;
execute immediate iSql;
commit;

----- Remove temp tables
<<end_program>>

--iModelPlan:='t_fes_model_plan';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iModelPlan);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iModelPlan;
	execute immediate iSql;
end if;

--iModelStyle:='t_fes_model_style';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iModelStyle);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iModelStyle;
	execute immediate iSql;
end if;

end;

/
