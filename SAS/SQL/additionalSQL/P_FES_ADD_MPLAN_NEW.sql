--------------------------------------------------------
--  DDL for Procedure P_FES_ADD_MPLAN_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_FES_ADD_MPLAN_NEW" (
iWkShtID	number,
iPeriodID    number,
iAttribCol_update_0 varchar2,
iAttribCol_update_4 varchar2
)
/*------------------------------------------------------------------------------------------------
Date Created: 10/21/2013
Description:
Parameter Description: None
Note: This process add the mplan record for the new sku and change the shop sku to be unassigned.
Date		Who  Desc
10/21/2013	ec   Created
------------------------------------------------------------------------------------------------*/
as

--- Temp tables

iTmpShopSku		varchar2(30) :='t_fes_shop_sku';
iTmpOldNew		varchar2(30) :='t_fes_oldnew';

iTmpMplanNew		varchar2(30) :='t_fes_mplan_new';
iTmpMplanOld		varchar2(30) :='t_fes_mplan_old';
iTmpNewTmplMB		varchar2(30) :='t_fes_tmpl_new_mb';

iTmpMAttribNew		varchar2(30) :='t_fes_mattrib_new';
iTmpMAttribOld		varchar2(30) :='t_fes_mattrib_old';

--- local temp tables
iTmpTab			varchar2(30) :='t_fes_tmp';
iTmpOld			varchar2(30) :='t_fes_old';
iTmpMB			varchar2(30) :='t_fes_mb';
iTmpAttrib		varchar2(30) :='t_fes_attrib';

iCount			number(5);
iCtr			number(5);
iToTimeLevel		number(5);
iToLocLevel		number(5);
iToMerchLevel		number(5);
iMerchTmplId		number(10);
IPartital       number(2);
iSql  			long;

iShopLvl		number(2);
iType			number(2);
iTypeCol       		varchar2(30);
iKeySeq			number(2);
iSName			varchar2(30);
iShopTM			number(2);
iTM             	varchar2(5);

iMerchTab		varchar2(30);
iCtreeTab		varchar2(30);

iCmastID		number(10);
iCtreeID		number(10);
iNewCmastID		number(10);
iNewCtreeID		number(10);

BEGIN

select to_time_level, to_loc_level, (to_merch_level-10), nvl(merch_template_id,0) into iToTimeLevel,iToLocLevel,iToMerchLevel,iMerchTmplId
	from maxdata.planworksheet where planworksheet_id=iWkShtID;

if iMerchTmplId > 0 then
	select partial_flag into iPartital from maxdata.DIMSET_TEMPLATE_LEV
		where template_id=iMerchTmplId and level_number=iToMerchLevel+10 and level_incl_flag=1;

end if;

select to_number(nvl(value_1,'10'))  into iShopLvl from maxdata.client_config where upper(key_1)='SHOP MERCH LEVEL';
select value_1 into iTypeCol from maxdata.client_config where upper(key_1)='SHOP TYPE';
select to_number(nvl(value_1,'48')) into iShopTM from maxdata.client_config where upper(key_1)='SHOP BOUNDARY';
select substr(level_name,1,1) into iTM from hier_level where hier_id=50 and level_id=iShopTM;

iSName:=iTM||iPeriodID;

iCtreeTab:='lv'||iShopLvl||'ctree';

if iShopLvl = 10 then
	iMerchTab:='lv10mast';
else
	iMerchTab:='lv'||iShopLvl||'cmast';
end if;

---------------------------------------------------------------
--- Create skus's temp tables
---------------------------------------------------------------
--iTmpMplanNew:='t_fes_mplan_new';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMplanNew);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMplanNew;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpMplanNew||' nologging pctfree 0 as select * from maxdata.mplan_working where 1=2';
execute immediate iSql;


--iTmpMplanOld:='t_fes_mplan_old';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMplanOld);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMplanOld;
	execute immediate iSql;
end if;
iSql := 'create table maxdata.'||iTmpMplanOld||' nologging pctfree 0 as select * from maxdata.mplan_working where 1=2';
execute immediate iSql;

--iTmpMAttribOld:='t_fes_mattrib_old';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMAttribOld);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMAttribOld;
	execute immediate iSql;
end if;
iSql := 'create table maxdata.'||iTmpMAttribOld||' nologging pctfree 0 as select * from maxdata.mplan_attrib where 1=2';
execute immediate iSql;

--iTmpMAttribNew:='t_fes_mattrib_new';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMAttribNew);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMAttribNew;
	execute immediate iSql;
end if;
iSql := 'create table maxdata.'||iTmpMAttribNew||' nologging pctfree 0 as select * from maxdata.mplan_attrib where 1=2';
execute immediate iSql;


--iTmpOld:='t_fes_old';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpOld);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpOld;
	execute immediate iSql;
end if;
iSql := 'create table maxdata.'||iTmpOld||' nologging pctfree 0 as select * from maxdata.mplan_working where 1=2';
execute immediate iSql;


-- iTmpNewTmplMB:='t_fes_tmpl_new_mb';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewTmplMB);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpNewTmplMB;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpNewTmplMB||' nologging pctfree 0 as select * from maxdata.DIMSET_TEMPLATE_MEM where 1=2';
execute immediate iSql;

-- iTmpAttrib:='t_fes_tmpl_new_mb';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpAttrib);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpAttrib;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpAttrib||' nologging pctfree 0 as select * from maxdata.mplan_attrib where 1=2';
execute immediate iSql;

------------------------------------------------------------------------------------

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpOldNew);

if iCtr = 0 then
	goto end_program;
else
	iSql := 'select count(*) from maxdata.'||iTmpOldNew;
	execute immediate iSql;

	if iCount = 0 then
		goto end_program;
	end if;
end if;

-----------------------------------------------------------------------------------------
--- set to 'Drop' if it is Basic, 'Unassigned' if it is fashion
---  changed to Drop if it is fashion. Keep 'if' statement, incase need to roll back
iSql:='Update maxdata.'||iMerchTab||' a set '||substr(iAttribCol_update_4,2,length(iAttribCol_update_4)-1)||
    ' where a.'||iMerchTab||'_id in (select distinct cmast_id from maxdata.'||iTmpOldNew||') and '||iTypeCol||' ='||
    '( select Merch_type_lkup_id from maxdata.fes_merch_type_lkup where upper(Merch_type_desc)=''BASIC'')';
execute immediate iSql;

iSql:='Update maxdata.'||iMerchTab||' a set '||substr(iAttribCol_update_4,2,length(iAttribCol_update_4)-1)||
    ' where a.'||iMerchTab||'_id in (select distinct cmast_id from  maxdata.'||iTmpOldNew||') and '||iTypeCol||' ='||
    '( select Merch_type_lkup_id from maxdata.fes_merch_type_lkup where upper(Merch_type_desc)=''FASHION'')';
execute immediate iSql;
commit;

-- Move mplan records of shop sku to new sku table
-- iTmpTab:='t_fes_tmp';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpTab);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpTab;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpTab||' nologging pctfree 0 as select * from maxdata.'||iTmpOldNew;
execute immediate iSql;

-- iTmpMB:='t_fes_mb';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMB);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMB;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpMB||' nologging pctfree 0 as select * from maxdata.DIMSET_TEMPLATE_MEM where 1=2';
execute immediate iSql;

------
iSql:='select count(*) from maxdata.'||iTmpTab;
execute immediate iSql into iCount;

while iCount > 0 loop

	iSql:='select merch_id, cmast_id, new_merch_id, new_cmast_id, merch_type, no_copy from maxdata.'||iTmpTab||' where rownum=1';
	execute immediate iSql into  iCtreeID, iCmastID, iNewCtreeID, iNewCmastID, iType, iKeySeq;

	iSql:='Truncate table maxdata.'||iTmpOld;
	execute immediate iSql;

	iSql:='Insert into maxdata.'||iTmpOld||' select * from maxdata.mplan_working'||
		' where workplan_id='||iWkShtID||' and merch_level='||iToMerchLevel||
		' and merch_id ='||iCtreeID;
	execute immediate iSql;
	commit;

-- mplan attributes

	iSql:='Truncate table maxdata.'||iTmpAttrib;
	execute immediate iSql;

	iSql:='Insert into maxdata.'||iTmpAttrib||' select * from maxdata.mplan_attrib'||
		' where workplan_id='||iWkShtID||' and merch_level='||iToMerchLevel||
		' and merch_id ='||iCtreeID;
	execute immediate iSql;
	commit;

	--- Set the plan records to be the new model ID
	iSql:='Update maxdata.'||iTmpOld||' set merch_id='||iNewCtreeID;
	execute immediate iSql;

	iSql:='Insert into maxdata.'||iTmpMplanNew||' select * from maxdata.'||iTmpOld;
	execute immediate iSql;
	commit;

	--- Set the attrib records to be the new model ID
	iSql:='Update maxdata.'||iTmpAttrib||' set merch_id='||iNewCtreeID;
	execute immediate iSql;

	iSql:='Insert into maxdata.'||iTmpMAttribNew||' select * from maxdata.'||iTmpAttrib;
	execute immediate iSql;
	commit;


	if iKeySeq = 1 then

		--- Set the plan records back to old model ID
		iSql:='Update maxdata.'||iTmpOld||' set merch_id='||iCtreeID;
		execute immediate iSql;

		---- zero out WP values if it is Fashion
		if iType = 1 then --- Fashion merch type
			iSql:='';
			for c1 in (select column_name from sys.all_tab_columns where owner='MAXDATA' and table_name = 'MPLAN_WORKING'
				and column_name like 'WP_%' order by column_id) loop
				iSql:=iSql||c1.column_name||'=0,';
			end loop;

			iSql:='update maxdata.'||iTmpOld||' a set '||iSql||' CHANGED_BY_BATCH=99, MP_INIT_DATE=NULL';
			execute immediate iSql;
			commit;
		end if;

		iSql:='Insert into maxdata.'||iTmpMplanOld||' select * from maxdata.'||iTmpOld;
		execute immediate iSql;
		commit;

	end if;

	if iMerchTmplId > 0 and iPartital =1 then

		iSql:='Insert into maxdata.'||iTmpMB||' select * from maxdata.DIMSET_TEMPLATE_MEM'||
			    ' where template_id='||iMerchTmplId||' and level_number='||(iToMerchLevel+10)||
			    ' and member_id='||iCtreeID;
		execute immediate iSql;

		iSql:='Update maxdata.'||iTmpMB||' set member_id='||iNewCtreeID||', member_name='''||iSName||'-''||member_name'||
			' where member_id='||iCtreeID;
		execute immediate iSql;
		commit;

		iSql:='Insert into maxdata.'||iTmpNewTmplMB||' select * from maxdata.'||iTmpMB;
		execute immediate iSql;
		commit;

		iSql:='Truncate table maxdata.'||iTmpMB;
		execute immediate iSql;

	end if;

	iSql:='delete from maxdata.'||iTmpTab||' where merch_id='||iCtreeID||' and new_merch_id='||iNewCtreeID;
	execute immediate iSql;
	commit;

	iSql:='select count(*) from maxdata.'||iTmpTab;
	execute immediate iSql into iCount;

end loop;
--- set mp_init_date to null for the new records

iSql:='Update maxdata.'||iTmpMplanNew||' set MP_INIT_DATE=NULL';
execute immediate iSql;
commit;

---- update the old plan records
iSql:='delete from maxdata.mplan_working a where exists (select 1 from maxdata.'||iTmpMplanOld||
	' b where a.time_level=b.time_level and a.time_id=b.time_id and a.location_level=b.location_level and a.location_id=b.location_id'||
	' and a.merch_level=b.merch_level and a.merch_id=b.merch_id and a.workplan_id=b.workplan_id and a.workplan_id='||iWkShtID||')';
execute immediate iSql;

iSql:='insert into maxdata.mplan_working select * from maxdata.'||iTmpMplanOld;
execute immediate iSql;

iSql:='Insert into maxdata.mplan_working select * from maxdata.'||iTmpMplanNew||' a'||
    ' where not exists (select 1 from maxdata.mplan_working b where a.time_level=b.time_level and a.time_id=b.time_id'||
    ' and a.location_level=b.location_level and a.location_id=b.location_id'||
    ' and a.merch_level=b.merch_level and a.merch_id=b.merch_id and a.workplan_id=b.workplan_id and b.workplan_id='||iWkShtID||')';
execute immediate iSql;

iSql:='Insert into maxdata.mplan_attrib select * from maxdata.'||iTmpMAttribNew||' a'||
    ' where not exists (select 1 from maxdata.mplan_attrib b where a.time_level=b.time_level and a.time_id=b.time_id'||
    ' and a.location_level=b.location_level and a.location_id=b.location_id'||
    ' and a.merch_level=b.merch_level and a.merch_id=b.merch_id and a.workplan_id=b.workplan_id and b.workplan_id='||iWkShtID||')';
execute immediate iSql;

if iPartital =1 then
	iSql:='Insert into maxdata.DIMSET_TEMPLATE_MEM select * from maxdata.'||iTmpNewTmplMB||' a'||
        ' where not exists (select 1 from maxdata.DIMSET_TEMPLATE_MEM b where a.template_id=b.template_id and a.member_id=b.member_id'||
        ' and a.level_number=b.level_number)';
    execute immediate iSql;

end if;

commit;


----- Remove temp tables
<<end_program>>

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMplanNew);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMplanNew;
	execute immediate iSql;
end if;


--iTmpMplanOld:='t_fes_mplan_old';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMplanOld);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMplanOld;
	execute immediate iSql;
end if;

--iTmpMAttribOld:='t_fes_mattrib_old';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMAttribOld);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMAttribOld;
	execute immediate iSql;
end if;

--iTmpMAttribNew:='t_fes_mattrib_new';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMAttribNew);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMAttribNew;
	execute immediate iSql;
end if;

--iTmpOld:='t_fes_old';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpOld);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpOld;
	execute immediate iSql;
end if;


-- iTmpNewTmplMB:='t_fes_tmpl_new_mb';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewTmplMB);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpNewTmplMB;
	execute immediate iSql;
end if;

-- iTmpAttrib:='t_fes_attrib';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpAttrib);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpAttrib;
	execute immediate iSql;
end if;


-- iTmpMB:='t_fes_mb';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMB);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMB;
	execute immediate iSql;
end if;

-- iTmpTab:='t_fes_tmp';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpTab);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpTab;
	execute immediate iSql;
end if;



-----------------------------------------------------------------------------
end;

/
