--------------------------------------------------------
--  DDL for Procedure P_FES_COPY_SHOP_WKSHT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_FES_COPY_SHOP_WKSHT" (iOnline integer, iWkShtID Integer, iRtn out number)
/*------------------------------------------------------------------------------------------------
Date Created: 10/21/2013
Description:
Parameter Description: None
Note: This is process copying the shop items in the planworksheets flagged by the users
Date		Who  Desc
10/21/2013	ec   Created
1/8/2014	ec   corrected the drop temp table 'iTmpTMShop' issue
------------------------------------------------------------------------------------------------*/
as
t_from_time_level	number(5);
t_from_time_id		number(10);
t_to_time_level		number(5);
t_from_merch_level	number(5);
t_from_merch_id		number(10);
t_to_merch_level	number(5);
t_to_loc_level	number(5);
iName			Varchar2(50);
t_planversion_id	number(10);

iShopLvl		number(2);
iShopTM			number(2);
iMultiTM		char(1);
iShopOE			char(1);
iStop			varchar2(1);

iUnassgign		number(1);
iKeep			number(1);
iShop			number(1);
iNew			number(1);
iDrop			number(1);

--- Temp tables
iTmpShopSku		varchar2(30) :='t_fes_shop_sku';
iTmpUnassign		varchar2(30) :='t_fes_unassign_sku';
iTmpNewCmast		varchar2(30) :='t_fes_new_cmast';
iTmpNewpMerch		varchar2(30) :='t_fes_new_merch';
iTmpNewCtree		varchar2(30) :='t_fes_new_ctree';

iTmpOldNew		varchar2(30) :='t_fes_oldnew';

iTmpMplanNew		varchar2(30) :='t_fes_mplan_new';
iTmpMplanOld		varchar2(30) :='t_fes_mplan_old';
iTmpNewTmplMB		varchar2(30) :='t_fes_tmpl_new_mb';

iTmpLocks		varchar2(30) :='t_fes_wk_locks';
iTmpTMShop		varchar2(30) :='t_fes_tmshop';
iMerchTab		varchar2(30);
iPeriod			number(5);
iCount			number(10);
iCtr			number(10);
iTimeID			number(10);
iTimeLvl		number(10);
iFlag           	number(2);
iEvenOdd           	number(2);
iTypeCol		varchar2(30);
iNoCopyCol		varchar2(30);

iSql  			long;

type AttribCol_type IS TABLE OF VARCHAR2(30)
      INDEX BY BINARY_INTEGER;
iAttribCol              AttribCol_type;
iAttribCol_reset	varchar2(1000);
iAttribCol_E          	AttribCol_type;
iAttribCol_reset_E	varchar2(1000);
iAttribCol_O         	AttribCol_type;
iAttribCol_reset_O	varchar2(1000);


iAttribCol_where_0	varchar2(1000);
iAttribCol_where_2	varchar2(1000);
iAttribCol_update_0	varchar2(1000);
iAttribCol_update_4 	varchar2(1000);
iAttribCol_update_3	varchar2(1000);


errcode             integer;
errmsg              varchar2(1000);

BEGIN

select upper(nvl(value_1,'N')) into iStop from maxdata.client_config where upper(key_1)='STOP COPY SHOP';

if iStop ='Y' then
	return;
end if;

select value_1 into iTypeCol from maxdata.client_config where upper(key_1)='SHOP TYPE';
select value_1 into iNoCopyCol from maxdata.client_config where upper(key_1)='SHOP NO OF COPY';

select to_number(nvl(value_1,'0'))  into iUnassgign from maxdata.client_config where upper(key_1)='UNASSIGNED STATUS CODE';
select to_number(nvl(value_1,'1'))  into iKeep from maxdata.client_config where upper(key_1)='KEEP STATUS CODE';
select to_number(nvl(value_1,'2'))  into iShop from maxdata.client_config where upper(key_1)='SHOP STATUS CODE';
select to_number(nvl(value_1,'3'))  into iNew from maxdata.client_config where upper(key_1)='NEW STATUS CODE';
select to_number(nvl(value_1,'4'))  into iDrop from maxdata.client_config where upper(key_1)='DROP STATUS CODE';

select to_number(nvl(value_1,'10'))  into iShopLvl from maxdata.client_config where upper(key_1)='SHOP MERCH LEVEL';
---select upper(nvl(value_1,'N'))  into iMultiTM from maxdata.client_config where upper(key_1)='SHOP MULTI-BOUNDARY';
select to_number(nvl(value_1,'48'))   into iShopTM from maxdata.client_config where upper(key_1)='SHOP BOUNDARY';
select upper(nvl(value_1,'N'))  into iShopOE from maxdata.client_config where upper(key_1)='SHOP ODD EVEN';

if iShopOE = 'N' then
	for iCtr in 1..4 loop
 --   dbms_output.put_line(ictr);
		iAttribCol(iCtr):='  ';

	end loop;

	select upper(nvl(value_1,'ERROR')) into iAttribCol(1) from maxdata.client_config where upper(key_1)='SHOP DROP COL1';
	iAttribCol_reset:=iAttribCol(1)||'=NULL';

	if iShopTM = 48 then
		select upper(nvl(value_1,'ERROR')) into iAttribCol(2) from maxdata.client_config where upper(key_1)='SHOP DROP COL2';
		iAttribCol_reset:=iAttribCol_reset||','||iAttribCol(2)||'=NULL';
	else
		select upper(nvl(value_1,'ERROR')) into iAttribCol(3) from maxdata.client_config where upper(key_1)='SHOP DROP COL3';
		iAttribCol_reset:=iAttribCol_reset||','||iAttribCol(3)||'=NULL';
		select upper(nvl(value_1,'ERROR')) into iAttribCol(4) from maxdata.client_config where upper(key_1)='SHOP DROP COL4';
		iAttribCol_reset:=iAttribCol_reset||','||iAttribCol(4)||'=NULL';
	end if;

else

    for iCtr in 1..4 loop
        iAttribCol_O(iCtr):='  ';
        iAttribCol_E(iCtr):='  ';
    end loop;

	select upper(nvl(value_1,'ERROR')) into iAttribCol_O(1) from maxdata.client_config where upper(key_1)='SHOP DROP 1 ODD';
	iAttribCol_reset:=iAttribCol_O(1)||'=NULL';
         --   dbms_output.put_line('iAttribCol_reset:'||iAttribCol_reset);

	select upper(nvl(value_1,'ERROR')) into iAttribCol_E(1) from maxdata.client_config where upper(key_1)='SHOP DROP 1 EVEN';
	iAttribCol_reset:=iAttribCol_reset||','||iAttribCol_E(1)||'=NULL';

	if iShopTM = 48 then
		select upper(nvl(value_1,'ERROR')) into iAttribCol_O(2) from maxdata.client_config where upper(key_1)='SHOP DROP 2 ODD';
		iAttribCol_reset:=iAttribCol_reset||','||iAttribCol_O(2)||'=NULL';

		select upper(nvl(value_1,'ERROR')) into iAttribCol_E(2) from maxdata.client_config where upper(key_1)='SHOP DROP 2 EVEN';
		iAttribCol_reset:=iAttribCol_reset||','||iAttribCol_E(2)||'=NULL';

	else
		select upper(nvl(value_1,'ERROR')) into iAttribCol_O(3) from maxdata.client_config where upper(key_1)='SHOP DROP 3 ODD';
		iAttribCol_reset:=iAttribCol_reset||','||iAttribCol_O(3)||'=NULL';
		select upper(nvl(value_1,'ERROR')) into iAttribCol_E(3) from maxdata.client_config where upper(key_1)='SHOP DROP 3 EVEN';
		iAttribCol_reset:=iAttribCol_reset||','||iAttribCol_E(3)||'=NULL';
		select upper(nvl(value_1,'ERROR')) into iAttribCol_O(4) from maxdata.client_config where upper(key_1)='SHOP DROP 4 ODD';
		iAttribCol_reset:=iAttribCol_reset||','||iAttribCol_O(4)||'=NULL';
		select upper(nvl(value_1,'ERROR')) into iAttribCol_E(4) from maxdata.client_config where upper(key_1)='SHOP DROP 4 EVEN';
		iAttribCol_reset_E:=iAttribCol_reset_E||','||iAttribCol_E(4)||'=NULL';
	end if;


end if;

---------------------------------------------------------------
--- Create temp tables
--iTmpNewCtree:='t_fes_new_ctree';
--iTmpNewTmplMB:='t_fes_tmpl_new_mb';
---------------------------------------------------------------

-- iTmpLocks:='t_fes_wk_locks';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpLocks);

if iCtr = 0 then
	iSql := 'create table maxdata.'||iTmpLocks||' as select * from maxdata.mmax_locks where 1=2';
	execute immediate iSql;
else
	iSql :='truncate table maxdata.'||iTmpLocks;
	execute immediate iSql;
end if;


--iTmpTMShop:='t_fes_tmshop';';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpTMShop);

if iCtr > 0 then
	iSql := 'drop table maxdata.'||iTmpTMShop;
end if;

iSql := 'create table maxdata.'||iTmpTMShop||
	'(EvenOdd number(2), period_id number(5), time_level number(3), time_id number(10), no_children number(3))'||
	'  nologging pctfree 0';
execute immediate iSql;

----------------------------------------------------------------------------------------------------------------------------
--- worksheet has to be;
--- copy_shop is flagged
--- AP Worksheets,  saved/submitted and not a whatif plan
--- To_merch_level = shop merch level defined in the client config
--- To_time_level >= the Shop time Boundary (iShopTM)

select name, from_time_level,from_time_id, to_time_level, from_merch_level, from_merch_id, to_merch_level, to_loc_level, planversion_id
  into iName,t_from_time_level,t_from_time_id, t_to_time_level, t_from_merch_level, t_from_merch_id, t_to_merch_level, t_to_loc_level, t_planversion_id
	from maxdata.planworksheet where planworksheet_id=iWkShtID;

select count(*) into iFlag from maxdata.mmax_locks where worksheet_id=iWkShtID;

if iFlag > 0 then --- worksheet open by other user

	if iOnline = 2 then -- this is a batch process

		update maxdata.mplan_attrib set at_num_001=0 where workplan_id=iWkShtID;

		update maxdata.planworksheet set copy_shop_message='Err:'||to_char(sysdate,'mm/dd/yy')||' WKSht Opended by Others', copy_shop=10
				where planworksheet_id=iWkShtID;

		insert into maxdata.fes_log(FES_Process,process_date,Process_status,Process_result)
				Values('Copy Shop - Batch',sysdate,1,'Err:'||to_char(sysdate,'mm/dd/yy')||' WKSht('||iWkShtID||') Opended by Others');
		iRtn:=1;
		commit;
	else
		insert into maxdata.fes_log(FES_Process,process_date,Process_status,Process_result)
				Values('Copy Shop - OnLine',sysdate,0,'Info:'||to_char(sysdate,'mm/dd/yy')||' WKSht('||iWkShtID||') Opended by Others');
		iRtn:=0;
		commit;
	end if;

	goto end_program;

end if;


--- lock worksheet
iSql:='Insert into maxdata.'||iTmpLocks||'(LOCK_ID,LOCK_LEVEL,LOCK_TYPE,LOCK_DATE,NAME,USER_NAME,SERVER,SESSION_ID,'||
	'WORKSHEET_ID,PLAN_SIZE,PLAN_PHYSICAL_MEM,PLAN_VIRTUAL_MEM,HASOPENED,STATUS,CURR_STATUS,CURR_TIME,PATHWAY_TYPE)'||
	' Values ('||iWkShtID||',94,''WK_OPEN'',sysdate,'''||iName||''',''Admin'',''0.0.0.0'','||
	'''FES-Create New SKU/Class'','||iWkShtID||',0,0,0,''Y'',''E'',''O'',123456789,''ap'')';
execute immediate iSql;
commit;

/*
---- Check if there is a split worksheets due to the cell counts) with same dimension opened

select count(*) into iFlag from maxdata.planworksheet where whatif=0 and from_time_level=t_from_time_level
	and from_time_id=t_from_time_id and to_time_level=t_to_time_level
	and from_merch_level= t_from_merch_level and from_merch_id= t_from_merch_id
	and to_merch_level=t_to_merch_level and planversion_id=t_planversion_id and planworksheet_id <> iWkShtID;

--- split merch. not support yet
if iFlag > 0 then

	update maxdata.mplan_attrib set at_num_001=0 where workplan_id=iWkShtID;
	update maxdata.planworksheet set copy_shop_message='Err:'||to_char(sysdate,'mm/dd/yy')||' Split WKSht('||iWkShtID||') Not Support', copy_shop=10
			where planworksheet_id=iWkShtID;

	if iOnline = 2 then -- this is a batch process

		insert into maxdata.fes_log(FES_Process,process_date,Process_status,Process_result)
			Values('Copy Shop - Batch',sysdate,1,'Err:'||to_char(sysdate,'mm/dd/yy')||' Split WKSht('||iWkShtID||') Not Support');
		iRtn:=1;

	else
		insert into maxdata.fes_log(FES_Process,process_date,Process_status,Process_result)
			Values('Copy Shop - OnLine',sysdate,0,'Info:'||to_char(sysdate,'mm/dd/yy')||' Split WKSht('||iWkShtID||') Not Support');
		iRtn:=0;


	end if;

	commit;
	goto end_program;
end if;
*/

iSql:='insert into maxdata.mmax_locks select * from  maxdata.'||iTmpLocks;
execute immediate iSql;
commit;

--iTmpShopSku:='t_fes_shop_sku';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpShopSku);

if iCtr > 0 then
	iSql := 'drop table maxdata.'||iTmpShopSku;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpShopSku||'(merch_id number(10),location_id number(10),time_id number(10),'||
	'workplan_id number(10), new_merch_id number(10)) nologging pctfree 0';
execute immediate iSql;

iSql:='Create index idx_'||iTmpShopSku||' on maxdata.'||iTmpShopSku||'(merch_id,location_id,time_id)';
execute immediate iSql;


-- gett the time IDs at the Time Boundary level

iSql:='Insert into maxdata.'||iTmpTMShop||'(time_id)'||
	'select distinct lv'||(iShopTM-46)||'time_lkup_id from maxapp.lv'||(t_to_time_level-46)||'time'||
	' where lv'||(t_to_time_level-46)||'time_lkup_id in ('||
	' select distinct time_id from maxdata.mplan_working where workplan_id='||iWkShtID||
	' and time_level ='||t_to_time_level||')';
execute immediate iSql;
commit;

iSql:='update maxdata.'||iTmpTMShop||' a set time_level='||iShopTM||', no_children=('||
	'select count(distinct time_id) from maxdata.mplan_working b'||
	' where b.workplan_id='||iWkShtID||' and b.time_level='||t_to_time_level||
	'  and b.time_id in (select lv'||(t_to_time_level-46)||'time_lkup_id from maxapp.lv'||(t_to_time_level-46)||'time'||
	'  where lv'||(iShopTM-46)||'time_lkup_id=b.time_id) and b.time_id=a.time_id)';
execute immediate iSql;
commit;

iSql:='select count(*) from maxdata.'||iTmpTMShop;
execute immediate iSql into iCtr;

--- in the time set, choose the planning period with the most children in the plan

if iCtr > 1  then -- use the time id has most of child periods in the table

	iSql:='delete from maxdata.'||iTmpTMShop||' where no_children <> (select max(no_children) from maxdata.'||iTmpTMShop||')';
	execute immediate iSql;
	commit;

	iSql:='select count(*) from maxdata.'||iTmpTMShop;
	execute immediate iSql into iCtr;

	if iCtr > 1 then --- keep the first one
		iSql:='delete from maxdata.'||iTmpTMShop||' where time_id <> (select time_id from maxdata.'||iTmpTMShop||
			' where rownum=1)';
		execute immediate iSql;
		commit;
	end if;
end if;


iSql:='Update maxdata.'||iTmpTMShop||' a set (EvenOdd, period_id)=(select mod(cycle_id,2), lv'||(iShopTM-46)||'time_id'||
	' from maxapp.lv'||(iShopTM-46)||'time b where a.time_id=b.lv'||(iShopTM-46)||'time_lkup_id)';
execute immediate iSql;
commit;

--- Get the planning period id to process

iSql:='select EvenOdd,period_id,time_level,time_id from maxdata.'||iTmpTMShop;
execute immediate iSql into iEvenOdd,iPeriod,iTimeLvl,iTimeID;

if iShopOE ='N' then

	iAttribCol_where_0:=iAttribCol_where_0||' and b.'||iAttribCol(iPeriod)||'='''||iUnassgign||'''';
	iAttribCol_where_2:=iAttribCol_where_2||' and b.'||iAttribCol(iPeriod)||'='''||iShop||'''';
	iAttribCol_update_0:=iAttribCol_update_0||','||iAttribCol(iPeriod)||'='''||iUnassgign||'''';
        iAttribCol_update_4:=iAttribCol_update_4||','||iAttribCol(iPeriod)||'='''||iDrop||'''';
	iAttribCol_update_3:=iAttribCol_update_3||','||iAttribCol(iPeriod)||'='''||iNew||'''';
else

	if iEvenOdd = 0 then -- even year
		iAttribCol_where_0:=iAttribCol_where_0||' and b.'||iAttribCol_E(iPeriod)||'='''||iUnassgign||'''';
		iAttribCol_where_2:=iAttribCol_where_2||' and b.'||iAttribCol_E(iPeriod)||'='''||iShop||'''';
		iAttribCol_update_0:=iAttribCol_update_0||','||iAttribCol_E(iPeriod)||'='''||iUnassgign||'''';
        	iAttribCol_update_4:=iAttribCol_update_4||','||iAttribCol_E(iPeriod)||'='''||iDrop||'''';
		iAttribCol_update_3:=iAttribCol_update_3||','||iAttribCol_E(iPeriod)||'='''||iNew||'''';
	else
		iAttribCol_where_0:=iAttribCol_where_0||' and b.'||iAttribCol_O(iPeriod)||'='''||iUnassgign||'''';
		iAttribCol_where_2:=iAttribCol_where_2||' and b.'||iAttribCol_O(iPeriod)||'='''||iShop||'''';
		iAttribCol_update_0:=iAttribCol_update_0||','||iAttribCol_O(iPeriod)||'='''||iUnassgign||'''';
        	iAttribCol_update_4:=iAttribCol_update_4||','||iAttribCol_O(iPeriod)||'='''||iDrop||'''';
		iAttribCol_update_3:=iAttribCol_update_3||','||iAttribCol_O(iPeriod)||'='''||iNew||'''';

	end if;

end if;

--- Get Shop Sku

select decode(iShopLvl,10,'mast','cmast') into iMerchTab from dual;

iSql:='insert into maxdata.'||iTmpShopSku||
	' select merch_id,location_id,time_id,workplan_id, 0 from maxdata.mplan_working a'||
	' where workplan_id='||iWkShtID||' and merch_level='||(t_to_merch_level-10)||
	' and time_level='||t_to_time_level||' and location_level='||t_to_loc_level||
	' and exists (select 1 from maxdata.lv'||iShopLvl||iMerchTab||' b, maxdata.lv'||iShopLvl||'ctree c'||
	' where a.merch_id=c.lv'||iShopLvl||'ctree_id and b.lv'||iShopLvl||iMerchTab||'_id=c.lv'||iShopLvl||iMerchTab||'_id'||
	iAttribCol_where_2||'and nvl(b.'||iTypeCol||',-1)>=0 and nvl(b.'||iNoCopyCol||',0)>0)';

execute immediate iSql;
commit;

iSql:='select count(*) from maxdata.'||iTmpShopSku;
execute immediate iSql into iCount;

if iCount > 0 then
		maxdata.p_fes_add_new_sku(iWkShtID,iPeriod,iAttribCol_update_3,iAttribCol_reset);

		maxdata.p_fes_add_mplan_new(iWkShtID,iPeriod,iAttribCol_update_0,iAttribCol_update_4);
end if;

--- Done with copy shop
update maxdata.mplan_attrib set at_num_001=0 where workplan_id=iWkShtID;

update maxdata.planworksheet set copy_shop_message=to_char(sysdate,'mm/dd/yy')||
	' Copy Shop Complete Sucessfully', invalidate=1, copy_shop=10
	where planworksheet_id=iWkShtID;

delete from  maxdata.mmax_locks where LOCK_ID=iWkShtID and lock_level=94 and lock_type='WK_OPEN';
commit;


----- Remove temp tables
<<end_program>>

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpTMShop);

if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpTMShop;
	execute immediate iSql;
end if;

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpShopSku);


if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpShopSku;
	execute immediate iSql;
end if;

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpOldNew);

if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpOldNew;
	execute immediate iSql;
end if;

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewCmast);

if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpNewCmast;
	execute immediate iSql;
end if;

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewpMerch);

if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpNewpMerch;
	execute immediate iSql;
end if;

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewTmplMB);

if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpNewTmplMB;
	execute immediate iSql;
end if;

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpLocks);

if iCtr = 1 then
	iSql := 'drop table maxdata.'||iTmpLocks;
	execute immediate iSql;
end if;
/*
---------- Exception Handeling
     EXCEPTION
      WHEN others THEN
        errcode := SQLCODE;
        errmsg := sqlerrm;
        ROLLBACK;

        insert into maxdata.fes_log(FES_Process,process_date,Process_status,Process_result)
		Values('Copy Shop',sysdate,2,'Err:'||to_char(sysdate,'mm/dd/yy')||' WKSht('||iWkShtID||')'||substr(errmsg,1,20));

    	update maxdata.mplan_attrib set at_num_001=0 where workplan_id=iWkShtID;

	update maxdata.planworksheet
            set copy_shop_message='Err:'||to_char(sysdate,'mm/dd/yy')||substr(errmsg,1,20), copy_shop=10
			where planworksheet_id=iWkShtID;

	iSql:='delete from  maxdata.mmax_locks where LOCK_ID in (select lock_id from maxdata.'||iTmpLocks||')';
        execute immediate iSql;

        iRtn:=99;

	commit;
*/
      END;

/

  GRANT EXECUTE ON "MAXDATA"."P_FES_COPY_SHOP_WKSHT" TO "MADMAX";
