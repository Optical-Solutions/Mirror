--------------------------------------------------------
--  DDL for Procedure P_FES_DUP_NEW_SKU
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_FES_DUP_NEW_SKU" 
/*------------------------------------------------------------------------------------------------
Date Created: 10/21/2013
Description:
Parameter Description: None
Note: This process duplicate the 'shop' skus in the planworksheets to be Model sku
Date		Who  Desc
10/21/2013	ec   Created
------------------------------------------------------------------------------------------------*/
as

--- Temp tables

--- Global Temp tables
iTmpNewCmast		varchar2(30) :='t_fes_new_cmast';
iTmpNewpMerch		varchar2(30) :='t_fes_new_merch';
iTmpNewCtree		varchar2(30) :='t_fes_new_ctree';
iTmpOldNew		varchar2(30) :='t_fes_oldnew';
iTmpShopSku		varchar2(30) :='t_fes_shop_sku';

--- Local Temp tables
iTmpDupNew		varchar2(30) :='t_fes_dupnew';
iTmpNew			varchar2(30) :='t_fes_tmpnew';
iTmpCmast		varchar2(30) :='t_fes_tmpcmast';
iTmpCtree		varchar2(30) :='t_fes_tmpctree';
iTmpMerch		varchar2(30) :='t_fes_tmpmerch';
iTmpSku			varchar2(30) :='t_fes_sku';


iMerchTab		varchar2(30);
iCtreeTab		varchar2(30);
iMerchUserID		varchar2(30);

iCmastID        number(10);
iCtreeID        number(10);

iCount			number(5);
iCtr			number(5);
iLoop			number(5);

iSql  			long;
iShopLvl		number(2);

begin

----	Check if need to create multiple copy of shop sku
iSql:='select count(*) from maxdata.'||iTmpOldNew||' where no_copy > 1';
execute immediate iSql into iCtr;

if iCtr = 0 then
	return;
end if;

select to_number(nvl(value_1,'10'))  into iShopLvl from maxdata.client_config where upper(key_1)='SHOP MERCH LEVEL';

iCtreeTab:='lv'||iShopLvl||'ctree';

if iShopLvl = 10 then
	iMerchTab:='lv10mast';
	iMerchUserID:='order_code';
else
	iMerchTab:='lv'||iShopLvl||'cmast';
	iMerchUserID:='lv'||iShopLvl||'cmast_userid';
end if;

--iTmpNew:='t_fes_tmpnew';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNew);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpNew;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpNew||' nologging pctfree 0 as'||
	' select * from maxdata.'||iTmpOldNew||' where 1=2';
execute immediate iSql;

--iTmpSku:='t_fes_sku';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpSku);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpSku;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpSku||' nologging pctfree 0 as'||
	' select * from maxdata.'||iTmpShopSku||' where 1=2';
execute immediate iSql;

--iTmpCmast:='t_fes_tmpcmast';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpCmast);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpCmast;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpCmast||' nologging pctfree 0 as'||
	' select * from maxdata.'||iTmpNewCmast||' where 1=2';
execute immediate iSql;

--iTmpCtree:='t_fes_tmpctree';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpCtree);

if iCtr > 0  then
	iSql:='Drop table maxdata.'||iTmpCtree;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpCtree||' nologging pctfree 0 as'||
	' select * from maxdata.'||iTmpNewCtree||' where 1=2';
execute immediate iSql;

--iTmpMerch:='t_fes_tmpmerch';
select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMerch);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpMerch;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpMerch||' nologging pctfree 0 as'||
	' select * from maxdata.'||iTmpNewpMerch||' where 1=2';
execute immediate iSql;


--iTmpDupNew:='t_fes_dupnew';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpDupNew);

if iCtr > 0 then
	iSql:='Drop table maxdata.'||iTmpDupNew;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpDupNew||' nologging pctfree 0 as'||
	' select * from maxdata.'||iTmpOldNew||' where no_copy > 1';
execute immediate iSql;

iSql:='update maxdata.'||iTmpDupNew||' set no_copy=no_copy-1';
execute immediate iSql;
commit;

iSql:='select count(*) from maxdata.'||iTmpDupNew;
execute immediate iSql into iCtr;

while iCtr > 0 loop

	iSql:='select merch_id, cmast_id, no_copy from maxdata.'||iTmpDupNew||' where rownum=1';
	execute immediate iSql into  iCtreeID, iCmastID, iCount;

	iSql:='insert into  maxdata.'||iTmpNew||' select * from maxdata.'||iTmpDupNew||' where merch_id='||iCtreeID;
	execute immediate iSql;

	iSql:='insert into  maxdata.'||iTmpSku||' select * from maxdata.'||iTmpShopSku||' where merch_id='||iCtreeID;
	execute immediate iSql;

	iSql:='insert into  maxdata.'||iTmpCmast||' select * from maxdata.'||iTmpNewCmast||' where '||iMerchTab||'_id='||iCmastID;
	execute immediate iSql;

	iSql:='insert into  maxdata.'||iTmpCtree||' select * from maxdata.'||iTmpNewCtree||' where '||iCtreeTab||'_id='||iCtreeID;
	execute immediate iSql;

	if iShopLvl = 10 then
		iSql:='insert into  maxdata.'||iTmpMerch||' select * from maxdata.'||iTmpNewpMerch||' where lv10mast_id='||iCmastID;
		execute immediate iSql;
	end if;

	commit;

	for iLoop in 1..iCount loop

		--- insert back to the global tables
		iSql:='update  maxdata.'||iTmpNew||' set no_copy='||iLoop;
		execute immediate iSql;

		iSql:='insert into  maxdata.'||iTmpOldNew||' select * from maxdata.'||iTmpNew;
		execute immediate iSql;

		iSql:='update  maxdata.'||iTmpSku||' set seq_key='||iLoop;
		execute immediate iSql;

		iSql:='insert into  maxdata.'||iTmpShopSku||' select * from maxdata.'||iTmpSku;
		execute immediate iSql;

		iSql:='update  maxdata.'||iTmpCmast||' set seq_key='||iLoop;
		execute immediate iSql;

		iSql:='insert into  maxdata.'||iTmpNewCmast||' select * from maxdata.'||iTmpCmast;
		execute immediate iSql;

		iSql:='update  maxdata.'||iTmpCtree||' set seq_key='||iLoop;
		execute immediate iSql;

		iSql:='insert into  maxdata.'||iTmpNewCtree||' select * from maxdata.'||iTmpCtree;
		execute immediate iSql;
		if iShopLvl = 10 then
			iSql:='update  maxdata.'||iTmpMerch||' set seq_key='||iLoop;
			execute immediate iSql;

			iSql:='insert into  maxdata.'||iTmpNewpMerch||' select * from maxdata.'||iTmpMerch;
			execute immediate iSql;
		end if;

	end loop;

           --- clear the previous records
        iSql:='delete from maxdata.'||iTmpNew;
        execute immediate iSql;

        iSql:='delete from maxdata.'||iTmpSku;
        execute immediate iSql;

        iSql:='delete from maxdata.'||iTmpCmast;
        execute immediate iSql;

        iSql:='delete from maxdata.'||iTmpCtree;
        execute immediate iSql;

        iSql:='delete from maxdata.'||iTmpMerch;
        execute immediate iSql;
        commit;

    	iSql:='delete from maxdata.'||iTmpDupNew||' where merch_id='||iCtreeID||' and cmast_id='||iCmastID;
     	execute immediate iSql;
   	 commit;

	iSql:='select count(*) from maxdata.'||iTmpDupNew;
	execute immediate iSql into iCtr;

end loop;


----- Remove temp tables
<<end_program>>

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpDupNew);
if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpDupNew;
	execute immediate iSql;
end if;

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNew);
if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpNew;
	execute immediate iSql;
end if;

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpSku);
if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpSku;
	execute immediate iSql;
end if;

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpCmast);
if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpCmast;
	execute immediate iSql;
end if;

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpCtree);
if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpCtree;
	execute immediate iSql;
end if;


select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpMerch);
if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpMerch;
	execute immediate iSql;
end if;

end;

/
