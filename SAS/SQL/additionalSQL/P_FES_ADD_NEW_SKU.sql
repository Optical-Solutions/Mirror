--------------------------------------------------------
--  DDL for Procedure P_FES_ADD_NEW_SKU
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_FES_ADD_NEW_SKU" (
iWkShtID	number,
iPeriodID    number,
iAttribCol_update_3 varchar2,
reset_shop	varchar2
)
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


iTmpShopSku		varchar2(30) :='t_fes_shop_sku';
iTmpNewCmast		varchar2(30) :='t_fes_new_cmast';
iTmpNewpMerch		varchar2(30) :='t_fes_new_merch';
iTmpNewModel		varchar2(30) :='t_fes_new_Model';
iTmpNewcat		varchar2(30) :='t_fes_new_cat';
iTmpNewCtree		varchar2(30) :='t_fes_new_ctree';
iTmpOldNew		varchar2(30) :='t_fes_oldnew';

iTmpTab			varchar2(30) :='t_fes_temp';


iMerchTab		varchar2(30);
iCtreeTab		varchar2(30);
iMerchUserID		varchar2(30);
iCopyFrom		varchar2(30);
iParentList		varchar2(100);
iCount			number(5);
iCtr			number(5);
iLoop			number(5);
iSeqNo			number(10);
iSkuID			number(10);
iCatID			number(10);
iCmastID		number(10);
iCtreeID		number(10);
iNewCmastID		number(10);
iNewCtreeID		number(10);

iSql  			long;
iNewProd		varchar2(30);
iSName			varchar2(30);
iTM             	varchar2(5);
iAttribCol		varchar2(30);

iShopTM        number(2);
iShopLvl		number(2);
iTypeCol		varchar2(30);
iNoCopyCol		varchar2(30);

BEGIN

select to_number(nvl(value_1,'10'))  into iShopLvl from maxdata.client_config where upper(key_1)='SHOP MERCH LEVEL';
select value_1 into iCopyFrom from maxdata.client_config where upper(key_1)='SHOP COPY FROM';
select value_1 into iTypeCol from maxdata.client_config where upper(key_1)='SHOP TYPE';
select to_number(nvl(value_1,'48')) into iShopTM from maxdata.client_config where upper(key_1)='SHOP BOUNDARY';
select value_1 into iNoCopyCol from maxdata.client_config where upper(key_1)='SHOP NO OF COPY';

iCtreeTab:='lv'||iShopLvl||'ctree';

select level_name into iNewProd from hier_level where hier_id=11 and level_id=iShopLvl+10;
select substr(level_name,1,1) into iTM from hier_level where hier_id=50 and level_id=iShopTM;

iNewProd:=iNewProd||'-'||iTM||iPeriodID;
iSName:=iTM||iPeriodID;

if iShopLvl = 10 then
	iMerchTab:='lv10mast';
	iMerchUserID:='order_code';
else
	iMerchTab:='lv'||iShopLvl||'cmast';
	iMerchUserID:='lv'||iShopLvl||'cmast_userid';
end if;

---------------------------------------------------------------
--- Create skus's temp tables
---------------------------------------------------------------
--iTmpNewCmast:='t_fes_new_sku';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewCmast);

if iCtr = 1 then
	iSql:='Drop table maxdata.'||iTmpNewCmast;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpNewCmast||' nologging pctfree 0 as select * from maxdata.'||iMerchTab||' where 1=2';
execute immediate iSql;


--iTmpNewMerch:='t_fes_new_merch';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewpMerch);

if iCtr = 1 then
	iSql:='Drop table maxdata.'||iTmpNewpMerch;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpNewpMerch||' nologging pctfree 0 as select * from maxdata.lv10merch where 1=2';
execute immediate iSql;


--iTmpNewCtree:='t_fes_new_ctree';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewCtree);

if iCtr = 1 then
	iSql:='Drop table maxdata.'||iTmpNewCtree;
	execute immediate iSql;
end if;


iSql := 'create table maxdata.'||iTmpNewCtree||' nologging pctfree 0 as select * from maxdata.'||iCtreeTab||' where 1=2';
execute immediate iSql;



--iTmpOldNew:='t_fes_oldnew';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpOldNew);

if iCtr = 1 then
	iSql:='drop table maxdata.'||iTmpOldNew;
	execute immediate iSql;
end if;

iSql := 'create table maxdata.'||iTmpOldNew||'(merch_id number(10), cmast_id number(10), new_merch_id number(10), new_cmast_id number(10),'||
	'prod_id varchar2(30), record_type varchar2(2), merch_type number(2), no_copy number(2)) nologging pctfree 0';
execute immediate iSql;


------------------------------------------------------------------------------------
--iTmpShopSku:='t_fes_shop_sku';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpShopSku);

if iCtr = 0 then
	goto end_program;
else
	iSql := 'select count(*) from maxdata.'||iTmpShopSku;
	execute immediate iSql;

	if iCount = 0 then
		goto end_program;
	end if;
end if;
-----------------------------------------------------------------------------------------
-- Build Old/New ref table
iSql:='insert into maxdata.'||iTmpOldNew||'(merch_id) select distinct merch_id from maxdata.'||iTmpShopSku||' order by merch_id';
execute immediate iSql;
commit;

---
iSql:='Update maxdata.'||iTmpOldNew||' a set (cmast_id,record_type,prod_id, merch_type, no_copy)=(';
if iShopLvl=10 then
        iSql:=iSql||'select lv10mast_id, record_type, order_code,'||iTypeCol||',nvl('||iNoCopyCol||',1`) from maxdata.lv10mast b'||
        	' where a.merch_id=b.lv10mast_id)'||
        	' where exists (select 1 from maxdata.lv10mast c where a.merch_id=c.lv10mast_id)';
  else
 	iSql:=iSql||'select c.'||iMerchTab||'_id, c.record_type,b.'||iMerchUserID||','||iTypeCol||',nvl('||iNoCopyCol||',1)'||
 		' from maxdata.'||iMerchTab||' b, maxdata.'||iCtreeTab||' c'||
---- removed the record type restriction
----- ' where a.merch_id=c.'||iCtreeTab||'_id and b.'||iMerchTab||'_id=c.'||iMerchTab||'_id and b.record_type=''L'')'||
 		' where a.merch_id=c.'||iCtreeTab||'_id and b.'||iMerchTab||'_id=c.'||iMerchTab||'_id)'||
		' where exists (select 1 from maxdata.'||iCtreeTab||' d where a.merch_id=d.'||iCtreeTab||'_id)';
   end if;

execute immediate iSql;
commit;

-- Duplicate the shop sku/cmast to new sku table
if iShopLvl=10 then

	iSql:='Insert into maxdata.'||iTmpNewCmast||' select * from maxdata.lv10mast a where exists (select 1 from maxdata.'||iTmpOldNew||' b'||
		' where a.lv10mast_id=b.merch_id)';
	execute immediate iSql;

	iSql:='Insert into maxdata.'||iTmpNewCtree||' select * from maxdata.'||iCtreeTab||' a where exists (select 1 from maxdata.'||iTmpOldNew||' b'||
		' where a.lv10mast_id=b.merch_id)';
	execute immediate iSql;

	iSql:='Insert into maxdata.'||iTmpNewpMerch||' select * from maxdata.lv10merch a where exists (select 1 from maxdata.'||iTmpNewCmast||' b'||
		' where a.lv10mast_id=b.lv10mast_id)';
	execute immediate iSql;
	commit;

--- add seq to merch keys

	iSql:='alter table maxdata.'||iTmpShopSku||' add (seq_key number(2))';
	execute immediate iSql;

	iSql:='update maxdata.'||iTmpShopSku||' a set seq_key=(select no_copy from maxdata.'||iTmpOldNew||' b'||
		' where a.merch_id=b.merch_id)';
	execute immediate iSql;
	commit;

	iSql:='alter table maxdata.'||iTmpNewCmast||' add (seq_key number(2))';
	execute immediate iSql;

	iSql:='update maxdata.'||iTmpNewCmast||' a set last_update=sysdate, seq_key=(select no_copy from maxdata.'||iTmpOldNew||' b'||
		' where a.lv10mast_id=b.merch_id)';
	execute immediate iSql;
	commit;

	iSql:='alter table maxdata.'||iTmpNewCtree||' add (seq_key number(2))';
	execute immediate iSql;

	iSql:='update maxdata.'||iTmpNewCtree||' a set last_update=sysdate, seq_key=(select no_copy from maxdata.'||iTmpOldNew||' b'||
		' where a.lv10mast_id=b.merch_id)';
	execute immediate iSql;
	commit;

	iSql:='alter table maxdata.'||iTmpNewpMerch||' add (seq_key number(2))';
	execute immediate iSql;

	iSql:='update maxdata.'||iTmpNewpMerch||' a set seq_key=(select no_copy from maxdata.'||iTmpNewCmast||' b'||
		' where a.lv10mast_id=b.lv10mast_id)';
	execute immediate iSql;
	commit;

else

	iSql:='Insert into maxdata.'||iTmpNewCtree||' select * from maxdata.'||iCtreeTab||' a where exists (select 1 from maxdata.'||iTmpOldNew||' b'||
		' where a.'||iCtreeTab||'_id=b.merch_id)';
        execute immediate iSql;

	iSql:='Insert into maxdata.'||iTmpNewCmast||' select * from maxdata.'||iMerchTab||' a where exists (select 1 from maxdata.'||iTmpNewCtree||' b'||
		' where a.'||iMerchTab||'_id=b.'||iMerchTab||'_id)';
        execute immediate iSql;
	commit;

--- add seq to merch keys


	iSql:='alter table maxdata.'||iTmpShopSku||' add (seq_key number(2))';
	execute immediate iSql;

	iSql:='update maxdata.'||iTmpShopSku||' a set seq_key=(select no_copy from maxdata.'||iTmpOldNew||' b'||
		' where a.merch_id=b.merch_id)';
	execute immediate iSql;
	commit;

	iSql:='alter table maxdata.'||iTmpNewCtree||' add (seq_key number(2))';
	execute immediate iSql;

	iSql:='update maxdata.'||iTmpNewCtree||' a set last_update=sysdate, seq_key=(select no_copy from maxdata.'||iTmpOldNew||' b'||
	    ' where a.'||iCtreeTab||'_id=b.merch_id)';
	execute immediate iSql;

	iSql:='alter table maxdata.'||iTmpNewCmast||' add (seq_key number(2))';
	execute immediate iSql;

	iSql:='update maxdata.'||iTmpNewCmast||' a set last_update=sysdate, seq_key=(select no_copy from maxdata.'||iTmpOldNew||' b'||
		' where a.'||iMerchTab||'_id=b.cmast_id)';
	execute immediate iSql;
	commit;

end if;


if iCtr > 0 then
	maxdata.p_fes_dup_new_sku;
end if;

iSql:='select count(1) from maxdata.'||iTmpNewCmast;
execute immediate iSql into iCount;

if iCount = 0 then
	goto end_program;
end if;

--- Clear out the sku shop type
iSql:='Update maxdata.'||iTmpNewCmast||' set '||reset_shop;
execute immediate iSql;

if iShopLvl=10 then

   --- Get the sequence number (lv10mast)
	iSql:='select count(1) from maxdata.'||iTmpNewCmast;
	execute immediate iSql into iCount;

	update maxapp.sequence set seq_num=(select max(lv10mast_id) from maxdata.lv10mast) where entity_type=1 and level_type=10;
	commit;

	select seq_num into iSeqNo from maxapp.sequence where entity_type=1 and level_type=10 for update;

	update maxapp.sequence set seq_num=seq_num + iCount where entity_type=1 and level_type=10;
	commit;

	iSql:='Update maxdata.'||iTmpOldNew||' set new_merch_id=rownum'||'+'||iSeqNo;
	execute immediate iSql;

--- use no_copy as key sequence
	iSql:='Update maxdata.'||iTmpNewCmast||' a set (lv10mast_id,'||iCopyFrom||')=('||
		' select new_merch_id, prod_id from maxdata.'||iTmpOldNew||' b where a.lv10mast_id=b.merch_id and a.seq_key=b.no_copy)';
	execute immediate iSql;
	commit;

	--- Get the sequence number (merch)

	iSql:='select count(1) from maxdata.'||iTmpNewpMerch;
	execute immediate iSql into iCount;

	update maxapp.sequence set seq_num=(select max(lv10merch_id) from maxdata.lv10merch) where entity_type=13 and level_type=10;
	commit;

	select seq_num into iSeqNo from maxapp.sequence where entity_type=13 and level_type=10 for update;

	update maxapp.sequence set seq_num=seq_num + iCount where entity_type=13 and level_type=10;
	commit;

	iSql:='Update maxdata.'||iTmpOldNew||' set new_cmast_id=rownum'||'+'||iSeqNo;
	execute immediate iSql;

	iSql:='Update maxdata.'||iTmpShopSku||' a set (new_merch_id)=('||
		' select new_merch_id from maxdata.'||iTmpOldNew||' b where a.merch_id=b.merch_id and a.seq_key=b.no_copy)';
	execute immediate iSql;
	commit;

	iSql:='Update maxdata.'||iTmpNewpMerch||' a set (lv10merch_id, lv10mast_id)=('||
		' select new_cmast_id, new_merch_id from maxdata.'||iTmpOldNew||' b where a.lv10mast_id=b.merch_id and a.seq_key=b.no_copy)';
	execute immediate iSql;
	commit;

    	iSql:='Update maxdata.'||iTmpNewCmast||' set '||iMerchTab||'_userid='''||iNewProd||'-''||'||iMerchTab||'_id ,name='''||iNewProd||'-''||name, record_type=''M'''||iAttribCol_update_3;
    	execute immediate iSql;

 	iSql:='alter table maxdata.'||iTmpNewCmast||' drop column seq_key';
    	execute immediate iSql;

	iSql:='alter table maxdata.'||iTmpNewpMerch||' drop column seq_key';
    	execute immediate iSql;

	iSql:='insert into maxdata.lv10mast select * from maxdata.'||iTmpNewCmast;
	execute immediate iSql;

	iSql:='insert into maxdata.lv10merch select * from maxdata.'||iTmpNewpMerch;
	execute immediate iSql;

	commit;

   else
	--- Get the sequence number (ctree)
	iSql:='select count(1) from maxdata.'||iTmpNewCmast;
	execute immediate iSql into iCount;

	iSql:='update maxapp.sequence set seq_num=(select max('||iMerchTab||'_id) from maxdata.'||iMerchTab||')'||
		' where  entity_type=9 and level_type='||iShopLvl;
    	execute immediate iSql;
	commit;

	iSql:='select seq_num from maxapp.sequence where entity_type=9 and level_type='||iShopLvl||' for update';
	execute immediate iSql into iSeqNo;

	iSql:='update maxapp.sequence set seq_num=seq_num + '||iCount||' where entity_type=9 and level_type='||iShopLvl;
	execute immediate iSql;
	commit;

	iSql:='Update maxdata.'||iTmpOldNew||' set new_cmast_id=rownum'||'+'||iSeqNo;
	execute immediate iSql;

	iSql:='Update maxdata.'||iTmpNewCmast||' a set ('||iMerchTab||'_id,'||iCopyFrom||')=('||
		' select new_cmast_id, prod_id from maxdata.'||iTmpOldNew||' b where a.'||iMerchTab||'_id=b.cmast_id and a.seq_key=b.no_copy)';

	execute immediate iSql;
	commit;

	iSql:='select max('||iCtreeTab||'_id) from maxdata.'||iCtreeTab;
	execute immediate iSql into iSeqNo;

	iSql:='drop sequence '||iCtreeTab||'_seq';
	execute immediate iSql;

	isql:='create sequence maxdata.'||iCtreeTab||'_seq increment by 1'||
  		' start with '||(iSeqNo+iCount)||
  		' maxvalue 999999999999999999999999999'||
  		' minvalue 1000 cache 20 nocycle noorder';
	execute immediate iSql;

	iSql:='Update maxdata.'||iTmpOldNew||' set new_merch_id=rownum'||'+'||iSeqNo;
	execute immediate iSql;

	iSql:='Update maxdata.'||iTmpNewCtree||' a set record_type=''M'',  ('||iMerchTab||'_id,'||iCtreeTab||'_id) =('||
		' select new_cmast_id, new_merch_id from maxdata.'||iTmpOldNew||' b where a.'||iCtreeTab||'_id=b.merch_id and a.seq_key=b.no_copy)';
	execute immediate iSql;
	commit;

	iSql:='Update maxdata.'||iTmpNewCmast||' set '||iMerchTab||'_userid='''||iNewProd||'-''||'||iMerchTab||'_id ,name='''||iSName||'-''||name, record_type=''M'''||iAttribCol_update_3;
	execute immediate iSql;

	iSql:='Update maxdata.'||iTmpShopSku||' a set (new_merch_id)=('||
		' select new_merch_id from maxdata.'||iTmpOldNew||' b where a.merch_id=b.merch_id and a.seq_key=b.no_copy)';
	execute immediate iSql;

	commit;

--- Create model skus
	select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpTab);

	if iCtr = 1 then
		iSql:='Drop table maxdata.'||iTmpTab;
		execute immediate iSql;
	end if;

	iSql := 'create table maxdata.'||iTmpTab||' nologging pctfree 0 as select new_merch_id,new_cmast_id from maxdata.'||iTmpOldNew;
	execute immediate iSql;

	iSql:='select count(*) from maxdata.'||iTmpOldNew;
	execute immediate iSql into iCount;

	--- set lv10cat id sequence
	select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewCat);

	if iCtr = 1 then
		iSql:='Drop table maxdata.'||iTmpNewCat;
		execute immediate iSql;
	end if;

	iSql := 'create table maxdata.'||iTmpNewCat||' nologging pctfree 0 as select * from maxdata.lv10cat where 1=2';
	execute immediate iSql;

	update maxapp.sequence set seq_num=(select max(lv10cat_id) from maxdata.lv10cat) where entity_type=3 and level_type=10;
	commit;

 	select seq_num into iCatID from maxapp.sequence where entity_type=3 and level_type=10 for update;

	update maxapp.sequence set seq_num=seq_num + iCount where entity_type=3 and level_type=10;
	commit;

 ----   set lv10mast sequence
   	select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewModel);

   	if iCtr = 1 then
   		iSql:='Drop table maxdata.'||iTmpNewModel;
   		execute immediate iSql;
   	end if;

   	iSql := 'create table maxdata.'||iTmpNewModel||' nologging pctfree 0 as select * from maxdata.lv10mast where 1=2';
   	execute immediate iSql;

   	update maxapp.sequence set seq_num=(select max(lv10mast_id) from maxdata.lv10mast) where entity_type=1 and level_type=10;
   	commit;

   	select seq_num into iSkuID from maxapp.sequence where entity_type=1 and level_type=10 for update;

   	update maxapp.sequence set seq_num=seq_num + iCount where entity_type=1 and level_type=10;
 	commit;

 --- loop through each new cmast
 	iLoop:=0;
 	iSql:='select count(*)  from maxdata.'||iTmpTab;
    	execute immediate iSql  into iCtr;

 	while iCtr > 0 loop

        iSql:='select new_merch_id,new_cmast_id from maxdata.'||iTmpTab||' where rownum=1';
        execute immediate iSql  into  iCtreeID, iCmastID;

 	 	iLoop:=iLoop+1;

 	 --- insert lv10cat
		iParentList:='lv1cmast_id';
		for iLoop in 2..iShopLvl loop
			iParentList:=iParentList||',lv'||iLoop||'cmast_id';
		end loop;

		iSql:='Insert into MAXDATA.'||iTmpNewCat||'(LV10CAT_ID, LAST_UPDATE, CHANGED_BY_BATCH,'||iParentList||',RECORD_TYPE)'||
			' select '||(iCatID+iLoop)||', sysdate, 0,'||iParentList||',''M'''||
			' from maxdata.'||iTmpNewCtree||' where '||iMerchTab||'_id='||iCmastID;
		execute immediate iSql;
		commit;

   	--- insert lv10mast


		iParentList:='lv1cmast_id';
		for iLoop in 2..iShopLvl loop
			iParentList:=iParentList||',lv'||iLoop||'ctree_id';
		end loop;

		iSql:='insert into maxdata.'||iTmpNewModel||'(lv10mast_id, last_update, changed_by_batch, lv10cat_id, order_code, merch_lkup, name,'||
   			    'phys_type, prim_type, active_lkup, height, width, depth, weight,'||
   			    'min_pack, units_per_case, items_per_unit, item_cost, current_item_price, case_cost, case_price,convert_to_normal,'||
			    'interval_left1, interval_left2, interval_left3, interval_top1, interval_top2, interval_top3,'||
			    'interval_1used, interval_2used, interval_3used, record_type,'||iParentList||',reclass_status)'||
			    ' select '||(iSkuID+iLoop)||', sysdate, 0,'||(iCatID+iLoop)||',''SKU-'||(iSkuID+iLoop)||''', 1, ''[SKU]-'||(iSkuID+iLoop)||''','||
			    ' 0, 0, 1, 0.02, 0.02, 0.02, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0,'||
   			    ' 0, 0, 0, 0, 0, ''L'','||iParentList||', ''N'''||
			    ' from maxdata.'||iTmpNewCtree||' where '||iCtreeTab||'_id='||iCtreeID;

		execute immediate iSql;
		commit;

        iSql:='delete from  maxdata.'||iTmpTab||' where new_merch_id='||iCtreeID||' and new_cmast_id='||iCmastID;
        execute immediate iSql;
        commit;

        iSql:='select count(*) from maxdata.'||iTmpTab;
        execute immediate iSql  into iCtr;

	end loop;

	--- remove seq_key

	iSql:='alter table maxdata.'||iTmpNewCmast||' drop column seq_key';
    	execute immediate iSql;

	iSql:='alter table maxdata.'||iTmpNewCtree||' drop column seq_key';
    	execute immediate iSql;

	iSql:='insert into maxdata.'||iMerchTab||' select * from maxdata.'||iTmpNewCmast;
	execute immediate iSql;

	iSql:='insert into maxdata.'||iCtreeTab||' select * from maxdata.'||iTmpNewCtree;
	execute immediate iSql;

	iSql:='insert into maxdata.lv10cat select * from maxdata.'||iTmpNewCat;
	execute immediate iSql;

	iSql:='insert into maxdata.lv10mast select * from maxdata.'||iTmpNewModel;
	execute immediate iSql;

	commit;

end if;
-----------------------------------------------------------------------------

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpTab);
if iCtr = 1 then
	iSql := 'Drop table maxdata.'||iTmpTab;
	execute immediate iSql;
end if;



----- Remove temp tables
<<end_program>>

--iTmpNewCmast:='t_fes_new_sku';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewCmast);

if iCtr = 1 then
	iSql:='Drop table maxdata.'||iTmpNewCmast;
	execute immediate iSql;
end if;

--iTmpNewMerch:='t_fes_new_merch';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewpMerch);

if iCtr = 1 then
	iSql:='Drop table maxdata.'||iTmpNewpMerch;
	execute immediate iSql;
end if;

--iTmpNewCtree:='t_fes_new_ctree';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewCtree);

if iCtr = 1 then
	iSql:='Drop table maxdata.'||iTmpNewCtree;
	execute immediate iSql;
end if;


--iTmpNewcat:='t_fes_new_cat';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewcat);

if iCtr = 1 then
	iSql:='drop table maxdata.'||iTmpNewcat;
	execute immediate iSql;
end if;


--iTmpNewModel:='t_fes_new_model';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iTmpNewModel);

if iCtr = 1 then
	iSql:='drop table maxdata.'||iTmpNewModel;
	execute immediate iSql;
end if;

end;

/
