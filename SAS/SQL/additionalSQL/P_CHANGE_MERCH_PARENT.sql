--------------------------------------------------------
--  DDL for Procedure P_CHANGE_MERCH_PARENT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_CHANGE_MERCH_PARENT" (iOldID number, iNewID number, iMerchLevel number)
--- Modify
---  11/03	ec Added logic to remove the entry in the change_queue_merch
---  11/07	ec update model's lv10cat to record_type='L'
as

iSql 		long;
iCtr 		number;

iLowestLvl	number(5);
iCtreeId	number(10);
iOldCmastId	number(10);
iNewCmastId	number(10);
iOldCatId	number(10);
iNewCatId	number(10);
iTmpTab		varchar2(30);

begin

select count(1) into iLowestLvl from path_seg where path_id=11;

if iMerchLevel > iLowestLvl then --- not support sku repleace
	return;
end if;

iSql:='select lv'||iMerchLevel||'cmast_id from maxdata.lv'||iMerchLevel||'ctree where lv'||iMerchLevel||'ctree_id='||iNewID;
execute immediate iSql into iNewCmastId;

iSql:='select lv'||iMerchLevel||'cmast_id from maxdata.lv'||iMerchLevel||'ctree where lv'||iMerchLevel||'ctree_id='||iOldID;
execute immediate iSql into iOldCmastId;


if iMerchLevel < iLowestLvl then

	iSql:='select count(1) from maxdata.lv'||(iMerchLevel+1)||'ctree where lv'||iMerchLevel||'ctree_id='||iOldID;
	execute immediate iSql into iCtr;

	while iCtr > 0 loop

		iSql:='select lv'||(iMerchLevel+1)||'ctree_id from maxdata.lv'||(iMerchLevel+1)||'ctree'||
            ' where lv'||iMerchLevel||'ctree_id='||iOldID||' and rownum=1';
		execute immediate iSql into iCtreeId;

		for iCtr in (iMerchLevel+1)..iLowestLvl loop
			iSql:='Update MAXDATA.LV'||iCtr||'CTREE set LV'||(iMerchLevel)||'CTREE_ID='||iNewID||
				', LV'||(iMerchLevel)||'CMAST_ID='||iNewCmastId||
				' where lv'||(iMerchLevel+1)||'ctree_id='||iCtreeId;
			execute immediate iSql;
		end loop;

		iSql:='Update MAXDATA.LV10CTREE set LV'||(iMerchLevel)||'CTREE_ID='||iNewID||', LV'||(iMerchLevel)||'CMAST_ID='||iNewCmastId||
				' where lv'||(iMerchLevel+1)||'ctree_id='||iCtreeId;
		execute immediate iSql;

		iSql:='Update MAXDATA.LV10MAST set LV'||(iMerchLevel)||'CTREE_ID='||iNewID||
				' where lv'||(iMerchLevel+1)||'ctree_id='||iCtreeId;
		execute immediate iSql;

		iSql:='Update MAXDATA.LV10CAT set LV'||(iMerchLevel)||'CMAST_ID='||iNewCmastId||
			' where lv10cat_id in (select lv10cat_id from maxdata.lv10ctree where lv'||(iMerchLevel+1)||'ctree_id='||iCtreeId||')';
		execute immediate iSql;

		iSql:='select count(1) from maxdata.lv'||(iMerchLevel+1)||'ctree where lv'||iMerchLevel||'ctree_id='||iOldID;
		execute immediate iSql into iCtr;

	end loop;

else  -- at the lowest merch level

	iSql:='select count(1) from maxdata.lv10ctree where lv'||iMerchLevel||'ctree_id='||iOldID||' and rownum=1';
	execute immediate iSql into iOldCatID;

	if iOldCatID > 0 then
		iSql:='select lv10cat_id from maxdata.lv10ctree where lv'||iMerchLevel||'ctree_id='||iOldID||' and rownum=1';
		execute immediate iSql into iOldCatID;
	else --- no live sku
		return;
	end if;

	iSql:='select count(1) from maxdata.lv10ctree where lv'||iMerchLevel||'ctree_id='||iNewID||' and rownum=1';
	execute immediate iSql into iNewCatID;

	if iNewCatID > 0 then
		iSql:='select lv10cat_id from maxdata.lv10ctree where lv'||iMerchLevel||'ctree_id='||iNewID||' and rownum=1';
		execute immediate iSql into iNewCatID;

--- Remove records from change queue after the lv10cat changed
		iTmpTab:='t_sku_changed';

		begin
		    iSql := 'drop table maxdata.'||iTmpTab;
		    execute immediate iSql;

		    exception
		    	WHEN OTHERS THEN
		      	NULL;
		end;

		iSql:='Create table maxdata.'||iTmpTab||' as select lv10mast_id, sysdate change_date from maxdata.lv10mast'||
			' where lv10cat_id='||iOldCatID;
		execute immediate iSql;


		iSql:='Update MAXDATA.LV10MAST set LV10CAT_ID='||iNewCatID||' where lv10cat_id='||iOldCatID;
		execute immediate iSql;

		iSql:='delete from maxdata.change_queue_merch a'||
			' where exists (select 1 from maxdata.'||iTmpTab||' b'||
			' where a.lv10mast_id=b.lv10mast_id and a.last_update>=b.change_date)';
  		execute immediate iSql;

	  	update maxdata.lv10cat set record_type='L' where lv10cat_id=iNewCatID;

		select count(1) into iCtr from maxdata.lv10mast where lv10cat_id=iOldCatID;

		if iCtr = 0 then

			delete from maxdata.lv10cat where lv10cat_id=iOldCatID;

			iSql:='delete from maxdata.lv'||iMerchLevel||'ctree where lv'||iMerchLevel||'ctree_id='||iOldID;
			execute immediate iSql;

			iSql:='delete from maxdata.lv'||iMerchLevel||'cmast where lv'||iMerchLevel||'cmast_id='||iOldCmastId;
			execute immediate iSql;

		end if;

		begin
		    iSql := 'drop table maxdata.'||iTmpTab;
		    execute immediate iSql;

		    exception
		    	WHEN OTHERS THEN
		      	NULL;
		end;


	end if;

end if;

end;

/
