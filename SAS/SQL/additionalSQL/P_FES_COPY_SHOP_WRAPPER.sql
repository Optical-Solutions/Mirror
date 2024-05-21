--------------------------------------------------------
--  DDL for Procedure P_FES_COPY_SHOP_WRAPPER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_FES_COPY_SHOP_WRAPPER" (iType integer)
/*------------------------------------------------------------------------------------------------
Date Created: 10/21/2013
Description:
Parameter Description: None
Note: This is process copying the shop items in the planworksheets flagged by the users
Date		Who  Desc
10/21/2013	ec   Created
------------------------------------------------------------------------------------------------*/
as
iShopLvl		number(2);
iShopTM			number(2);
iStop			varchar2(1);
iRtn			number(5);

BEGIN

select upper(nvl(value_1,'N')) into iStop from maxdata.client_config where upper(key_1)='STOP COPY SHOP';

if iStop ='Y' or iType not in (1,2) then
	return;
end if;

select to_number(nvl(value_1,'10')) into iShopLvl from maxdata.client_config where upper(key_1)='SHOP MERCH LEVEL';
select to_number(nvl(value_1,'48')) into iShopTM from maxdata.client_config where upper(key_1)='SHOP BOUNDARY';


for c1 in (select * from maxdata.planworksheet where plantype_id = 3 and whatif=0 and nvl(copy_shop,0) = iType and planwork_stat_id > 0
		and to_merch_level=(iShopLvl+10) and to_time_level >= iShopTM) loop

	MAXDATA.p_fes_copy_shop_wksht(iType, c1.planworksheet_id,iRtn);

end loop;


END;

/

  GRANT EXECUTE ON "MAXDATA"."P_FES_COPY_SHOP_WRAPPER" TO "MADMAX";
