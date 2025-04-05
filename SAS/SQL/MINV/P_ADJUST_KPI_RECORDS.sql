--------------------------------------------------------
--  DDL for Procedure P_ADJUST_KPI_RECORDS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_ADJUST_KPI_RECORDS" (iWkshtID int)
as
icalc_str varchar2(1000);
iselect_str  varchar2(1000);
iusing_str  varchar2(1000);
icalc_func  varchar2(30);
itab  		varchar2(5);

iCtr 		int;
iCount         int;
iMinPeriod	int;
iPrevSeq	int;
iCurSeq		int;
iLoaded		int;
iPlanType	varchar(5);


iSessionId	int;
iTimeLevel	number(10);
iTimeID		number(10);
iFirstTimeID	number(10);
iPrevTimeID	number(10);
iLocLevel	number(10);
iMerchLevel	number(10);
iString		varchar2(2000);
iSql		long;

--V_62174		number(20,3);
--V_62115		number(20,3);

begin

/* do mplan working records exist */
select count(1) into iCtr from maxdata.mplan_working where workplan_id=iWkshtID and rownum=1;

if iCtr = 0 then
	return;
end if;

Select userenv('sessionid') into iSessionId from dual;

iCtr:=1;

select decode(plantype_id,3,'AP','FP') into iPlanType from maxdata.planworksheet where planworksheet_id=iWkshtID;
	
select max(time_level), max(location_level), max(merch_level) into iTimeLevel, iLocLevel, iMerchLevel 
	from maxdata.mplan_working where workplan_id=iWkshtID;
   

iSql:='select count(1) from sys.all_tables where owner=''MAXDATA'' and table_name =upper(''t_mplan_data_'||iSessionId||''')';
execute immediate iSql into iCtr;
			
if iCtr > 0 then
	iSql:='drop table  maxdata.t_mplan_data_'||iSessionId;
	execute immediate iSql;

end if;
  	
iSql:='create table maxdata.t_mplan_data_'||iSessionId||
    	' as select a.*, cast(0 as number(20,3)) gm_ty, cast(0 as number(20,3)) gm_ly, cast(0 as number(20,3)) Inv_EOP_retail_ty, cast(0 as number(20,3)) Inv_receipts_retail_ty,'||
    	' cast(0 as number(20,3)) Inv_receipts_retail_ly, cast(0 as number(20,3)) order_total, cast(0 as number(20,3)) PREV_62428,'||
    	' cast(0 as number(20,3)) PREV_62426'||
    	' from maxdata.mplan_working a where workplan_id='||iWkshtID||
    	' and time_level='||iTimeLevel||' and location_level='||iLocLevel||' and merch_level='||iMerchLevel;
--dbms_output.put_line(iSql);
execute immediate iSql;

iSql:='create unique index ui_t_m_data_'||iSessionId||' on maxdata.t_mplan_data_'||iSessionId||'(time_level,time_id,location_level,location_id,merch_level,merch_id)';
execute immediate iSql;

---- Get TY hist data

iSql:='select count(1) from sys.all_tables where owner=''MAXDATA'' and table_name =upper(''t_hist_'||iSessionId||''')';
execute immediate iSql into iCtr;
			
			
if iCtr > 0 then
	iSql:='drop table  maxdata.t_hist_'||iSessionId;
	execute immediate iSql;

end if;

iSql:='create table maxdata.t_hist_'||iSessionId||
	' as select a.time_level time_level, a.time_id time_id, a.location_level location_level,a.location_id location_id,'||
	' a.merch_level merch_level,a.merch_id merch_id,'||
	' (a.Net_Sales_retail+a.Sales_retail_4+a.Sales_retail_5) sales_gm,'||
	' 0 inv_gm, 0 Inv_EOP_retail, 0 Inv_receipts_retail, '||
	' 0 I_OORD_PASS_RETAIL,0 I_OORD_CURR_RETAIL'||
	' from maxdata.mfinc a, maxdata.t_mplan_data_'||iSessionId||' b'||
	' where a.time_level=b.time_level and a.time_id=b.time_id and a.location_level=a.location_level and a.location_id=b.location_id'||
	' and a.merch_level=b.merch_level and a.merch_id=b.merch_id';
execute immediate iSql;

iSql:='insert into maxdata.t_hist_'||iSessionId||
	' select a.time_level time_level, a.time_id time_id, a.location_level location_level,a.location_id location_id,'||
	' a.merch_level merch_level,a.merch_id merch_id, 0 sales_gm,'||
	' (a.Inv_BOP_Cost+a.Inv_EOP_Cost-a.Inv_receipts_Cost-a.inv_cost_4+a.inv_cost_5+a.Inv_cost_2) inv_gm,'||
	' a.Inv_EOP_retail Inv_EOP_retail, a.Inv_receipts_retail Inv_receipts_retail, 0 I_OORD_PASS_RETAIL,0 I_OORD_CURR_RETAIL'||
	' from maxdata.minventory a, maxdata.t_mplan_data_'||iSessionId||' b'||
	' where a.time_level=b.time_level and a.time_id=b.time_id and a.location_level=a.location_level and a.location_id=b.location_id'||
	' and a.merch_level=b.merch_level and a.merch_id=b.merch_id';
execute immediate iSql;

iSql:='insert into maxdata.t_hist_'||iSessionId||
	' select a.time_level time_level, a.time_id time_id, a.location_level location_level,a.location_id location_id,'||
	' a.merch_level merch_level,a.merch_id merch_id,'||
	' 0 sales_gm, 0 inv_gm, 0 Inv_EOP_retail, 0 Inv_receipts_retail,'||
	' a.I_OORD_PASS_RETAIL,a.I_OORD_CURR_RETAIL from maxdata.monorder a, maxdata.t_mplan_data_'||iSessionId||' b'||
	' where a.time_level=b.time_level and a.time_id=b.time_id and a.location_level=a.location_level and a.location_id=b.location_id'||
	' and a.merch_level=b.merch_level and a.merch_id=b.merch_id';
execute immediate iSql;


iSql:='select count(1) from sys.all_tables where owner=''MAXDATA'' and table_name =upper(''t_hist_ty_'||iSessionId||''')';
execute immediate iSql into iCtr;
			
if iCtr > 0 then
	iSql:='drop table  maxdata.t_hist_ty_'||iSessionId;
	execute immediate iSql;

end if;

iSql:='create table maxdata.t_hist_ty_'||iSessionId||
	' as select a.time_level time_level, a.time_id time_id, a.location_level location_level,a.location_id location_id,'||
	' a.merch_level merch_level,a.merch_id merch_id,'||
	' sum(sales_gm) sales_gm_ty,sum(inv_gm) inv_gm_ty,sum(Inv_EOP_retail) Inv_EOP_retail, sum(Inv_receipts_retail) Inv_receipts_retail,'||
	' sum(I_OORD_PASS_RETAIL)+sum(I_OORD_CURR_RETAIL) order_total'||
	' from maxdata.t_hist_'||iSessionId||' a'||
	' group by a.time_level, a.time_id, a.location_level, a.location_id,a.merch_level, a.merch_id';
execute immediate iSql;

iSql:='update maxdata.t_mplan_data_'||iSessionId||' a set (gm_ty,Inv_EOP_retail_ty, Inv_receipts_retail_ty, order_total) = (select gm_ty,Inv_EOP_retail,Inv_receipts_retail,order_total '||
	' from  maxdata.t_hist_ty_'||iSessionId||' b where a.time_level=b.time_level and a.time_id=b.time_id'||
	' and a.location_level=b.location_level and a.location_id=b.location_id and a.merch_level=b.merch_level and a.merch_id=b.merch_id)';
execute immediate iSql;
commit;


---- Get LY hist data

iSql:='select count(1) from sys.all_tables where owner=''MAXDATA'' and table_name =upper(''t_p_ly'||iSessionId||''')';
execute immediate iSql into iCtr;
			
			
if iCtr > 0 then
	iSql:='drop table  maxdata.t_p_ly'||iSessionId;
	execute immediate iSql;
end if;

iSql:='create table maxdata.t_p_ly'||iSessionId||
    ' as select distinct time_level, (time_id-100) time_id, location_level, location_id, merch_level,merch_id from maxdata.t_mplan_data_'||iSessionId;
execute immediate iSql;

iSql:='create unique index u_t_p_ly'||iSessionId||' on maxdata.t_p_ly'||iSessionId||'(time_level,time_id,location_level,location_id,merch_level,merch_id)';
execute immediate iSql;

-----------------
iSql:='truncate table maxdata.t_hist_'||iSessionId;
execute immediate iSql;

iSql:='insert into maxdata.t_hist_'||iSessionId||
	' select a.time_level time_level, (a.time_id+100) time_id, a.location_level location_level,a.location_id location_id,'||
	' a.merch_level merch_level,a.merch_id merch_id,'||
	' (a.Net_Sales_retail+a.Sales_retail_4+a.Sales_retail_5) sales_gm,'||
	' 0 inv_gm, 0 Inv_eop_retail,0 Inv_receipts_retail, '||
	' 0 I_OORD_PASS_RETAIL,0 I_OORD_CURR_RETAIL'||
	' from maxdata.mfinc a, maxdata.t_p_ly'||iSessionId||' b'||
	' where a.time_level=b.time_level and a.time_id=b.time_id and a.location_level=a.location_level and a.location_id=b.location_id'||
	' and a.merch_level=b.merch_level and a.merch_id=b.merch_id';
execute immediate iSql;

iSql:='insert into maxdata.t_hist_'||iSessionId||
	' select a.time_level time_level, (a.time_id+100) time_id, a.location_level location_level,a.location_id location_id,'||
	' a.merch_level merch_level,a.merch_id merch_id,0 sales_gm,'||
	' (a.Inv_BOP_Cost+a.Inv_EOP_Cost-a.Inv_receipts_Cost-a.inv_cost_4+a.inv_cost_5+a.Inv_cost_2) inv_gm,'||
	'  a.Inv_EOP_retail Inv_EOP_retail, a.Inv_receipts_retail Inv_receipts_retail, 0 I_OORD_PASS_RETAIL,0 I_OORD_CURR_RETAIL'||
	' from maxdata.minventory a, maxdata.t_p_ly'||iSessionId||' b'||
	' where a.time_level=b.time_level and a.time_id=b.time_id and a.location_level=a.location_level and a.location_id=b.location_id'||
	' and a.merch_level=b.merch_level and a.merch_id=b.merch_id';
execute immediate iSql;


iSql:='select count(1) from sys.all_tables where owner=''MAXDATA'' and table_name =upper(''t_hist_ly_'||iSessionId||''')';
execute immediate iSql into iCtr;
			
if iCtr > 0 then
	iSql:='drop table  maxdata.t_hist_ly_'||iSessionId;
	execute immediate iSql;

end if;

iSql:='create table maxdata.t_hist_ly_'||iSessionId||
	' as select a.time_level time_level, a.time_id time_id, a.location_level location_level,a.location_id location_id,'||
	' a.merch_level merch_level,a.merch_id merch_id,'||
	' sum(sales_gm) sales_gm_ty,sum(inv_gm) inv_gm_ty,sum(Inv_receipts_retail) Inv_receipts_retail,'||
	' 0 order_total'||
	' from maxdata.t_hist_'||iSessionId||' a'||
	' group by time_level,time_id,location_level,location_id,merch_level,merch_id';
execute immediate iSql;

iSql:='update maxdata.t_mplan_data_'||iSessionId||' a set (gm_ly,Inv_receipts_retail_ly,order_total)= (select gm_ly,Inv_receipts_retail,0 '||
	' from maxdata.t_hist_ly_'||iSessionId||' b where a.time_level=b.time_level and a.time_id=b.time_id'||
	' and a.location_level=b.location_level and a.location_id=b.location_id and a.merch_level=b.merch_level and a.merch_id=b.merch_id)';

execute immediate iSql;
commit;

------------------------

iSql:='select count(1) from sys.all_tables where owner=''MAXDATA'' and table_name =upper(''t_p_tm'||iSessionId||''')';
execute immediate iSql into iCtr;
			
			
if iCtr > 0 then
	iSql:='drop table  maxdata.t_p_tm'||iSessionId;
	execute immediate iSql;

end if;

iSql:='create table maxdata.t_p_tm'||iSessionId||
    ' as select distinct time_level, time_id, cast(0 as number(1)) loaded, cast(0 as number(2)) Seq from maxdata.t_mplan_data_'||iSessionId;
execute immediate iSql;

iSql:='update maxdata.t_p_tm'||iSessionId||' a set (loaded, Seq) =(select loaded_flag, lv'||(iTimeLevel-46)||'time_id'||
	' from maxapp.lv'||(iTimeLevel-46)||'time b where b.lv'||(iTimeLevel-46)||'time_lkup_id=a.time_id)';
execute immediate iSql;
commit;

iSql:='select min(seq), min(time_id) from maxdata.t_p_tm'||iSessionId;
execute immediate iSql into iMinPeriod, iFirstTimeID;

iPrevSeq:=0;
iCurSeq:=iMinPeriod;
iPrevTimeID:=iFirstTimeID;
iCount:=1;
while iCount > 0 loop

    	iSql:='select time_level,time_id,loaded from  maxdata.t_p_tm'||iSessionId||' where seq='||iCurSeq;
       	execute immediate iSql into iTimeLevel, iTimeId, iLoaded;
       	
  	if iTimeId = iFirstTimeID then
  	
  		iSql:='update t_mplan_data_'||iSessionId||' set PREV_62428=0, PREV_62426=0'||
  			'where time_id='||iPrevTimeID;
  		execute immediate iSql;
  		commit;
  	else
  		iSql:='update maxdata.t_mplan_data_'||iSessionId||' a set (PREV_62428, PREV_62426)=('||
  			'select PREV_62428, PREV_62426 from t_mplan_data_'||iSessionId||' b'||
  			' where b.time_id='||iPrevTimeID||' and a.time_level=b.time_level'||
  			' and a.location_level=b.location_level and a.location_id=b.location_id'||
  			' and a.merch_level=b.merch_level and a.merch_id=b.merch_id)'||
  			' where a.time_id='||iTimeId;
  		execute immediate iSql;
  		commit;  	
  		
  	end if;

 ----------------------------------------------------
 --- calc depend KPI
 --------------------------------------------------
 -- V_62422=PREV_62426
 -- V_62424=PREV_62428
 -- V_62428
 -- V_62426
 --62706	RTV C
 --WP_NUM_113=WP_NUM_054*(V-62428/V-62426)
	iSql:='update maxdata.t_mplan_data_'||iSessionId||' set '||
		'WP_NUM_113=nvl(WP_NUM_054,0)*('||
			   '(nvl(WP_NUM_015,0)+nvl(WP_NUM_036,0))'||
			   '/decode(nvl(WP_NUM_016,0)+nvl(WP_NUM_037,0),0,1,nvl(WP_NUM_016,0)+nvl(WP_NUM_037,0)))'||
		' where time_id='||iTimeId||' and nvl(WP_NUM_070,0)>0';
	 execute immediate iSql;

	iSql:='update maxdata.t_mplan_data_'||iSessionId||' set '||
		'WP_NUM_113=nvl(WP_NUM_054,0)* (PREV_62428+nvl(WP_NUM_036,0)'||
			   '/decode( (PREV_62426+nvl(WP_NUM_037,0)),0,1,(PREV_62426+nvl(WP_NUM_037,0))))'||
		' where time_id='||iTimeId||' and nvl(WP_NUM_070,0)<=0';
      --  dbms_output.put_line(iSql);
	 execute immediate iSql;

--62248	Shrink C
--WP_NUM_028=WP_NUM_060*(V-62428/V-62426)

	iSql:='update maxdata.t_mplan_data_'||iSessionId||' set '||
		'WP_NUM_028=nvl(WP_NUM_060,0)*('||
			   '(nvl(WP_NUM_015,0)+nvl(WP_NUM_036,0))'||
			   '/decode(nvl(WP_NUM_016,0)+nvl(WP_NUM_037,0),0,1,nvl(WP_NUM_016,0)+nvl(WP_NUM_037,0)))'||
		' where time_id='||iTimeId||' and nvl(WP_NUM_070,0)>0';
	 execute immediate iSql;

	iSql:='update maxdata.t_mplan_data_'||iSessionId||' set '||
		'WP_NUM_028=nvl(WP_NUM_060,0)*(PREV_62428+nvl(WP_NUM_036,0)'||
			   '/decode((PREV_62426+nvl(WP_NUM_037,0)),0,1,(PREV_62426+nvl(WP_NUM_037,0))))'||
		' where time_id='||iTimeId||' and nvl(WP_NUM_070,0)<=0';
	 execute immediate iSql;
	 commit;
	 
-----------------------------------------
--- Main update
-----------------------------------------

 	--- FP and AP Worksheets
	----------------------------------------------
	--62060	EOP U
	iString:=
	'WP_NUM_062=nvl(WP_NUM_017,0)+nvl(WP_NUM_038,0)-nvl(WP_NUM_049,0)-nvl(WP_NUM_053,0)+nvl(WP_NUM_008,0)'||
		  '-nvl(WP_NUM_114,0)+nvl(WP_NUM_105,0)-nvl(WP_NUM_108,0)';
	
	--62094	EOP AUR
	iString:=iString||','||
	'WP_NUM_001=nvl(WP_NUM_023,0)/decode(nvl(WP_NUM_062,0),0,1,nvl(WP_NUM_062,0))';
	
	--62346	SUM EOP U (Shadow)
	iString:=iString||','||
	'WP_NUM_010=nvl(WP_NUM_062,0)*(nvl(WP_NUM_013,0)/7)';
	
	--62354	Tot Str Cnt Gross
	iString:=iString||','||
	'WP_NUM_019=nvl(WP_NUM_061,0)*(nvl(WP_NUM_013,0)/7)+(nvl(WP_NUM_011,0)-nvl(WP_NUM_011,0))'||
			'+(nvl(WP_NUM_009,0)-nvl(WP_NUM_009,0))+(nvl(WP_NUM_010,0)-nvl(WP_NUM_010,0))+(nvl(WP_NUM_067,0)-nvl(WP_NUM_067,0))';
			
	--62047	EOP C
	iString:=iString||','||
	'WP_NUM_022=nvl(WP_NUM_015,0)+nvl(WP_NUM_036,0)-nvl(WP_NUM_047,0)-nvl(WP_NUM_071,0)-nvl(WP_NUM_028,0)'||
			'+nvl(WP_NUM_006,0)-nvl(WP_NUM_113,0)+nvl(WP_NUM_104,0)-nvl(WP_NUM_107,0)';
	
	-- 62340	SUM EOP C (Shadow)
	iString:=iString||','||
	'WP_NUM_009=nvl(WP_NUM_022,0)*(nvl(WP_NUM_013,0)/7)';
	
	--62484	GM $ TY
	--WP_NUM_078="dvval(12,Net_Sales_retail)-(dvval(12,Inv_BOP_Cost)-dvval(12,Inv_EOP_Cost)+dvval(12,Inv_receipts_Cost)
	--            +dvval(12,inv_cost_4)-dvval(12,inv_cost_5)-dvval(12,Inv_cost_2))+dvval(12,Sales_retail_4)+dvval(12,Sales_retail_5)
	--		+(nvl(WP_NUM_048,0)-nvl(WP_NUM_048)"
	iString:=iString||','||
	'WP_NUM_078=gm_ty';
	
	--62485	GM $ LY
	--WP_NUM_079="dvval(11,Net_Sales_retail)-(dvval(11,Inv_BOP_Cost)-dvval(11,Inv_EOP_Cost)+dvval(11,Inv_receipts_Cost)+dvval(11,inv_cost_4)
	--	      -dvval(11,inv_cost_5)-dvval(11,Inv_cost_2))+dvval(11,Sales_retail_4)+dvval(11,Sales_retail_5)
	--		+(nvl(WP_NUM_048,0)-nvl(WP_NUM_048)"
	iString:=iString||','||
	'WP_NUM_079=gm_ly';
	
	-------------------------------
	--V-62174		
	--V-62115
	-- 62172	Proj EOP $
	/*if iLoaded = 1 then
		iString:=iString||','||
		'WP_NUM_090=nvl(Inv_EOP_retail_ty,0)';
	else*/
		iString:=iString||','||
		'WP_NUM_090=nvl(WP_NUM_066,0)+'||
	--V_62174
		'Inv_receipts_retail_ty+order_total'||
	        '+nvl(WP_NUM_034,0)-nvl(WP_NUM_020,0)+nvl(WP_NUM_030,0)+nvl(WP_NUM_035,0)'||
		'-nvl(WP_NUM_003,0)+nvl(WP_NUM_074,0)-nvl(WP_NUM_054,0)+nvl(WP_NUM_103,0)'||
		'-nvl(WP_NUM_106,0)+(nvl(WP_NUM_016,0)-nvl(WP_NUM_016,0))'||
			'-nvl(WP_NUM_048,0)-'||
	--V_62115
		'nvl(WP_NUM_058,0)+nvl(WP_NUM_029,0)'||
		'-nvl(WP_NUM_060,0)+nvl(WP_NUM_007,0)+nvl(WP_NUM_109,0)';
	--- end if;

	----------------------------------------------
	--For AP, FPworksheets:
	----------------------------------------------
	--- 62050	EOP $

	iString:=iString||','||
	'WP_NUM_023=nvl(WP_NUM_016,0)+nvl(WP_NUM_037,0)-nvl(WP_NUM_048,0)-(nvl(WP_NUM_058,0)+nvl(WP_NUM_029,0))'||
			'-nvl(WP_NUM_060,0)+nvl(WP_NUM_007,0)-nvl(WP_NUM_054,0)+nvl(WP_NUM_103,0)-nvl(WP_NUM_106,0)';
			
	-------------------------------------------------------------------------------------
	--For FP worksheets:
	-------------------------------------------------------------------------------------
	-- 62708	RTV U
	if iPlanType = 'FP' then
		iString:=iString||','||
    'WP_NUM_114=nvl(WP_NUM_054,0)/((decode((nvl(WP_NUM_037,0)+nvl(WP_NUM_016,0)),0,1,(nvl(WP_NUM_037,0)'||
    '+nvl(WP_NUM_016,0))))/(decode((nvl(WP_NUM_038,0)+nvl(WP_NUM_017,0)),0,1,(nvl(WP_NUM_038,0)+nvl(WP_NUM_017,0)))))';
	end if;

	iSql:='update maxdata.t_mplan_data_'||iSessionId||' set '||iString||' where time_id='||iTimeId;
   dbms_output.put_line(iSql);
	execute immediate iSql ;
	commit;

 --- end main update	
	iPrevTimeID:=iTimeId;
    	iCurSeq:=iCurSeq+1;

     	iSql:='select count(*) from  maxdata.t_p_tm'||iSessionId||' where seq='||iCurSeq;
       	execute immediate iSql into iCount;

end loop;


--- update the working plan data
iSql:='delete from maxdata.mplan_working where workplan_id='||iWkshtID||
    	' and time_level='||iTimeLevel||' and location_level='||iLocLevel||' and merch_level='||iMerchLevel;
execute immediate iSql ;

iSql:='alter table maxdata.t_mplan_data_'||iSessionId||' drop column gm_ty';
execute immediate iSql;

iSql:='alter table maxdata.t_mplan_data_'||iSessionId||' drop column gm_ly';
execute immediate iSql;

iSql:='alter table maxdata.t_mplan_data_'||iSessionId||' drop column Inv_receipts_retail_ty';
execute immediate iSql;

iSql:='alter table maxdata.t_mplan_data_'||iSessionId||' drop column Inv_receipts_retail_ly';
execute immediate iSql;

iSql:='alter table maxdata.t_mplan_data_'||iSessionId||' drop column order_total';
execute immediate iSql;

iSql:='alter table maxdata.t_mplan_data_'||iSessionId||' drop column PREV_62428';
execute immediate iSql;

iSql:='alter table maxdata.t_mplan_data_'||iSessionId||' drop column Inv_EOP_retail_ty';
execute immediate iSql;

iSql:='alter table maxdata.t_mplan_data_'||iSessionId||' drop column PREV_62426';
execute immediate iSql;

iSql:='insert into maxdata.mplan_working select * from maxdata.t_mplan_data_'||iSessionId;
execute immediate iSql ;
commit;

update maxdata.planworksheet set invalidate=1 where planworksheet_id=iWkshtID;
commit;

iSql:='drop table maxdata.t_mplan_data_'||iSessionId;
execute immediate iSql;

iSql:='drop table maxdata.t_hist_ty_'||iSessionId;
execute immediate iSql;

iSql:='drop table maxdata.t_hist_ly_'||iSessionId;
execute immediate iSql;

iSql:='drop table maxdata.t_hist_'||iSessionId;
execute immediate iSql;

iSql:='drop table maxdata.t_p_tm'||iSessionId;
 execute immediate iSql;

end;

/
