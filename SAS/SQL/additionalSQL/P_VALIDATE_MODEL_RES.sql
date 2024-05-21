--------------------------------------------------------
--  DDL for Procedure P_VALIDATE_MODEL_RES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_VALIDATE_MODEL_RES" 
--- 9/26/11	modified logic if the live member has plan records, reject the resultion
as
cnt number;
cnt1 number;
cnt2 number;
cnt3 number;
cursor c_xref is select * from maxdata.lvxref_model_res where comment_1='3';

begin
-- must account for all fact tables that are not truncated on each load --
-- minventory, mfinc, custom fact tables 	 	 		   	  	   		--
for c1 in c_xref loop

	select count(1)  into cnt from maxdata.mplan_working where merch_level=c1.merch_level and merch_id=c1.live_item_id and rownum=1;
	-- 9/26/ reject resolution if live member has mplan records --
	if cnt>0 then
		update lvxref_model_res set comment_1='4' where merch_level=c1.merch_level and live_item_id=c1.live_item_id;
	else
		select count(1)  into cnt1 from maxdata.minventory
			where merch_level=c1.merch_level and merch_id=c1.live_item_id and location_level=1 and time_level=47 and rownum=1;
		if cnt1>0 then
			update lvxref_model_res set comment_1='4' where merch_level=c1.merch_level and live_item_id=c1.live_item_id;
		else
			select count(1) into cnt2 from maxdata.mfinc
			 	where merch_level=c1.merch_level and merch_id=c1.live_item_id and location_level=1 and time_level=47 and rownum=1;
			if cnt2>0 then
			   update lvxref_model_res set comment_1='4' where merch_level=c1.merch_level and live_item_id=c1.live_item_id;
			end if;
		end if;
	end if;

end loop;

commit;

end;

/
