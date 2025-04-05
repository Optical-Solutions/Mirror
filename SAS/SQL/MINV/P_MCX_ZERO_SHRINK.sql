--------------------------------------------------------
--  DDL for Procedure P_MCX_ZERO_SHRINK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_MCX_ZERO_SHRINK" (iYear number)
As
ictr int;

Begin

for c1 in (select * from maxapp.lv5time where cycle_id=iYear order by lv5time_lkup_id ) loop
	UPDATE maxdata.minventory
	SET Inv_retail_1 =0, Inv_cost_1=0, Inv_items_1 =0
	WHERE time_level=51 and time_id=c1.lv5time_lkup_id;
	commit;
end loop;

for c2 in (select * from maxapp.lv4time where cycle_id=iYear order by lv4time_lkup_id ) loop
	UPDATE maxdata.minventory
	SET Inv_retail_1 =0, Inv_cost_1=0, Inv_items_1 =0
	WHERE time_level=50 and time_id=c2.lv4time_lkup_id;
	commit;
end loop;

for c3 in (select * from maxapp.lv3time where cycle_id=iYear order by lv3time_lkup_id ) loop
	UPDATE maxdata.minventory
	SET Inv_retail_1 =0, Inv_cost_1=0, Inv_items_1 =0
	WHERE time_level=49 and time_id=c3.lv3time_lkup_id;
	commit;
end loop;

for c4 in (select * from maxapp.lv2time where cycle_id=iYear order by lv2time_lkup_id ) loop
	UPDATE maxdata.minventory
	SET Inv_retail_1 =0, Inv_cost_1=0, Inv_items_1 =0
	WHERE time_level=48 and time_id=c4.lv2time_lkup_id;
	commit;
end loop;

for c5 in (select * from maxapp.lv1time where cycle_id=iYear order by lv1time_lkup_id ) loop
	UPDATE maxdata.minventory
	SET Inv_retail_1 =0, Inv_cost_1=0, Inv_items_1 =0
	WHERE time_level=47 and time_id=c5.lv1time_lkup_id;
	commit;
end loop;

end;

/
