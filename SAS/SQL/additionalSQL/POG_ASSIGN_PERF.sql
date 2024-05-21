--------------------------------------------------------
--  DDL for Procedure POG_ASSIGN_PERF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."POG_ASSIGN_PERF" (l_pog_assign_id number, l_level number)
As
l_val1 number(13,3);
l_val2 number(13,3);
l_val3 number(13,3);
l_val4 number(13,3);
l_val5 number(13,3);
l_val6 number(13,3);
l_val7 number(13,3);
l_val8 number(13,3);
l_lv7loc_id number(10);

BEGIN
select pog_model_id into l_lv7loc_id from pogmaster where pog_master_id = l_pog_assign_id;

select fixture_cost, subfixt_cost, total_item_cost, total_pog_cost, avg_item_cost, avg_item_price, total_wros, total_items into l_val1, l_val2, l_val3, l_val4, l_val5, l_val6, l_val7, l_val8
from lv7loc
where lv7loc_id = l_lv7loc_id;

update pogmaster  --avg_item_cost, l_val4,
set fixture_cost= l_val1, subfixt_cost= l_val2, total_item_cost= l_val3, total_pog_cost = l_val4, avg_item_cost= l_val5, avg_item_price= l_val6, total_wros= l_val7, total_items= l_val8
where pog_master_id = l_pog_assign_id;

commit;
end; --pog_assign_perf

/

  GRANT EXECUTE ON "MAXDATA"."POG_ASSIGN_PERF" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."POG_ASSIGN_PERF" TO "MAXUSER";
