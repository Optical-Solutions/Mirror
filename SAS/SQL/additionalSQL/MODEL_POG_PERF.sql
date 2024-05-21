--------------------------------------------------------
--  DDL for Procedure MODEL_POG_PERF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."MODEL_POG_PERF" 
  (l_lv7loc_id in number,
  l_level in number,
  l_fixt out number,
  l_subfixt out number,
  l_tot_item_cost out number,
  l_tot_pog_cost out number,
  l_prod_cost out number,
  l_prod_price out number,
  l_tot_items out number,
  l_tot_wros out number
  )
As

BEGIN
--input: [l_lv7loc_id]  [model planogram being worked on]

--level 8 - fixture components
select sum(lv8mast.fixture_cost) into l_fixt
from lv8loc, lv8mast
where lv8loc.lv7loc_id = l_lv7loc_id and lv8loc.lv8mast_id = lv8mast.lv8mast_id;

--level 9 -- subfixt
select sum(lv9mast.fixture_cost) into l_subfixt
from lv9loc, lv9mast
where lv9loc.lv7loc_id = l_lv7loc_id and lv9loc.lv9mast_id = lv9mast.lv9mast_id;

--level 10 - product
select sum( lv10loc.total_items * lv10mast.item_cost), avg(lv10mast.item_cost),avg(lv10mast.current_item_price), sum(lv10loc.total_items), sum(lv10loc.wros)
into l_tot_item_cost, l_prod_cost, l_prod_price, l_tot_items, l_tot_wros
from lv10loc,lv10mast
where lv10loc.lv7loc_id = l_lv7loc_id and lv10loc.lv10mast_id = lv10mast.lv10mast_id;

l_tot_pog_cost  := l_tot_item_cost + l_fixt + l_subfixt;
end; --model_pog_perf

/

  GRANT EXECUTE ON "MAXDATA"."MODEL_POG_PERF" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."MODEL_POG_PERF" TO "MAXUSER";
