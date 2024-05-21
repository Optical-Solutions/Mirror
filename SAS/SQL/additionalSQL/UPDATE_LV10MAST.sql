--------------------------------------------------------
--  DDL for Procedure UPDATE_LV10MAST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LV10MAST" 
AS

iCtr integer;

Cursor cur_dm_lv10mast is
Select a.lv10cat_id, a.lv10mast_id from maxdata.dm_lv10mast a, maxdata.lv10mast b
where a.lv10mast_id = b.lv10mast_id
and a.lv10cat_id <> b.lv10cat_Id;

Cursor cur2_dm_lv10mast is
Select a.lv10cat_id_2, a.lv10mast_id from maxdata.dm_lv10mast a, maxdata.lv10mast b
where a.lv10mast_id = b.lv10mast_id
and a.lv10cat_id_2 <> b.lv10cat_Id_2;

Cursor cur3_dm_lv10mast is
Select a.lv10cat_id_3, a.lv10mast_id from maxdata.dm_lv10mast a, maxdata.lv10mast b
where a.lv10mast_id = b.lv10mast_id
and a.lv10cat_id_3 <> b.lv10cat_Id_3;

Begin



Update maxdata.lv10mast
Set
(
	merch_lkup,
  changed_by_batch,
-- lv10cat_id,
 name,
-- long_name,
-- unit_of_measure,
-- phys_type,
-- prim_type,

--prim_filename,
 active_lkup,
-- height,

-- width,
-- depth,
-- weight,
-- start_date,

--end_date,
-- color,
-- flavor,
-- manufact,
 min_pack,
 units_per_case,

items_per_unit,
 item_cost,
 current_item_price,
-- case_cost,
-- case_price,

convert_to_normal,
-- interval_left1,
-- interval_left2,
-- interval_left3,

-- interval_top1,
-- interval_top2,
-- interval_top3,
-- image_back,
-- image_bottom,

--image_front,
-- image_left,
-- image_right,
-- image_top,
-- line_back,

-- line_bottom,
 -- line_top,
 -- line_front,
-- line_left,
-- line_right,
-- supplier_item_cost,

-- supp_current_price,
-- interval_1used,
-- interval_2used,
-- interval_3used,

num_user1,
-- num_user2,
-- num_user3,
-- num_user4,
-- num_user5,
-- num_user6,

-- date_user7,
-- date_user8,
 char_user9,
 char_user10,
 char_user11,

char_user12,
 char_user13,
-- bigchar_user14,
-- bigchar_user15,
 record_type,

-- attrib1_id,
-- attrib2_id,
-- attrib3_id,
-- attrib4_id,
-- attrib5_id,

-- cc_id,
-- cs_id,
User_attrib1,
User_attrib2,
User_attrib3,
User_attrib4,
User_attrib5,
User_attrib6,
User_attrib7,
User_attrib8,
User_attrib9,
User_attrib10,
User_attrib11,
User_attrib12,
User_attrib13,
User_attrib14,
User_attrib15,
User_attrib16,
User_attrib17,
User_attrib18,
User_attrib19,
User_attrib20,
User_attrib21,
User_attrib22,
User_attrib23,
User_attrib24,
User_attrib25,
User_attrib26,
User_attrib27,
User_attrib28,
User_attrib29,
User_attrib30,
User_attrib31,
User_attrib32,
User_attrib33,
User_attrib34,
User_attrib35,
User_attrib36,
User_attrib37,
User_attrib38,
User_attrib39,
User_attrib40,
User_attrib41,
User_attrib42,
User_attrib43,
User_attrib44,
User_attrib45,
User_attrib46,
User_attrib47,
User_attrib48,
User_attrib49,
User_attrib50,
User_attrib51,
User_attrib52,
User_attrib53,
User_attrib54,
User_attrib55,
User_attrib56,
User_attrib57,
User_attrib58,
User_attrib59,
User_attrib60



)=
(Select
	merch_lkup,
  changed_by_batch,
-- lv10cat_id,
 name,
-- long_name,
-- unit_of_measure,
-- phys_type,
-- prim_type,

--prim_filename,
 active_lkup,
-- height,

-- width,
-- depth,
-- weight,
-- start_date,

--end_date,
-- color,
-- flavor,
-- manufact,
 min_pack,
 units_per_case,

items_per_unit,
 item_cost,
 current_item_price,
-- case_cost,
-- case_price,

convert_to_normal,
-- interval_left1,
-- interval_left2,
-- interval_left3,

-- interval_top1,
-- interval_top2,
-- interval_top3,
-- image_back,
-- image_bottom,

--image_front,
-- image_left,
-- image_right,
-- image_top,
-- line_back,

-- line_bottom,
 -- line_top,
 -- line_front,
-- line_left,
-- line_right,
-- supplier_item_cost,

-- supp_current_price,
-- interval_1used,
-- interval_2used,
-- interval_3used,

num_user1,
-- num_user2,
-- num_user3,
-- num_user4,
-- num_user5,
-- num_user6,

-- date_user7,
-- date_user8,
 char_user9,
 char_user10,
 char_user11,

char_user12,
 char_user13,
-- bigchar_user14,
-- bigchar_user15,
 record_type,

-- attrib1_id,
-- attrib2_id,
-- attrib3_id,
-- attrib4_id,
-- attrib5_id,

-- cc_id,
-- cs_id,
User_attrib1,
User_attrib2,
User_attrib3,
User_attrib4,
User_attrib5,
User_attrib6,
User_attrib7,
User_attrib8,
User_attrib9,
User_attrib10,
User_attrib11,
User_attrib12,
User_attrib13,
User_attrib14,
User_attrib15,
User_attrib16,
User_attrib17,
User_attrib18,
User_attrib19,
User_attrib20,
User_attrib21,
User_attrib22,
User_attrib23,
User_attrib24,
User_attrib25,
User_attrib26,
User_attrib27,
User_attrib28,
User_attrib29,
User_attrib30,
User_attrib31,
User_attrib32,
User_attrib33,
User_attrib34,
User_attrib35,
User_attrib36,
User_attrib37,
User_attrib38,
User_attrib39,
User_attrib40,
User_attrib41,
User_attrib42,
User_attrib43,
User_attrib44,
User_attrib45,
User_attrib46,
User_attrib47,
User_attrib48,
User_attrib49,
User_attrib50,
User_attrib51,
User_attrib52,
User_attrib53,
User_attrib54,
User_attrib55,
User_attrib56,
User_attrib57,
User_attrib58,
User_attrib59,
User_attrib60

From maxdata.dm_lv10mast where
dm_lv10mast.lv10mast_id
= lv10mast.lv10mast_id
)

where lv10mast.lv10mast_id =
( select lv10mast_id  from maxdata.dm_lv10mast where dm_lv10mast.lv10mast_id = lv10mast.lv10mast_id);

commit;

For n1 in cur_dm_lv10mast loop
		Update maxdata.lv10mast set lv10cat_id = n1.lv10cat_id where lv10mast_id = n1.lv10mast_id;

end loop;
commit;
For n2 in cur2_dm_lv10mast loop
		Update maxdata.lv10mast set lv10cat_id_2 = n2.lv10cat_id_2 where lv10mast_id = n2.lv10mast_id;

end loop;
commit;
For n3 in cur3_dm_lv10mast loop
		Update maxdata.lv10mast set lv10cat_id_3 = n3.lv10cat_id_3 where lv10mast_id = n3.lv10mast_id;

end loop;

commit;

select Product_identifier into ictr from maxapp.mmax_config ;

if ictr = 1 THEN

	Update maxdata.lv10mast set
	order_code = ( select order_code from maxdata.dm_lv10mast
		where dm_lv10mast.lv10mast_id = lv10mast.lv10mast_id)
		where lv10mast.lv10mast_id =
		( select lv10mast_id  from maxdata.dm_lv10mast
			where dm_lv10mast.lv10mast_id = lv10mast.lv10mast_id);

END IF;

if ictr = 2 THEN

	Update maxdata.lv10mast set
	upc = ( select upc from maxdata.dm_lv10mast
		where dm_lv10mast.lv10mast_id = lv10mast.lv10mast_id)
		where lv10mast.lv10mast_id =
		( select lv10mast_id  from maxdata.dm_lv10mast
			where dm_lv10mast.lv10mast_id = lv10mast.lv10mast_id);

END IF;


commit;
End;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV10MAST" TO "MAXAPP";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV10MAST" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV10MAST" TO "MAXUSER";
