--------------------------------------------------------
--  DDL for Procedure P_LV10FINC_PERIOD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_LV10FINC_PERIOD" (period_id IN integer, cycle_id IN integer)
AS

-- made lv10merch changes on 3/1  --rashmi

-- made changes necessary to make lv10finc_id a sequence entry from earlier auto increment --Suresh 03/14/2000

	t_max_finc_id number(10,0);
	t_min_finc_id number(10,0);
	finc_count number(10,0);
	t_seq_num number(10,0);

begin
	insert into maxdata.temp_lv10finc
	(last_update,
	 changed_by_batch,
	 period,
	 days_in_period,
	 cycle,
	 version_id,
	 lv10loc_id,
	 lv10mvmt_id,
	 used_cubic_meters,
	 used_dsp_sqmeters,
	 used_flr_sqmeters,
	 used_linear_meters,
	 gm_roii,
	 posit_sales,
	 posit_mvmt,
	 posit_norm_mvmt,
	 item_direct_cost,
	 item_indirect_cost,
	 item_average_price,
	 case_cost,
	 case_price,
	 days_supply,
	 total_caps,
	 total_items,
	 total_units,
	 vat,
	 include_in_avg,
	 supplier_item_cost,
	 supplier_avg_price,
	 supp_posit_mvmt,
	 supplier_norm_mvmt,
	 num_user1,
	 num_user2,
	 num_user3,
	 num_user4,
	 num_user5,
	 num_user6,
	 live_lv7loc_id,
	 oh_qty,
	 oh_cost,
	 lv10mast_id,
	 ALLOC_CUBIC_METERS,
	 ALLOC_DSP_SQMETERS,
	 ALLOC_FLR_SQMETERS,
	 ALLOC_LINEAR_METER,
	 posit_mvmt_reg ,
	 posit_sales_reg ,
	 Posit_mvmt_promo ,
	 Posit_sales_promo ,
	 Posit_mvmt_clrnc ,
	 Posit_sales_clrnc ,
	 Posit_mvmt_retn ,
	 Posit_sales_retn,
	 begin_inv_items,
	 end_inv_items,
	 received_items,
	 min_pres_items
	 )
	select sysdate,
	 1,
	 lv10mvmt.period,
	 lv10mvmt.days_in_period,
	 lv10mvmt.cycle,
	 1,
	 lv10loc.lv10loc_id,
	 lv10mvmt.lv10mvmt_id,
	 calc_used_space('C', lv10loc.rotate, lv10loc.slope, lv10loc.orient,
	                      lv10merch.height, lv10merch.width, lv10merch.depth,
                           lv10loc.xcoord_facings, lv10loc.ypos_facings,
                           lv10loc.zcoord_facings),
	 calc_used_space('D', lv10loc.rotate, lv10loc.slope, lv10loc.orient,
                           lv10merch.height, lv10merch.width, lv10merch.depth,
                           lv10loc.xcoord_facings, lv10loc.ypos_facings,
                           lv10loc.zcoord_facings),
	 calc_used_space('F', lv10loc.rotate, lv10loc.slope, lv10loc.orient,
                           lv10merch.height, lv10merch.width, lv10merch.depth,
                           lv10loc.xcoord_facings, lv10loc.ypos_facings,
                           lv10loc.zcoord_facings),
	 calc_used_space('L', lv10loc.rotate, lv10loc.slope, lv10loc.orient,
                           lv10merch.height, lv10merch.width, lv10merch.depth,
                           lv10loc.xcoord_facings, lv10loc.ypos_facings,
                           lv10loc.zcoord_facings),
	 1,
	 (lv10mvmt.avg_price*lv10mvmt.item_mvmt)
	 /lv10mvmt.positions,
	 (lv10mvmt.item_mvmt/lv10mvmt.positions),
	 (lv10mvmt.item_mvmt/lv10mvmt.positions)
	 *lv10mast.convert_to_normal,
	 lv10mvmt.item_direct_cost,
	 lv10mvmt.item_indirect_cost,
	 lv10mvmt.avg_price,
	 lv10mast.case_cost,
	 lv10mast.case_price,
	 1,
	 lv10loc.total_caps,
	 lv10loc.total_items,
	 lv10loc.total_units,
	 (lv10mvmt.vat/lv10mvmt.positions),
	 lv10mvmt.include_in_avg,
	 lv10mvmt.supplier_item_cost,
	 lv10mvmt.supplier_avg_price,
	 lv10mvmt.supplier_item_mvmt/lv10mvmt.positions,
	 (lv10mvmt.supplier_item_mvmt/lv10mvmt.positions)
	 *lv10mast.convert_to_normal,
	 (lv10mvmt.num_user1),
	 (lv10mvmt.num_user2),
	 (lv10mvmt.num_user3),
	 (lv10mvmt.num_user4),
	 (lv10mvmt.num_user5),
	 (lv10mvmt.num_user6),
	 pogmaster.live_lv7loc_id,
	 (lv10mvmt.inventory_items/lv10mvmt.positions),
	 (lv10mvmt.inventory_items/lv10mvmt.positions*lv10mvmt.item_direct_cost),
	 lv10mast.lv10mast_id,
		pogmaster.ALLOC_CUBIC_METERS * lv10loc.pct_used_cubic,
		pogmaster.ALLOC_DSP_SQMETERS * lv10loc.pct_used_dsp,
	 pogmaster.ALLOC_FLR_SQMETERS * lv10loc.pct_used_flr,
	 pogmaster.ALLOC_LINEAR_METER * lv10loc.pct_used_linear,
	 lv10mvmt.posit_mvmt_Reg,
	 (lv10mvmt.posit_mvmt_Reg * lv10mvmt.Avg_Price_Reg),
	 lv10mvmt.posit_mvmt_Promo ,
	 (lv10mvmt.posit_mvmt_Promo * lv10mvmt.Avg_Price_Promo  ),
	 lv10mvmt.Posit_mvmt_clrnc ,
	 ( lv10mvmt.Posit_mvmt_clrnc  * lv10mvmt.Avg_Price_Clrnc ) ,
	 lv10mvmt.Posit_mvmt_retn,
	 ( lv10mvmt.Posit_mvmt_retn * lv10mvmt.Avg_Price_Retn),
	 lv10mvmt.begin_inv_items,
	 lv10mvmt.end_inv_items,
	 lv10mvmt.received_items,
	 lv10mvmt.min_pres_items
	from maxdata.lv10loc,
	maxdata.pogmaster,
	maxdata.lv10mvmt,
	maxdata.lv10mast,
	maxdata.lv10merch
        where lv10mvmt.period = period_id and
		lv10mvmt.cycle = cycle_id and
		lv10mvmt.store_loc_id = pogmaster.pog_lv4loc_id and
		lv10mvmt.lv10mast_id = lv10loc.lv10mast_id and
		lv10loc.lv7loc_id = pogmaster.pog_model_id and
		lv10mast.lv10mast_id = lv10loc.lv10mast_id and
		pogmaster.current_pog = 1 and
		nvl(lv10loc.no_mvmt_flag,0) <> 1 and
		nvl(lv10mvmt.positions,0) > 0 and
		lv10mast.lv10mast_id = lv10merch.lv10mast_id and
		lv10loc.lv10merch_id = lv10merch.lv10merch_id ;


	commit ;


	Select seq_num into t_seq_num from maxapp.sequence
	where level_type = 10 and entity_type = 5;

-- As the sequence number can not be insert in temp table it is always higher than necessary and may out of synch with
-- Sequence entry for the original table. So if you take id - min(id) + 1 from temp table it gives a counter from 1
-- for the records so id - min(id) + 1 + seq_num from sequence table for the original table always gets right id next to seq_num.


	select max(lv10finc_id) into t_max_finc_id
	from maxdata.temp_lv10finc;

	select min(lv10finc_id) into t_min_finc_id
	from maxdata.temp_lv10finc;

	finc_count := t_max_finc_id - t_min_finc_id;

	insert into maxdata.lv10finc
	(lv10finc_id,
	 last_update,
	 changed_by_batch,
	 period,
	 days_in_period,
	 cycle,
	 version_id,
	 lv10loc_id,
	 lv10mvmt_id,
	 used_cubic_meters,
	 used_dsp_sqmeters,
	 used_flr_sqmeters,
	 used_linear_meters,
	 gm_roii,
	 posit_sales,
	 posit_mvmt,
	 posit_norm_mvmt,
	 item_direct_cost,
	 item_indirect_cost,
	 item_average_price,
	 case_cost,
	 case_price,
	 days_supply,
	 total_caps,
	 total_items,
	 total_units,
	 vat,
	 include_in_avg,
	 supplier_item_cost,
	 supplier_avg_price,
	 supp_posit_mvmt,
	 supplier_norm_mvmt,
	 num_user1,
	 num_user2,
	 num_user3,
	 num_user4,
	 num_user5,
	 num_user6,
	 live_lv7loc_id,
	 oh_qty,
	 oh_cost,
	 lv10mast_id,
	 ALLOC_CUBIC_METERS,
	 ALLOC_DSP_SQMETERS,
	 ALLOC_FLR_SQMETERS,
	 ALLOC_LINEAR_METER,
	 posit_mvmt_reg ,
	 posit_sales_reg ,
	 Posit_mvmt_promo ,
	 Posit_sales_promo ,
	 Posit_mvmt_clrnc ,
	 Posit_sales_clrnc ,
	 Posit_mvmt_retn ,
	 Posit_sales_retn,
	 begin_inv_items,
	 end_inv_items,
	 received_items,
	 min_pres_items)
	select lv10finc_id + t_seq_num + 1 - t_min_finc_id,
	 last_update,
	 changed_by_batch,
	 period,
	 days_in_period,
	 cycle,
	 version_id,
	 lv10loc_id,
	 lv10mvmt_id,
	 used_cubic_meters,
	 used_dsp_sqmeters,
	 used_flr_sqmeters,
	 used_linear_meters,
	 gm_roii,
	 posit_sales,
	 posit_mvmt,
	 posit_norm_mvmt,
	 item_direct_cost,
	 item_indirect_cost,
	 item_average_price,
	 case_cost,
	 case_price,
	 days_supply,
	 total_caps,
	 total_items,
	 total_units,
	 vat,
	 include_in_avg,
	 supplier_item_cost,
	 supplier_avg_price,
	 supp_posit_mvmt,
	 supplier_norm_mvmt,
	 num_user1,
	 num_user2,
	 num_user3,
	 num_user4,
	 num_user5,
	 num_user6,
	 live_lv7loc_id,
	 oh_qty,
	 oh_cost,
	 lv10mast_id,
	 ALLOC_CUBIC_METERS,
	 ALLOC_DSP_SQMETERS,
	 ALLOC_FLR_SQMETERS,
	 ALLOC_LINEAR_METER,
	 posit_mvmt_reg ,
	 posit_sales_reg ,
	 Posit_mvmt_promo ,
	 Posit_sales_promo ,
	 Posit_mvmt_clrnc ,
	 Posit_sales_clrnc ,
	 Posit_mvmt_retn ,
	 Posit_sales_retn,
	 begin_inv_items,
	 end_inv_items,
	 received_items,
	 min_pres_items
	from maxdata.temp_lv10finc;
	commit;

	Update maxapp.sequence
	set seq_num = seq_num + finc_count + 1
	where level_type = 10 and entity_type = 5;
	commit;

	delete from maxdata.temp_lv10finc;

	update maxdata.lv10finc
   	set period_lkup_id = ( select period_lkup_id from maxapp.period_lkup
        				where period_lkup.period_id = lv10finc.period and
					period_lkup.cycle_id = lv10finc.cycle and
					period_lkup.period_type = ( select period_type from maxapp.mmax_config ) )
	where lv10finc.period = period_id and lv10finc.cycle = cycle_id;


-- Use this for no logging deletion if feasible and comment out delete statement.
-- truncate table maxdata.temp_lv10finc;

	commit;

end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_LV10FINC_PERIOD" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_LV10FINC_PERIOD" TO "MAXUSER";
