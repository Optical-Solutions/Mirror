--------------------------------------------------------
--  DDL for Procedure P_REPLACE_MODEL_RES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_REPLACE_MODEL_RES" 
--- 9/26/11	Modified for MCX
as
iSql 		long;
cnt 		number;

cursor hier_level is
	select hier_level,value_1 from datamgr.Loadpref where upper(key_1)='MODEL_RES' order by hier_level asc;
begin

-- aso specific -- copy lookup_id to user_attrib --
maxdata.p_update_reskey_attrib;

For c1 in hier_level loop

	maxdata.p_log_xref_model_res_status;
	maxdata.P_pop_lvxref_model_res(c1.hier_level,c1.value_1);
	maxdata.p_validate_model_res;
	maxdata.p_log_model_res_status;
	maxdata.p_do_model_res;
	-- aso specific --
	maxdata.p_update_resolved_member;
	maxdata.p_update_res_status_attrib;

end loop;

maxdata.p_log_xref_model_res_status;
maxdata.p_log_model_res_status;

-- Rebuild the inv and sales view
maxdata.p_mfinc_modelsku_view;
maxdata.p_minventory_modelsku_view;

end;

/
