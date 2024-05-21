--------------------------------------------------------
--  DDL for Procedure UPDATE_TOT_ITEMS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_TOT_ITEMS" (local_lv10loc_row IN OUT lv10loc%ROWTYPE, items_per_unit IN NUMBER)
 AS

BEGIN
	local_lv10loc_row.TOTAL_CAPS := local_lv10loc_row.XCOORD_CAP_FACINGS *
		local_lv10loc_row.ZCOORD_CAP_FACINGS *
		local_lv10loc_row.YPOS_CAP_FACINGS;
	local_lv10loc_row.TOTAL_UNITS := local_lv10loc_row.XCOORD_FACINGS *
		local_lv10loc_row.ZCOORD_FACINGS * local_lv10loc_row.YPOS_FACINGS +
		local_lv10loc_row.TOTAL_CAPS;
	local_lv10loc_row.TOTAL_ITEMS := (local_lv10loc_row.XCOORD_FACINGS *
		local_lv10loc_row.ZCOORD_FACINGS * 	local_lv10loc_row.YPOS_FACINGS *
		items_per_unit) + (local_lv10loc_row.TOTAL_CAPS * items_per_unit);
END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_TOT_ITEMS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_TOT_ITEMS" TO "MAXUSER";
