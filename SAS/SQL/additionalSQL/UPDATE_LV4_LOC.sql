--------------------------------------------------------
--  DDL for Procedure UPDATE_LV4_LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LV4_LOC" (local_lv10loc_row IN lv10loc%ROWTYPE)
AS

BEGIN
	UPDATE LV4LOC SET
	LV4LOC.USED_CUBIC_METERS = LV4LOC.USED_CUBIC_METERS +
	nvl(local_lv10loc_row.USED_CUBIC_METERS, 0),
	LV4LOC.USED_DSP_SQMETERS = LV4LOC.USED_DSP_SQMETERS +
	nvl(local_lv10loc_row.USED_DSP_SQMETERS, 0),
	LV4LOC.USED_LINEAR_METERS = LV4LOC.USED_LINEAR_METERS +
	nvl(local_lv10loc_row.USED_LINEAR_METERS, 0) ,
	LV4LOC.TOTAL_ITEMS = nvl(LV4LOC.TOTAL_ITEMS, 0) +
	nvl(local_lv10loc_row.TOTAL_ITEMS, 0),
	LV4LOC.TOTAL_UNITS = nvl(LV4LOC.TOTAL_UNITS, 0) +
	nvl(local_lv10loc_row.TOTAL_UNITS, 0)
	WHERE LV4LOC.LV4LOC_ID = local_lv10loc_row.LV4LOC_ID;
END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV4_LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV4_LOC" TO "MAXUSER";
