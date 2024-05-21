--------------------------------------------------------
--  DDL for Procedure UPDATE_LV3_LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LV3_LOC" (local_lv10loc_row IN lv10loc%ROWTYPE)
AS

BEGIN
	UPDATE LV3LOC SET
	LV3LOC.USED_CUBIC_METERS = LV3LOC.USED_CUBIC_METERS +
	nvl(local_lv10loc_row.USED_CUBIC_METERS, 0),
	LV3LOC.USED_DSP_SQMETERS = LV3LOC.USED_DSP_SQMETERS +
	nvl(local_lv10loc_row.USED_DSP_SQMETERS, 0),
	LV3LOC.USED_LINEAR_METERS = LV3LOC.USED_LINEAR_METERS +
	nvl(local_lv10loc_row.USED_LINEAR_METERS, 0) ,
	LV3LOC.TOTAL_ITEMS = nvl(LV3LOC.TOTAL_ITEMS, 0) +
	nvl(local_lv10loc_row.TOTAL_ITEMS, 0),
	LV3LOC.TOTAL_UNITS = nvl(LV3LOC.TOTAL_UNITS, 0) +
	nvl(local_lv10loc_row.TOTAL_UNITS, 0)
	WHERE LV3LOC.LV3LOC_ID = local_lv10loc_row.LV3LOC_ID;
END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV3_LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV3_LOC" TO "MAXUSER";
