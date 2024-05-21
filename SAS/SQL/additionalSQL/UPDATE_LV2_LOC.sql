--------------------------------------------------------
--  DDL for Procedure UPDATE_LV2_LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LV2_LOC" (local_lv10loc_row IN lv10loc%ROWTYPE)
AS

BEGIN
	UPDATE LV2LOC SET
	LV2LOC.USED_CUBIC_METERS = LV2LOC.USED_CUBIC_METERS +
	nvl(local_lv10loc_row.USED_CUBIC_METERS, 0),
	LV2LOC.USED_DSP_SQMETERS = LV2LOC.USED_DSP_SQMETERS +
	nvl(local_lv10loc_row.USED_DSP_SQMETERS, 0),
	LV2LOC.USED_LINEAR_METERS = LV2LOC.USED_LINEAR_METERS +
	nvl(local_lv10loc_row.USED_LINEAR_METERS, 0) ,
	LV2LOC.TOTAL_ITEMS = nvl(LV2LOC.TOTAL_ITEMS, 0) +
	nvl(local_lv10loc_row.TOTAL_ITEMS, 0),
	LV2LOC.TOTAL_UNITS = nvl(LV2LOC.TOTAL_UNITS, 0) +
	nvl(local_lv10loc_row.TOTAL_UNITS, 0)
	WHERE LV2LOC.LV2LOC_ID = local_lv10loc_row.LV2LOC_ID;
END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV2_LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV2_LOC" TO "MAXUSER";
