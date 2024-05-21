--------------------------------------------------------
--  DDL for Procedure UPDATE_LV5_LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LV5_LOC" (local_lv10loc_row IN lv10loc%ROWTYPE) 							AS
BEGIN
	UPDATE LV5LOC SET
	LV5LOC.USED_CUBIC_METERS = LV5LOC.USED_CUBIC_METERS +
	nvl(local_lv10loc_row.USED_CUBIC_METERS, 0),
	LV5LOC.USED_DSP_SQMETERS = LV5LOC.USED_DSP_SQMETERS +
	nvl(local_lv10loc_row.USED_DSP_SQMETERS, 0),
	LV5LOC.USED_LINEAR_METERS = LV5LOC.USED_LINEAR_METERS +
	nvl(local_lv10loc_row.USED_LINEAR_METERS, 0) ,
	LV5LOC.TOTAL_ITEMS = nvl(LV5LOC.TOTAL_ITEMS, 0) +
	nvl(local_lv10loc_row.TOTAL_ITEMS, 0),
	LV5LOC.TOTAL_UNITS = nvl(LV5LOC.TOTAL_UNITS, 0) +
	nvl(local_lv10loc_row.TOTAL_UNITS, 0)
	WHERE LV5LOC.LV5LOC_ID = local_lv10loc_row.LV5LOC_ID;
END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV5_LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV5_LOC" TO "MAXUSER";
