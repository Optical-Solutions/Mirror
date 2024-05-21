--------------------------------------------------------
--  DDL for Procedure UPDATE_LV1_LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LV1_LOC" (local_lv10loc_row IN lv10loc%ROWTYPE)
AS

BEGIN
	UPDATE LV1LOC SET
	LV1LOC.USED_CUBIC_METERS = LV1LOC.USED_CUBIC_METERS +
	nvl(local_lv10loc_row.USED_CUBIC_METERS, 0),
	LV1LOC.USED_DSP_SQMETERS = LV1LOC.USED_DSP_SQMETERS +
	nvl(local_lv10loc_row.USED_DSP_SQMETERS, 0),
	LV1LOC.USED_LINEAR_METERS = LV1LOC.USED_LINEAR_METERS +
	nvl(local_lv10loc_row.USED_LINEAR_METERS, 0) ,
	LV1LOC.TOTAL_ITEMS = nvl(LV1LOC.TOTAL_ITEMS, 0) +
	nvl(local_lv10loc_row.TOTAL_ITEMS, 0),
	LV1LOC.TOTAL_UNITS = nvl(LV1LOC.TOTAL_UNITS, 0) +
	nvl(local_lv10loc_row.TOTAL_UNITS, 0)
	WHERE LV1LOC.LV1LOC_ID = local_lv10loc_row.LV1LOC_ID;
END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV1_LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV1_LOC" TO "MAXUSER";
