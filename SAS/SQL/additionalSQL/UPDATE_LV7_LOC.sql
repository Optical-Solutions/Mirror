--------------------------------------------------------
--  DDL for Procedure UPDATE_LV7_LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LV7_LOC" (local_lv10loc_row IN lv10loc%ROWTYPE)
AS

BEGIN
	UPDATE LV7LOC SET
	LV7LOC.USED_CUBIC_METERS = LV7LOC.USED_CUBIC_METERS +
      nvl(local_lv10loc_row.USED_CUBIC_METERS, 0),
	LV7LOC.USED_DSP_SQMETERS = LV7LOC.USED_DSP_SQMETERS +
      nvl(local_lv10loc_row.USED_DSP_SQMETERS, 0),
	LV7LOC.USED_LINEAR_METERS = LV7LOC.USED_LINEAR_METERS +
      nvl(local_lv10loc_row.USED_LINEAR_METERS, 0) ,
	LV7LOC.TOTAL_ITEMS = nvl(LV7LOC.TOTAL_ITEMS, 0) +
      nvl(local_lv10loc_row.TOTAL_ITEMS, 0),
	LV7LOC.TOTAL_UNITS = nvl(LV7LOC.TOTAL_UNITS, 0) +
      nvl(local_lv10loc_row.TOTAL_UNITS, 0)
	WHERE LV7LOC.LV7LOC_ID = local_lv10loc_row.LV7LOC_ID;
END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV7_LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV7_LOC" TO "MAXUSER";
