--------------------------------------------------------
--  DDL for Procedure UPDATE_LV8_LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LV8_LOC" (local_lv10loc_row IN lv10loc%ROWTYPE)
AS

BEGIN
	UPDATE LV8LOC SET
		 LV8LOC.USED_CUBIC_METERS = LV8LOC.USED_CUBIC_METERS +
		 nvl(local_lv10loc_row.USED_CUBIC_METERS, 0),
		 LV8LOC.USED_DSP_SQMETERS = LV8LOC.USED_DSP_SQMETERS +
		 nvl(local_lv10loc_row.USED_DSP_SQMETERS, 0),
		 LV8LOC.USED_LINEAR_METERS = LV8LOC.USED_LINEAR_METERS +
		 nvl(local_lv10loc_row.USED_LINEAR_METERS, 0),
	   	 LV8LOC.TOTAL_ITEMS = nvl(LV8LOC.TOTAL_ITEMS, 0) +
		 nvl(local_lv10loc_row.TOTAL_ITEMS, 0),
		 LV8LOC.TOTAL_UNITS = nvl(LV8LOC.TOTAL_UNITS, 0) +
		 nvl(local_lv10loc_row.TOTAL_UNITS, 0)
      WHERE  LV8LOC.LV8LOC_ID = local_lv10loc_row.LV8LOC_ID;
END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV8_LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV8_LOC" TO "MAXUSER";
