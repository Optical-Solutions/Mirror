--------------------------------------------------------
--  DDL for Procedure UPDATE_LV9_LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LV9_LOC" (local_lv10loc_row IN
			lv10loc%ROWTYPE) AS
BEGIN
	UPDATE LV9LOC SET
		 LV9LOC.USED_CUBIC_METERS = nvl(LV9LOC.USED_CUBIC_METERS, 0)+
		 nvl(local_lv10loc_row.USED_CUBIC_METERS, 0),
		 LV9LOC.USED_DSP_SQMETERS = nvl(LV9LOC.USED_DSP_SQMETERS, 0) +
		 nvl(local_lv10loc_row.USED_DSP_SQMETERS, 0),
		 LV9LOC.USED_LINEAR_METERS = nvl(LV9LOC.USED_LINEAR_METERS, 0) +
		 nvl(local_lv10loc_row.USED_LINEAR_METERS, 0)
	WHERE  LV9LOC.LV9LOC_ID = local_lv10loc_row.LV9LOC_ID;

END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV9_LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV9_LOC" TO "MAXUSER";
