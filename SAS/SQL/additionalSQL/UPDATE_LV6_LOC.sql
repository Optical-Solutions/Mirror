--------------------------------------------------------
--  DDL for Procedure UPDATE_LV6_LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LV6_LOC" (local_lv10loc_row IN lv10loc%ROWTYPE)
AS

BEGIN
	UPDATE LV6LOC SET
	LV6LOC.USED_CUBIC_METERS = LV6LOC.USED_CUBIC_METERS +
	nvl(local_lv10loc_row.USED_CUBIC_METERS, 0),
	LV6LOC.USED_DSP_SQMETERS = LV6LOC.USED_DSP_SQMETERS +
	nvl(local_lv10loc_row.USED_DSP_SQMETERS, 0),
	LV6LOC.USED_LINEAR_METERS = LV6LOC.USED_LINEAR_METERS +
	nvl(local_lv10loc_row.USED_LINEAR_METERS, 0) ,
	LV6LOC.TOTAL_ITEMS = nvl(LV6LOC.TOTAL_ITEMS, 0) +
	nvl(local_lv10loc_row.TOTAL_ITEMS, 0),
	LV6LOC.TOTAL_UNITS = nvl(LV6LOC.TOTAL_UNITS, 0) +
	nvl(local_lv10loc_row.TOTAL_UNITS, 0)
	WHERE LV6LOC.LV6LOC_ID = local_lv10loc_row.LV6LOC_ID;
END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV6_LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LV6_LOC" TO "MAXUSER";
