--------------------------------------------------------
--  DDL for Procedure UPDATE_PARENTS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_PARENTS" (local_lv10loc_row lv10loc%ROWTYPE)
AS
BEGIN
	--		Update lv9loc dimensions

			UPDATE_LV9_LOC(local_lv10loc_row);

	--		Update lv8loc dimensions

			UPDATE_LV8_LOC(local_lv10loc_row);

	--		Update lv7loc dimensions

			UPDATE_LV7_LOC(local_lv10loc_row);

	--		Update lv6loc dimensions

			UPDATE_LV6_LOC(local_lv10loc_row);

	--		Update lv5loc dimensions

			UPDATE_LV5_LOC(local_lv10loc_row);

	--		Update lv4loc dimensions

			UPDATE_LV4_LOC(local_lv10loc_row);

	--		Update lv3loc dimensions

			UPDATE_LV3_LOC(local_lv10loc_row);

	--		Update lv2loc dimensions

			UPDATE_LV2_LOC(local_lv10loc_row);

	--		Update lv1loc dimensions

			UPDATE_LV1_LOC(local_lv10loc_row);

END;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_PARENTS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_PARENTS" TO "MAXUSER";
