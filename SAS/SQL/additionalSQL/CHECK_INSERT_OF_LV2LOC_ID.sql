--------------------------------------------------------
--  DDL for Procedure CHECK_INSERT_OF_LV2LOC_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_INSERT_OF_LV2LOC_ID" (local_lv2loc_id lv10loc.lv2loc_id%TYPE)
AS

BEGIN
	DECLARE
		INVALID_COUNTRY EXCEPTION;
		loc_id LV2LOC.LV2LOC_ID%TYPE;

	BEGIN
		SELECT COUNT(*)
		INTO loc_id
		FROM LV2LOC
		WHERE LV2LOC.LV2LOC_ID = local_lv2loc_id;

		IF loc_id = 0 THEN
			RAISE INVALID_COUNTRY;
		END IF;

		EXCEPTION
			WHEN INVALID_COUNTRY THEN
			raise_application_error(-20000,'Invalid Country (Lv2loc_id)'||local_lv2loc_id);
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV2LOC_ID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV2LOC_ID" TO "MAXUSER";
