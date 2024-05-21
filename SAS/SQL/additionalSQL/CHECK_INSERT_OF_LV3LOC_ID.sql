--------------------------------------------------------
--  DDL for Procedure CHECK_INSERT_OF_LV3LOC_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_INSERT_OF_LV3LOC_ID" (local_lv3loc_id lv10loc.lv3loc_id%TYPE)
AS

BEGIN
	DECLARE
		INVALID_STATE EXCEPTION;
		loc_id LV3LOC.LV3LOC_ID%TYPE;

	BEGIN
		SELECT COUNT(*)
		INTO loc_id
		FROM LV3LOC
		WHERE LV3LOC.LV3LOC_ID = local_lv3loc_id;

		IF loc_id = 0 THEN
			RAISE INVALID_STATE;
		END IF;

		EXCEPTION
			WHEN INVALID_STATE THEN
			raise_application_error(-20000,'Invalid State (Lv3loc_id)'||local_lv3loc_id);
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV3LOC_ID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV3LOC_ID" TO "MAXUSER";
