--------------------------------------------------------
--  DDL for Procedure CHECK_INSERT_OF_LV5LOC_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_INSERT_OF_LV5LOC_ID" (local_lv5loc_id lv10loc.lv5loc_id%TYPE)
AS

BEGIN
	DECLARE
		INVALID_DEPARTMENT EXCEPTION;
		loc_id LV5LOC.LV5LOC_ID%TYPE;

	BEGIN
		SELECT COUNT(*)
		INTO loc_id
		FROM LV5LOC
		WHERE LV5LOC.LV5LOC_ID = local_lv5loc_id;

		IF loc_id = 0 THEN
			RAISE INVALID_DEPARTMENT;
		END IF;

		EXCEPTION
			WHEN INVALID_DEPARTMENT THEN
			raise_application_error(-20000,'Invalid Department (Lv5loc_id)'||local_lv5loc_id);
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV5LOC_ID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV5LOC_ID" TO "MAXUSER";
