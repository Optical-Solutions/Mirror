--------------------------------------------------------
--  DDL for Procedure CHECK_INSERT_OF_LV8LOC_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_INSERT_OF_LV8LOC_ID" (local_lv8loc_id lv10loc.lv8loc_id%TYPE)
AS

BEGIN
	DECLARE
		INVALID_FIXTURE EXCEPTION;
		loc_id LV8LOC.LV8LOC_ID%TYPE;

	BEGIN
		SELECT COUNT(*)
		INTO loc_id
		FROM LV8LOC
		WHERE LV8LOC.LV8LOC_ID = local_lv8loc_id;

		IF loc_id = 0 THEN
			RAISE INVALID_FIXTURE;
		END IF;

		EXCEPTION
			WHEN INVALID_FIXTURE THEN
			raise_application_error(-20000,'Invalid Fixture (Lv8loc_id)'||local_lv8loc_id);
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV8LOC_ID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV8LOC_ID" TO "MAXUSER";
