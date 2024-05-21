--------------------------------------------------------
--  DDL for Procedure CHECK_INSERT_OF_LV7LOC_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_INSERT_OF_LV7LOC_ID" (local_lv7loc_id lv10loc.lv7loc_id%TYPE)
AS

BEGIN
	DECLARE
		INVALID_SECTION EXCEPTION;
		loc_id LV7LOC.LV7LOC_ID%TYPE;

	BEGIN
		SELECT COUNT(*)
		INTO loc_id
		FROM LV7LOC
		WHERE LV7LOC.LV7LOC_ID = local_lv7loc_id;

		IF loc_id = 0 THEN
			RAISE INVALID_SECTION;
		END IF;

		EXCEPTION
			WHEN INVALID_SECTION THEN
			raise_application_error(-20000,'Invalid Section (Lv7loc_id)'||local_lv7loc_id);
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV7LOC_ID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV7LOC_ID" TO "MAXUSER";
