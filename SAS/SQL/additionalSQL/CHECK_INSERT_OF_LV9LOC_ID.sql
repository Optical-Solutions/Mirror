--------------------------------------------------------
--  DDL for Procedure CHECK_INSERT_OF_LV9LOC_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_INSERT_OF_LV9LOC_ID" (local_lv9loc_id lv10loc.lv9loc_id%TYPE)
AS

BEGIN
	DECLARE
		INVALID_SUBFIXTURE EXCEPTION;
		loc_id LV9LOC.LV9LOC_ID%TYPE;

	BEGIN
		SELECT COUNT(*)
		INTO loc_id
		FROM LV9LOC
		WHERE LV9LOC.LV9LOC_ID = local_lv9loc_id;

		IF loc_id = 0 THEN
			RAISE INVALID_SUBFIXTURE;
		END IF;

		EXCEPTION
			WHEN INVALID_SUBFIXTURE THEN
			raise_application_error(-20000,'Invalid Sub Fixture (Lv9loc_id)'||local_lv9loc_id);
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV9LOC_ID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV9LOC_ID" TO "MAXUSER";
