--------------------------------------------------------
--  DDL for Procedure CHECK_INSERT_OF_LV1LOC_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_INSERT_OF_LV1LOC_ID" (local_lv1loc_id lv10loc.lv1loc_id%TYPE)
AS

BEGIN
	DECLARE
		INVALID_WORLD EXCEPTION;
		loc_id LV1LOC.LV1LOC_ID%TYPE;

	BEGIN
		SELECT COUNT(*)
		INTO loc_id
		FROM LV1LOC
		WHERE LV1LOC.LV1LOC_ID = local_lv1loc_id;

		IF loc_id = 0 THEN
			RAISE INVALID_WORLD;
		END IF;

		EXCEPTION
			WHEN INVALID_WORLD THEN
			raise_application_error(-10000,'Invalid World (Lv1loc_id)'||local_lv1loc_id);
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV1LOC_ID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV1LOC_ID" TO "MAXUSER";
