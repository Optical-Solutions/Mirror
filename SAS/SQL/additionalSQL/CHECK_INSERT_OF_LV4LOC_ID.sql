--------------------------------------------------------
--  DDL for Procedure CHECK_INSERT_OF_LV4LOC_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_INSERT_OF_LV4LOC_ID" (local_lv4loc_id lv10loc.lv4loc_id%TYPE)
AS

BEGIN
	DECLARE
		INVALID_STORE EXCEPTION;
		loc_id LV4LOC.LV4LOC_ID%TYPE;

	BEGIN
		SELECT COUNT(*)
		INTO loc_id
		FROM LV4LOC
		WHERE LV4LOC.LV4LOC_ID = local_lv4loc_id;

		IF loc_id = 0 THEN
			RAISE INVALID_STORE;
		END IF;

		EXCEPTION
			WHEN INVALID_STORE THEN
			raise_application_error(-20000,'Invalid Store (Lv4loc_id)'||local_lv4loc_id);
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV4LOC_ID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV4LOC_ID" TO "MAXUSER";
