--------------------------------------------------------
--  DDL for Procedure CHECK_INSERT_OF_LV6LOC_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_INSERT_OF_LV6LOC_ID" (local_lv6loc_id lv10loc.lv6loc_id%TYPE)
AS
BEGIN
	DECLARE
		INVALID_AREA EXCEPTION;
		loc_id LV6LOC.LV6LOC_ID%TYPE;

	BEGIN
		SELECT COUNT(*)
		INTO loc_id
		FROM LV6LOC
		WHERE LV6LOC.LV6LOC_ID = local_lv6loc_id;

		IF loc_id = 0 THEN
			RAISE INVALID_AREA;
		END IF;

		EXCEPTION
			WHEN INVALID_AREA THEN
			raise_application_error(-20000, 'Invalid Area (Lv6loc_id) '||local_lv6loc_id);
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV6LOC_ID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_INSERT_OF_LV6LOC_ID" TO "MAXUSER";
