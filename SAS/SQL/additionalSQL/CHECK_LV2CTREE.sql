--------------------------------------------------------
--  DDL for Procedure CHECK_LV2CTREE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_LV2CTREE" (local_lv1cmast_id IN NUMBER,local_lv2cmast_id IN NUMBER, local_record_type IN VARCHAR, ctree_id OUT NUMBER)
AS
BEGIN
	DECLARE x NUMBER;
	        l_hier_type number;

	BEGIN

        Select hier_type into l_hier_type
        from maxdata.lv1cmast
        where lv1cmast_id =  local_lv1cmast_id;


		SELECT COUNT(*)
		INTO x
		FROM LV2CTREE
		WHERE LV2CTREE.LV1CMAST_ID = local_lv1cmast_id AND
		LV2CTREE.LV2CMAST_ID = local_lv2cmast_id;
		IF x = 0 THEN
	--	LV2CTREE Record not present so insert the record..
			INSERT INTO LV2CTREE (LV2CTREE_ID,LV1CMAST_ID,
  					LV2CMAST_ID, record_type, hier_type)
			values (LV2CTREE_SEQ.NEXTVAL,local_lv1cmast_id, local_lv2cmast_id,local_record_type, l_hier_type);
		END IF;
		SELECT LV2CTREE_ID
		INTO ctree_id
		FROM LV2CTREE
		WHERE LV2CTREE.LV1CMAST_ID = local_lv1cmast_id AND
			LV2CTREE.LV2CMAST_ID = local_lv2cmast_id;
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_LV2CTREE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_LV2CTREE" TO "MAXUSER";
