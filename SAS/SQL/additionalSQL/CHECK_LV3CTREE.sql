--------------------------------------------------------
--  DDL for Procedure CHECK_LV3CTREE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_LV3CTREE" ( local_lv1cmast_id IN NUMBER, local_lv2cmast_id IN NUMBER,
		local_lv3cmast_id IN NUMBER, local_record_type IN VARCHAR,local_lv2ctree_id OUT NUMBER, local_lv3ctree_id OUT NUMBER)
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
		FROM LV3CTREE
		WHERE LV3CTREE.LV1CMAST_ID = local_lv1cmast_id AND
			LV3CTREE.LV2CMAST_ID = local_lv2cmast_id AND
			LV3CTREE.LV3CMAST_ID = local_lv3cmast_id;
		IF x = 0 THEN
			CHECK_LV2CTREE(local_lv1cmast_id, local_lv2cmast_id, local_record_type, local_lv2ctree_id);

			INSERT INTO LV3CTREE (LV3CTREE_ID,LV1CMAST_ID, LV2CMAST_ID,LV3CMAST_ID, LV2CTREE_ID, record_type, hier_type)
			values (LV3CTREE_SEQ.NEXTVAL,local_lv1cmast_id, local_lv2cmast_id,
			local_lv3cmast_id, local_lv2ctree_id,local_record_type, l_hier_type);
		END IF;

		SELECT LV2CTREE_ID, LV3CTREE_ID
		INTO local_lv2ctree_id, local_lv3ctree_id
		FROM LV3CTREE
		WHERE LV3CTREE.LV1CMAST_ID = local_lv1cmast_id AND
			LV3CTREE.LV2CMAST_ID = local_lv2cmast_id AND
			LV3CTREE.LV3CMAST_ID = local_lv3cmast_id;
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_LV3CTREE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_LV3CTREE" TO "MAXUSER";
