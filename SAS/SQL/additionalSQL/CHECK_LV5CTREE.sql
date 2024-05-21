--------------------------------------------------------
--  DDL for Procedure CHECK_LV5CTREE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_LV5CTREE" (local_lv1cmast_id IN NUMBER, local_lv2cmast_id IN NUMBER, local_lv3cmast_id IN NUMBER,
			local_lv4cmast_id IN NUMBER, local_lv5cmast_id IN NUMBER, local_record_type IN VARCHAR,
			local_lv2ctree_id OUT NUMBER, local_lv3ctree_id OUT NUMBER, local_lv4ctree_id OUT NUMBER, local_lv5ctree_id OUT NUMBER)
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
		FROM LV5CTREE
		WHERE LV5CTREE.LV1CMAST_ID = local_lv1cmast_id AND
			LV5CTREE.LV2CMAST_ID = local_lv2cmast_id AND
			LV5CTREE.LV3CMAST_ID = local_lv3cmast_id AND
			LV5CTREE.LV4CMAST_ID = local_lv4cmast_id AND
			LV5CTREE.LV5CMAST_ID = local_lv5cmast_id;
		IF x = 0 THEN
			CHECK_LV4CTREE(local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id,
				         local_lv4cmast_id, local_record_type, local_lv2ctree_id, local_lv3ctree_id,
					   local_lv4ctree_id);

			INSERT INTO LV5CTREE (LV5CTREE_ID, LV1CMAST_ID, LV2CMAST_ID, LV3CMAST_ID, LV4CMAST_ID, 						LV5CMAST_ID, LV2CTREE_ID, LV3CTREE_ID,
			  			    LV4CTREE_ID, record_type, hier_type)
			values (LV5CTREE_SEQ.NEXTVAL, local_lv1cmast_id, local_lv2cmast_id,
				  local_lv3cmast_id, local_lv4cmast_id, local_lv5cmast_id,
				  local_lv2ctree_id, local_lv3ctree_id, local_lv4ctree_id, local_record_type, l_hier_type);
		END IF;

		SELECT LV2CTREE_ID, LV3CTREE_ID, LV4CTREE_ID, LV5CTREE_ID
		INTO local_lv2ctree_id, local_lv3ctree_id, local_lv4ctree_id, local_lv5ctree_id
		FROM LV5CTREE
		WHERE LV5CTREE.LV1CMAST_ID = local_lv1cmast_id AND
			LV5CTREE.LV2CMAST_ID = local_lv2cmast_id AND
			LV5CTREE.LV3CMAST_ID = local_lv3cmast_id AND
			LV5CTREE.LV4CMAST_ID = local_lv4cmast_id AND
			LV5CTREE.LV5CMAST_ID = local_lv5cmast_id;
	END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_LV5CTREE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_LV5CTREE" TO "MAXUSER";
