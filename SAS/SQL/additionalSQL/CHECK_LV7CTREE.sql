--------------------------------------------------------
--  DDL for Procedure CHECK_LV7CTREE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_LV7CTREE" 
(local_lv1cmast_id IN NUMBER, local_lv2cmast_id IN NUMBER, local_lv3cmast_id IN NUMBER, local_lv4cmast_id IN NUMBER,
 local_lv5cmast_id IN NUMBER, local_lv6cmast_id IN NUMBER, local_lv7cmast_id IN NUMBER, local_record_type IN VARCHAR, local_lv2ctree_id OUT NUMBER,
 local_lv3ctree_id OUT NUMBER, local_lv4ctree_id OUT NUMBER, local_lv5ctree_id OUT NUMBER, local_lv6ctree_id OUT NUMBER,
 local_lv7ctree_id OUT NUMBER)

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
		FROM LV7CTREE
		WHERE LV7CTREE.LV1CMAST_ID = local_lv1cmast_id AND
			    LV7CTREE.LV2CMAST_ID = local_lv2cmast_id AND
			    LV7CTREE.LV3CMAST_ID = local_lv3cmast_id AND
			    LV7CTREE.LV4CMAST_ID = local_lv4cmast_id AND
			    LV7CTREE.LV5CMAST_ID = local_lv5cmast_id AND
			    LV7CTREE.LV6CMAST_ID = local_lv6cmast_id AND
			    LV7CTREE.LV7CMAST_ID = local_lv7cmast_id ;
		IF x = 0 THEN
      CHECK_LV6CTREE(local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_lv4cmast_id, local_lv5cmast_id,
                     local_lv6cmast_id, local_record_type, local_lv2ctree_id, local_lv3ctree_id, local_lv4ctree_id, local_lv5ctree_id,
                     local_lv6ctree_id);
		  INSERT INTO LV7CTREE (LV7CTREE_ID, LV1CMAST_ID, LV2CMAST_ID, LV3CMAST_ID, LV4CMAST_ID, LV5CMAST_ID,
                            LV6CMAST_ID, LV7CMAST_ID, LV2CTREE_ID, LV3CTREE_ID, LV4CTREE_ID, LV5CTREE_ID, LV6CTREE_ID, record_type, hier_type)
		  values (LV7CTREE_SEQ.NEXTVAL, local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_lv4cmast_id,
              local_lv5cmast_id, local_lv6cmast_id, local_lv7cmast_id, local_lv2ctree_id, local_lv3ctree_id,
              local_lv4ctree_id, local_lv5ctree_id, local_lv6ctree_id, local_record_type, l_hier_type);
		END IF;

    select LV2CTREE_ID,LV3CTREE_ID,LV4CTREE_ID,LV5CTREE_ID,LV6CTREE_ID,LV7CTREE_ID
    into   local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id,local_lv6ctree_id,local_lv7ctree_id
    from   LV7CTREE
    where LV7CTREE.LV1CMAST_ID=local_lv1cmast_id
      and LV7CTREE.LV2CMAST_ID=local_lv2cmast_id
      and LV7CTREE.LV3CMAST_ID=local_lv3cmast_id
      and LV7CTREE.LV4CMAST_ID=local_lv4cmast_id
      and LV7CTREE.LV5CMAST_ID=local_lv5cmast_id
      and LV7CTREE.LV6CMAST_ID=local_lv6cmast_id
      and LV7CTREE.LV7CMAST_ID=local_lv7cmast_id ;

  END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_LV7CTREE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_LV7CTREE" TO "MAXUSER";
