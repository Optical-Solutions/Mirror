--------------------------------------------------------
--  DDL for Procedure CHECK_LV9CTREE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."CHECK_LV9CTREE" 
(local_lv1cmast_id IN NUMBER, local_lv2cmast_id IN NUMBER, local_lv3cmast_id IN NUMBER, local_lv4cmast_id IN NUMBER,
 local_lv5cmast_id IN NUMBER, local_lv6cmast_id IN NUMBER, local_lv7cmast_id IN NUMBER, local_lv8cmast_id IN NUMBER,
 local_lv9cmast_id IN NUMBER, local_record_type IN VARCHAR, local_lv2ctree_id OUT NUMBER, local_lv3ctree_id OUT NUMBER, local_lv4ctree_id OUT NUMBER, local_lv5ctree_id OUT NUMBER,
 local_lv6ctree_id OUT NUMBER, local_lv7ctree_id OUT NUMBER, local_lv8ctree_id OUT NUMBER, local_lv9ctree_id OUT NUMBER) AS
BEGIN
		DECLARE x NUMBER;
			l_hier_type number;
		BEGIN

        	Select hier_type into l_hier_type
        	from maxdata.lv1cmast
        	where lv1cmast_id =  local_lv1cmast_id;

		SELECT COUNT(*)
		INTO x
		FROM LV9CTREE
		WHERE LV9CTREE.LV1CMAST_ID = local_lv1cmast_id AND
			    LV9CTREE.LV2CMAST_ID = local_lv2cmast_id AND
			    LV9CTREE.LV3CMAST_ID = local_lv3cmast_id AND
			    LV9CTREE.LV4CMAST_ID = local_lv4cmast_id AND
			    LV9CTREE.LV5CMAST_ID = local_lv5cmast_id AND
			    LV9CTREE.LV6CMAST_ID = local_lv6cmast_id AND
			    LV9CTREE.LV7CMAST_ID = local_lv7cmast_id AND
			    LV9CTREE.LV8CMAST_ID = local_lv8cmast_id AND
			    LV9CTREE.LV9CMAST_ID = local_lv9cmast_id ;
		IF x = 0 THEN
      CHECK_LV8CTREE(local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_lv4cmast_id, local_lv5cmast_id,
                     local_lv6cmast_id, local_lv7cmast_id, local_lv8cmast_id, local_record_type, local_lv2ctree_id, local_lv3ctree_id,
                     local_lv4ctree_id, local_lv5ctree_id, local_lv6ctree_id, local_lv7ctree_id, local_lv8ctree_id);
		  INSERT INTO LV9CTREE (LV9CTREE_ID, LV1CMAST_ID, LV2CMAST_ID, LV3CMAST_ID, LV4CMAST_ID, LV5CMAST_ID, LV6CMAST_ID,
                            LV7CMAST_ID, LV8CMAST_ID, LV9CMAST_ID, LV2CTREE_ID, LV3CTREE_ID, LV4CTREE_ID, LV5CTREE_ID,
                            LV6CTREE_ID, LV7CTREE_ID, LV8CTREE_ID, record_type, hier_type)
		  values (LV9CTREE_SEQ.NEXTVAL, local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_lv4cmast_id,
              local_lv5cmast_id, local_lv6cmast_id, local_lv7cmast_id, local_lv8cmast_id, local_lv9cmast_id,
              local_lv2ctree_id, local_lv3ctree_id, local_lv4ctree_id, local_lv5ctree_id, local_lv6ctree_id,
              local_lv7ctree_id, local_lv8ctree_id, local_record_type, l_hier_type);
		END IF;

    select LV2CTREE_ID,LV3CTREE_ID,LV4CTREE_ID,LV5CTREE_ID,LV6CTREE_ID,LV7CTREE_ID,LV8CTREE_ID,LV9CTREE_ID
    into   local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id,local_lv6ctree_id,local_lv7ctree_id,
           local_lv8ctree_id,local_lv9ctree_id
    from   LV9CTREE
    where LV9CTREE.LV1CMAST_ID=local_lv1cmast_id
      and LV9CTREE.LV2CMAST_ID=local_lv2cmast_id
      and LV9CTREE.LV3CMAST_ID=local_lv3cmast_id
      and LV9CTREE.LV4CMAST_ID=local_lv4cmast_id
      and LV9CTREE.LV5CMAST_ID=local_lv5cmast_id
      and LV9CTREE.LV6CMAST_ID=local_lv6cmast_id
      and LV9CTREE.LV7CMAST_ID=local_lv7cmast_id
      and LV9CTREE.LV8CMAST_ID=local_lv8cmast_id
      and LV9CTREE.LV9CMAST_ID=local_lv9cmast_id ;

  END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."CHECK_LV9CTREE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."CHECK_LV9CTREE" TO "MAXUSER";
