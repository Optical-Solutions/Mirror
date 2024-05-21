--------------------------------------------------------
--  DDL for Procedure INSERT_CTREE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."INSERT_CTREE" 
( local_lv1cmast_id IN NUMBER, local_lv2cmast_id IN NUMBER, local_lv3cmast_id IN NUMBER, local_lv4cmast_id IN NUMBER,
  local_lv5cmast_id IN NUMBER, local_lv6cmast_id IN NUMBER, local_lv7cmast_id IN NUMBER, local_lv8cmast_id IN NUMBER,
  local_lv9cmast_id IN NUMBER, local_record_type IN VARCHAR)
AS

BEGIN
  DECLARE
    dummy_id NUMBER;
  BEGIN
    IF    local_lv9cmast_id IS NOT NULL AND
          local_lv8cmast_id IS NOT NULL AND
          local_lv7cmast_id IS NOT NULL AND
          local_lv6cmast_id IS NOT NULL AND
          local_lv5cmast_id IS NOT NULL AND
          local_lv4cmast_id IS NOT NULL AND
          local_lv3cmast_id IS NOT NULL AND
          local_lv2cmast_id IS NOT NULL AND
          local_lv1cmast_id IS NOT NULL THEN
          CHECK_LV9CTREE(local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_lv4cmast_id, local_lv5cmast_id,
                         local_lv6cmast_id, local_lv7cmast_id, local_lv8cmast_id, local_lv9cmast_id, local_record_type,
                         dummy_id, dummy_id,dummy_id, dummy_id, dummy_id, dummy_id, dummy_id, dummy_id );
    ELSIF local_lv8cmast_id IS NOT NULL AND
          local_lv7cmast_id IS NOT NULL AND
          local_lv6cmast_id IS NOT NULL AND
          local_lv5cmast_id IS NOT NULL AND
          local_lv4cmast_id IS NOT NULL AND
          local_lv3cmast_id IS NOT NULL AND
          local_lv2cmast_id IS NOT NULL AND
          local_lv1cmast_id IS NOT NULL THEN
          CHECK_LV8CTREE(local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_lv4cmast_id,
                         local_lv5cmast_id, local_lv6cmast_id, local_lv7cmast_id, local_lv8cmast_id,local_record_type,
                         dummy_id, dummy_id, dummy_id, dummy_id, dummy_id, dummy_id, dummy_id );
    ELSIF local_lv7cmast_id IS NOT NULL AND
          local_lv6cmast_id IS NOT NULL AND
          local_lv5cmast_id IS NOT NULL AND
          local_lv4cmast_id IS NOT NULL AND
          local_lv3cmast_id IS NOT NULL AND
          local_lv2cmast_id IS NOT NULL AND
          local_lv1cmast_id IS NOT NULL THEN
          CHECK_LV7CTREE(local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_lv4cmast_id,
                         local_lv5cmast_id, local_lv6cmast_id, local_lv7cmast_id,local_record_type, dummy_id, dummy_id,
                         dummy_id, dummy_id, dummy_id, dummy_id );
    ELSIF local_lv6cmast_id IS NOT NULL AND
          local_lv5cmast_id IS NOT NULL AND
          local_lv4cmast_id IS NOT NULL AND
          local_lv3cmast_id IS NOT NULL AND
          local_lv2cmast_id IS NOT NULL AND
          local_lv1cmast_id IS NOT NULL THEN
          CHECK_LV6CTREE(local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_lv4cmast_id,
                         local_lv5cmast_id, local_lv6cmast_id, local_record_type,dummy_id, dummy_id, dummy_id, dummy_id, dummy_id );
    ELSIF local_lv5cmast_id IS NOT NULL AND
          local_lv4cmast_id IS NOT NULL AND
          local_lv3cmast_id IS NOT NULL AND
          local_lv2cmast_id IS NOT NULL AND
          local_lv1cmast_id IS NOT NULL THEN
          CHECK_LV5CTREE(local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_lv4cmast_id,
                         local_lv5cmast_id, local_record_type, dummy_id, dummy_id, dummy_id, dummy_id);
    ELSIF local_lv4cmast_id IS NOT NULL AND
          local_lv3cmast_id IS NOT NULL AND
          local_lv2cmast_id IS NOT NULL AND
          local_lv1cmast_id IS NOT NULL THEN
          CHECK_LV4CTREE(local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_lv4cmast_id,local_record_type,
                         dummy_id, dummy_id, dummy_id);
    ELSIF local_lv3cmast_id IS NOT NULL AND
          local_lv2cmast_id IS NOT NULL AND
          local_lv1cmast_id IS NOT NULL THEN
          CHECK_LV3CTREE(local_lv1cmast_id, local_lv2cmast_id, local_lv3cmast_id, local_record_type, dummy_id, dummy_id);
    ELSIF local_lv1cmast_id IS NOT NULL AND
          local_lv2cmast_id IS NOT NULL THEN
          CHECK_LV2CTREE(local_lv1cmast_id, local_lv2cmast_id, local_record_type, dummy_id);
    END IF;
  END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."INSERT_CTREE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."INSERT_CTREE" TO "MAXUSER";
