--------------------------------------------------------
--  DDL for Procedure P_UPD_CTREE_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_UPD_CTREE_ID" (New_lv10mast_id number, New_lv10cat_id number)
as

  local_lv1cmast_id numeric(10);
  local_lv2cmast_id numeric(10);
  local_lv3cmast_id numeric(10);
  local_lv4cmast_id numeric(10);
  local_lv5cmast_id numeric(10);
  local_lv6cmast_id numeric(10);
  local_lv7cmast_id numeric(10);
  local_lv8cmast_id numeric(10);
  local_lv9cmast_id numeric(10);

  local_lv2ctree_id numeric(10);
  local_lv3ctree_id numeric(10);
  local_lv4ctree_id numeric(10);
  local_lv5ctree_id numeric(10);
  local_lv6ctree_id numeric(10);
  local_lv7ctree_id numeric(10);
  local_lv8ctree_id numeric(10);
  local_lv9ctree_id numeric(10);

begin

  select LV1CMAST_ID,LV2CMAST_ID,LV3CMAST_ID,LV4CMAST_ID,LV5CMAST_ID,
         LV6CMAST_ID,LV7CMAST_ID,LV8CMAST_ID,LV9CMAST_ID
  into   local_lv1cmast_id,local_lv2cmast_id,local_lv3cmast_id,local_lv4cmast_id,
         local_lv5cmast_id,local_lv6cmast_id,local_lv7cmast_id,local_lv8cmast_id,local_lv9cmast_id
  from   maxdata.LV10CAT
  where  LV10CAT_ID=new_lv10cat_id;

  if  local_lv9cmast_id is not null and
      local_lv8cmast_id is not null and
      local_lv7cmast_id is not null and
      local_lv6cmast_id is not null and
      local_lv5cmast_id is not null and
      local_lv4cmast_id is not null and
      local_lv3cmast_id is not null and
      local_lv2cmast_id is not null and
      local_lv1cmast_id is not null then

    select LV2CTREE_ID,LV3CTREE_ID,LV4CTREE_ID,LV5CTREE_ID,LV6CTREE_ID,
           LV7CTREE_ID,LV8CTREE_ID,LV9CTREE_ID
    into   local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id,
           local_lv6ctree_id,local_lv7ctree_id,local_lv8ctree_id,local_lv9ctree_id
    from   maxdata.LV9CTREE
    where  LV1CMAST_ID=local_lv1cmast_id and
           LV2CMAST_ID=local_lv2cmast_id and
           LV3CMAST_ID=local_lv3cmast_id and
           LV4CMAST_ID=local_lv4cmast_id and
           LV5CMAST_ID=local_lv5cmast_id and
           LV6CMAST_ID=local_lv6cmast_id and
           LV7CMAST_ID=local_lv7cmast_id and
           LV8CMAST_ID=local_lv8cmast_id and
           LV9CMAST_ID=local_lv9cmast_id ;

  elsif  local_lv8cmast_id is not null and
         local_lv7cmast_id is not null and
         local_lv6cmast_id is not null and
         local_lv5cmast_id is not null and
         local_lv4cmast_id is not null and
         local_lv3cmast_id is not null and
         local_lv2cmast_id is not null and
         local_lv1cmast_id is not null then

    select LV2CTREE_ID,LV3CTREE_ID,LV4CTREE_ID,LV5CTREE_ID,LV6CTREE_ID,
           LV7CTREE_ID,LV8CTREE_ID
    into   local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id,
           local_lv6ctree_id,local_lv7ctree_id,local_lv8ctree_id
    from   maxdata.LV8CTREE
    where  LV1CMAST_ID=local_lv1cmast_id and
           LV2CMAST_ID=local_lv2cmast_id and
           LV3CMAST_ID=local_lv3cmast_id and
           LV4CMAST_ID=local_lv4cmast_id and
           LV5CMAST_ID=local_lv5cmast_id and
           LV6CMAST_ID=local_lv6cmast_id and
           LV7CMAST_ID=local_lv7cmast_id and
           LV8CMAST_ID=local_lv8cmast_id ;

  elsif  local_lv7cmast_id is not null and
         local_lv6cmast_id is not null and
         local_lv5cmast_id is not null and
         local_lv4cmast_id is not null and
         local_lv3cmast_id is not null and
         local_lv2cmast_id is not null and
         local_lv1cmast_id is not null then

    select LV2CTREE_ID,LV3CTREE_ID,LV4CTREE_ID,LV5CTREE_ID,LV6CTREE_ID,LV7CTREE_ID
    into   local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id,
           local_lv6ctree_id,local_lv7ctree_id
    from   maxdata.LV7CTREE
    where  LV1CMAST_ID=local_lv1cmast_id and
           LV2CMAST_ID=local_lv2cmast_id and
           LV3CMAST_ID=local_lv3cmast_id and
           LV4CMAST_ID=local_lv4cmast_id and
           LV5CMAST_ID=local_lv5cmast_id and
           LV6CMAST_ID=local_lv6cmast_id and
           LV7CMAST_ID=local_lv7cmast_id ;

  elsif  local_lv6cmast_id is not null and
         local_lv5cmast_id is not null and
         local_lv4cmast_id is not null and
         local_lv3cmast_id is not null and
         local_lv2cmast_id is not null and
         local_lv1cmast_id is not null then

    select LV2CTREE_ID,LV3CTREE_ID,LV4CTREE_ID,LV5CTREE_ID,LV6CTREE_ID
    into   local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id,
           local_lv6ctree_id
    from   maxdata.LV6CTREE
    where  LV1CMAST_ID=local_lv1cmast_id and
           LV2CMAST_ID=local_lv2cmast_id and
           LV3CMAST_ID=local_lv3cmast_id and
           LV4CMAST_ID=local_lv4cmast_id and
           LV5CMAST_ID=local_lv5cmast_id and
           LV6CMAST_ID=local_lv6cmast_id ;

  elsif  local_lv5cmast_id is not null and
         local_lv4cmast_id is not null and
         local_lv3cmast_id is not null and
         local_lv2cmast_id is not null and
         local_lv1cmast_id is not null then

    select LV2CTREE_ID,LV3CTREE_ID,LV4CTREE_ID,LV5CTREE_ID
    into   local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id
    from   maxdata.LV5CTREE
    where  LV1CMAST_ID=local_lv1cmast_id and
           LV2CMAST_ID=local_lv2cmast_id and
           LV3CMAST_ID=local_lv3cmast_id and
           LV4CMAST_ID=local_lv4cmast_id and
           LV5CMAST_ID=local_lv5cmast_id ;

  elsif  local_lv4cmast_id is not null and
         local_lv3cmast_id is not null and
         local_lv2cmast_id is not null and
         local_lv1cmast_id is not null then

    select LV2CTREE_ID,LV3CTREE_ID,LV4CTREE_ID
    into   local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id
    from   maxdata.LV4CTREE
    where  LV1CMAST_ID=local_lv1cmast_id and
           LV2CMAST_ID=local_lv2cmast_id and
           LV3CMAST_ID=local_lv3cmast_id and
           LV4CMAST_ID=local_lv4cmast_id ;

  elsif  local_lv3cmast_id is not null and
         local_lv2cmast_id is not null and
         local_lv1cmast_id is not null then

    select LV2CTREE_ID,LV3CTREE_ID
    into   local_lv2ctree_id,local_lv3ctree_id
    from   maxdata.LV3CTREE
    where  LV1CMAST_ID=local_lv1cmast_id and
           LV2CMAST_ID=local_lv2cmast_id and
           LV3CMAST_ID=local_lv3cmast_id ;

  elsif  local_lv2cmast_id is not null and
         local_lv1cmast_id is not null then

    select LV2CTREE_ID
    into   local_lv2ctree_id
    from   maxdata.LV2CTREE
    where  LV1CMAST_ID=local_lv1cmast_id and
           LV2CMAST_ID=local_lv2cmast_id ;

  end if ;

  update maxdata.lv10mast
     set lv1cmast_id = local_lv1cmast_id,
         lv2ctree_id = local_lv2ctree_id,
         lv3ctree_id = local_lv3ctree_id,
         lv4ctree_id = local_lv4ctree_id,
         lv5ctree_id = local_lv5ctree_id,
         lv6ctree_id = local_lv6ctree_id,
         lv7ctree_id = local_lv7ctree_id,
         lv8ctree_id = local_lv8ctree_id,
         lv9ctree_id = local_lv9ctree_id
   where lv10mast_id = new_lv10mast_id ;
end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_UPD_CTREE_ID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_UPD_CTREE_ID" TO "MAXUSER";
