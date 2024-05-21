--------------------------------------------------------
--  DDL for Procedure P_POP_MERCH_QUEUE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_POP_MERCH_QUEUE" 
(ilv10mast_id IN NUMBER, old_lv10cat_id IN NUMBER, new_lv10cat_id IN NUMBER) AS
BEGIN
  DECLARE
    iMerch_level NUMBER;
    iold_lv1cmast_id NUMBER;
    iold_lv2cmast_id NUMBER;
    iold_lv3cmast_id NUMBER;
    iold_lv4cmast_id NUMBER;
    iold_lv5cmast_id NUMBER;
    iold_lv6cmast_id NUMBER;
    iold_lv7cmast_id NUMBER;
    iold_lv8cmast_id NUMBER;
    iold_lv9cmast_id NUMBER;
    inew_lv1cmast_id NUMBER;
    inew_lv2cmast_id NUMBER;
    inew_lv3cmast_id NUMBER;
    inew_lv4cmast_id NUMBER;
    inew_lv5cmast_id NUMBER;
    inew_lv6cmast_id NUMBER;
    inew_lv7cmast_id NUMBER;
    inew_lv8cmast_id NUMBER;
    inew_lv9cmast_id NUMBER;
   iold_lv2ctree_id NUMBER;
    iold_lv3ctree_id NUMBER;
    iold_lv4ctree_id NUMBER;
    iold_lv5ctree_id NUMBER;
    iold_lv6ctree_id NUMBER;
    iold_lv7ctree_id NUMBER;
    iold_lv8ctree_id NUMBER;
    iold_lv9ctree_id NUMBER;
   inew_lv2ctree_id NUMBER;
    inew_lv3ctree_id NUMBER;
    inew_lv4ctree_id NUMBER;
    inew_lv5ctree_id NUMBER;
    inew_lv6ctree_id NUMBER;
    inew_lv7ctree_id NUMBER;
    inew_lv8ctree_id NUMBER;
    inew_lv9ctree_id NUMBER;




	BEGIN

    SELECT LV10CAT.LV1CMAST_ID, LV10CAT.LV2CMAST_ID, LV10CAT.LV3CMAST_ID, LV10CAT.LV4CMAST_ID, LV10CAT.LV5CMAST_ID,
           LV10CAT.LV6CMAST_ID, LV10CAT.LV7CMAST_ID, LV10CAT.LV8CMAST_ID, LV10CAT.LV9CMAST_ID
    INTO   iold_lv1cmast_id, iold_lv2cmast_id, iold_lv3cmast_id, iold_lv4cmast_id, iold_lv5cmast_id,
           iold_lv6cmast_id, iold_lv7cmast_id, iold_lv8cmast_id, iold_lv9cmast_id
    FROM LV10CAT
    WHERE LV10CAT.LV10CAT_ID = old_lv10cat_id;

maxdata.GET_CTREE_FROM_CMAST_ID
    (iold_lv1cmast_id, iold_lv2cmast_id, iold_lv3cmast_id, iold_lv4cmast_id, iold_lv5cmast_id,
     iold_lv6cmast_id, iold_lv7cmast_id, iold_lv8cmast_id, iold_lv9cmast_id,
     iold_lv2ctree_id, iold_lv3ctree_id, iold_lv4ctree_id, iold_lv5ctree_id,
     iold_lv6ctree_id, iold_lv7ctree_id, iold_lv8ctree_id, iold_lv9ctree_id ) ;


    SELECT LV10CAT.LV1CMAST_ID, LV10CAT.LV2CMAST_ID, LV10CAT.LV3CMAST_ID, LV10CAT.LV4CMAST_ID, LV10CAT.LV5CMAST_ID,
           LV10CAT.LV6CMAST_ID, LV10CAT.LV7CMAST_ID, LV10CAT.LV8CMAST_ID, LV10CAT.LV9CMAST_ID
    INTO   inew_lv1cmast_id, inew_lv2cmast_id, inew_lv3cmast_id, inew_lv4cmast_id, inew_lv5cmast_id,
           inew_lv6cmast_id, inew_lv7cmast_id, inew_lv8cmast_id, inew_lv9cmast_id
    FROM LV10CAT
    WHERE LV10CAT.LV10CAT_ID = new_lv10cat_id;

maxdata.GET_CTREE_FROM_CMAST_ID
    (inew_lv1cmast_id, inew_lv2cmast_id, inew_lv3cmast_id, inew_lv4cmast_id, inew_lv5cmast_id,
     inew_lv6cmast_id, inew_lv7cmast_id, inew_lv8cmast_id, inew_lv9cmast_id,
     inew_lv2ctree_id, inew_lv3ctree_id, inew_lv4ctree_id, inew_lv5ctree_id,
     inew_lv6ctree_id, inew_lv7ctree_id, inew_lv8ctree_id, inew_lv9ctree_id ) ;

if (iold_lv1cmast_id <> inew_lv1cmast_id) AND ( iold_lv1cmast_id is not null or inew_lv1cmast_id is not null) Then

		Insert into maxdata.change_queue_merch
		( lv10mast_id, merch_level, old_merch_id, new_merch_id, Status_flag)
		VAlues ( ilv10mast_id, 1, iold_lv1cmast_id, inew_lv1cmast_id, 'C');

END IF;
if (iold_lv2ctree_id <> inew_lv2ctree_id) AND ( iold_lv2ctree_id is not null or inew_lv2ctree_id is not null) Then

		Insert into maxdata.change_queue_merch
		( lv10mast_id, merch_level, old_merch_id, new_merch_id, Status_flag)
		VAlues ( ilv10mast_id, 2, iold_lv2ctree_id, inew_lv2ctree_id, 'C');

END IF;
if (iold_lv3ctree_id <> inew_lv3ctree_id) AND ( iold_lv3ctree_id is not null or inew_lv3ctree_id is not null) Then

		Insert into maxdata.change_queue_merch
		( lv10mast_id, merch_level, old_merch_id, new_merch_id, Status_flag)
		VAlues ( ilv10mast_id, 3, iold_lv3ctree_id, inew_lv3ctree_id, 'C');

END IF;

if (iold_lv4ctree_id <> inew_lv4ctree_id) AND ( iold_lv4ctree_id is not null or inew_lv4ctree_id is not null) Then

		Insert into maxdata.change_queue_merch
		( lv10mast_id, merch_level, old_merch_id, new_merch_id, Status_flag)
		VAlues ( ilv10mast_id, 4, iold_lv4ctree_id, inew_lv4ctree_id, 'C');

END IF;

if (iold_lv5ctree_id <> inew_lv5ctree_id) AND ( iold_lv5ctree_id is not null or inew_lv5ctree_id is not null) Then

		Insert into maxdata.change_queue_merch
		( lv10mast_id, merch_level, old_merch_id, new_merch_id, Status_flag)
		VAlues ( ilv10mast_id, 5, iold_lv5ctree_id, inew_lv5ctree_id, 'C');

END IF;

if (iold_lv6ctree_id <> inew_lv6ctree_id) AND ( iold_lv6ctree_id is not null or inew_lv6ctree_id is not null) Then

		Insert into maxdata.change_queue_merch
		( lv10mast_id, merch_level, old_merch_id, new_merch_id, Status_flag)
		VAlues ( ilv10mast_id, 6, iold_lv6ctree_id, inew_lv6ctree_id, 'C');

END IF;

if (iold_lv7ctree_id <> inew_lv7ctree_id) AND ( iold_lv7ctree_id is not null or inew_lv7ctree_id is not null) Then

		Insert into maxdata.change_queue_merch
		( lv10mast_id, merch_level, old_merch_id, new_merch_id, Status_flag)
		VAlues ( ilv10mast_id, 7, iold_lv7ctree_id, inew_lv7ctree_id, 'C');

END IF;

if (iold_lv8ctree_id <> inew_lv8ctree_id) AND ( iold_lv8ctree_id is not null or inew_lv8ctree_id is not null) Then

		Insert into maxdata.change_queue_merch
		( lv10mast_id, merch_level, old_merch_id, new_merch_id, Status_flag)
		VAlues ( ilv10mast_id, 8, iold_lv8ctree_id, inew_lv8ctree_id, 'C');

END IF;

if (iold_lv9ctree_id <> inew_lv9ctree_id) AND ( iold_lv9ctree_id is not null or inew_lv9ctree_id is not null) Then

		Insert into maxdata.change_queue_merch
		( lv10mast_id, merch_level, old_merch_id, new_merch_id, Status_flag)
		VAlues ( ilv10mast_id, 9, iold_lv9ctree_id, inew_lv9ctree_id, 'C');

END IF;



  END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_POP_MERCH_QUEUE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_POP_MERCH_QUEUE" TO "MAXUSER";
