--------------------------------------------------------
--  DDL for Procedure P_POP_LOC_QUEUE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_POP_LOC_QUEUE" (ilv4loc_id IN NUMBER, ilocation_level IN NUMBER, old_location_id IN NUMBER, new_location_id IN NUMBER)
AS
BEGIN
insert into maxdata.change_queue_loc (
	lv4loc_id,
	last_update,
	location_level,
	old_location_id,
	new_location_id,
	Status_flag)
Values(
	ilv4loc_id,
	sysdate,
	ilocation_level,
	old_location_id,
	new_location_id,
	'C'
	);
END;

/
