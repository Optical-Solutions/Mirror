--------------------------------------------------------
--  DDL for Procedure P_ACTIVATE_LAYERS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_ACTIVATE_LAYERS" 
(p_current_lv4loc_id number, p_version_lv4loc_id number, p_outgoing_version_id in number,p_status out number, p_ErrorMsg out varchar2)
as

begin

	p_status := 1;
	p_errormsg := 'Activate_layer: successful';

end;

/

  GRANT EXECUTE ON "MAXDATA"."P_ACTIVATE_LAYERS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_ACTIVATE_LAYERS" TO "MAXUSER";
