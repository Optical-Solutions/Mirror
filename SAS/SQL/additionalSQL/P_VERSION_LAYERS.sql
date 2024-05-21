--------------------------------------------------------
--  DDL for Procedure P_VERSION_LAYERS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_VERSION_LAYERS" 
(p_lv4loc_id number, p_basePeriodID number, p_targetPeriodID number, p_CAD_file_name varchar2,
p_new_lv4loc_id in number, p_status out number, p_ErrorMsg  out varchar2)
as
begin

	p_status := 1;
	p_errormsg := 'Version_layer: successful';

end;

/

  GRANT EXECUTE ON "MAXDATA"."P_VERSION_LAYERS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_VERSION_LAYERS" TO "MAXUSER";
