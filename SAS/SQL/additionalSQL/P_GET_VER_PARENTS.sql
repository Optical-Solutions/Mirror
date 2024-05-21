--------------------------------------------------------
--  DDL for Procedure P_GET_VER_PARENTS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GET_VER_PARENTS" 
(p_lv1loc_id out number, p_lv2loc_id out number, p_lv3loc_id out number,p_status out number, p_ErrorMsg out varchar2)
as


	t_ErrorNo integer;
	t_lv1loc_userid varchar2(20);
	t_lv2loc_userid varchar2(20);
	t_lv3loc_userid varchar2(20);
	t_ErrorStr varchar2(255);

begin

	t_lv1loc_userid := 'Version Company';

	t_lv2loc_userid := 'Version Region';

	t_lv3loc_userid := 'Version District';

	Select lv1loc_id into p_lv1loc_id from maxdata.lv1loc
	where lv1loc_userid = t_lv1loc_userid;

	Select lv2loc_id into p_lv2loc_id from maxdata.lv2loc
	where lv2loc_userid = t_lv2loc_userid;

	Select lv3loc_id into p_lv3loc_id from maxdata.lv3loc
	where lv3loc_userid = t_lv3loc_userid;

	p_Status := 1;
	p_ErrorMsg := 'Get_ver: successful';

exception
	when No_data_found then
		p_Status := 0;
		p_ErrorMsg:='Version Hierarchy is missing';

	when others then
		t_ErrorNo := SQLCODE;
		p_ErrorMsg := SQLERRM;
		p_Status := 0;

end;

/

  GRANT EXECUTE ON "MAXDATA"."P_GET_VER_PARENTS" TO "MAXAPP";
  GRANT EXECUTE ON "MAXDATA"."P_GET_VER_PARENTS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_GET_VER_PARENTS" TO "MAXUSER";
