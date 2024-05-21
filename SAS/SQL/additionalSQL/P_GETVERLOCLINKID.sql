--------------------------------------------------------
--  DDL for Procedure P_GETVERLOCLINKID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GETVERLOCLINKID" 
(p_location_level number, p_location_id number, p_isVersionLocation out char,
p_OutPut_location_id  out number, p_status out int ,  -- 0 for success , 1 for failure only in this proc. other store Ver procs have reverse
p_ErrorMsg out varchar2)
as

	businessException exception;

	t_Error integer;
	t_ErrorStr varchar2(255);
	t_parent_lv1_id number(10,0);
	t_ver_lv1loc_id number(10,0);
	t_ver_lv2loc_id number(10,0);
	t_ver_lv3loc_id number(10,0);
	t_status_local integer;
	t_link_id number(10,0);


begin

-- Store versioning is applicable to level 4 to 7. If any other level is sent to the proc it returns the sent id as the output.
-- This is done to accomodate generalization

	Maxdata.p_get_ver_parents (t_ver_lv1loc_id, t_ver_lv2loc_id, t_ver_lv3loc_id, t_status_local, t_ErrorStr);

	if (t_status_local = 0) then

		t_ErrorStr := 'Failed to find Version company hierarchy' ;
		raise businessException;
	end if;

	if (p_location_level = 1 ) then
		p_OutPut_location_id := p_location_id;
	end if;

	if (p_location_level = 2 ) then
		p_OutPut_location_id := p_location_id;
	end if;

	if (p_location_level = 3 ) then
		p_OutPut_location_id := p_location_id;
	end if;


	if (p_location_level = 4 ) then

		select lv1loc_id, ver_loc_link_id  into t_parent_lv1_id, t_link_id
		from maxdata.lv4loc
		where lv4loc_id = p_location_id;

		if (t_parent_lv1_id = t_ver_lv1loc_id) then

			p_isVersionLocation := 'Y';
			p_OutPut_location_id := t_link_id;
		else
			p_isVersionLocation := 'N';
			p_OutPut_location_id := p_location_id;
		end if;

	end if;

	if (p_location_level = 5 ) then

		Select lv1loc_id, ver_loc_link_id into t_parent_lv1_id, t_link_id
		from maxdata.lv5loc
		where lv5loc_id = p_location_id;

		if (t_parent_lv1_id = t_ver_lv1loc_id) then

			p_isVersionLocation := 'Y';
			p_OutPut_location_id := t_link_id;
		else
			p_isVersionLocation := 'N';
			p_OutPut_location_id := P_location_id;
		end if;

	end if;

	if (p_location_level = 6 ) then

		Select lv1loc_id, ver_loc_link_id into t_parent_lv1_id, t_link_id
		from maxdata.lv6loc
		where lv6loc_id = p_location_id;

		if (t_parent_lv1_id = t_ver_lv1loc_id) then

			p_isVersionLocation := 'Y';
			p_OutPut_location_id := t_link_id;
		else
			p_isVersionLocation := 'N';
			p_OutPut_location_id := p_location_id;
		end if;

	end if;


	if (p_location_level = 7 ) then

		select lv1loc_id, ver_loc_link_id  into t_parent_lv1_id, t_link_id
		from maxdata.lv7loc
		where lv7loc_id = p_location_id;

		if (t_parent_lv1_id = t_ver_lv1loc_id) then

			p_isVersionLocation := 'Y';
			p_OutPut_location_id := t_link_id;

		else
			p_isVersionLocation := 'N';
			p_OutPut_location_id := p_location_id;
		end if;

	end if;

	if (p_location_level = 8) then

		p_OutPut_location_id := p_location_id;
	end if;

	if (p_location_level = 9 ) then

		p_OutPut_location_id := p_location_id;
	end if;

	if (p_location_level = 10 ) then

		p_OutPut_location_id := p_location_id;
	end if;

	p_status := 0;


Exception
	when businessException then
		p_ErrorMsg := 'p_getverloc: ' || t_ErrorStr;
		p_status := 1;

	when others then
		p_status := 1;
		p_ErrorMsg :=  'p_getverloc: ' || SQLERRM;

end;  -- End of procedure p_getVerLocLinkID

/

  GRANT EXECUTE ON "MAXDATA"."P_GETVERLOCLINKID" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_GETVERLOCLINKID" TO "MAXUSER";
