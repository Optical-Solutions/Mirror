--------------------------------------------------------
--  DDL for Procedure P_DELETE_VERSION
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DELETE_VERSION" (p_version_lv4loc_id number, p_status out number, p_ErrorMsg out varchar2)
as

--Change history:
-- V 5.3.4
-- 03/20/2003  DR    Bug# 15149. removed commit and rollback statements


	BusinessException exception;

	t_lv1loc_id number(10,0);
	t_lv2loc_id number(10,0);
	t_lv3loc_id number(10,0);
 	t_ver_lv1loc_id number(10,0);
	t_ver_lv2loc_id number(10,0);
	t_ver_lv3loc_id number(10,0);
	t_tmp_lv4loc_id number(10,0);
	t_Status_from_called integer;
	t_ErrorStr varchar2(255);
	t_cnt number(10);
	t_Error integer;

begin

-- Basic validations start here.

	if (p_version_lv4loc_id is null) then

		t_ErrorStr := 'Store version ID Can not be null';
		raise BusinessException;
	end if;

	select count(*) into t_cnt
	from maxdata.lv4loc
	where lv4loc_id = p_version_lv4loc_id;

	if t_cnt = 0 then
		t_ErrorStr := 'lv4loc row not found. ID:' || p_version_lv4loc_id;
		raise BusinessException;
	end	if;

	select lv1loc_id, lv2loc_id, lv3loc_id, lv4loc_id  into t_lv1loc_id, t_lv2loc_id, t_lv3loc_id, t_tmp_lv4loc_id
	from maxdata.lv4loc
	where lv4loc_id = p_version_lv4loc_id;

	if (t_lv1loc_id is null) then

		t_ErrorStr := 'Store/version Does not have valid parent at Level 1';
		raise BusinessException;
	end if;

	if (t_lv2loc_id is null) then

		t_ErrorStr := 'Store/version Does not have valid parent at Level 2';
		raise BusinessException;
	end if;

	if (t_lv3loc_id is null) then

		t_ErrorStr := 'Store/version Does not have valid parent at Level 3';
		raise BusinessException;
	end if;

	maxdata.p_get_ver_parents ( t_ver_lv1loc_id, t_ver_lv2loc_id, t_ver_lv3loc_id, t_Status_from_called, t_ErrorStr);

	if (t_ver_lv1loc_id is null) then

		t_ErrorStr := 'failed to Locate version hierarchy at level 1';
		raise BusinessException;
	end if;

	if (t_ver_lv2loc_id is null) then

		t_ErrorStr := 'failed to Locate version hierarchy at level 2';
		raise BusinessException;
	end if;

	if (t_ver_lv3loc_id is null) then

		t_ErrorStr := 'failed to Locate version hierarchy at level 3';
		raise BusinessException;
	end if;

	if (t_ver_lv1loc_id <> t_lv1loc_id) then
		t_ErrorStr := 'Store ID specified Does not belong to Version Hierarchy: validation failed at level 1';
		raise BusinessException;
	end if;

	if (t_ver_lv2loc_id <> t_lv2loc_id) then

		t_ErrorStr := 'Store ID specified Does not belong to Version Hierarchy: validation failed at level 2';
		raise BusinessException;
	end if;

	if ( t_ver_lv3loc_id <> t_lv3loc_id) then

		t_ErrorStr := 'Store ID specified Does not belong to Version Hierarchy: validation failed at level 3';
		raise BusinessException;
	end if;

-- Logic for delete.

	delete from maxdata.lv7loc
	where lv4loc_id = p_version_lv4loc_id;

	delete from maxdata.lv6loc
	where lv4loc_id = p_version_lv4loc_id;

	delete from maxdata.lv5loc
	where lv4loc_id = p_version_lv4loc_id;

	delete from maxdata.lv4loc
	where lv4loc_id = p_version_lv4loc_id;


	p_status := 1; -- no error yet.
	p_ErrorMsg := 'Delete: successful';

exception
	when BusinessException then

		p_status := 0;
		p_errorMsg := 'Delete: ' || t_errorStr
				|| '; Params: '
				|| p_version_lv4loc_id;

	when others then

		p_status := 0;
		p_errorMsg := 'Delete: ' ||  SQLERRM
				|| '; Params: '
				|| p_version_lv4loc_id;
end;

/

  GRANT EXECUTE ON "MAXDATA"."P_DELETE_VERSION" TO "MAXUSER";
  GRANT EXECUTE ON "MAXDATA"."P_DELETE_VERSION" TO "MADMAX";
