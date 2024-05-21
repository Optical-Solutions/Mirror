--------------------------------------------------------
--  DDL for Procedure P_DM_SET_CL_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DM_SET_CL_STATUS" (change_code char)
 as
/*----------------------------------------------------------------------
$Log: 2164_p_dm_set_cl_status.sql,v $
Revision 1.8  2007/06/19 14:39:35  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2006/02/17 22:18:56  healja
Replace $id with $Log
 2164_p_dm_set_cl_status.sql,v 1.2 2005/07/29 17:38:39 dirapa Exp $

-- Change history:
V6.1
6.1.0-001 07/29/05 Diwakar	Added future parameters for p_set_cl_status procedural call.
-- v5.2.4.3
-- 1/31/2002 Joseph cho	Support different change code.
-- 1/26/2002 Eric Chao	Initial entry.

-- This procedure is executed after each Financial Data Load (i.e. MFINC, MCOMP or other
--   FACT tables). It will check the maxdata.finc_linage table to determinate the periods
--   had been just loaded. It then calls p_set_cl_status with the time_level, and time_id.
--   to flag the clusters been affected.
--   If the period loaded are coing from MCOMP ot other FACT tables,
--       call p_set_cl_status to flag all cluster as obsolete
----------------------------------------------------------------------*/

v_mfinc_timeid number(10,0);
v_max_timelevel integer;
v_row_count integer;

begin

if change_code = 'R' then
	-- It is for reclassification.

	maxdata.p_set_cl_status('C', -1, -1, -1, -1, -1);

elsif change_code = 'F' then
	-- It is for financial load, backload, fact table load.
	-- Get the load type from finc_lineage.

	select count(*) into v_row_count from maxdata.finc_lineage
		where table_id > 50 and status_flag = 'A';

	-- if table_id > 50, means MCOMP or other FACT tables had been just loaded
	-- set all clusters be obsolete

	if v_row_count > 0 then
		maxdata.p_set_cl_status('F',-1,-1,-1,-1,-1);
	else
		-- there only deal with MFINC periods
		select count(*) into v_row_count  from maxdata.finc_lineage
			where table_id between 40 and 49 and status_flag = 'A';

		if v_row_count > 0 then
			select max(time_level) into v_max_timelevel from maxdata.finc_lineage
				where table_id between 40 and 49 and status_flag = 'A';

			-- There should be only one row in the loop, just in case if more records found, the program wouldn't crash
			for c1 in (select time_id from maxdata.finc_lineage
					where table_id between 40 and 49 and status_flag = 'A'
						and time_level = v_max_timelevel) loop
				maxdata.p_set_cl_status('N',c1.time_id,v_max_timelevel,-1,-1,-1);

			end loop;
		end if;
	end if;
else
	raise_application_error (-20001, 'Unsupported change code: '||change_code);
end if;

commit;

end; -- End p_dm_set_cl_status

/
