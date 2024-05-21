--------------------------------------------------------
--  DDL for Procedure P_ACTIVATE_VERSION
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_ACTIVATE_VERSION" 
(p_current_lv4loc_id number, p_version_lv4loc_id number, p_outgoing_version_id out number,
p_status out number, p_ErrorMsg out varchar2)
as

/* -- Change history

$Log: 4152_p_activate_version.sql,v $
Revision 1.1.8.1.12.1  2008/10/30 19:21:54  vejang
checked in from 612 CD location on D12176

Revision 1.2  2006/01/18 15:25:43  raabuh
Apply 5.6.1 and 5.6.2 space scripts


--5.6.1	2/8/05	helmi 	issue 17469	adding delete_flag to the where clause in level7 in addition to placed flag.
--
-- V 5.3.9       RG	   Bug #16015 Allow missing levels for location 2 and 3.
-- V 5.3.4
-- 03/20/2003  DR	   Bug# 9968. Made lvxloc_userid to fit within the length of 20 by truncating the name and append with actual lvx_id (where x is 5,6,7).
-- 03/20/2003  DR    Bug# 15149. removed commit and rollback statements
-- V
-- 05/07/01	Joseph 	Bugfix: replace trim with ltrim(rtrim).
-- 3/19/01 Joseph	Append 'locid' to 'userid' in order to prevent
--					name conflict.
-- 3/2/01 Joseph	Support 'moving a section to a different area'.
--					Reset ver_loc_link_id to itself at activation.
--					Remove unnecessary update of source_loc_id for 'unplaced' rows.
*/

/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
This procedure activates a version store to a current (IMSE) store by
replacing current store with version store. Because lvxloc_ids are
used by financial data, we keep the current store rows replacing their
contents with the version store.  If both of sides are identical, then
we would only copy over the rows, but because of the changes done on
the version store, we have to consider the following:

STORE:         current store                      version store
                   /     \                          /        \
DEPT:          Dept1     Dept2                   Dept1      Dept3
              /  /  \       \                   / / \          \
AREA:      Ar1 Ar2 Ar3     Ar4               Ar5 Ar1 Ar2       Ar6
	      / \   / \			 \					 /\	  \	       /\
SECTION: S1 S2 S3 S4         S5                 S6 S1 S2      S3 S7

There are three kinds of rows that we have to consider:
(1) Rows that are both in the current store and the version store
	(Dept1, Ar1, Ar2, S1, S2, S3). Call them 'common'. Contents will
	be copied over, but we need 'reparenting' because some of them
	may be moved to an existing parent (S2 from Ar1 to Ar2) or
	to a new (added) parent (S3 from Ar2 to Ar6).
(2) Rows that are in the current store but are deleted from the
	version store (Dept2, Ar4, Ar3, S5, S4). Call them 'current_only'.
(3) Rows that are NOT in the current store but are added to the
	version store (Dept3, Ar5, Ar6, S6, S7). Call them 'version_only'.
	Some of them are added to an existing parent (Ar5 to Dept1) or
	to a new parent (Ar6 to Dept3).

We process rows level by level considering possible missing levels
(e.g., CC doesn't have lv6).

Get a list of depts, areas, and sections for current store and
version store. In Sqlserver, make a list of current_only and
version_only. In Oracle, lvxloc_id and source_loc_id are used
to distinguish common, current_only, and version_only.

Lv4: Copy lv4loc of the version store to lv4loc of the current store.

Lv5:
	For common, copy lv5.
	For current_only, mark it unplaced (PLACED=0).
	For version_only, change parent ids (lv4..lv1) to current store in
	order to 'reparent'.

Lv6:
	For common, copy lv6. A row might have been moved to a different
	parent. Search all version depts (common + version_only)
	to find the original parent and replace it with the new parent id.
	For current_only, mark it unplaced.
	For version_only, reparent it as above.

Lv7: same as lv6. Fix lv6..lv1.
------------------------------------------------------------------*/

	BusinessException Exception;
	integrityException Exception;


	type lv4loc_tab_type is table of maxdata.lv4loc%rowtype index by binary_integer;
	type lv5loc_tab_type is table of maxdata.lv5loc%rowtype index by binary_integer;
	type lv6loc_tab_type is table of maxdata.lv6loc%rowtype index by binary_integer;
	type lv7loc_tab_type is table of maxdata.lv7loc%rowtype index by binary_integer;

	type location_cursor_type is ref cursor;
	Location_cursor location_cursor_type;

	-- End of Type definitions for PL/SQL

	-- Variables of user defined types

	lv4loc_current maxdata.lv4loc%rowtype;
	lv4loc_version maxdata.lv4loc%rowtype;
	lv5loc_rec maxdata.lv5loc%rowtype;
	lv6loc_rec maxdata.lv6loc%rowtype;
	lv7loc_rec maxdata.lv7loc%rowtype;

	lv5loc_current lv5loc_tab_type;
	lv5loc_version lv5loc_tab_type;
	lv6loc_current lv6loc_tab_type;
	lv6loc_version lv6loc_tab_type;
	lv7loc_current lv7loc_tab_type;
	lv7loc_version lv7loc_tab_type;

	curcnt integer;
	vercnt integer;
	start_ver integer;
	match_found boolean := false;

	t_lv5loc_id number(10,0);
	t_lv6loc_id number(10,0);

	t_new_lv4loc_userid varchar2(20);
	t_effectiveDate date;
	t_basePeriodID number(6,0);
	t_ignore_lv4loc_id number(10,0);
	t_errorStr varchar2(255);
	t_errorStr2 varchar2(255);
	t_Error integer;
	t_Status_from_proc integer;
	t_lv1loc_id number(10,0);
	t_lv2loc_id number(10,0);
	t_lv3loc_id number(10,0);
	t_fileName varchar2(255);
	t_loc_4_count integer;
	t_time_ext varchar2(50);
	t_sqlnum number(10);
begin
	curcnt :=0;
	vercnt := 0;
	t_sqlnum := 0;

	p_status := 0;
	p_ErrorMsg := 'Active_version: Unknown error';

	t_fileName := 'Ignore';

	t_Status_from_proc := 1;

	t_ErrorStr := null;

	Select * into lv4loc_current
	from maxdata.lv4loc
	where lv4loc_id = p_current_lv4loc_id;

	t_sqlnum := 10;

	if lv4loc_current.time_level = 50 then
		select  ltrim(rtrim(Season_name)) || '-' || to_char(cycle_id)
		into t_time_ext from maxapp.season_lkup
		where season_lkup_id = lv4loc_current.time_id;
	elsif lv4loc_current.time_level = 51 then
		select  ltrim(rtrim(period_name)) || '-' || to_char(cycle_id)
		into t_time_ext from maxapp.period_lkup
		where period_lkup_id = lv4loc_current.time_id;
	else
		t_errorStr := 'Unsupported lv4loc.time_level:' || lv4loc_current.time_level;
		raise BusinessException;
	end if;


	t_new_lv4loc_userid :=
		substr(lv4loc_current.lv4loc_userid || ' ' || t_time_ext,1,20);

	if (lv4loc_current.lv1loc_id is null) then

		t_errorStr := 'Lv1 parent of the store chosen as base for activation doesnt exist';
		raise BusinessException;
	end if;

	--if (lv4loc_current.lv2loc_id is null) then

	--	t_errorStr := 'Lv2 Parent of the store chosen as base for activation doesnt exist';
	--	raise BusinessException;
	--end if;

	--if (lv4loc_current.lv3loc_id is null) then

	--	t_errorStr := 'Lv3 Parent of the store chosen as base for activation doesnt exist';
	--	raise BusinessException;
	--end if;

	t_basePeriodID := lv4loc_current.time_id;

-- The following logic is common for all levels
-- Seperate the children as A and B, A-B, B-A legend: A - current, B - version
-- A and B get updated on to current(other than pks, A-B get updated on column deleted to 'Y'
-- B-A are new to the version under MMAX directory hence get created.

-- Select lv4loc record for version into temp table so that it can be used to update the current version

	t_sqlnum := 20;

	select * into lv4loc_version
	from maxdata.lv4loc
	where lv4loc_id = p_version_lv4loc_id;

	if (lv4loc_version.lv4loc_id is null) then

		t_errorStr := 'version to be activated doesnt exist';
		raise BusinessException;
	end if;


-- Select all lv5 records into PL/SQL tables for each of the versions

	t_sqlnum := 30;

	curcnt :=0;

	Open Location_cursor for
		select * from maxdata.lv5loc where lv4loc_id = p_current_lv4loc_id
		order by ver_loc_link_id;
	loop
		fetch Location_cursor into lv5loc_rec;
		exit when Location_cursor%notfound;

		curcnt := curcnt + 1;
		lv5loc_current(curcnt) := lv5loc_rec;
	end loop;
	close Location_cursor;

	t_sqlnum := 40;

	curcnt :=0;

	Open Location_cursor for
		select * from maxdata.lv5loc where lv4loc_id = p_version_lv4loc_id
		order by ver_loc_link_id;
	loop
		fetch Location_cursor into lv5loc_rec;
		exit when Location_cursor%notfound;

		curcnt := curcnt + 1;
		lv5loc_version(curcnt) := lv5loc_rec;

	end loop;
	close Location_cursor;



-- Select all lv6 records into PL/SQL tables from each of the versions

	t_sqlnum := 50;

	curcnt :=0;

	Open Location_cursor for
		select * from maxdata.lv6loc where lv4loc_id = p_current_lv4loc_id
		order by ver_loc_link_id;
	loop
		fetch Location_cursor into lv6loc_rec;
		exit when Location_cursor%notfound;

		curcnt := curcnt + 1;
		lv6loc_current(curcnt) := lv6loc_rec;

	end loop;
	close Location_cursor;

	t_sqlnum := 60;

	curcnt :=0;

	Open Location_cursor for
		select * from maxdata.lv6loc where lv4loc_id = p_version_lv4loc_id
		order by ver_loc_link_id;
	loop
		fetch Location_cursor into lv6loc_rec;
		exit when Location_cursor%notfound;

		curcnt := curcnt + 1;
		lv6loc_version(curcnt) := lv6loc_rec;

	end loop;
	close Location_cursor;

-- Select all lv7 records into temp tables for each of the versions

	t_sqlnum := 70;

	curcnt :=0;

	Open Location_cursor for
		select * from maxdata.lv7loc where lv4loc_id = p_current_lv4loc_id
					and record_type = 'L' and layer = 1
		order by ver_loc_link_id;
	loop
		fetch Location_cursor into lv7loc_rec;
		exit when Location_cursor%notfound;

		curcnt := curcnt + 1;
		lv7loc_current(curcnt) := lv7loc_rec;

	end loop;
	close Location_cursor;

	t_sqlnum := 80;

	curcnt :=0;

	Open Location_cursor for
		select * from maxdata.lv7loc where lv4loc_id = p_version_lv4loc_id
					and record_type = 'L' and layer = 1
		order by ver_loc_link_id;
	loop
		fetch Location_cursor into lv7loc_rec;
		exit when Location_cursor%notfound;

		curcnt := curcnt + 1;
		lv7loc_version(curcnt) := lv7loc_rec;

	end loop;
	close Location_cursor;

-- create version for outgoing.

	t_sqlnum := 90;

	maxdata.p_new_version (p_current_lv4loc_id, t_new_lv4loc_userid,
					TO_CHAR(lv4loc_current.VER_EFFECTIVE_DATE, 'MM/DD/YYYY'), t_basePeriodID,
					lv4loc_current.time_id, lv4loc_current.aso_root_filename, P_outgoing_version_id,
					t_Status_from_proc, t_ErrorStr);


-- if proc failed rollback and error out

	if (t_Status_from_proc = 0) then
		raise businessException;
	end if;


-- Update current store with the versioned store.

	t_sqlnum := 91;

	Update maxdata.lv4loc
	Set
		ver_loc_link_id = lv4loc_id, --reset to itself.
		source_loc_id = p_version_lv4loc_id,
		last_update =lv4loc_version.last_update,
		changed_by_batch = lv4loc_version.changed_by_batch,
		version_id = lv4loc_version.version_id,
--			lv4loc_userid = lv4loc_version.lv4loc_userid,    removed on request from MnS team Bug no SB26.
		project_type = lv4loc_version.project_type,
		archive_flag = lv4loc_version.archive_flag,
		lv4mast_id = lv4loc_version.lv4mast_id,
		model_id = lv4loc_version.model_id,
		facet1_attach = lv4loc_version.facet1_attach,
		start_date = lv4loc_version.start_date,
		end_date = lv4loc_version.end_date,
		x_coord = lv4loc_version.x_coord,
		y_coord = lv4loc_version.y_coord,
		z_coord = lv4loc_version.z_coord,
		placed = lv4loc_version.placed,
		total_items = lv4loc_version.total_items,
		total_units = lv4loc_version.total_units,
		total_caps = lv4loc_version.total_caps,
		overlvls_id = lv4loc_version.overlvls_id,
		xcoord_just_lkup = lv4loc_version.xcoord_just_lkup,
		ycoord_just_lkup = lv4loc_version.ycoord_just_lkup,
		zcoord_just_lkup = lv4loc_version.zcoord_just_lkup,
		top_over = lv4loc_version.top_over,
		left_over = lv4loc_version.left_over,
		right_over = lv4loc_version.right_over,
		front_over = lv4loc_version.front_over,
		loc_color = lv4loc_version.loc_color,
		orient = lv4loc_version.orient,
		rotate = lv4loc_version.rotate,
		slope = lv4loc_version.slope,
		xcoord_gap = lv4loc_version.xcoord_gap,
		ycoord_gap = lv4loc_version.ycoord_gap,
		zcoord_gap = lv4loc_version.zcoord_gap,
		min_xcoord_gap = lv4loc_version.min_xcoord_gap,
		min_ycoord_gap = lv4loc_version.min_ycoord_gap,
		min_zcoord_gap = lv4loc_version.min_zcoord_gap,
		max_xcoord_gap = lv4loc_version.max_xcoord_gap,
		max_ycoord_gap = lv4loc_version.max_ycoord_gap,
		max_zcoord_gap = lv4loc_version.max_zcoord_gap,
		anchor_lkup = lv4loc_version.anchor_lkup,
		used_cubic_meters = lv4loc_version.used_cubic_meters,
		used_dsp_sqmeters = lv4loc_version.used_dsp_sqmeters,
		used_linear_meters = lv4loc_version.used_linear_meters,
		target_days_supply = lv4loc_version.target_days_supply,
		address_1 = lv4loc_version.address_1,
		address_2 = lv4loc_version.address_2,
		city = lv4loc_version.city,
		state = lv4loc_version.state,
		zip = lv4loc_version.zip,
		longitude = lv4loc_version.longitude,
		latitude = lv4loc_version.latitude,
		num_user1 = lv4loc_version.num_user1,
		num_user2 = lv4loc_version.num_user2,
		num_user3 = lv4loc_version.num_user3,
		num_user4 = lv4loc_version.num_user4,
		num_user5 = lv4loc_version.num_user5,
		num_user6 = lv4loc_version.num_user6,
		date_user7 = lv4loc_version.date_user7,
		date_user8 = lv4loc_version.date_user8,
		char_user9 = lv4loc_version.char_user9,
		char_user10 = lv4loc_version.char_user10,
		char_user11 = lv4loc_version.char_user11,
		char_user12 = lv4loc_version.char_user12,
		char_user13 = lv4loc_version.char_user13,
		bigchar_user14 = lv4loc_version.bigchar_user14,
		bigchar_user15 = lv4loc_version.bigchar_user15,
		height = lv4loc_version.height,
		width = lv4loc_version.width,
		depth = lv4loc_version.depth,
		shape_lkup_id = lv4loc_version.shape_lkup_id,
		aso_filename = lv4loc_version.aso_filename,
		aso_root_filename = lv4loc_version.aso_root_filename,
		aso_area = lv4loc_version.aso_area,
		aso_area_id = lv4loc_version.aso_area_id,
		tot_lv7dsp = lv4loc_version.tot_lv7dsp,
		tot_lv7lin = lv4loc_version.tot_lv7lin,
		shape_id = lv4loc_version.shape_id,
		destination_set_id = lv4loc_version.destination_set_id,
		merch_id = lv4loc_version.merch_id,
		merch_level = lv4loc_version.merch_level,
		time_id = lv4loc_version.time_id,
		time_level = lv4loc_version.time_level,
		loc_link_id = lv4loc_version.loc_link_id,
		merch_plan_id = lv4loc_version.merch_plan_id,
		layer_active = lv4loc_version.layer_active,
		layer = lv4loc_version.layer,
		store_state = lv4loc_version.store_state,
		lastsetdate = lv4loc_version.lastsetdate,
		newsetdate = lv4loc_version.newsetdate,
		lastapprovedate = lv4loc_version.lastapprovedate,
		recalc_flag = lv4loc_version.recalc_flag,
		Ver_Effective_date = lv4loc_version.Ver_Effective_date
	where lv4loc.lv4loc_id = p_Current_lv4loc_id;


-- In order to avoid any kind of name conflicts, append loc_ids to loc_userid
-- of the current store to make loc_userids unique between version store and current store.
-- (Think about the following case: drop and add a section to the version store
-- with the same name.  Also, swap the names of two sections.)

update lv5loc
set  lv5loc_userid = substr(substr(lv5loc_userid ,1, 20 - 1 -length(rtrim(to_char(lv5loc_id)))) || '-' || rtrim(to_char(lv5loc_id)), 1, 20)
where lv4loc_id=p_current_lv4loc_id
and placed = 1;

update lv6loc
set  lv6loc_userid = substr(substr(lv6loc_userid ,1, 20 - 1 -length(rtrim(to_char(lv6loc_id)))) || '-' || rtrim(to_char(lv6loc_id)), 1, 20)
where lv4loc_id=p_current_lv4loc_id
and placed = 1;

update lv7loc
set  lv7loc_userid = substr(substr(lv7loc_userid ,1, 20 - 1 -length(rtrim(to_char(lv7loc_id)))) || '-' || rtrim(to_char(lv7loc_id)), 1, 20)
where lv4loc_id=p_current_lv4loc_id
and placed = 1 and (delete_flag = 0 OR delete_flag is null);


-- Level 5

-- a-b

	vercnt := 0;
	curcnt := 0;

	t_sqlnum := 95;

	-- Set source_loc_id to its own id. It will be used later to tell whether
	-- the row is a 'common' or a 'version_only'.

	for vercnt in 1..lv5loc_version.count
	loop
		lv5loc_version(vercnt).source_loc_id := lv5loc_version(vercnt).lv5loc_id;
	end loop;



	curcnt :=0;
	vercnt := 0;

	start_ver := 1;

	match_found := false;

	t_sqlnum := 100;

	-- Loop thru the current store's depts and find the corresponding
	-- dept in the version store.

	for curcnt in 1..lv5loc_current.count
	loop
		for vercnt in start_ver..lv5loc_version.count
		loop
			if lv5loc_current(curcnt).lv5loc_id = lv5loc_version(vercnt).ver_loc_link_id then
				-- This row is a common between current and versioned.

				match_found := true;
				start_ver := vercnt + 1;  --rows sorted by ver_loc_link_id, so start next ver loop from next row.

				-- Set the version's id to its corresponding current dept's id.

				lv5loc_version(vercnt).lv5loc_id := lv5loc_current(curcnt).lv5loc_id;

				update maxdata.lv5loc
				set
					ver_loc_link_id = lv5loc_id, --reset to itself.
					source_loc_id = lv5loc_version(vercnt).source_loc_id,
					lv5loc_userid = lv5loc_version(vercnt).lv5loc_userid,
					last_update = lv5loc_version(vercnt).last_update,
					changed_by_batch = lv5loc_version(vercnt).changed_by_batch,
					version_id = lv5loc_version(vercnt).version_id,
					model_id = lv5loc_version(vercnt).model_id,
					facet1_attach = lv5loc_version(vercnt).facet1_attach,
					start_date = lv5loc_version(vercnt).start_date,
					end_date = lv5loc_version(vercnt).end_date,
					x_coord = lv5loc_version(vercnt).x_coord,
					y_coord = lv5loc_version(vercnt).y_coord,
					z_coord = lv5loc_version(vercnt).z_coord,
					placed = lv5loc_version(vercnt).placed,
					total_items = lv5loc_version(vercnt).total_items,
					total_units = lv5loc_version(vercnt).total_units,
					total_caps = lv5loc_version(vercnt).total_caps,
					overlvls_id = lv5loc_version(vercnt).overlvls_id,
					xcoord_just_lkup = lv5loc_version(vercnt).xcoord_just_lkup,
					ycoord_just_lkup = lv5loc_version(vercnt).ycoord_just_lkup,
					zcoord_just_lkup = lv5loc_version(vercnt).zcoord_just_lkup,
					top_over = lv5loc_version(vercnt).top_over,
					left_over = lv5loc_version(vercnt).left_over,
					right_over = lv5loc_version(vercnt).right_over,
					front_over = lv5loc_version(vercnt).front_over,
					loc_color = lv5loc_version(vercnt).loc_color,
					orient = lv5loc_version(vercnt).orient,
					rotate = lv5loc_version(vercnt).rotate,
					slope = lv5loc_version(vercnt).slope,
					xcoord_gap = lv5loc_version(vercnt).xcoord_gap,
					ycoord_gap = lv5loc_version(vercnt).ycoord_gap,
					zcoord_gap = lv5loc_version(vercnt).zcoord_gap,
					min_xcoord_gap = lv5loc_version(vercnt).min_xcoord_gap,
					min_ycoord_gap = lv5loc_version(vercnt).min_ycoord_gap,
					min_zcoord_gap = lv5loc_version(vercnt).min_zcoord_gap,
					max_xcoord_gap = lv5loc_version(vercnt).max_xcoord_gap,
					max_ycoord_gap = lv5loc_version(vercnt).max_ycoord_gap,
					max_zcoord_gap = lv5loc_version(vercnt).max_zcoord_gap,
					anchor_lkup = lv5loc_version(vercnt).anchor_lkup,
					used_cubic_meters = lv5loc_version(vercnt).used_cubic_meters,
					used_dsp_sqmeters = lv5loc_version(vercnt).used_dsp_sqmeters,
					used_linear_meters = lv5loc_version(vercnt).used_linear_meters,
					num_user1 = lv5loc_version(vercnt).num_user1,
					num_user2 = lv5loc_version(vercnt).num_user2,
					num_user3 = lv5loc_version(vercnt).num_user3,
					num_user4 = lv5loc_version(vercnt).num_user4,
					num_user5 = lv5loc_version(vercnt).num_user5,
					num_user6 = lv5loc_version(vercnt).num_user6,
					date_user7 = lv5loc_version(vercnt).date_user7,
					date_user8 = lv5loc_version(vercnt).date_user8,
					char_user9 = lv5loc_version(vercnt).char_user9,
					char_user10 = lv5loc_version(vercnt).char_user10,
					char_user11 = lv5loc_version(vercnt).char_user11,
					char_user12 = lv5loc_version(vercnt).char_user12,
					char_user13 = lv5loc_version(vercnt).char_user13,
					bigchar_user14 = lv5loc_version(vercnt).bigchar_user14,
					bigchar_user15 = lv5loc_version(vercnt).bigchar_user15,
					height = lv5loc_version(vercnt).height,
					width = lv5loc_version(vercnt).width,
					depth = lv5loc_version(vercnt).depth,
					shape_lkup_id = lv5loc_version(vercnt).shape_lkup_id,
					aso_filename = lv5loc_version(vercnt).aso_filename,
					aso_root_filename = lv5loc_version(vercnt).aso_root_filename,
					aso_area = lv5loc_version(vercnt).aso_area,
					aso_area_id = lv5loc_version(vercnt).aso_area_id,
					recalc_flag = lv5loc_version(vercnt).recalc_flag,
					tot_lv7flr = lv5loc_version(vercnt).tot_lv7flr,
					tot_lv7dsp = lv5loc_version(vercnt).tot_lv7dsp,
					tot_lv7cub = lv5loc_version(vercnt).tot_lv7cub,
					tot_lv7lin = lv5loc_version(vercnt).tot_lv7lin,
					main_space = lv5loc_version(vercnt).main_space,
					shape_id = lv5loc_version(vercnt).shape_id,
					merch_id = lv5loc_version(vercnt).merch_id,
					merch_level = lv5loc_version(vercnt).merch_level,
					time_id = lv5loc_version(vercnt).time_id,
					time_level = lv5loc_version(vercnt).time_level,
					loc_link_id = lv5loc_version(vercnt).loc_link_id,
					merch_plan_id = lv5loc_version(vercnt).merch_plan_id,
					layer_active = lv5loc_version(vercnt).layer_active,
					layer = lv5loc_version(vercnt).layer
				where lv5loc_id = lv5loc_version(vercnt).lv5loc_id;

				exit;
			end if;
		end loop;

		if (match_found = false) then
			t_sqlnum := 105;

			update maxdata.lv5loc
			set placed = 0
			where lv5loc_id = lv5loc_current(curcnt).lv5loc_id;
		end if;

		-- reset back to false for next iteration

		match_found:= false;

	end loop;



	vercnt := 0;
	curcnt := 0;

	for vercnt in 1..lv5loc_version.count
	loop
		-- If the source_loc_id is pointing to itself, then it is a new (added) dept.
		-- Just reparent it.

		if lv5loc_version(vercnt).source_loc_id = lv5loc_version(vercnt).lv5loc_id then
			t_sqlnum := 110;


			update maxdata.lv5loc
			set lv4loc_id = lv4loc_current.lv4loc_id,
				lv3loc_id = lv4loc_current.lv3loc_id,
				lv2loc_id = lv4loc_current.lv2loc_id,
				lv1loc_id = lv4loc_current.lv1loc_id,
				ver_loc_link_id = lv5loc_id, --reset to itself.
				last_update = sysdate
			where lv5loc_id = lv5loc_version(vercnt).lv5loc_id;


		end if;
	end loop;





-- Level 6

	vercnt := 0;
	curcnt := 0;

	t_sqlnum := 123;

	for vercnt in 1..lv6loc_version.count
	loop
		lv6loc_version(vercnt).source_loc_id := lv6loc_version(vercnt).lv6loc_id;
	end loop;



	curcnt :=0;
	vercnt := 0;
	start_ver := 1;
	match_found := false;

	t_sqlnum := 126;

	for curcnt in 1..lv6loc_current.count
	loop
		for vercnt in start_ver..lv6loc_version.count
		loop
			if lv6loc_current(curcnt).lv6loc_id = lv6loc_version(vercnt).ver_loc_link_id then
				-- This row is a common between current and versioned.

				match_found := true;
				start_ver := vercnt + 1;

				lv6loc_version(vercnt).lv6loc_id := lv6loc_current(curcnt).lv6loc_id;

				-- Locate new parent id.
				-- This row might have been moved to a different parent.

				t_lv5loc_id := null;

				for parentcnt in 1..lv5loc_version.count
				loop
					if lv6loc_version(vercnt).lv5loc_id = lv5loc_version(parentcnt).source_loc_id then
						t_lv5loc_id := lv5loc_version(parentcnt).lv5loc_id;
						exit;
					end if;
				end loop;

				update maxdata.lv6loc
				set
					source_loc_id = lv6loc_version(vercnt).source_loc_id,
					ver_loc_link_id = lv6loc_id, --reset to itself.
					lv5loc_id = t_lv5loc_id,
					lv6loc_userid = lv6loc_version(vercnt).lv6loc_userid,
					last_update = lv6loc_version(vercnt).last_update,
					changed_by_batch = lv6loc_version(vercnt).changed_by_batch,
					version_id = lv6loc_version(vercnt).version_id,
					model_id = lv6loc_version(vercnt).model_id,
					facet1_attach = lv6loc_version(vercnt).facet1_attach,
					start_date = lv6loc_version(vercnt).start_date,
					end_date = lv6loc_version(vercnt).end_date,
					x_coord = lv6loc_version(vercnt).x_coord,
					y_coord = lv6loc_version(vercnt).y_coord,
					z_coord = lv6loc_version(vercnt).z_coord,
					placed = lv6loc_version(vercnt).placed,
					total_items = lv6loc_version(vercnt).total_items,
					total_units = lv6loc_version(vercnt).total_units,
					total_caps = lv6loc_version(vercnt).total_caps,
					overlvls_id = lv6loc_version(vercnt).overlvls_id,
					xcoord_just_lkup = lv6loc_version(vercnt).xcoord_just_lkup,
					ycoord_just_lkup = lv6loc_version(vercnt).ycoord_just_lkup,
					zcoord_just_lkup = lv6loc_version(vercnt).zcoord_just_lkup,
					top_over = lv6loc_version(vercnt).top_over,
					left_over = lv6loc_version(vercnt).left_over,
					right_over = lv6loc_version(vercnt).right_over,
					front_over = lv6loc_version(vercnt).front_over,
					loc_color = lv6loc_version(vercnt).loc_color,
					orient = lv6loc_version(vercnt).orient,
					rotate = lv6loc_version(vercnt).rotate,
					slope = lv6loc_version(vercnt).slope,
					xcoord_gap = lv6loc_version(vercnt).xcoord_gap,
					ycoord_gap = lv6loc_version(vercnt).ycoord_gap,
					zcoord_gap = lv6loc_version(vercnt).zcoord_gap,
					min_xcoord_gap = lv6loc_version(vercnt).min_xcoord_gap,
					min_ycoord_gap = lv6loc_version(vercnt).min_ycoord_gap,
					min_zcoord_gap = lv6loc_version(vercnt).min_zcoord_gap,
					max_xcoord_gap = lv6loc_version(vercnt).max_xcoord_gap,
					max_ycoord_gap = lv6loc_version(vercnt).max_ycoord_gap,
					max_zcoord_gap = lv6loc_version(vercnt).max_zcoord_gap,
					anchor_lkup = lv6loc_version(vercnt).anchor_lkup,
					used_cubic_meters = lv6loc_version(vercnt).used_cubic_meters,
					used_dsp_sqmeters = lv6loc_version(vercnt).used_dsp_sqmeters,
					used_linear_meters = lv6loc_version(vercnt).used_linear_meters,
					num_user1 = lv6loc_version(vercnt).num_user1,
					num_user2 = lv6loc_version(vercnt).num_user2,
					num_user3 = lv6loc_version(vercnt).num_user3,
					num_user4 = lv6loc_version(vercnt).num_user4,
					num_user5 = lv6loc_version(vercnt).num_user5,
					num_user6 = lv6loc_version(vercnt).num_user6,
					date_user7 = lv6loc_version(vercnt).date_user7,
					date_user8 = lv6loc_version(vercnt).date_user8,
					char_user9 = lv6loc_version(vercnt).char_user9,
					char_user10 = lv6loc_version(vercnt).char_user10,
					char_user11 = lv6loc_version(vercnt).char_user11,
					char_user12 = lv6loc_version(vercnt).char_user12,
					char_user13 = lv6loc_version(vercnt).char_user13,
					bigchar_user14 = lv6loc_version(vercnt).bigchar_user14,
					bigchar_user15 = lv6loc_version(vercnt).bigchar_user15,
					height = lv6loc_version(vercnt).height,
					width = lv6loc_version(vercnt).width,
					depth = lv6loc_version(vercnt).depth,
					shape_lkup_id = lv6loc_version(vercnt).shape_lkup_id,
					aso_filename = lv6loc_version(vercnt).aso_filename,
					aso_root_filename = lv6loc_version(vercnt).aso_root_filename,
					aso_area = lv6loc_version(vercnt).aso_area,
					aso_area_id = lv6loc_version(vercnt).aso_area_id,
					recalc_flag = lv6loc_version(vercnt).recalc_flag,
					tot_lv7flr = lv6loc_version(vercnt).tot_lv7flr,
					tot_lv7dsp = lv6loc_version(vercnt).tot_lv7dsp,
					tot_lv7cub = lv6loc_version(vercnt).tot_lv7cub,
					tot_lv7lin = lv6loc_version(vercnt).tot_lv7lin,
					shape_id = lv6loc_version(vercnt).shape_id,
					merch_id = lv6loc_version(vercnt).merch_id,
					merch_level = lv6loc_version(vercnt).merch_level,
					time_id = lv6loc_version(vercnt).time_id,
					time_level = lv6loc_version(vercnt).time_level,
					loc_link_id = lv6loc_version(vercnt).loc_link_id,
					merch_plan_id = lv6loc_version(vercnt).merch_plan_id,
					layer_active = lv6loc_version(vercnt).layer_active,
					layer = lv6loc_version(vercnt).layer
				where lv6loc_id = lv6loc_version(vercnt).lv6loc_id;

				exit;
			end if;
		end loop;

		if (match_found = false) then
			t_sqlnum := 129;

			update maxdata.lv6loc
			set placed = 0
			where lv6loc_id = lv6loc_current(curcnt).lv6loc_id;
		end if;

		-- reset back to false for next iteration

		match_found:= false;

	end loop;



	vercnt := 0;
	curcnt := 0;

	for vercnt in 1..lv6loc_version.count
	loop
		if lv6loc_version(vercnt).source_loc_id = lv6loc_version(vercnt).lv6loc_id then
			t_sqlnum := 131;


			-- Locate new parent id.

			t_lv5loc_id := null;

			for parentcnt in 1..lv5loc_version.count
			loop
				if lv6loc_version(vercnt).lv5loc_id = lv5loc_version(parentcnt).source_loc_id then
					t_lv5loc_id := lv5loc_version(parentcnt).lv5loc_id;
					exit;
				end if;
			end loop;

			update maxdata.lv6loc
			set lv4loc_id = lv4loc_current.lv4loc_id,
				lv3loc_id = lv4loc_current.lv3loc_id,
				lv2loc_id = lv4loc_current.lv2loc_id,
				lv1loc_id = lv4loc_current.lv1loc_id,
				lv5loc_id = t_lv5loc_id,
				ver_loc_link_id = lv6loc_id, --reset to itself.
				last_update = sysdate
			where lv6loc_id = lv6loc_version(vercnt).lv6loc_id;

		end if;
	end loop;








-- Level 7

	vercnt := 0;
	curcnt := 0;

	t_sqlnum := 133;

	for vercnt in 1..lv7loc_version.count
	loop
		lv7loc_version(vercnt).source_loc_id := lv7loc_version(vercnt).lv7loc_id;
	end loop;


	curcnt := 0;
	vercnt := 0;
	start_ver := 1;

	match_found := false;

	t_sqlnum := 137;

	for curcnt in 1..lv7loc_current.count
	loop
		for vercnt in start_ver..lv7loc_version.count
		loop
			if lv7loc_current(curcnt).lv7loc_id = lv7loc_version(vercnt).ver_loc_link_id then
				-- This row is a common between current and versioned.

				match_found := true;
				start_ver := vercnt + 1;

				lv7loc_version(vercnt).lv7loc_id := lv7loc_current(curcnt).lv7loc_id;

				-- Locate new parent id.

				t_lv5loc_id := null;

				for parentcnt in 1..lv5loc_version.count
				loop
					if lv7loc_version(vercnt).lv5loc_id = lv5loc_version(parentcnt).source_loc_id then
						t_lv5loc_id := lv5loc_version(parentcnt).lv5loc_id;
						exit;
					end if;
				end loop;

				t_lv6loc_id := null;

				for parentcnt in 1..lv6loc_version.count
				loop
					if lv7loc_version(vercnt).lv6loc_id = lv6loc_version(parentcnt).source_loc_id then
						t_lv6loc_id := lv6loc_version(parentcnt).lv6loc_id;
						exit;
					end if;
				end loop;

				update maxdata.lv7loc
				set
					source_loc_id = lv7loc_version(vercnt).source_loc_id,
					ver_loc_link_id = lv7loc_id, --reset to itself.
					lv5loc_id = t_lv5loc_id,
					lv6loc_id = t_lv6loc_id,
					lv7loc_userid = lv7loc_version(vercnt).lv7loc_userid,
					last_update = lv7loc_version(vercnt).last_update,
					changed_by_batch = lv7loc_version(vercnt).changed_by_batch,
					version_id = lv7loc_version(vercnt).version_id,
					model_id = lv7loc_version(vercnt).model_id,
					facet1_attach = lv7loc_version(vercnt).facet1_attach,
					start_date = lv7loc_version(vercnt).start_date,
					end_date = lv7loc_version(vercnt).end_date,
					x_coord = lv7loc_version(vercnt).x_coord,
					y_coord = lv7loc_version(vercnt).y_coord,
					z_coord = lv7loc_version(vercnt).z_coord,
					placed = lv7loc_version(vercnt).placed,
					total_items = lv7loc_version(vercnt).total_items,
					total_units = lv7loc_version(vercnt).total_units,
					total_caps = lv7loc_version(vercnt).total_caps,
					overlvls_id = lv7loc_version(vercnt).overlvls_id,
					xcoord_just_lkup = lv7loc_version(vercnt).xcoord_just_lkup,
					ycoord_just_lkup = lv7loc_version(vercnt).ycoord_just_lkup,
					zcoord_just_lkup = lv7loc_version(vercnt).zcoord_just_lkup,
					top_over = lv7loc_version(vercnt).top_over,
					left_over = lv7loc_version(vercnt).left_over,
					right_over = lv7loc_version(vercnt).right_over,
					front_over = lv7loc_version(vercnt).front_over,
					loc_color = lv7loc_version(vercnt).loc_color,
					orient = lv7loc_version(vercnt).orient,
					rotate = lv7loc_version(vercnt).rotate,
					slope = lv7loc_version(vercnt).slope,
					xcoord_gap = lv7loc_version(vercnt).xcoord_gap,
					ycoord_gap = lv7loc_version(vercnt).ycoord_gap,
					zcoord_gap = lv7loc_version(vercnt).zcoord_gap,
					min_xcoord_gap = lv7loc_version(vercnt).min_xcoord_gap,
					min_ycoord_gap = lv7loc_version(vercnt).min_ycoord_gap,
					min_zcoord_gap = lv7loc_version(vercnt).min_zcoord_gap,
					max_xcoord_gap = lv7loc_version(vercnt).max_xcoord_gap,
					max_ycoord_gap = lv7loc_version(vercnt).max_ycoord_gap,
					max_zcoord_gap = lv7loc_version(vercnt).max_zcoord_gap,
					anchor_lkup = lv7loc_version(vercnt).anchor_lkup,
					used_cubic_meters = lv7loc_version(vercnt).used_cubic_meters,
					used_dsp_sqmeters = lv7loc_version(vercnt).used_dsp_sqmeters,
					used_linear_meters = lv7loc_version(vercnt).used_linear_meters,
					target_days_supply = lv7loc_version(vercnt).target_days_supply,
					num_user1 = lv7loc_version(vercnt).num_user1,
					num_user2 = lv7loc_version(vercnt).num_user2,
					num_user3 = lv7loc_version(vercnt).num_user3,
					num_user4 = lv7loc_version(vercnt).num_user4,
					num_user5 = lv7loc_version(vercnt).num_user5,
					num_user6 = lv7loc_version(vercnt).num_user6,
					date_user7 = lv7loc_version(vercnt).date_user7,
					date_user8 = lv7loc_version(vercnt).date_user8,
					char_user9 = lv7loc_version(vercnt).char_user9,
					char_user10 = lv7loc_version(vercnt).char_user10,
					char_user11 = lv7loc_version(vercnt).char_user11,
					char_user12 = lv7loc_version(vercnt).char_user12,
					char_user13 = lv7loc_version(vercnt).char_user13,
					bigchar_user14 = lv7loc_version(vercnt).bigchar_user14,
					bigchar_user15 = lv7loc_version(vercnt).bigchar_user15,
					seg_fixed_width = lv7loc_version(vercnt).seg_fixed_width,
					seg_set_id = lv7loc_version(vercnt).seg_set_id,
					mvmt_applied = lv7loc_version(vercnt).mvmt_applied,
					record_type = lv7loc_version(vercnt).record_type,
					pog_formatted = lv7loc_version(vercnt).pog_formatted,
					group_master_id = lv7loc_version(vercnt).group_master_id,
					asst_master_id = lv7loc_version(vercnt).asst_master_id,
					depth = lv7loc_version(vercnt).depth,
					height = lv7loc_version(vercnt).height,
					width = lv7loc_version(vercnt).width,
					pog_opp_height = lv7loc_version(vercnt).pog_opp_height,
					pog_opp_width = lv7loc_version(vercnt).pog_opp_width,
					pog_opp_depth = lv7loc_version(vercnt).pog_opp_depth,
					merch_arriv_period = lv7loc_version(vercnt).merch_arriv_period,
					pog_status = lv7loc_version(vercnt).pog_status,
					pog_default_date = lv7loc_version(vercnt).pog_default_date,
					default_view = lv7loc_version(vercnt).default_view,
					pog_name = lv7loc_version(vercnt).pog_name,
					subasst_id = lv7loc_version(vercnt).subasst_id,
					cpw_days_in_pd = lv7loc_version(vercnt).cpw_days_in_pd,
					cpw_start_pd_lkup = lv7loc_version(vercnt).cpw_start_pd_lkup,
					cpw_end_pd_lkup = lv7loc_version(vercnt).cpw_end_pd_lkup,
					cpw_total_stores = lv7loc_version(vercnt).cpw_total_stores,
					cpw_stores_inc = lv7loc_version(vercnt).cpw_stores_inc,
					shape_lkup_id = lv7loc_version(vercnt).shape_lkup_id,
					aso_filename = lv7loc_version(vercnt).aso_filename,
					aso_root_filename = lv7loc_version(vercnt).aso_root_filename,
					aso_area = lv7loc_version(vercnt).aso_area,
					aso_area_id = lv7loc_version(vercnt).aso_area_id,
					recalc_flag = lv7loc_version(vercnt).recalc_flag,
					alloc_sq_meters = lv7loc_version(vercnt).alloc_sq_meters,
					on_dsp_front = lv7loc_version(vercnt).on_dsp_front,
					on_dsp_back = lv7loc_version(vercnt).on_dsp_back,
					on_dsp_left = lv7loc_version(vercnt).on_dsp_left,
					on_dsp_right = lv7loc_version(vercnt).on_dsp_right,
					alloc_space = lv7loc_version(vercnt).alloc_space,
					used_flr_sqmeters = lv7loc_version(vercnt).used_flr_sqmeters,
					main_space = lv7loc_version(vercnt).main_space,
					shape_id = lv7loc_version(vercnt).shape_id,
					merch_id = lv7loc_version(vercnt).merch_id,
					merch_level = lv7loc_version(vercnt).merch_level,
					time_id = lv7loc_version(vercnt).time_id,
					time_level = lv7loc_version(vercnt).time_level,
					loc_link_id = lv7loc_version(vercnt).loc_link_id,
					subassortment_id = lv7loc_version(vercnt).subassortment_id,
					layer_active = lv7loc_version(vercnt).layer_active,
					layer = lv7loc_version(vercnt).layer,
					item_index_order = lv7loc_version(vercnt).item_index_order,
					delete_flag = lv7loc_version(vercnt).delete_flag,
					state = lv7loc_version(vercnt).state
				where lv7loc_id = lv7loc_version(vercnt).lv7loc_id;

				exit;
			end if;
		end loop;

		if (match_found = false) then
			t_sqlnum := 139;

			update maxdata.lv7loc
			set delete_flag = 1
			where lv7loc_id = lv7loc_current(curcnt).lv7loc_id;
		end if;

		-- reset back to false for next iteration

		match_found:= false;

	end loop;



	vercnt := 0;
	curcnt := 0;

	for vercnt in 1..lv7loc_version.count
	loop
		if lv7loc_version(vercnt).source_loc_id = lv7loc_version(vercnt).lv7loc_id then
			t_sqlnum := 143;


			-- Locate new parent id.

			t_lv5loc_id := null;

			for parentcnt in 1..lv5loc_version.count
			loop
				if lv7loc_version(vercnt).lv5loc_id = lv5loc_version(parentcnt).source_loc_id then
					t_lv5loc_id := lv5loc_version(parentcnt).lv5loc_id;
					exit;
				end if;
			end loop;

			t_lv6loc_id := null;

			for parentcnt in 1..lv6loc_version.count
			loop
				if lv7loc_version(vercnt).lv6loc_id = lv6loc_version(parentcnt).source_loc_id then
					t_lv6loc_id := lv6loc_version(parentcnt).lv6loc_id;
					exit;
				end if;
			end loop;

			update maxdata.lv7loc
			set lv4loc_id = lv4loc_current.lv4loc_id,
				lv3loc_id = lv4loc_current.lv3loc_id,
				lv2loc_id = lv4loc_current.lv2loc_id,
				lv1loc_id = lv4loc_current.lv1loc_id,
				lv5loc_id = t_lv5loc_id,
				lv6loc_id = t_lv6loc_id,
				ver_loc_link_id = lv7loc_id, --reset to itself at activation.
				last_update = sysdate
			where lv7loc_id = lv7loc_version(vercnt).lv7loc_id;

		end if;
	end loop;




	t_sqlnum := 220;

	maxdata.p_activate_layers(p_current_lv4loc_id, p_version_lv4loc_id, p_outgoing_version_id, p_status, t_ErrorStr);

	if (p_status = 0) then
		raise BusinessException;
	end if;

	-- Log for debugging.

	t_Errorstr :=
		p_current_lv4loc_id
		|| ',ver:' || p_version_lv4loc_id
		|| ',out:' || p_outgoing_version_id;
	maxdata.ins_import_log ('p_activate_version','info',t_errorstr,	null,null,null);

	p_status := 1;
	p_ErrorMsg := 'Active_version: successful';


exception
	when businessException then
		goto errhndlr;

	when others then
		t_errorstr := SQLERRM;
		goto errhndlr;

<<errhndlr>>
		if t_errorstr is null then
			t_errorstr := 'Unknown error';
		end if;

		p_status := 0;


		if p_outgoing_version_id is not null then
			maxdata.p_delete_version (P_outgoing_version_id, t_status_from_proc, t_errorStr2);

			if t_status_from_proc = 0 and t_errorstr is not null then
				t_errorstr := t_errorstr || '. ' || t_errorStr2;
			end if;
		end if;

		p_errormsg := t_errorstr
			|| '.  Sqlnum:' || t_sqlnum
			|| '  Prm:' || p_current_lv4loc_id
			|| ',' || p_version_lv4loc_id
			|| ',' || nvl(p_outgoing_version_id,0);

		p_outgoing_version_id := null;

end; -- End of activation procedure.

/

  GRANT EXECUTE ON "MAXDATA"."P_ACTIVATE_VERSION" TO "MAXUSER";
  GRANT EXECUTE ON "MAXDATA"."P_ACTIVATE_VERSION" TO "MADMAX";
