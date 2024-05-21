--------------------------------------------------------
--  DDL for Procedure P_NEW_VERSION
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_NEW_VERSION" 
( p_lv4loc_id number, p_new_lv4loc_userid varchar2, p_varcharEffectiveDate varchar2,
p_basePeriodID number, p_targetPeriodID number, p_CAD_file_name varchar2,
p_new_lv4loc_id out number, p_status out int, p_ErrorMsg out varchar2)
as

/*--Change history:

$Log: 4150_p_new_version.sql,v $
Revision 1.1.8.1.12.1  2008/10/30 19:21:52  vejang
checked in from 612 CD location on D12176

Revision 1.2  2006/01/18 15:26:57  raabuh
Apply 5.6.1 and 5.6.2 space scripts


--5.6.1	2/8/05	helmi 	issue 17469	adding delete_flag to the where clause in level7 in addition to placed flag.
-- V 5.3.4
-- 03/20/2003  DR    Bug# 15149. removed commit and rollback statements
-- V
-- 05/07/01	Joseph cho	Bugfix: replace trim with ltrim(rtrim).
*/


	-- User defined Exceptions

	BusinessException exception;
	integrityException exception;

	-- end of User defined Exceptions

	-- Type definitions for PL/SQL

	type lv4loc_tab_type is table of maxdata.lv4loc%rowtype index by binary_integer;
	type lv5loc_tab_type is table of maxdata.lv5loc%rowtype index by binary_integer;
	type lv6loc_tab_type is table of maxdata.lv6loc%rowtype index by binary_integer;
	type lv7loc_tab_type is table of maxdata.lv7loc%rowtype index by binary_integer;

	type location_cursor_type is ref cursor;

	Location_cursor location_cursor_type;

	-- End of Type definitions for PL/SQL

	-- Variables of user defined types

	lv4loc_rec maxdata.lv4loc%rowtype;
	lv5loc_rec maxdata.lv5loc%rowtype;
	lv6loc_rec maxdata.lv6loc%rowtype;
	lv7loc_rec maxdata.lv7loc%rowtype;

	lv4loc_tab lv4loc_tab_type;
	lv5loc_tab lv5loc_tab_type;
	lv6loc_tab lv6loc_tab_type;
	lv7loc_tab lv7loc_tab_type;

	-- end of Variables of user defined types

	loopcounter integer;
	Parentcounter integer;

	t_Error integer;
	t_lv5_seq_num number(10,0);
	t_lv6_seq_num number(10,0);
	t_new_lv5loc_ids integer;
	t_new_lv6loc_ids integer;
	t_new_lv7loc_ids integer;
	t_ErrorStr varchar2(255);
	t_Status_from_called integer;
	t_time_level number(6,0);
	t_lv1loc_id number(10,0);
	t_lv2loc_id number(10,0);
	t_lv3loc_id number(10,0);
	t_check_count integer;
	t_EffectiveDate date;
	t_pd_count integer;
	t_New_file_name varchar2(255);
	t_higher_period_type varchar2(20);
	t_lower_period_type varchar2(20);
	t_sqlnum number(10);

begin
	p_status := 0; -- Assume that there is an error.
	p_ErrorMsg := 'Unknown error';

	t_sqlnum := 0;

	-- general intializations

	t_Status_from_called := 1;

	t_Errorstr := null;

	-- Convert Charcter datetime input to date

	t_effectiveDate := to_date(p_varcharEffectiveDate, 'MM/DD/YYYY');

	-- Check if the proc is running for future versioins(directly from SCP) or historic versions(during activation)

	if (p_CAD_file_name = 'ignore') then

		t_new_file_name := null;

	else

		t_new_file_name := p_CAD_file_name;
	end if;

	-- Can change to any one level of time hierarchy configure using mmax_config

	Select version_time_level, higher_period_type, lower_period_type
	into t_time_level, t_higher_period_type, t_lower_period_type
	from maxapp.mmax_config;

--	validation of inputs, if nay business level logic violation raise businessException

	-- Check if Store to be versioned exist

	t_sqlnum := 1;

	Select count(*) into t_check_count
	from maxdata.lv4loc
	where lv4loc_id = p_lv4loc_id;

	if (t_check_count = 0) then
		t_ErrorStr := 'Invalid Store Location Id';
		Raise BusinessException;
	end if;

	-- Check given new loc Userid is not null

	if (p_new_lv4loc_userid is null) then
		t_ErrorStr := 'Store Can not have null userid';
		Raise BusinessException;
	end if;

	-- Check if Source store has reasonable time id

	if (p_basePeriodID is null) then
		t_ErrorStr := 'Base Period can not be null';
		raise BusinessException;
	end if;

	t_sqlnum := 2;

	if t_time_level = 50 then
		Select count(*) into t_pd_count from maxapp.season_lkup
		where season_lkup_id = p_basePeriodID and season_type = t_higher_period_type;
	elsif t_time_level = 51 then
 		Select count(*) into t_pd_count from maxapp.period_lkup
		where period_lkup_id = p_basePeriodID and period_type = t_lower_period_type;
	else
		t_errorstr := 'Unsupported version_time_level:' || t_time_level;
		raise businessexception;
	end if;

	if (t_pd_count = 0 ) then
		t_ErrorStr := 'Base Period is out of scope from Business calendar';
		Raise BusinessException;
	end if;

	t_pd_count := -1;

	if (p_targetPeriodID is null) then
		t_ErrorStr := 'Target Period can not be null';
		Raise BusinessException;
	end if;

	-- Check against higher period_type for two levels. altrnatives have to be identified.

	if t_time_level=50 then
		Select count(*) into t_pd_count from maxapp.season_lkup
		where season_lkup_id = p_targetPeriodID and season_type = t_higher_period_type;
	else
		Select count(*) into t_pd_count from maxapp.period_lkup
		where period_lkup_id = p_targetPeriodID and period_type = t_lower_period_type;
	end if;

	if (t_pd_count = 0 ) then

		t_ErrorStr := 'Target Period is out of scope from Business calendar';
		Raise BusinessException;
	end if;

--reinitialize

	t_check_count := 0;

	t_sqlnum := 3;

	Select count(*) into t_check_count
	from maxdata.lv4loc
	where lv4loc_id = p_lv4loc_id and time_id = p_basePeriodID;

	if (t_check_count = 0 ) then

		t_ErrorStr := 'No such source Version exists';
		Raise BusinessException;
	end if;

	-- get parents for the verison store

	t_sqlnum := 4;

	maxdata.p_get_ver_parents (t_lv1loc_id, t_lv2loc_id, t_lv3loc_id, t_Status_from_called, t_ErrorStr);

	-- If an error occured in procedure call, report the error back and exit

	if (t_Status_from_called = 0) then
		Raise BusinessException;
	else
		t_ErrorStr := null;
	end if;

-- Validation of verion stores' parents begins here
-- validate parents for the verison store, If error report it and exit

	if (t_lv1loc_id is null) then

		t_ErrorStr := 'version Company not found';
		Raise BusinessException;
	end if;

	if (t_lv2loc_id is null) then

		t_ErrorStr := 'version region not found';
		Raise BusinessException;
	end if;

	if (t_lv3loc_id is null) then

		t_Errorstr := 'version district not found';
		Raise BusinessException;
	end if;

-- Validation of verion stores' parents ends here

	-- get source source information

	t_sqlnum := 5;

	select * into lv4loc_rec from maxdata.lv4loc where lv4loc_id = p_lv4loc_id;

	-- get next Pk id for store

	maxapp.p_get_next_key(4, 2, 1, p_new_lv4loc_id, t_Errorstr);

	-- if fails raise error

	if (t_Errorstr is not null) then
		raise integrityException;
	end if;

	-- reset the parents and pk id and effective date for store

		lv4loc_rec.source_loc_id := lv4loc_rec.lv4loc_id;
		lv4loc_rec.shape_id := 0;
		lv4loc_rec.lv4loc_id := p_new_lv4loc_id;
		lv4loc_rec.lv3loc_id := t_lv3loc_id;
		lv4loc_rec.lv2loc_id := t_lv2loc_id;
		lv4loc_rec.lv1loc_id := t_lv1loc_id;
		lv4loc_rec.lv4loc_userid := p_new_lv4loc_userid;
		lv4loc_rec.Ver_Effective_date := t_effectiveDate;
		lv4loc_rec.time_id := p_targetPeriodID;
		lv4loc_rec.last_update := sysdate;

	-- and StoreCAD file Name if future layout

	if (t_new_file_name is not null ) then
		-- reset the parents and pk id and effective date for store and aso_root_fileName if it is future
		lv4loc_rec.aso_root_fileName := ltrim(rtrim(t_New_file_name));
	end if;

	-- Insert the record at store level with new attribute

	t_sqlnum := 6;

	insert into maxdata.lv4loc
	(lv4loc_id, last_update, changed_by_batch, version_id, lv4loc_userid,
		project_type, archive_flag, lv4mast_id, lv3loc_id, lv2loc_id, lv1loc_id,
		model_id, facet1_attach, start_date, end_date, x_coord, y_coord, z_coord,
		placed, total_items, total_units, total_caps, overlvls_id, xcoord_just_lkup,
	 	ycoord_just_lkup, zcoord_just_lkup, top_over, left_over, right_over, front_over,
		loc_color, orient, rotate, slope, xcoord_gap, ycoord_gap, zcoord_gap, min_xcoord_gap,
	 	min_ycoord_gap, min_zcoord_gap, max_xcoord_gap, max_ycoord_gap, max_zcoord_gap,
		anchor_lkup, used_cubic_meters, used_dsp_sqmeters, used_linear_meters, target_days_supply,
	 	address_1, address_2, city, state, zip, longitude, latitude, num_user1,
		num_user2, num_user3, num_user4, num_user5, num_user6, date_user7, date_user8,
	 	char_user9, char_user10, char_user11, char_user12, char_user13, bigchar_user14,
		bigchar_user15, height, width, depth, shape_lkup_id, aso_filename, aso_root_filename,
	 	aso_area, aso_area_id, tot_lv7dsp, tot_lv7lin, shape_id, destination_set_id, merch_id,
		merch_level, time_id, time_level, loc_link_id, merch_plan_id, layer_active,
	 	layer, store_state, lastsetdate, newsetdate, lastapprovedate, recalc_flag,
		source_loc_id, ver_loc_link_id, Ver_Effective_date)
	values(	lv4loc_rec.lv4loc_id, lv4loc_rec.last_update, lv4loc_rec.changed_by_batch, lv4loc_rec.version_id, lv4loc_rec.lv4loc_userid,
		lv4loc_rec.project_type, lv4loc_rec.archive_flag, lv4loc_rec.lv4mast_id, lv4loc_rec.lv3loc_id, lv4loc_rec.lv2loc_id, lv4loc_rec.lv1loc_id,
		lv4loc_rec.model_id, lv4loc_rec.facet1_attach, lv4loc_rec.start_date, lv4loc_rec.end_date, lv4loc_rec.x_coord, lv4loc_rec.y_coord, lv4loc_rec.z_coord,
		lv4loc_rec.placed, lv4loc_rec.total_items, lv4loc_rec.total_units, lv4loc_rec.total_caps, lv4loc_rec.overlvls_id, lv4loc_rec.xcoord_just_lkup,
		lv4loc_rec.ycoord_just_lkup, lv4loc_rec.zcoord_just_lkup, lv4loc_rec.top_over, lv4loc_rec.left_over, lv4loc_rec.right_over, lv4loc_rec.front_over,
		lv4loc_rec.loc_color, lv4loc_rec.orient, lv4loc_rec.rotate, lv4loc_rec.slope, lv4loc_rec.xcoord_gap, lv4loc_rec.ycoord_gap, lv4loc_rec.zcoord_gap, lv4loc_rec.min_xcoord_gap,
		lv4loc_rec.min_ycoord_gap, lv4loc_rec.min_zcoord_gap, lv4loc_rec.max_xcoord_gap, lv4loc_rec.max_ycoord_gap, lv4loc_rec.max_zcoord_gap,
		lv4loc_rec.anchor_lkup, lv4loc_rec.used_cubic_meters, lv4loc_rec.used_dsp_sqmeters, lv4loc_rec.used_linear_meters,  lv4loc_rec.target_days_supply,
		lv4loc_rec.address_1, lv4loc_rec.address_2, lv4loc_rec.city, lv4loc_rec.state, lv4loc_rec.zip, lv4loc_rec.longitude, lv4loc_rec.latitude, lv4loc_rec.num_user1,
		lv4loc_rec.num_user2, lv4loc_rec.num_user3, lv4loc_rec.num_user4, lv4loc_rec.num_user5, lv4loc_rec.num_user6, lv4loc_rec.date_user7, lv4loc_rec.date_user8,
		lv4loc_rec.char_user9, lv4loc_rec.char_user10, lv4loc_rec.char_user11, lv4loc_rec.char_user12, lv4loc_rec.char_user13, lv4loc_rec.bigchar_user14,
		lv4loc_rec.bigchar_user15, lv4loc_rec.height, lv4loc_rec.width, lv4loc_rec.depth, lv4loc_rec.shape_lkup_id, lv4loc_rec.aso_filename, lv4loc_rec.aso_root_filename,
		lv4loc_rec.aso_area, lv4loc_rec.aso_area_id, lv4loc_rec.tot_lv7dsp, lv4loc_rec.tot_lv7lin, lv4loc_rec.shape_id, lv4loc_rec.destination_set_id, lv4loc_rec.merch_id,
		lv4loc_rec.merch_level, lv4loc_rec.time_id, lv4loc_rec.time_level, lv4loc_rec.loc_link_id, lv4loc_rec.merch_plan_id, lv4loc_rec.layer_active,
		lv4loc_rec.layer, lv4loc_rec.store_state, lv4loc_rec.lastsetdate, lv4loc_rec.newsetdate, lv4loc_rec.lastapprovedate, lv4loc_rec.recalc_flag,
		lv4loc_rec.source_loc_id, lv4loc_rec.ver_loc_link_id, lv4loc_rec.Ver_Effective_date);

	--reinitialize

	loopcounter:=0;

	-- get lv5s information from Source in a cursor

	t_sqlnum := 7;

	open location_cursor for select * from maxdata.lv5loc where lv4loc_id = p_lv4loc_id and placed <> 0;

	-- Cursor through each lv5 create new PK id

	loop
		fetch location_cursor into lv5loc_rec;
		exit when location_cursor%notfound;

		-- Reset all necessary attributes

		-- Source Loc Id refers to the origin of the version

		lv5loc_rec.source_loc_id := lv5loc_rec.lv5loc_id;

		-- if failed to reset this SCP runs into errors

		lv5loc_rec.shape_id := 0;

		-- Reset the parents and Pk id

		maxapp.p_get_next_key(5, 2, 1, lv5loc_rec.lv5loc_id, t_errorstr);

		if t_errorstr is not null then
			raise integrityException;
		end if;

		lv5loc_rec.lv4loc_id := p_new_lv4loc_id;
		lv5loc_rec.lv3loc_id := t_lv3loc_id;
		lv5loc_rec.lv2loc_id := t_lv2loc_id;
		lv5loc_rec.lv1loc_id := t_lv1loc_id;
		lv5loc_rec.last_update := sysdate;

		loopcounter := 	loopcounter + 1;

		-- fill PL/SQL table with each new lv5 entry, has to be used for lv6, lv7

		lv5loc_tab(loopcounter) := lv5loc_rec;

		-- insert the entries

		t_sqlnum := 8;

		insert into maxdata.lv5loc(lv5loc_id, last_update, changed_by_batch, version_id, lv5loc_userid, lv5mast_id,
			lv4loc_id, lv3loc_id, lv2loc_id, lv1loc_id, model_id, facet1_attach, start_date,
			end_date, x_coord, y_coord, z_coord, placed, total_items, total_units, total_caps,
			overlvls_id, xcoord_just_lkup, ycoord_just_lkup, zcoord_just_lkup, top_over, left_over,
			right_over, front_over, loc_color, orient, rotate, slope, xcoord_gap, ycoord_gap,
			zcoord_gap, min_xcoord_gap, min_ycoord_gap, min_zcoord_gap, max_xcoord_gap, max_ycoord_gap,
			max_zcoord_gap, anchor_lkup, used_cubic_meters, used_dsp_sqmeters, used_linear_meters,
			num_user1, num_user2, num_user3, num_user4, num_user5, num_user6, date_user7,
			date_user8, char_user9, char_user10, char_user11, char_user12, char_user13, bigchar_user14,
			bigchar_user15, height, width, depth, shape_lkup_id, aso_filename, aso_root_filename,
			aso_area, aso_area_id, recalc_flag, tot_lv7flr, tot_lv7dsp, tot_lv7cub, tot_lv7lin,
			main_space, shape_id, merch_id, merch_level, time_id, time_level, loc_link_id, merch_plan_id,
			layer_active, layer, source_loc_id, ver_loc_link_id)
		values( lv5loc_rec.lv5loc_id, lv5loc_rec.last_update, lv5loc_rec.changed_by_batch, lv5loc_rec.version_id, lv5loc_rec.lv5loc_userid, lv5loc_rec.lv5mast_id,
			lv5loc_rec.lv4loc_id, lv5loc_rec.lv3loc_id, lv5loc_rec.lv2loc_id, lv5loc_rec.lv1loc_id, lv5loc_rec.model_id, lv5loc_rec.facet1_attach, lv5loc_rec.start_date,
			lv5loc_rec.end_date, lv5loc_rec.x_coord, lv5loc_rec.y_coord, lv5loc_rec.z_coord, lv5loc_rec.placed, lv5loc_rec.total_items, lv5loc_rec.total_units, lv5loc_rec.total_caps,
			lv5loc_rec.overlvls_id, lv5loc_rec.xcoord_just_lkup, lv5loc_rec.ycoord_just_lkup, lv5loc_rec.zcoord_just_lkup, lv5loc_rec.top_over, lv5loc_rec.left_over,
			lv5loc_rec.right_over, lv5loc_rec.front_over, lv5loc_rec.loc_color, lv5loc_rec.orient, lv5loc_rec.rotate, lv5loc_rec.slope, lv5loc_rec.xcoord_gap, lv5loc_rec.ycoord_gap,
			lv5loc_rec.zcoord_gap, lv5loc_rec.min_xcoord_gap, lv5loc_rec.min_ycoord_gap, lv5loc_rec.min_zcoord_gap, lv5loc_rec.max_xcoord_gap, lv5loc_rec.max_ycoord_gap,
			lv5loc_rec.max_zcoord_gap, lv5loc_rec.anchor_lkup, lv5loc_rec.used_cubic_meters, lv5loc_rec.used_dsp_sqmeters, lv5loc_rec.used_linear_meters,
			lv5loc_rec.num_user1, lv5loc_rec.num_user2, lv5loc_rec.num_user3, lv5loc_rec.num_user4, lv5loc_rec.num_user5, lv5loc_rec.num_user6, lv5loc_rec.date_user7,
			lv5loc_rec.date_user8, lv5loc_rec.char_user9, lv5loc_rec.char_user10, lv5loc_rec.char_user11, lv5loc_rec.char_user12, lv5loc_rec.char_user13, lv5loc_rec.bigchar_user14,
			lv5loc_rec.bigchar_user15, lv5loc_rec.height, lv5loc_rec.width, lv5loc_rec.depth, lv5loc_rec.shape_lkup_id, lv5loc_rec.aso_filename, lv5loc_rec.aso_root_filename,
			lv5loc_rec.aso_area, lv5loc_rec.aso_area_id, lv5loc_rec.recalc_flag, lv5loc_rec.tot_lv7flr, lv5loc_rec.tot_lv7dsp, lv5loc_rec.tot_lv7cub, lv5loc_rec.tot_lv7lin,
			lv5loc_rec.main_space, lv5loc_rec.shape_id, lv5loc_rec.merch_id, lv5loc_rec.merch_level, lv5loc_rec.time_id, lv5loc_rec.time_level, lv5loc_rec.loc_link_id, lv5loc_rec.merch_plan_id,
			lv5loc_rec.layer_active, lv5loc_rec.layer, lv5loc_rec.source_loc_id, lv5loc_rec.ver_loc_link_id);
	end loop;
	close location_cursor;

	-- Save the lv5 count.

	t_lv5_seq_num := loopcounter;


	-- reintialize

	loopcounter:=0;

	Parentcounter :=0;

	-- Iterate through each of lv6 and set the attributes

	t_sqlnum := 9;

	open location_cursor for select * from maxdata.lv6loc where lv4loc_id = p_lv4loc_id and placed <> 0;

	-- Iterate through each lv6

	loop
		fetch location_cursor into lv6loc_rec;
		exit when location_cursor%notfound;

		-- Source Loc Id is the refernce to origin

		lv6loc_rec.source_loc_id := lv6loc_rec.lv6loc_id;

		-- SCP needs this to be set

		lv6loc_rec.shape_id := 0;

		-- set pk ids and lv1 to lv4 parents.

		maxapp.p_get_next_key(6, 2, 1, lv6loc_rec.lv6loc_id, t_errorstr);

		if t_errorstr is not null then
			raise integrityException;
		end if;

		lv6loc_rec.lv4loc_id := p_new_lv4loc_id;
		lv6loc_rec.lv3loc_id := t_lv3loc_id;
		lv6loc_rec.lv2loc_id := t_lv2loc_id;
		lv6loc_rec.lv1loc_id := t_lv1loc_id;

		-- reset lv5 id picking the relevent while iterating through the array of lv5s

		for Parentcounter in 1..t_lv5_seq_num
		loop
			if (lv5loc_tab(Parentcounter).source_loc_id = lv6loc_rec.lv5loc_id) then
				lv6loc_rec.lv5loc_id := lv5loc_tab(Parentcounter).lv5loc_id;
				exit;
			end if;

		end loop;

		loopcounter := 	loopcounter + 1;

		-- prepare array of entries for lv6 to be used for lv7

		lv6loc_tab(loopcounter) := lv6loc_rec;

		-- insert all the records

		t_sqlnum := 10;

		insert into maxdata.lv6loc(lv6loc_id, last_update, changed_by_batch, version_id, lv6loc_userid, lv6mast_id,
			lv5loc_id, lv4loc_id, lv3loc_id, lv2loc_id, lv1loc_id, model_id, facet1_attach,
			start_date, end_date, x_coord, y_coord, z_coord, placed, total_items, total_units,
			total_caps, overlvls_id, xcoord_just_lkup, ycoord_just_lkup, zcoord_just_lkup,
			top_over, left_over, right_over, front_over, loc_color, orient, rotate, slope,
			xcoord_gap, ycoord_gap, zcoord_gap, min_xcoord_gap, min_ycoord_gap, min_zcoord_gap,
			max_xcoord_gap, max_ycoord_gap, max_zcoord_gap, anchor_lkup, used_cubic_meters,
			used_dsp_sqmeters, used_linear_meters, num_user1, num_user2, num_user3, num_user4,
			num_user5, num_user6, date_user7, date_user8, char_user9, char_user10, char_user11,
			char_user12, char_user13, bigchar_user14, bigchar_user15, height, width, depth,
			shape_lkup_id, aso_filename, aso_root_filename, aso_area, aso_area_id, recalc_flag,
			tot_lv7flr, tot_lv7dsp, tot_lv7cub, tot_lv7lin, shape_id, merch_id, merch_level,
			time_id, time_level, loc_link_id, merch_plan_id, layer_active, layer, source_loc_id,
			ver_loc_link_id)
		values( lv6loc_rec.lv6loc_id, lv6loc_rec.last_update, lv6loc_rec.changed_by_batch, lv6loc_rec.version_id, lv6loc_rec.lv6loc_userid, lv6loc_rec.lv6mast_id,
			lv6loc_rec.lv5loc_id, lv6loc_rec.lv4loc_id, lv6loc_rec.lv3loc_id, lv6loc_rec.lv2loc_id, lv6loc_rec.lv1loc_id, lv6loc_rec.model_id, lv6loc_rec.facet1_attach,
			lv6loc_rec.start_date, lv6loc_rec.end_date, lv6loc_rec.x_coord, lv6loc_rec.y_coord, lv6loc_rec.z_coord, lv6loc_rec.placed, lv6loc_rec.total_items, lv6loc_rec.total_units,
			lv6loc_rec.total_caps, lv6loc_rec.overlvls_id, lv6loc_rec.xcoord_just_lkup, lv6loc_rec.ycoord_just_lkup, lv6loc_rec.zcoord_just_lkup,
			lv6loc_rec.top_over, lv6loc_rec.left_over, lv6loc_rec.right_over, lv6loc_rec.front_over, lv6loc_rec.loc_color, lv6loc_rec.orient, lv6loc_rec.rotate, lv6loc_rec.slope,
			lv6loc_rec.xcoord_gap, lv6loc_rec.ycoord_gap, lv6loc_rec.zcoord_gap, lv6loc_rec.min_xcoord_gap, lv6loc_rec.min_ycoord_gap, lv6loc_rec.min_zcoord_gap,
			lv6loc_rec.max_xcoord_gap, lv6loc_rec.max_ycoord_gap, lv6loc_rec.max_zcoord_gap, lv6loc_rec.anchor_lkup, lv6loc_rec.used_cubic_meters,
			lv6loc_rec.used_dsp_sqmeters, lv6loc_rec.used_linear_meters, lv6loc_rec.num_user1, lv6loc_rec.num_user2, lv6loc_rec.num_user3, lv6loc_rec.num_user4,
			lv6loc_rec.num_user5, lv6loc_rec.num_user6, lv6loc_rec.date_user7, lv6loc_rec.date_user8, lv6loc_rec.char_user9, lv6loc_rec.char_user10, lv6loc_rec.char_user11,
			lv6loc_rec.char_user12, lv6loc_rec.char_user13, lv6loc_rec.bigchar_user14, lv6loc_rec.bigchar_user15, lv6loc_rec.height, lv6loc_rec.width, lv6loc_rec.depth,
			lv6loc_rec.shape_lkup_id, lv6loc_rec.aso_filename, lv6loc_rec.aso_root_filename, lv6loc_rec.aso_area, lv6loc_rec.aso_area_id, lv6loc_rec.recalc_flag,
			lv6loc_rec.tot_lv7flr, lv6loc_rec.tot_lv7dsp, lv6loc_rec.tot_lv7cub, lv6loc_rec.tot_lv7lin, lv6loc_rec.shape_id, lv6loc_rec.merch_id, lv6loc_rec.merch_level,
			lv6loc_rec.time_id, lv6loc_rec.time_level, lv6loc_rec.loc_link_id, lv6loc_rec.merch_plan_id, lv6loc_rec.layer_active, lv6loc_rec.layer, lv6loc_rec.source_loc_id,
			lv6loc_rec.ver_loc_link_id);

	end loop;
	close location_cursor;

	-- Save the lv5 count.

	t_lv6_seq_num := loopcounter;



	-- reinitialize

	loopcounter:=0;

	Parentcounter :=0;

	-- Iterate through each lv7 and reset attributes

	t_sqlnum := 11;

	open location_cursor for select * from maxdata.lv7loc
		where lv4loc_id = p_lv4loc_id and record_type = 'L' and layer = 1 and placed <> 0 and (delete_flag = 0 OR delete_flag is null);

	-- Iterate through

	loop
		fetch location_cursor into lv7loc_rec;
		exit when location_cursor%notfound;

		-- Source Loc id is reference to immediate origin

		lv7loc_rec.source_loc_id := lv7loc_rec.lv7loc_id;

		-- SCP needs this to be set

		lv7loc_rec.shape_id := 0;

		-- reset PK ids and lv1 to lv4 parents.

		maxapp.p_get_next_key(7, 2, 1, lv7loc_rec.lv7loc_id, t_errorstr);

		if t_errorstr is not null then
			raise integrityException;
		end if;

		lv7loc_rec.lv4loc_id := p_new_lv4loc_id;
		lv7loc_rec.lv3loc_id := t_lv3loc_id;
		lv7loc_rec.lv2loc_id := t_lv2loc_id;
		lv7loc_rec.lv1loc_id := t_lv1loc_id;

		-- reset lv5 parents iterating through the array and matching the right one

		for Parentcounter in 1..t_lv5_seq_num
		loop
			if (lv5loc_tab(Parentcounter).source_loc_id = lv7loc_rec.lv5loc_id) then
				lv7loc_rec.lv5loc_id := lv5loc_tab(Parentcounter).lv5loc_id;
				exit;
			end if;

		end loop;

		-- reintiailize

		Parentcounter:=0;

		-- reset lv6 parents iterating through the array and matching the right one

		for Parentcounter in 1..t_lv6_seq_num
		loop
			if (lv6loc_tab(Parentcounter).source_loc_id = lv7loc_rec.lv6loc_id) then
				lv7loc_rec.lv6loc_id := lv6loc_tab(Parentcounter).lv6loc_id;
				exit;
			end if;

		end loop;

		loopcounter := 	loopcounter + 1;

		-- Can be useful when MLP support is implemented

		-- lv7loc_tab(loopcounter) := lv7loc_rec;

		-- Insert all the entries.

		t_sqlnum := 12;

		insert into maxdata.lv7loc(lv7loc_id, last_update, changed_by_batch, version_id, lv7loc_userid, lv7mast_id, lv6loc_id,
			lv5loc_id, lv4loc_id, lv3loc_id, lv2loc_id, lv1loc_id, model_id, facet1_attach, start_date,
			end_date, x_coord, y_coord, z_coord, placed, total_items, total_units, total_caps, overlvls_id,
			xcoord_just_lkup, ycoord_just_lkup, zcoord_just_lkup, top_over, left_over, right_over, front_over,
			loc_color, orient, rotate, slope, xcoord_gap, ycoord_gap, zcoord_gap, min_xcoord_gap, min_ycoord_gap,
			min_zcoord_gap, max_xcoord_gap, max_ycoord_gap, max_zcoord_gap, anchor_lkup, used_cubic_meters,
			used_dsp_sqmeters, used_linear_meters, target_days_supply, num_user1, num_user2, num_user3, num_user4,
			num_user5, num_user6, date_user7, date_user8, char_user9, char_user10, char_user11, char_user12,
			char_user13, bigchar_user14, bigchar_user15, seg_fixed_width, seg_set_id, mvmt_applied, record_type,
			pog_formatted, group_master_id, asst_master_id, depth, height, width, pog_opp_height, pog_opp_width,
			pog_opp_depth, merch_arriv_period, pog_status, pog_default_date, default_view, pog_name,
			subasst_id, cpw_days_in_pd, cpw_start_pd_lkup, cpw_end_pd_lkup, cpw_total_stores, cpw_stores_inc,
			shape_lkup_id, aso_filename, aso_root_filename, aso_area, aso_area_id, recalc_flag, alloc_sq_meters,
			on_dsp_front, on_dsp_back, on_dsp_left, on_dsp_right, alloc_space, used_flr_sqmeters, main_space,
			shape_id, merch_id, merch_level, time_id, time_level, loc_link_id, subassortment_id, layer_active,
			layer, item_index_order, delete_flag, state, source_loc_id, ver_loc_link_id)
		values (lv7loc_rec.lv7loc_id, lv7loc_rec.last_update, lv7loc_rec.changed_by_batch, lv7loc_rec.version_id, lv7loc_rec.lv7loc_userid, lv7loc_rec.lv7mast_id, lv7loc_rec.lv6loc_id,
			lv7loc_rec.lv5loc_id, lv7loc_rec.lv4loc_id, lv7loc_rec.lv3loc_id, lv7loc_rec.lv2loc_id, lv7loc_rec.lv1loc_id, lv7loc_rec.model_id, lv7loc_rec.facet1_attach, lv7loc_rec.start_date,
			lv7loc_rec.end_date, lv7loc_rec.x_coord, lv7loc_rec.y_coord, lv7loc_rec.z_coord, lv7loc_rec.placed, lv7loc_rec.total_items, lv7loc_rec.total_units, lv7loc_rec.total_caps, lv7loc_rec.overlvls_id,
			lv7loc_rec.xcoord_just_lkup, lv7loc_rec.ycoord_just_lkup, lv7loc_rec.zcoord_just_lkup, lv7loc_rec.top_over, lv7loc_rec.left_over, lv7loc_rec.right_over, lv7loc_rec.front_over,
			lv7loc_rec.loc_color, lv7loc_rec.orient, lv7loc_rec.rotate, lv7loc_rec.slope, lv7loc_rec.xcoord_gap, lv7loc_rec.ycoord_gap, lv7loc_rec.zcoord_gap, lv7loc_rec.min_xcoord_gap, lv7loc_rec.min_ycoord_gap,
			lv7loc_rec.min_zcoord_gap, lv7loc_rec.max_xcoord_gap, lv7loc_rec.max_ycoord_gap, lv7loc_rec.max_zcoord_gap, lv7loc_rec.anchor_lkup, lv7loc_rec.used_cubic_meters,
			lv7loc_rec.used_dsp_sqmeters, lv7loc_rec.used_linear_meters, lv7loc_rec.target_days_supply, lv7loc_rec.num_user1, lv7loc_rec.num_user2, lv7loc_rec.num_user3, lv7loc_rec.num_user4,
			lv7loc_rec.num_user5, lv7loc_rec.num_user6, lv7loc_rec.date_user7, lv7loc_rec.date_user8, lv7loc_rec.char_user9, lv7loc_rec.char_user10, lv7loc_rec.char_user11, lv7loc_rec.char_user12,
			lv7loc_rec.char_user13, lv7loc_rec.bigchar_user14, lv7loc_rec.bigchar_user15, lv7loc_rec.seg_fixed_width, lv7loc_rec.seg_set_id, lv7loc_rec.mvmt_applied, lv7loc_rec.record_type,
			lv7loc_rec.pog_formatted, lv7loc_rec.group_master_id, lv7loc_rec.asst_master_id, lv7loc_rec.depth, lv7loc_rec.height, lv7loc_rec.width, lv7loc_rec.pog_opp_height, lv7loc_rec.pog_opp_width,
			lv7loc_rec.pog_opp_depth, lv7loc_rec.merch_arriv_period, lv7loc_rec.pog_status, lv7loc_rec.pog_default_date, lv7loc_rec.default_view, lv7loc_rec.pog_name,
			lv7loc_rec.subasst_id, lv7loc_rec.cpw_days_in_pd, lv7loc_rec.cpw_start_pd_lkup, lv7loc_rec.cpw_end_pd_lkup, lv7loc_rec.cpw_total_stores, lv7loc_rec.cpw_stores_inc,
			lv7loc_rec.shape_lkup_id, lv7loc_rec.aso_filename, lv7loc_rec.aso_root_filename, lv7loc_rec.aso_area, lv7loc_rec.aso_area_id, lv7loc_rec.recalc_flag, lv7loc_rec.alloc_sq_meters,
			lv7loc_rec.on_dsp_front, lv7loc_rec.on_dsp_back, lv7loc_rec.on_dsp_left, lv7loc_rec.on_dsp_right, lv7loc_rec.alloc_space, lv7loc_rec.used_flr_sqmeters, lv7loc_rec.main_space,
			lv7loc_rec.shape_id, lv7loc_rec.merch_id, lv7loc_rec.merch_level, lv7loc_rec.time_id, lv7loc_rec.time_level, lv7loc_rec.loc_link_id, lv7loc_rec.subassortment_id, lv7loc_rec.layer_active,
			lv7loc_rec.layer, lv7loc_rec.item_index_order, lv7loc_rec.delete_flag, lv7loc_rec.state, lv7loc_rec.source_loc_id, lv7loc_rec.ver_loc_link_id);

	end loop;
	close location_cursor;

<<version_layers>>

	-- version layers to suuport MLP

	t_sqlnum := 13;

	maxdata.p_version_layers (p_lv4loc_id, p_basePeriodID, p_targetPeriodID, p_CAD_file_name, p_new_lv4loc_id, p_status, p_ErrorMsg);



	-- if fails then integrity issue

	if (p_status =0) then
		raise integrityException;
	end if;

	-- Log for debugging.

	t_Errorstr :=
		p_lv4loc_id
		|| ',"' || p_new_lv4loc_userid
		|| '",' || p_new_lv4loc_id
		|| ','  || p_varcharEffectiveDate;
	maxdata.ins_import_log (
		'p_new_version',
		'info',
		t_errorstr,
		substr(p_CAD_file_name,1,80),
		p_basePeriodID,
		p_targetPeriodID);

	-- If everything is fine then commit.
	-- Even when it is called from p_activate_version, it is still OK to
	-- commit here because when p_activate_version fails, its error handler
	-- calls p_delete_version to delete the committed outgoing version.

	p_status := 1;
	p_Errormsg := 'New_version: Successful';




exception

	-- Any business violation has to exit in a rollback and status = 0 and
	-- reasonable business violation message has to be passed back and new store id is null

	when businessException then
		goto errhndlr;

	-- Any integrity violation has to exit in a rollback and status = 0 and
	-- reasonable violation message has to be passed back and new store id is null

	when IntegrityException then
		goto errhndlr;

	-- Any unprecedented errors exit in a rollback and status = 0 and
	-- System Error message has to be passed back and new store id is null


	when others then
		t_error := SQLCODE;
		t_Errorstr := SQLERRM;
		goto errhndlr;


<<errhndlr>>


	if t_Errorstr is null then
		t_errorstr := 'Unknown error';
	end if;

	p_status := 0;
	p_new_lv4loc_id := null;
	p_ErrorMsg := 'New_version: ' || t_Errorstr
		|| '.  Sqlnum: ' || t_sqlnum
		|| '  Prm: ' || p_lv4loc_id
		|| ', "' || p_new_lv4loc_userid
		|| '", "' || p_varcharEffectiveDate
		|| '", ' || p_basePeriodID
		|| ', '  || p_targetPeriodID
		|| ', "' || p_CAD_file_name
		|| '"';


end; -- End of procedure p_new_version

/

  GRANT EXECUTE ON "MAXDATA"."P_NEW_VERSION" TO "MAXUSER";
  GRANT EXECUTE ON "MAXDATA"."P_NEW_VERSION" TO "MADMAX";
