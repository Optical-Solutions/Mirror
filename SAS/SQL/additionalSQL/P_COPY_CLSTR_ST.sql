--------------------------------------------------------
--  DDL for Procedure P_COPY_CLSTR_ST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COPY_CLSTR_ST" 
(
	src_clstr_st_id		in number,
	dstn_clstr_st_nm		in varchar2,
	dstn_clstr_st_desc	in varchar2,
	copy_usr_id			in number,
 	n_clstr_st_id		out number
)
as
 tmp_id		number(10);
 cnfg_prev_seq	number(10);
 grp_prev_seq	number(10);
 spc_prev_seq	number(10);
 ld_sqlcode		number;
 ls_sql		varchar2(80);  --import_log.log_text is varchar2(80)
 ls_sqlerrm		varchar2(80);

begin
-- created  for ims 5.0 clustering

	n_clstr_st_id := 0;
	tmp_id := 0;
 	cnfg_prev_seq := 0;
	grp_prev_seq := 0;
	spc_prev_seq := 0;


	-- delete the log records that belong to this procedure.

	delete from maxdata.import_log where log_id = 'p_copy_clstr_st';
	commit;


    ls_sql := 'Prm:'
	|| src_clstr_st_id || ','
	|| dstn_clstr_st_nm || ','
	|| dstn_clstr_st_desc || ','
	|| copy_usr_id || ','
	|| n_clstr_st_id;
    maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);
    commit;


	---------- copying into temp tables without changing values
	--- place a lock to ensure that the cluster set being copied is not altered by another process meanwhile.

	ls_sql := 'get next clstr_st_id';

	maxapp.f_get_seq(180,0,n_clstr_st_id);

	--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,n_clstr_st_id,null);

	---------- make sure that  temp tables are empty.

	ls_sql := 'deleting rows from temp tables';

	delete from tmp_clstr_spc_map;
	delete from tmp_clstr_str;		--- 186
	delete from tmp_clstr_spc;		--- 184
	delete from tmp_clstr_grp;		--- 182
	delete from tmp_clstr_cnfg_st;	--- 181
	delete from tmp_clstr_st;		--- 180

	--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

	-- save the current sequence values.  because sequence is not
	-- initialized, we have to continue from the current values.
	-- they will be used when we update ids later.
	-- note: use nextval instead of currval. currval wouldn't work at the
	-- first time.

	ls_sql := 'getting next sequence values';

	select tmp_clstr_cnfg_st_seq.nextval
	into cnfg_prev_seq from dual;

	select tmp_clstr_grp_seq.nextval
	into grp_prev_seq from dual;

	select tmp_clstr_spc_map_seq.nextval
	into spc_prev_seq from dual;

	--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,
	--	to_char(cnfg_prev_seq), grp_prev_seq, spc_prev_seq);

	--begin work;
		ls_sql := 'clstr_st->temp';

		insert into 	tmp_clstr_st(
			clstr_st_nm, clstr_st_desc, fr_loc_lvl, fr_loc_mmbr, fr_merch_lvl, fr_merch_mmbr, fr_time_lvl, fr_time_mmbr,path_id)
		select	clstr_st_nm, clstr_st_desc, fr_loc_lvl, fr_loc_mmbr, fr_merch_lvl, fr_merch_mmbr, fr_time_lvl, fr_time_mmbr, path_id
		from	maxdata.clstr_st
		where	clstr_st_id = src_clstr_st_id;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);


		ls_sql := 'cnfg->temp';

		insert into 	tmp_clstr_cnfg_st(
			o_clstr_cnfg_st_id,	clstr_cnfg_st_nm, kpi_fld_id, kpi_fld_lvl, kpi_mast_id, kpi_ordr, grpng_mthd, no_of_grps)
		select	clstr_cnfg_st_id,	clstr_cnfg_st_nm, kpi_fld_id, kpi_fld_lvl, kpi_mast_id, kpi_ordr, grpng_mthd, no_of_grps
		from	maxdata.clstr_cnfg_st
		where	clstr_st_id = src_clstr_st_id;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

		ls_sql := 'grp -> tmp';

		insert into	tmp_clstr_grp(
			o_clstr_grp_id,	o_clstr_cnfg_st_id,	nm_of_grp, pstn, clr, brkpnt_vlu, mxm_vlu)
		select	clstr_grp_id,	clstr_cnfg_st_id,	nm_of_grp, pstn, clr, brkpnt_vlu, mxm_vlu
		from	maxdata.clstr_grp
		where	clstr_st_id = src_clstr_st_id;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

		ls_sql := 'spc -> tmp';

		insert into	tmp_clstr_spc(
			o_clstr_spc_id,	o_clstr_grp_id,	clstr_spc_clr, clstr_spc_typ, clstr_spc_nm)
		select	clstr_spc_id,	clstr_grp_id,	clstr_spc_clr, clstr_spc_typ, clstr_spc_nm
		from	maxdata.clstr_spc
		where	clstr_st_id = src_clstr_st_id;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

		ls_sql := 'tmp_spc -> tmp_spc_map';

		insert into	tmp_clstr_spc_map( o_clstr_spc_id )
		select	distinct o_clstr_spc_id
		from	maxdata.tmp_clstr_spc;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

		ls_sql := 'str -> tmp';

		insert into	tmp_clstr_str(
			lvnloc_id, lvnloc_lvl,	 o_at_clstr_spc_id,	o_fnl_clstr_spc_id)
		select	lvnloc_id, lvnloc_lvl, at_clstr_spc_id,	fnl_clstr_spc_id
		from	maxdata.clstr_str
		where	clstr_st_id = src_clstr_st_id;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

	--commit work;

	---------- generating sequence ids.
	--begin work;

		ls_sql := 'get next cnfg id';

		maxapp.f_get_seq(181,0,tmp_id);

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,tmp_id,null);

		-- serial_id was started from the previous sequence value, so
		-- we have to subtract it.

		ls_sql := 'update cnfg ids';

		update tmp_clstr_cnfg_st
		set	n_clstr_cnfg_st_id = tmp_id + serial_id - cnfg_prev_seq;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

		-- update the sequence entry with the max id.
		-- if max id is null (that is, there was no rows in the temp table),
		-- then don't update the sequence.

		ls_sql := 'update sequence entry';

		select max(n_clstr_cnfg_st_id) into tmp_id from tmp_clstr_cnfg_st;

		if tmp_id is not null then
			update	maxapp.sequence
			set	seq_num = tmp_id
			where	level_type = 181
		    		and	entity_type = 0;
		end if;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,tmp_id,null);

	--commit work;

	--begin work;
		ls_sql := 'get next grp id';

		maxapp.f_get_seq(182,0,tmp_id);

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,tmp_id,null);

		ls_sql := 'update grp ids';

		update tmp_clstr_grp
		set	n_clstr_grp_id = tmp_id + serial_id - grp_prev_seq;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

		ls_sql := 'update sequence entry';

		select max(n_clstr_grp_id) into tmp_id from tmp_clstr_grp;

		if tmp_id is not null then
			update	maxapp.sequence
			set	seq_num = tmp_id
			where	level_type = 182
		    		and	entity_type = 0;
		end if;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,tmp_id,null);

	--commit work;

	--begin work;
		ls_sql := 'get next spc id';

		maxapp.f_get_seq(184,0,tmp_id);

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,tmp_id,null);

		ls_sql := 'reset ids';

		update tmp_clstr_spc_map
		set	n_clstr_spc_id = tmp_id + serial_id - spc_prev_seq;

		ls_sql := 'update sequence entry';

		select max(n_clstr_spc_id) into tmp_id from tmp_clstr_spc_map;

		if tmp_id is not null then
			update	maxapp.sequence
			set	seq_num = tmp_id
			where	level_type = 184
		    		and	entity_type = 0;
		end if;

		--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,tmp_id,null);

	--commit work;

	---------- copying to permanent tables

	ls_sql := 'tmp->clstr_st';

	insert into maxdata.clstr_st(
		clstr_st_id, 	clstr_st_nm,	crt_dt,	lst_usr_id, 	lst_updt,	clstr_st_desc,	status,
		fr_loc_lvl, fr_loc_mmbr, fr_merch_lvl, fr_merch_mmbr, fr_time_lvl, fr_time_mmbr, path_id)
	select	n_clstr_st_id, 	dstn_clstr_st_nm,	sysdate,	copy_usr_id, 	sysdate,	dstn_clstr_st_desc,	'inactive',
		fr_loc_lvl, fr_loc_mmbr, fr_merch_lvl, fr_merch_mmbr, fr_time_lvl, fr_time_mmbr,path_id
	from	tmp_clstr_st;

	--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

	ls_sql := 'tmp->cnfg';

	insert into maxdata.clstr_cnfg_st(
		clstr_cnfg_st_id,	clstr_st_id,	clstr_cnfg_st_nm, kpi_fld_id, kpi_fld_lvl, kpi_mast_id, kpi_ordr, grpng_mthd, no_of_grps)
	select	n_clstr_cnfg_st_id,	n_clstr_st_id,	clstr_cnfg_st_nm, kpi_fld_id, kpi_fld_lvl, kpi_mast_id, kpi_ordr, grpng_mthd, no_of_grps
	from	tmp_clstr_cnfg_st;

	--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

	ls_sql := 'tmp->grp';

	insert into maxdata.clstr_grp(
		clstr_grp_id,	clstr_st_id,	clstr_cnfg_st_id,	nm_of_grp, pstn, clr, brkpnt_vlu, mxm_vlu)
	select	n_clstr_grp_id,	n_clstr_st_id,	n_clstr_cnfg_st_id,	nm_of_grp, pstn, clr, brkpnt_vlu, mxm_vlu
	from	tmp_clstr_grp, tmp_clstr_cnfg_st
	where	tmp_clstr_grp.o_clstr_cnfg_st_id = tmp_clstr_cnfg_st.o_clstr_cnfg_st_id;

	--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

	ls_sql := 'tmp->spc';

	insert into maxdata.clstr_spc(
		clstr_spc_id,	clstr_st_id,	clstr_grp_id,	clstr_spc_clr, clstr_spc_typ, clstr_spc_nm)
	select	n_clstr_spc_id,	n_clstr_st_id,	n_clstr_grp_id,	clstr_spc_clr, clstr_spc_typ, clstr_spc_nm
	from	tmp_clstr_spc, tmp_clstr_spc_map, tmp_clstr_grp
	where	tmp_clstr_spc.o_clstr_spc_id = tmp_clstr_spc_map.o_clstr_spc_id
	    and	tmp_clstr_spc.o_clstr_grp_id = tmp_clstr_grp.o_clstr_grp_id;

	--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

	ls_sql := 'tmp_spc_map->spc';

	insert into maxdata.clstr_spc(
		clstr_spc_id,	clstr_st_id,	clstr_grp_id,	clstr_spc_clr, clstr_spc_typ, clstr_spc_nm)
	select	n_clstr_spc_id,	n_clstr_st_id,	-1,		clstr_spc_clr, clstr_spc_typ, clstr_spc_nm
	from	tmp_clstr_spc, tmp_clstr_spc_map
	where	tmp_clstr_spc.o_clstr_spc_id = tmp_clstr_spc_map.o_clstr_spc_id
	    and	tmp_clstr_spc.o_clstr_grp_id = -1;

	--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

	ls_sql := 'tmp->str';

	insert into maxdata.clstr_str(
		clstr_st_id,	lvnloc_id, lvnloc_lvl,		at_clstr_spc_id,		fnl_clstr_spc_id)
	select	n_clstr_st_id,	lvnloc_id, lvnloc_lvl,		map1.n_clstr_spc_id,	map2.n_clstr_spc_id
	from	tmp_clstr_str, tmp_clstr_spc_map map1, tmp_clstr_spc_map map2
	where	tmp_clstr_str.o_at_clstr_spc_id = map1.o_clstr_spc_id
	    and	tmp_clstr_str.o_fnl_clstr_spc_id = map2.o_clstr_spc_id;

	--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

	-- clean up the temp tables.

	ls_sql := 'cleanup tmp';

	delete from tmp_clstr_spc_map;
	delete from tmp_clstr_str;		--- 186
	delete from tmp_clstr_spc;		--- 184
	delete from tmp_clstr_grp;		--- 182
	delete from tmp_clstr_cnfg_st;	--- 181
	delete from tmp_clstr_st;		--- 180

	--maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);

	ls_sql := 'commit';

	commit;

	maxdata.ins_import_log('p_copy_clstr_st','info',ls_sql,null,null,null);
	commit;

-- exception handler.  rollback the current transaction, and
-- log where we were (ld_sql) and what was the error (ld_errsql, etc)
-- at every critical step, we save the status in ld_sql so that
-- when an error occurs, we may have info to log.

exception
  when others then
    rollback;

    ls_sql := SQLERRM
    || '(P_COPY_CLSTR_ST Prm:'
	|| src_clstr_st_id || ','
	|| dstn_clstr_st_nm || ','
	|| dstn_clstr_st_desc || ','
	|| copy_usr_id || ','
	|| n_clstr_st_id
    || ' error near:' || ls_sql || ')';
    maxdata.ins_import_log('p_copy_clstr_st','error',substr(ls_sql,1,255),null,null,null);
    commit;

    raise_application_error(-20001,ls_sql);

end; -- end of proc

/

  GRANT EXECUTE ON "MAXDATA"."P_COPY_CLSTR_ST" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_COPY_CLSTR_ST" TO "MAXAPP";
  GRANT EXECUTE ON "MAXDATA"."P_COPY_CLSTR_ST" TO "MAXUSER";
