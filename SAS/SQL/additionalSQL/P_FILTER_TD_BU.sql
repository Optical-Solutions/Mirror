--------------------------------------------------------
--  DDL for Procedure P_FILTER_TD_BU
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_FILTER_TD_BU" (
	in_src_merch_td_flag Number, -- TD/BU flag. 1 for TD, 2 for BU.
	in_src_loc_td_flag Number,
	in_src_priority_hier number,		-- 1 for LOC, 2 for MERCH
	in_debug_flag Number
) as
/*-----------------------------------------------------------------------
Change History:

$Log: 2143_p_filter_td_bu.sql,v $
Revision 1.6  2007/06/19 14:39:42  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.2  2005/11/29 19:35:49  joscho
Missed from the release script, so do a dummy checkin to make it released next time.

Revision 1.1  2005/09/09 19:56:15  joscho
orig: 5.6 new db script


-- V5.3.3
-- 06/18/02 Sachin		Commented Analyze command for temp table since it does not work for temp tables.
-- V5.3.2
-- 05/20/02 Joseph Cho		Use 1 for MERCH, and 2 for Loc for priority hier. Use >=2 for multi_td_bu_row.
-- 05/20/02 Joseph Cho		When debug, copy data before delete.
-- 05/20/02 Joseph Cho		Use distinct for td_bu_dup
-- 05/16/02 Joseph Cho		Analyze temp tables for optimizer.
-- 05/16/02 Joseph Cho		change logic to filter dup rows by TD/BU..
-- 05/13/2002 Joseph Cho		Fixed delete stmt. Added a debug_flag.
-- 05/10/2002 Joseph Cho		Support filtering for TD/BU.
-----------------------------------------------------------------------*/


-- Implicit input table to this procedure: t_pc_td_bu_pw

n_sqlnum Number(10,0) := 0;
v_sql varchar2(1000) := ' ';
v_sql2 varchar2(255);
v_sql3 varchar2(255);
v_param varchar2(255) := ' ';
t_td_bu_col varchar2(20);
t_loc_td_bu_col varchar2(20);
t_merch_td_bu_col varchar2(20);
t_loc_hier_type number(1) := 2;
t_merch_hier_type number(1) := 1;
t_cnt number;

begin

-- Find out TopDown/BottomUp worksheets.

v_sql:='truncate table maxdata.t_pc_td_bu_pw' ;
n_sqlnum := 200;
execute immediate v_sql;

n_sqlnum := 400;
insert into maxdata.t_pc_td_bu_pw(loc_td_pw_id, loc_bu_pw_id)
select td.pw_id, bu.pw_id
from maxdata.t_pc_pw td, maxdata.t_pc_pw bu
where td.to_loc_lev = bu.from_loc_lev
and td.from_loc_lev <> bu.to_loc_lev; -- eliminate PW whose from/to level are the same.

n_sqlnum := 600;
insert into maxdata.t_pc_td_bu_pw(merch_td_pw_id, merch_bu_pw_id)
select td.pw_id, bu.pw_id
from maxdata.t_pc_pw td, maxdata.t_pc_pw bu
where td.to_merch_lev = bu.from_merch_lev
and td.from_merch_lev <> bu.to_merch_lev; -- eliminate PW whose from/to level are the same.

--v_sql := 'analyze table maxdata.t_pc_td_bu_pw estimate statistics sample 10 percent';
--execute immediate v_sql;

select count(*) into t_cnt
from maxdata.t_pc_td_bu_pw
where rownum<=1;

if t_cnt = 0 then return; end if;

-- Find dup row's TD/BU_PW_IDs for loc and merch.

n_sqlnum := 2500;
v_sql := 'truncate table maxdata.t_pc_td_bu_dup';
execute immediate v_sql;

-- Loc
-- NOTE: 'distinct' is to handle the case that the same pw is involved with
-- more than one TD/BU.

n_sqlnum := 2800;
v_sql :=
'insert into maxdata.t_pc_td_bu_dup(loc_td_pw_id, loc_bu_pw_id,m_lev, m_id, l_lev, l_id, t_lev, t_id) '||
' select distinct t.loc_td_pw_id, t.loc_bu_pw_id, '||
'	m1.m_lev, m1.m_id, m1.l_lev, m1.l_id, m1.t_lev, m1.t_id '||
' from maxdata.t_pc_src_mem m1, maxdata.t_pc_src_mem m2, maxdata.t_pc_td_bu_pw t' ||
' where m1.m_lev = m2.m_lev  and m1.m_id = m2.m_id  '||
' and m1.l_lev = m2.l_lev  and m1.l_id = m2.l_id  '||
' and m1.t_lev = m2.t_lev  and m1.t_id = m2.t_id  '||
' and m1.pw_id <> m2.pw_id '||								   -- eliminate join same row.
' and t.loc_td_pw_id= m1.pw_id and t.loc_bu_pw_id= m2.pw_id';

if in_debug_flag = 1 then
	v_sql2 := substr(v_sql,1,255);
	v_sql3 := substr(v_sql,256,255);
	maxdata.ins_import_log ('p_filter_td_bu','info', v_sql2, v_sql3, n_sqlnum, null);
end if;
execute immediate v_sql;

-- Merch

n_sqlnum := 2900;
v_sql :=
'insert into maxdata.t_pc_td_bu_dup(merch_td_pw_id, merch_bu_pw_id,m_lev, m_id, l_lev, l_id, t_lev, t_id) '||
' select distinct t.merch_td_pw_id, t.merch_bu_pw_id, '||
'	m1.m_lev, m1.m_id, m1.l_lev, m1.l_id, m1.t_lev, m1.t_id '||
' from maxdata.t_pc_src_mem m1, maxdata.t_pc_src_mem m2, maxdata.t_pc_td_bu_pw t' ||
' where m1.m_lev = m2.m_lev  and m1.m_id = m2.m_id  '||
' and m1.l_lev = m2.l_lev  and m1.l_id = m2.l_id  '||
' and m1.t_lev = m2.t_lev  and m1.t_id = m2.t_id  '||
' and m1.pw_id <> m2.pw_id '||								   -- eliminate join same row.
' and t.merch_td_pw_id= m1.pw_id and t.merch_bu_pw_id= m2.pw_id';

if in_debug_flag = 1 then
	v_sql2 := substr(v_sql,1,255);
	v_sql3 := substr(v_sql,256,255);
	maxdata.ins_import_log ('p_filter_td_bu','info', v_sql2, v_sql3, n_sqlnum, null);
end if;
execute immediate v_sql;

--v_sql := 'analyze table maxdata.t_pc_td_bu_dup estimate statistics sample 10 percent';
--execute immediate v_sql;


-- First, delete rows that are dup by both loc and merch TD/BU PWs.

n_sqlnum := 3000;
v_sql := 'truncate table maxdata.t_pc_multi_td_bu_row';
execute immediate v_sql;

n_sqlnum := 3100;
v_sql :=
'insert into maxdata.t_pc_multi_td_bu_row '||
' select m_lev,m_id,l_lev,l_id,t_lev,t_id '||
' from maxdata.t_pc_td_bu_dup '||
' group by m_lev,m_id,l_lev,l_id,t_lev,t_id '||
' having count(*) >= 2';

if in_debug_flag = 1 then
	v_sql2 := substr(v_sql,1,255);
	v_sql3 := substr(v_sql,256,255);
	maxdata.ins_import_log ('p_filter_td_bu','info', v_sql2, v_sql3, n_sqlnum, null);
end if;
execute immediate v_sql;

--v_sql := 'analyze table maxdata.t_pc_multi_td_bu_row estimate statistics sample 10 percent';
--execute immediate v_sql;


select count(*) into t_cnt
from maxdata.t_pc_multi_td_bu_row
where rownum<=1;

if in_debug_flag = 1 then
	n_sqlnum := 3199;
	v_sql := 'truncate table maxdata.t_pc_src_mem_bkup1';
	execute immediate v_sql;
	insert into maxdata.t_pc_src_mem_bkup1
	select * from maxdata.t_pc_src_mem;
end if;

if in_debug_flag = 1 then
	n_sqlnum := 3299;
	v_sql := 'truncate table maxdata.t_pc_td_bu_dup_bkup1';
	execute immediate v_sql;
	insert into maxdata.t_pc_td_bu_dup_bkup1
	select * from maxdata.t_pc_td_bu_dup;
end if;

if t_cnt > 0 then
	if in_src_priority_hier = t_loc_hier_type then
		if in_src_loc_td_flag = 1 then
			t_td_bu_col := 'loc_td_pw_id';
		else
			t_td_bu_col := 'loc_bu_pw_id';
		end if;
	else
		if in_src_merch_td_flag = 1 then
			t_td_bu_col := 'merch_td_pw_id';
		else
			t_td_bu_col := 'merch_bu_pw_id';
		end if;
	end if;

	n_sqlnum := 3200;
	v_sql :=
	'delete from maxdata.t_pc_src_mem s '||
	' where exists ( '||
	'	select * '||
	'	from maxdata.t_pc_multi_td_bu_row m, maxdata.t_pc_td_bu_dup d '||
	'	where s.m_lev = m.m_lev '||
	'	and s.m_id = m.m_id '||
	'	and s.l_lev = m.l_lev '||
	'	and s.l_id = m.l_id '||
	'	and s.t_lev = m.t_lev '||
	'	and s.t_id = m.t_id '||
	'	and d.m_lev = m.m_lev '||
	'	and d.m_id = m.m_id '||
	'	and d.l_lev = m.l_lev '||
	'	and d.l_id = m.l_id '||
	'	and d.t_lev = m.t_lev '||
	'	and d.t_id = m.t_id '||
	'	and (s.pw_id <> d.' || t_td_bu_col|| ')'||
	'   )';

	if in_debug_flag = 1 then
		v_sql2 := substr(v_sql,1,255);
		v_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log ('p_filter_td_bu','info', v_sql2, v_sql3, n_sqlnum, null);
	end if;
	execute immediate v_sql;

	-- Remove the processed TD/BU rows from TD/BU table.

	n_sqlnum := 3300;
	v_sql :=
	'delete from maxdata.t_pc_td_bu_dup s '||
	' where exists ( '||
	'	select * '||
	'	from maxdata.t_pc_multi_td_bu_row m '||
	'	where s.m_lev = m.m_lev '||
	'	and s.m_id = m.m_id '||
	'	and s.l_lev = m.l_lev '||
	'	and s.l_id = m.l_id '||
	'	and s.t_lev = m.t_lev '||
	'	and s.t_id = m.t_id )';

	if in_debug_flag = 1 then
		v_sql2 := substr(v_sql,1,255);
		v_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log ('p_filter_td_bu','info', v_sql2, v_sql3, n_sqlnum, null);
	end if;
	execute immediate v_sql;

end if; -- resolve dup rows for both loc/merch td/bu.



-- Now, the rest of duplicate rows are of either loc or merch TD/BU, not both.
-- Delete TD/BU rows from the member table leaving only the priority hierarchy's TD/BU.

if in_src_loc_td_flag = 1 then
	t_loc_td_bu_col := 'loc_td_pw_id';
else
	t_loc_td_bu_col := 'loc_bu_pw_id';
end if;
if in_src_merch_td_flag = 1 then
	t_merch_td_bu_col := 'merch_td_pw_id';
else
	t_merch_td_bu_col := 'merch_bu_pw_id';
end if;


if in_debug_flag = 1 then
	n_sqlnum := 3499;
	v_sql := 'truncate table maxdata.t_pc_src_mem_bkup2';
	execute immediate v_sql;
	insert into maxdata.t_pc_src_mem_bkup2
	select * from maxdata.t_pc_src_mem;
end if;

n_sqlnum := 3500;
v_sql :=
'delete from maxdata.t_pc_src_mem s '||
' where exists ( '||
'	select * '||
'	from maxdata.t_pc_td_bu_dup d '||
'	where s.m_lev = d.m_lev '||
'	and s.m_id = d.m_id '||
'	and s.l_lev = d.l_lev '||
'	and s.l_id = d.l_id '||
'	and s.t_lev = d.t_lev '||
'	and s.t_id = d.t_id '||
'	and (d.' || t_loc_td_bu_col||' is not null'||
'		and s.pw_id <> d.' || t_loc_td_bu_col||
'		or '||
'		d.' || t_merch_td_bu_col||' is not null'||
'		and s.pw_id <> d.' || t_merch_td_bu_col||')'||
'   )';

if in_debug_flag = 1 then
	v_sql2 := substr(v_sql,1,255);
	v_sql3 := substr(v_sql,256,255);
	maxdata.ins_import_log ('p_filter_td_bu','info', v_sql2, v_sql3, n_sqlnum, null);
end if;
execute immediate v_sql;

n_sqlnum := 6000;
--v_sql := 'analyze table maxdata.t_pc_src_mem estimate statistics sample 10 percent';
--execute immediate v_sql;

if in_debug_flag = 1 then
	n_sqlnum := 3499;
	v_sql := 'truncate table maxdata.t_pc_src_mem_bkup3';
	execute immediate v_sql;
	insert into maxdata.t_pc_src_mem_bkup3
	select * from maxdata.t_pc_src_mem;
end if;

if in_debug_flag = 0 then
		n_sqlnum := 10000;

		v_sql:='truncate table maxdata.t_pc_td_bu_pw' ;
		execute immediate v_sql;
		v_sql:='truncate table maxdata.t_pc_td_bu_dup';
		execute immediate v_sql;
		v_sql:='truncate table maxdata.t_pc_multi_td_bu_row';
		execute immediate v_sql;
end if;

EXCEPTION
   /* If an exception is raised, close cursor before exiting. */
   WHEN OTHERS THEN
		--rollback;
		commit; -- no harm.
		v_sql2 := substr(v_sql,1,255);
		v_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log ('p_filter_td_bu','error' , v_sql2, v_sql3, n_sqlnum, null);
		commit;

		v_sql := SQLERRM || ' ( p_filter_td_bu ' || v_param  ||
				', SQL#:' || n_sqlnum || ' )';
		-- Log the error message.
		v_sql2 := substr(v_sql,1,255);
		v_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log ('p_filter_td_bu','error' , v_sql2, v_sql3, n_sqlnum, null);
		commit;

		raise_application_error (-20001,v_sql);

end;

/

  GRANT EXECUTE ON "MAXDATA"."P_FILTER_TD_BU" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_FILTER_TD_BU" TO "MAXUSER";
