--------------------------------------------------------
--  DDL for Procedure P_SET_HIER_LEV_SEQ
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_SET_HIER_LEV_SEQ" 
as
-- Change history:
-- V5.2.4.3
-- 01/10/2002 Joseph cho 	Initial entry.
--                          Set sequence id of hier level entries using data in t_hier_level_full.

v_sql varchar2(1000);
n_sqlnum number := 0;
n_cnt number;
n_hier_id number;
n_seq_id number;

begin

n_hier_id := -1;

n_sqlnum := 1000;

DECLARE CURSOR hier_cur IS
	SELECT * FROM maxdata.t_hier_level_full
	order by hier_id,level_seq;
BEGIN
FOR c1 in hier_cur LOOP

	n_sqlnum := n_sqlnum + 1;

	if n_hier_id <> c1.hier_id then
		n_hier_id := c1.hier_id;
		n_seq_id := 1;
	end if;

	select count(*) into n_cnt
	from maxdata.hier_level
	where hier_id = c1.hier_id
	and level_id = c1.level_id;

	if n_cnt = 1 then
		update maxdata.hier_level
		set dimension_type = c1.dimension_type,
			level_seq = n_seq_id
		where hier_id = c1.hier_id
		and level_id = c1.level_id;

		n_seq_id := n_seq_id + 1;
	end if;

END LOOP;
END;

commit;

select count(*) into n_cnt
from maxdata.hier_level
where level_seq is null
or dimension_type is null;

if n_cnt <> 0 then
	raise_application_error (-20001, 'Some levels in HIER_LEVEL not supported');
end if;


EXCEPTION
   WHEN OTHERS THEN
		rollback;

		v_sql := SQLERRM ||
				' ( p_set_hier_lev_seq(), ' ||
				' SQL#:' || n_sqlnum || ')';

		-- Log the error message.

		maxdata.ins_import_log ('p_set_hier_lev_seq','error', v_sql, null, null, null);
		commit;

		raise_application_error (-20001,v_sql);

end;

/

  GRANT EXECUTE ON "MAXDATA"."P_SET_HIER_LEV_SEQ" TO "MADMAX";
