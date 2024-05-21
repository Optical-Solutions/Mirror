--------------------------------------------------------
--  DDL for Procedure P_CONVERT_STDHIER2DIMSET
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_CONVERT_STDHIER2DIMSET" (
	in_pw_id		NUMBER,
	in_planning_type_cd	NUMBER,		-- 1 Planning
	in_future2		NUMBER,
	in_future3		NUMBER
) AS
/*----------------------------------------------------------------------

$Log: 2151_p_convert_stdhier2dimset.sql,v $
Revision 1.14  2007/06/19 14:39:38  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.9  2005/12/06 21:52:23  dirapa
Removed the code that has dependency on 5.6 level numbers for clusters.

Review by Joseph Cho

Revision 1.8  2005/11/01 15:01:17  saghai
Added set_type column. Reviewer: Diwakar

Revision 1.7  2005/10/07 21:04:38  dirapa
As per request from application team, processing of time templates were skipped.

Revision 1.6  2005/10/05 21:02:11  dirapa
The path_id column of dimset_template table should have the same value of planworksheet table's loc/merch/time_path_id, specifically for clustered worksheets

Reviewer: Joseph Cho

Revision 1.5  2005/10/03 21:12:35  dirapa
Added condition to check loc_path_id

Revision 1.4  2005/09/06 17:17:51  dirapa
Removed allocation changes.

Changed parameter name in_plan_type_cd to in_planning_type_cd

Revision 1.3  2005/08/31 19:49:06  dirapa
changed in_future parameter to in_plan_type_cd to support allocation dimension sets

Revision 1.2  2005/08/09 18:29:27  dirapa
Changed column order for maxdata.dimset_template_lev insert statement.

Revision 1.1  2005/08/04 21:16:34  joscho
2151_p_add_tmpl.sql renamed to 2151_p_convert_stdhier2dimset
Added for rename from 2151_p_add_tmpl.sql.
See originally named file for history prior to the rename.

Revision 1.2  2005/08/04 21:02:39  joscho
v6.1-004 rename p_add_tmpl to p_convert_stdhier2dimset


Change History

V6.1
6.1.0-001 07/05/05 Diwakar	Initial Entry

Usage:

CALLED BY THE APP.

Description:

Creates templates when a worksheet doesn't have an associated template for each
hierarchy.

Depends on:

p_get_next_key

Parameters:

in_pw_id 		: Planworksheet ID whose dimension sets are checked and, if necessary, generated.
in_planning_type_cd 	: Type of module called this procedure. 1 means Planning
in_future2 		: placeholder. Pass in -1.
in_future3 		: placeholder. Pass in -1.
--------------------------------------------------------------------------------*/



n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 		:= 'p_convert_stdhier2dimset';
t_error_level      	VARCHAR2(6) 		:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(1000) 		:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);

t_from_level		NUMBER(10);
t_to_level		NUMBER(10);
t_from_id		NUMBER(10);
t_max_user_id		NUMBER(10);
t_max_group_id		NUMBER(10);
t_path_id		NUMBER(10);
t_template_id		NUMBER(10);
t_level_type		NUMBER(10)		:= 1200;
t_entity_type		NUMBER(10)		:= 0;
t_increment		NUMBER(10)		:= 1;
t_errmsg		VARCHAR2(255)		:= NULL;
t_mark_obsolete		NUMBER;
t_level_seq		NUMBER;
t_hier			VARCHAR2(5);
t_table_nm		VARCHAR2(255)		:= NULL;
t_column_nm		VARCHAR2(255)		:= NULL;
t_plan_path_id		NUMBER(10);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_pw_id, -1) || ',' ||
	COALESCE(in_planning_type_cd, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
	COALESCE(in_future3, -1) ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
COMMIT;

n_sqlnum := 1100;

IF in_planning_type_cd NOT IN (1) THEN
BEGIN
	RAISE_APPLICATION_ERROR (-20001, 'Invalid plan type code : ' || in_planning_type_cd);
END;
END IF;

n_sqlnum := 1200;

IF in_planning_type_cd = 1 THEN
BEGIN
 	t_table_nm  := 'maxdata.planworksheet';
 	t_column_nm := 'planworksheet_id';
END;
END IF;

--- Loop thru each hierarchy
n_sqlnum := 2000;

-- Build templates using a single transaction.
-- SS: begin tran
-- n_tran_started=1

BEGIN
FOR n1 IN 1..3 LOOP

	t_template_id := NULL;

	IF n1 = 1 THEN -- location
	BEGIN

		IF in_planning_type_cd = 1 THEN -- planning
		BEGIN
			-- NOTE: location levels on planworksheet for cluster has 1001 for cluster set and 1002 for cluster.

			SELECT loc_template_id,from_loc_level, to_loc_level, from_loc_id, max_user_id, max_group_id, loc_path_id
			INTO t_template_id, t_from_level, t_to_level, t_from_id, t_max_user_id, t_max_group_id, t_path_id
			FROM maxdata.planworksheet
			WHERE planworksheet_id = in_pw_id;

			t_plan_path_id := t_path_id;

			IF t_path_id > 1000 THEN
			BEGIN
				t_path_id := 999;

			END;
			END IF;
		END;
		END IF; -- if in_planning_type_cd = 1

		t_hier := 'loc';
 	END;
	ELSIF n1 = 2 THEN -- merch
	BEGIN
		IF in_planning_type_cd = 1 THEN -- planning
		BEGIN

			SELECT merch_template_id,from_merch_level, to_merch_level, from_merch_id, max_user_id, max_group_id, merch_path_id
			INTO  t_template_id, t_from_level, t_to_level, t_from_id, t_max_user_id, t_max_group_id, t_path_id
			FROM maxdata.planworksheet
			WHERE planworksheet_id = in_pw_id;

			t_plan_path_id := t_path_id;
		END;
		END IF; -- if in_planning_type_cd = 1

		t_hier := 'merch';
	END;
	ELSE	-- time
	BEGIN
		IF in_planning_type_cd = 1 THEN -- planning
		BEGIN
			SELECT time_template_id, from_time_level, to_time_level, from_time_id, max_user_id, max_group_id, time_path_id
			INTO  t_template_id, t_from_level, t_to_level, t_from_id, t_max_user_id, t_max_group_id, t_path_id
			FROM maxdata.planworksheet
			WHERE planworksheet_id = in_pw_id;

			t_plan_path_id := t_path_id;

		END;
		END IF; -- if in_planning_type_cd = 1

		t_hier := 'time';
	END;
	END IF;


	-- We intentionally not processing time templates i.e (n1 = 3)

	IF ((t_template_id IS NULL or t_template_id = 0) AND n1 != 3) THEN
	BEGIN
		-- get new template id
		n_sqlnum := 3000 + n1;

		maxapp.p_get_next_key(t_level_type,t_entity_type,t_increment,t_template_id, t_errmsg);

		IF t_errmsg IS NOT NULL THEN
		BEGIN
			RAISE_APPLICATION_ERROR (-20001, 'Error while getting new template id: ' || t_errmsg);
		END;
		END IF;

		-- Insert into maxapp.template
		n_sqlnum := 4000 + n1;

		INSERT INTO maxdata.dimset_template
			(	template_id,
				dimension_type,
				name,
				from_id,
				from_level,
				to_level,
				max_user_id,
				max_group_id,
				path_id,
				template_type,
				start_date,
				end_date,
				filterset_id,
				planworksheet_id,
				member_count,
				set_type
			)
		VALUES
			(
				t_template_id,
				n1,
				null,
				t_from_id,
				t_from_level,
				t_to_level,
				t_max_user_id,
				t_max_group_id,
				t_plan_path_id,
				'A',
				NULL,
				NULL,
				NULL,
				in_pw_id,
				0,
				n1
			);

		-- Now, insert template levels.

		n_sqlnum := 5000 + n1;

		t_level_seq := -2;

		DECLARE CURSOR hier_cur IS
			SELECT * FROM maxdata.hier_level
			WHERE dimension_type = n1
			AND hier_id = t_path_id
			ORDER BY level_seq;
		BEGIN
		FOR c1 in hier_cur LOOP

			IF c1.level_id = t_from_level THEN
				t_level_seq := -1;
			END IF;

			n_sqlnum := 6000;

			BEGIN
			IF t_level_seq >= -1 THEN

				t_level_seq := t_level_seq + 1;

				INSERT INTO maxdata.dimset_template_lev
				(
					template_id,
					level_number,
					level_seq,
					level_name,
					dynamic_flag,
					kpi_field_id,
					kpi_field_level,
					method_type,
					no_of_groups,
					level_incl_flag,
					partial_flag,
					autocreate_flag

				)
				VALUES
				(
					t_template_id,
					c1.level_id,
					t_level_seq,
					c1.level_name,
					0,
					0,
					0,
					0,
					0,
					1,
					0,
					0
				);
			END IF;
			END;

			IF c1.level_id = t_to_level THEN
			BEGIN
				EXIT;
			END;
			END IF;
		END LOOP; -- hier level cursor
		END;

		-- Set the *_template_id to the generated template id.
		n_sqlnum := 7000;

		v_sql := 'UPDATE  ' || t_table_nm  ||
			 ' SET ' || t_hier || '_template_id = :t_template_id' ||
			 ' WHERE ' || t_column_nm || ' = :in_pw_id';

		n_sqlnum := 8000;

		EXECUTE IMMEDIATE v_sql
		USING  t_template_id, in_pw_id;

		COMMIT;
	END;
	END IF;

END LOOP; --- End for loop;
END; -- for n1 in 1..3 loop

COMMIT;

EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := substr(v_sql,1,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := substr(v_sql,1,255);
		t_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
		COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_CONVERT_STDHIER2DIMSET" TO "MADMAX";
