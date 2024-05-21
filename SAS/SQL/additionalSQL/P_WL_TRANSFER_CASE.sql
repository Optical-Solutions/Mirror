--------------------------------------------------------
--  DDL for Procedure P_WL_TRANSFER_CASE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_TRANSFER_CASE" (
	in_cube_id			NUMBER,  	-- required if source or target object is in WORKING tables;
						 	-- else pass -1 if both of them are in PERMANENT tables.
	in_object_name			VARCHAR2,	-- Name of object being copied
	in_src_object_prefix_cd 	VARCHAR2,	-- Table prefix of the source object.
	in_src_template_id 		NUMBER, 	-- of the source object.
	in_src_object_no 		NUMBER, 	-- of the source object. (-1 for NULL)
	in_tar_object_prefix_cd 	VARCHAR2, 	-- Table prefix of the target object.
	in_tar_template_id 		NUMBER,	 	-- 0 'PMMODEL', NOT NULL 'PMACTIVE',(Only for Save As: -1 'WKACTIVE')
	in_last_post_time  		DATE,		-- Required for Posting only. Else pass in NULL.
	out_transfer_case	OUT	VARCHAR2
) AS

/*
--------------------------------------------------------------------------------

Change History
$Log: 2333_p_wl_transfer_case.sql,v $
Revision 1.7  2007/06/19 14:39:05  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2006/04/14 16:39:31  makirk
Removed import_log delete code (redundant), added change history logging where needed, commented out param logging commit


V6.1
6.1.0-001 01/30/06 Sachin	Initial Entry

Description:

This procedure is the called by p_wl_copy_subtree and p_wl_copy_template procedures
to get the copy transfer case.

--------------------------------------------------------------------------------
*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_wl_transfer_case';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_future_param_int		NUMBER(10,0)		:= -1;
t_int_null			NUMBER(10,0)		:= NULL;

t_from_transfer_case		VARCHAR2(10) := NULL;
t_to_transfer_case		VARCHAR2(10) := NULL;
t_transfer_case			VARCHAR2(20) := NULL;
t_usage_type_cd			VARCHAR2(3);
t_predefined_flg		NUMBER(1);

BEGIN

n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call:= t_proc_name || ' ( ' ||
	COALESCE(in_cube_id, -123) || ',''' ||
	in_object_name||''',''' ||
	in_src_object_prefix_cd|| ''',' ||
	COALESCE(in_src_template_id, -123) || ',' ||
	COALESCE(in_src_object_no, -123) || ',''' ||
	in_tar_object_prefix_cd || ''',' ||
	COALESCE(in_tar_template_id, -123) ||',' ||
	TO_CHAR(in_last_post_time,'MM/DD/YYYY HH24:MI:SS') ||
	'OUT out_transfer_case  ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;

-- Find the transfer case based on input parameters
n_sqlnum := 20000;
IF in_cube_id = -1 THEN
	-- This is Permanent to Permanent case
	-- Now to find Named or Active

	-- Check source
	IF in_src_object_prefix_cd IN ('WLWT','WLW1','WLKS') THEN
		BEGIN
		IF in_src_object_prefix_cd IN ('WLWT') THEN
			n_sqlnum := 21000;
			v_sql:=	' SELECT usage_type_cd'||
				' FROM maxdata.'||in_src_object_prefix_cd||'_'||in_object_name||
				' WHERE worksheet_template_id ='||CAST(in_src_template_id AS VARCHAR2);
		ELSE
			n_sqlnum := 22000;
			v_sql:=	' SELECT usage_type_cd'||
				' FROM maxdata.'||in_src_object_prefix_cd||'_'||in_object_name||
				' WHERE worksheet_template_id ='||CAST(in_src_template_id AS VARCHAR2)||
				' AND '||in_object_name||'_no = '||CAST(in_src_object_no AS VARCHAR2);
		END IF;
		EXECUTE IMMEDIATE v_sql
		INTO t_usage_type_cd;

		EXCEPTION
			WHEN NO_DATA_FOUND THEN
				RAISE_APPLICATION_ERROR(-20001,'Source row does not exist.');
		END;

		IF t_usage_type_cd = 'M' THEN
			t_from_transfer_case := 'PMMODEL';
		ELSIF t_usage_type_cd = 'A' THEN
			t_from_transfer_case := 'PMACTIVE';
		ELSE
			RAISE_APPLICATION_ERROR(-20001,'Invalid Usage Type Code for '||in_src_object_prefix_cd);
		END IF;
	ELSE
		BEGIN
		n_sqlnum := 23000;
		v_sql:= ' SELECT predefined_flg '||
			' FROM maxdata.'||in_src_object_prefix_cd||'_'||in_object_name ||
			' WHERE worksheet_template_id = '||CAST(in_src_template_id AS VARCHAR2)||
			' AND '||in_object_name||'_no = '||CAST(in_src_object_no AS VARCHAR2);

		EXECUTE IMMEDIATE v_sql
		INTO t_predefined_flg;

		EXCEPTION
			WHEN NO_DATA_FOUND THEN
				RAISE_APPLICATION_ERROR(-20001,'Source row does not exist.');
		END;

		IF t_predefined_flg = 1 THEN
			t_from_transfer_case := 'PMMODEL';
		ELSIF t_predefined_flg = 0 THEN
			t_from_transfer_case := 'PMACTIVE';
		ELSE
			RAISE_APPLICATION_ERROR(-20001,'Invalid Predefined Flag for '||in_src_object_prefix_cd);
		END IF;
	END IF;

	-- Now check target
	IF in_tar_template_id = 0 THEN
		t_to_transfer_case := 'PMMODEL';
	ELSE
		t_to_transfer_case := 'PMACTIVE';
	END IF;

ELSE
	-- This has to do with working
	-- Check source
	IF LENGTH(in_src_object_prefix_cd) = 5 THEN
		BEGIN
		IF in_src_object_prefix_cd IN ('WLWTW') THEN
			n_sqlnum := 24000;
			v_sql:=	' SELECT ''WORKING'''||
				' FROM maxdata.'||in_src_object_prefix_cd||'_'||in_object_name||
				' WHERE cube_id = '||CAST(in_cube_id AS VARCHAR2)||
				' AND worksheet_template_id ='||CAST(in_src_template_id AS VARCHAR2);

		ELSE

			n_sqlnum := 25000;
			v_sql:=	' SELECT ''WORKING'''||
				' FROM maxdata.'||in_src_object_prefix_cd||'_'||in_object_name||
				' WHERE cube_id = '||CAST(in_cube_id AS VARCHAR2)||
				' AND worksheet_template_id ='||CAST(in_src_template_id AS VARCHAR2)||
				' AND '||in_object_name||'_no = '||CAST(in_src_object_no AS VARCHAR2);

		END IF;

		EXECUTE IMMEDIATE v_sql
		INTO t_from_transfer_case;

		EXCEPTION
			WHEN NO_DATA_FOUND THEN
				RAISE_APPLICATION_ERROR(-20001,'Source row does not exist.');
		END;
	ELSE
		--Check whether Model or Active
		IF in_src_object_prefix_cd IN ('WLWT','WLW1','WLKS') THEN
			BEGIN
			IF in_src_object_prefix_cd IN ('WLWT') THEN
				n_sqlnum := 26000;
				v_sql:=	' SELECT usage_type_cd'||
					' FROM maxdata.'||in_src_object_prefix_cd||'_'||in_object_name||
					' WHERE worksheet_template_id ='||CAST(in_src_template_id AS VARCHAR2);
			ELSE
				n_sqlnum := 27000;
				v_sql:=	' SELECT usage_type_cd'||
					' FROM maxdata.'||in_src_object_prefix_cd||'_'||in_object_name||
					' WHERE worksheet_template_id ='||CAST(in_src_template_id AS VARCHAR2)||
					' AND '||in_object_name||'_no = '||CAST(in_src_object_no AS VARCHAR2);
			END IF;
			EXECUTE IMMEDIATE v_sql
			INTO t_usage_type_cd;

			EXCEPTION
				WHEN NO_DATA_FOUND THEN
					RAISE_APPLICATION_ERROR(-20001,'Source row does not exist.');
			END;

			IF t_usage_type_cd = 'M' THEN
				t_from_transfer_case := 'PMMODEL';
			ELSIF t_usage_type_cd = 'A' THEN
				t_from_transfer_case := 'PMACTIVE';
			ELSE
				RAISE_APPLICATION_ERROR(-20001,'Invalid Usage Type Code for '||in_src_object_prefix_cd);
			END IF;
		ELSE
			BEGIN
			n_sqlnum := 28000;
			v_sql:= ' SELECT predefined_flg '||
				' FROM maxdata.'||in_src_object_prefix_cd||'_'||in_object_name ||
				' WHERE worksheet_template_id = '||CAST(in_src_template_id AS VARCHAR2)||
				' AND '||in_object_name||'_no = '||CAST(in_src_object_no AS VARCHAR2);

			EXECUTE IMMEDIATE v_sql
			INTO t_predefined_flg;

			EXCEPTION
				WHEN NO_DATA_FOUND THEN
					RAISE_APPLICATION_ERROR(-20001,'Source row does not exist.');
			END;

			IF t_predefined_flg = 1 THEN
				t_from_transfer_case := 'PMMODEL';
			ELSIF t_predefined_flg = 0 THEN
				t_from_transfer_case := 'PMACTIVE';
			ELSE
				RAISE_APPLICATION_ERROR(-20001,'Invalid Predefined Flag for '||in_src_object_prefix_cd);
			END IF;
		END IF;
	END IF;

	-- Check target
	IF LENGTH(in_tar_object_prefix_cd) = 5 THEN
		t_to_transfer_case := 'WORKING';
	ELSE
		IF in_tar_template_id = 0 THEN
			t_to_transfer_case := 'PMMODEL';
		ELSIF in_tar_template_id = -1 THEN
		-- Special Case (SaveAs) only for 'WLWT' object
			t_to_transfer_case := 'WKACTIVE';
		ELSE
			t_to_transfer_case := 'PMACTIVE';
		END IF;
	END IF;
END IF;

t_transfer_case := t_from_transfer_case || '_TO_' ||t_to_transfer_case;

-- Finished finding the transfer case


-- Parameter checks based on the transfer case
n_sqlnum := 29000;
IF t_transfer_case = 'WORKING_TO_WORKING' AND in_src_template_id <> in_tar_template_id THEN
	RAISE_APPLICATION_ERROR(-20001,'Source and Target Template Id need to be same for '|| t_transfer_case ||' transfer case.');
END IF;

IF t_transfer_case = 'WORKING_TO_PMACTIVE' AND in_last_post_time IS NULL THEN
	RAISE_APPLICATION_ERROR(-20001,'Last Post Time cannot be NULL for Transfer Case: '|| t_transfer_case);
END IF;

IF t_transfer_case = 'PMMODEL_TO_PMACTIVE' AND in_tar_template_id IS NULL THEN
	RAISE_APPLICATION_ERROR(-20001,'Target template id cannot be NULL');
END IF;

IF t_transfer_case = 'WORKING_TO_WKACTIVE' AND in_src_object_prefix_cd <> 'WLWTW' THEN
	RAISE_APPLICATION_ERROR(-20001,'Transfer case '||t_transfer_case||' not applicable for '||in_object_name);
END IF;

IF t_transfer_case IN ('PMMODEL_TO_WORKING','WORKING_TO_WORKING') AND SUBSTR(in_src_object_prefix_cd,1,4) = 'WLWT' THEN
	-- Case not valid for Worksheet Template
	RAISE_APPLICATION_ERROR(-20001,'Invalid Transfer Case for worksheet template: '|| t_transfer_case);
END IF;

out_transfer_case := t_transfer_case;

EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := substr(v_sql,1,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := substr(v_sql,1,255);
		t_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
		--COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/
