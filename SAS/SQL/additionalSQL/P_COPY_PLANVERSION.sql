--------------------------------------------------------
--  DDL for Procedure P_COPY_PLANVERSION
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COPY_PLANVERSION" (
	in_src_planversion_id 		NUMBER,
	in_tar_planversion_id 		NUMBER, 	-- -1 only when in_batch_mode=1
	in_tar_planvers_nam 		VARCHAR2,
	in_tar_planvers_desc  		VARCHAR2,
	in_create_userid 		NUMBER,
	in_batch_mode			NUMBER,		-- 0/1
	in_batchStatusFlag 		NUMBER,
	in_copy_cl_hist			NUMBER,		-- 0/1
	in_copy_submitted		NUMBER,		-- 0/1
	in_copy_whatif			NUMBER,		-- 0/1
	in_trend_flag 			NUMBER,		-- 0/1
	in_future1			NUMBER,		-- placeholder. Pass in -1.
	in_future2			NUMBER,		-- placeholder
	in_future3			VARCHAR2,	-- placeholder
	out_tar_planversion_id 	OUT	NUMBER,
	out_errcode 		OUT 	NUMBER,
	out_errmsg 		OUT 	VARCHAR2
	)
AS

/*
$Log: 2356_p_copy_planversion.sql,v $
Revision 1.11  2007/06/19 14:38:58  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.6  2006/09/08 11:38:55  saghai
S0356582 Version_short_nm column populated

Revision 1.5  2006/06/07 15:40:38  makirk
Set batch_mode so that it carries over from parent record.

Revision 1.4  2006/05/05 15:13:01  anchan
Removed a hash symbol

Revision 1.3  2006/05/03 21:02:52  saghai
S0344931. Not copying APPROVE_DATE and APPROVE_USERID columns

----------------------------------------------------------------------

-- Change History:
--V5.6.2
-- 5.6.2-072	06/25/05	helmi	 MM018048 some cols have to be excluded from the copy.

--V5.6.1
-- 5.6.1-064	02/08/05	Joseph	#17964 Let COPIEDFROMVER_ID and COPIEDFROM_ID both hold the same source ID.
-- 5.6.1-062	01/12/05	Sachin	Enh#2193,2200,2345: Support time-out, batch, cl hist copy, and whatif changes.

--V5.6
-- 5.6.0-040	07/16/04	Sachin	#17112 Adding missing columns time_template_id and shifttime_offset to insert of planversion

--V5.4
-- 5.4.0-018 	9/26/02		Helmi 	adding functionality to copy fp_exception records.

--V5.3.4
-- 1/28/02  	helmi  	adding 4 cols. to the planversion copy stmt.
-- 12/17/02 	Sachin  Adding commit and calling p_del_plantable at exception.
--		    	It is to resolve concurrency bottleneck at seq id.

--V5.3.3
-- 6/26/02  	Sachin  Trend Flag removed from insert of Planversion (trend flag commented)
-- 6/14/02  	Sachin 	added column timeshift_kpi
-- 6/10/02 	helmi 	replace Balance col with batch_status in the insert stmt.
-- 5/30/02      Helmi 	Update batchstatusflag in planworksheet table if flag<> 0 or null
--5/29/02	helmi	Set the batchStatusFlag in planversion balance field and Set the CopiedFromVersionID
--			to the ID of the original Version
-- 5/24/02 	helmi 	adding input param i_plan_data to p_copy_planworksheet call.

--V532
-- 5/1/02       Rg: 	remove balance from copy.
-- 4/10/02	Rg : 	added multi_overlap_flag. bug #12865
-- 4/03/02	helmi 	adding max_group_id to p_copy_planworksht params.
-- 3/21/02   	GMN - 	adding new columns for 5.3.2 for planversion table

-- 2/11/02   	RG -- 	version_group_id shd be 0
-- 1/10/2002 	helmi 	adding Version_group_id to the insert stmt
-- 12/20/01 	Joseph 	Support trend_flag
-- 9/24/01		Value of column original shd be '0' when copied.
-- 9/7/01   		added new columns to planversion
-- 08/13/01 		added merch_alt_path to planversion as part of built 64
-- 05/17/01 	Joseph	Copy worksheet whose whatif=0.

******************************************************************************************************
1. Change in P_copy_planversion:
 	A)  Last_update_date set to Sysdate and last_update_userid to in_create_userid
	B)  Added 6 additional columns when inserting into Planversion table
						Sreeram 03/27/01
******************************************************************************************************
*/

t_cnt 			NUMBER;
t_planvers_status_name 	planvers_stat_lkup.name%type;
t_planversion_id 	NUMBER;
t_planvers_status 	NUMBER;
t_planversion 		VARCHAR2(255);
v_sql			VARCHAR2(4000)		:= NULL;
v_sql2              	VARCHAR2(4000) 		:= NULL;
t_online_copy		NUMBER			:= 0;
t_batch_prepare		NUMBER			:= 0;
t_batch_execute		NUMBER			:= 0;
out_errcode2		NUMBER;
out_errmsg2		VARCHAR2(1000);

n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 		:= 'p_copy_planversion';
t_error_level      	VARCHAR2(6) 		:= 'info';
t_call            	VARCHAR2(1000);
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);
t_col_value		VARCHAR2(100);
t_tar_planworksheet_id	NUMBER(10);

BEGIN

n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
		NVL(in_src_planversion_id, -1)   || ',' ||
		NVL(in_tar_planversion_id, -1)   || ',' ||
		NVL(in_tar_planvers_nam, 'NULL') || ',' ||
		NVL(in_tar_planvers_desc,'NULL') || ',' ||
		NVL(in_create_userid, -1)        || ',' ||
		NVL(in_batch_mode, -1)           || ',' ||
		NVL(in_batchStatusFlag, -1)      || ',' ||
		NVL(in_copy_cl_hist, -1)         || ',' ||
		NVL(in_copy_submitted, -1)       || ',' ||
		NVL(in_copy_whatif, -1)          || ',' ||
		NVL(in_trend_flag, -1)           || ',' ||
		NVL(in_future1, -1)              || ',' ||
		NVL(in_future2, -1)              || ',' ||
		NVL(in_future3, 'NULL')          || ',' ||
		' OUT out_tar_planversion_id OUT out_errcode, OUT out_errmsg '||
		' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
--COMMIT;


-- Initialize the error code/msg.
-- 0:success, 1:informational, 2:warning, 3:error

out_errcode := 0;
out_errmsg := '';
out_errcode2 := 0;
out_errmsg2 := '';
t_planvers_status_name := UPPER('In-progress');
t_planversion_id := NULL;


-- Check if the source planversion exists.

SELECT COUNT(*) INTO t_cnt
FROM planversion
WHERE planversion_id = in_src_planversion_id;

IF t_cnt <> 1 THEN
	out_errcode := 3;
	out_errmsg := 'Specified planversion not found';
	ROLLBACK;
	RETURN;
END IF;

-- Check the sequence entry for planversion.

SELECT seq_name INTO t_planversion
FROM maxapp.sequence
WHERE level_type=93 AND entity_type=31;

IF UPPER(t_planversion) <> UPPER('PLANVERSION') THEN
	out_errcode := 3;
	out_errmsg := 'Planversion sequence entry level_type ids are wrong.';
	ROLLBACK;
	RETURN;
END IF;

-- Check for planver_stat_id from planvers_stat_lkup table

SELECT planvers_stat_id INTO t_planvers_status
FROM planvers_stat_lkup
WHERE UPPER(name) = t_planvers_status_name;

IF t_planvers_status = NULL THEN
	out_errcode := 3;
	out_errmsg := 'planvers_stat_id not found.';
	ROLLBACK;
	RETURN;
END IF;

-- There are 3 modes under which this procedure is called.
--	online copy
--	prepare for batch processing
--	execute the prepared batch processing during off hours
--
-- Set up t_online_copy, t_batch_prepare and t_batch_execute flags
-- If batch processing is 0 then do copy of planversion online.

IF in_batch_mode = 0 THEN
	t_online_copy := 1;
ELSE
BEGIN
	t_online_copy := 0;

	-- If target planverison_id is not supplied then it is prepare for batch.
	IF in_tar_planversion_id = -1 THEN
	BEGIN
		t_batch_prepare := 1;
		t_batch_execute := 0;
	END;
	ELSE
	BEGIN
		-- Target planversion_id is supplied when we want to execute in batch
		t_batch_prepare := 0;
		t_batch_execute := 1;
	END;
	END IF;
END;
END IF;


-- Check invalid case in_batch_mode = 0 AND in_tar_planversion_id <> -1
IF (t_online_copy = 1 OR t_batch_prepare = 1) AND in_tar_planversion_id <> -1 THEN
	out_errcode := 3;
	out_errmsg := 'Target planversion should be -1';
	ROLLBACK;
	RETURN;
END IF;



-- If executing a batch copy, then use the passed in target id
-- else (i.e., copy online or prepare for batch copy), then
-- get the target id from the sequence table and create header.

n_sqlnum := 2000;

IF t_batch_execute = 1 THEN

	t_planversion_id := in_tar_planversion_id;
	out_tar_planversion_id := t_planversion_id;
ELSE
BEGIN
	-- Get the next id for planversion.
	-- Do not use p_get_next_key because it commits the tranx.
	-- We will commit at the end.

	maxapp.f_get_seq(93,31,t_planversion_id);
	COMMIT;

	out_tar_planversion_id := t_planversion_id;

	n_sqlnum := 4000;
	v_sql := NULL;
	v_sql2 := NULL;

	DECLARE CURSOR c_wrksht_cols IS
	SELECT UPPER(column_name) column_name FROM user_tab_columns
	WHERE table_name = 'PLANVERSION' AND UPPER(column_name) NOT IN ('BAT_OPER_FLAG','BATCH_STATUS','APPROVE_DATE','APPROVE_USERID')
	ORDER BY column_name;
	BEGIN
	FOR c1 IN c_wrksht_cols LOOP
		IF v_sql IS NULL  THEN
			v_sql := 'INSERT INTO maxdata.planversion ( ';
		ELSE
			v_sql := v_sql ||',';
		END IF;
		v_sql := v_sql || c1.column_name;

		-- Fill column values.
		-- Originally, COPYEDFROMVER_ID used to have the immediate source ID, and
		-- COPYEDFROM_ID has the original source ID. Not anymore for version. They both have
		-- the same immediate source ID. This change is only for version. A worksheet doesn't change.

		t_col_value := 	CASE c1.column_name
				WHEN 'ACTIVE' THEN '0'
				WHEN 'BATCH_MODE' THEN ':t_batch_prepare'
				--WHEN 'BATCH_STATUS' THEN ':in_batchStatusFlag'
				WHEN 'COPIEDFROMVER_ID' THEN 'planversion_id'
				WHEN 'COPIEDFROM_ID' THEN 'planversion_id'
				WHEN 'COPY_CL_HIST_FLG' THEN ':in_copy_cl_hist'
				WHEN 'COPY_SUBMITTED_FLG' THEN ':in_copy_submitted'
				WHEN 'COPY_WHATIF_FLG' THEN ':in_copy_whatif'
				WHEN 'CREATE_DATE' THEN 'SYSDATE'
				WHEN 'CREATE_USERID' THEN ':in_create_userid'
				WHEN 'DESCRIPTION' THEN ':in_tar_planvers_desc'
				WHEN 'LAST_UPDATE_DATE' THEN 'SYSDATE'
				WHEN 'LAST_UPDATE_USERID' THEN ':in_create_userid'
				WHEN 'NAME' THEN ':in_tar_planvers_nam'
				WHEN 'ORIGINAL' THEN '0'
				WHEN 'PLANVERSION_ID' THEN ':t_planversion_id'
				WHEN 'PLANVERS_STAT_ID' THEN ':t_planvers_status'
				WHEN 'VERSION_GROUP_ID' THEN '0'
				WHEN 'VERSION_SHORT_NM' THEN 'SUBSTR(:in_tar_planvers_nam,1,10)'
				ELSE c1.column_name
				END;


		IF v_sql2 IS NULL THEN
			v_sql2 := ' ) SELECT ';
		ELSE
			v_sql2 := v_sql2 ||',';
		END IF;
		v_sql2 := v_sql2 || t_col_value;
	END LOOP;
	END;

	--WARNING: Do not CHANGE THE ORDER OF THE COLUMNS in the USING clause.
	--They must be in the alphatical order of the columns.

	n_sqlnum := 5000;
	EXECUTE IMMEDIATE v_sql || v_sql2 ||
			' FROM maxdata.planversion' ||
			' WHERE planversion_id = :in_src_planversion_id'
	USING 	t_batch_prepare,
		--in_batchStatusFlag,
		in_copy_cl_hist,
		in_copy_submitted,
		in_copy_whatif,
		in_create_userid,
		in_tar_planvers_desc,
		in_create_userid,
		in_tar_planvers_nam,
		t_planversion_id,
		t_planvers_status,
		in_tar_planvers_nam,
		in_src_planversion_id;

	COMMIT;

END; -- if batch execute else
END IF;

-- If called to prepare for batch
-- then exit after header (planversion) record is created.
IF t_batch_prepare = 1 THEN
	n_sqlnum := 6000;
	COMMIT;
	RETURN;
END IF;



-- Make an entry in mmax_locks so that other users may not
-- use or delete the target version that is being copied.

n_sqlnum := 6500;

INSERT INTO maxdata.mmax_locks
SELECT
t_planversion_id,    -- LOCK_ID,
lock_level,
'PM',
lock_date,
in_tar_planvers_nam, -- NAME,
user_name,
server,
session_id,
worksheet_id,
plan_size,
plan_physical_mem,
plan_virtual_mem,
hasopened,
status,
curr_status,
curr_time,
pathway_type,
cube_key
FROM maxdata.mmax_locks
WHERE lock_id = in_src_planversion_id;

COMMIT;


-- When a planversion is copied, fp_exception rows for that planversion should be
-- copied over with the new planworksheet ids.
-- Get the original id's into temp tables.

v_sql := 'TRUNCATE TABLE maxdata.tmp_expt_top';
EXECUTE IMMEDIATE v_sql;

n_sqlnum := 7000;
v_sql := 'INSERT INTO maxdata.tmp_expt_top ';
v_sql := v_sql || ' SELECT DISTINCT NULL, topworkplan_id ';
v_sql := v_sql || ' FROM  maxdata.fp_exception WHERE globalplan_id = :t_in_src_planversion_id';

EXECUTE IMMEDIATE v_sql
USING in_src_planversion_id;
COMMIT;

v_sql := 'TRUNCATE TABLE maxdata.tmp_expt_bottom';
EXECUTE IMMEDIATE v_sql;

n_sqlnum := 8000;
v_sql := 'INSERT INTO maxdata.tmp_expt_bottom ';
v_sql := v_sql || ' SELECT DISTINCT NULL, bottomwrkplan_id  ';
v_sql := v_sql || ' FROM  maxdata.fp_exception WHERE globalplan_id = :t_in_src_planversion_id';

EXECUTE IMMEDIATE v_sql
USING in_src_planversion_id;
COMMIT;


-- Loop through the cursor for child planworksheet and copy
-- them and their child mplans.

n_sqlnum := 9000;
DECLARE CURSOR c_planwork IS
	SELECT planworksheet_id, name, description, max_user_id, max_group_id
	FROM maxdata.planworksheet
	WHERE planversion_id = in_src_planversion_id
	AND ((trend_flag = in_trend_flag AND in_copy_whatif= 0 AND whatif = 0) OR
		(trend_flag = in_trend_flag AND in_copy_whatif = 1));
BEGIN

	FOR c1 IN c_planwork LOOP

		maxdata.p_copy_planworksht (
			-1,			--in_cube_id
			c1.planworksheet_id,	--src_planwork_id
			c1.name,		--tar_planwork_nam. Name is unique under its parent.
			c1.description,		--plantar_planwork_desc
			in_create_userid,
			c1.max_user_id,
			c1.max_group_id,
			t_planversion_id,	--parent id to which the planworksht is copied to.
			1, 			-- i_plan_data param should be 1 when calling from this proc.
			in_copy_cl_hist,
			in_copy_submitted,
			0,			-- in_set_whatif
			in_future1,
			in_future2,
			in_future3,
			t_tar_planworksheet_id,
			out_errcode,
			out_errmsg
		);
		COMMIT;

	END LOOP;

	-- now we insert the exception records back after the tmp table is being updated by p_copy_planworksheet.
	INSERT INTO maxdata.fp_exception
	SELECT  ex.location_id,
		ex.company_name,
		ex.merch_id,
		ex.merch_name,
		ex.parent_merch_id,
		ex.parent_merch_name,
		ex.merch_level,
		ex.time_id,
		ex.period_disp_name,
		ex.wps_sales,
		ex.wps_gm,
		ex.wps_avg_inv,
		ex.sbu_sales,
		ex.sbu_gm,
		ex.sbu_avg_inv,
		ex.location_level,
		ex.time_level,
		t.new_top_ws_id,
		b.new_bottom_ws_id,
		t_planversion_id,
		ex.exception_type
	FROM fp_exception ex, tmp_expt_top t, tmp_expt_bottom b
	WHERE ex.globalplan_id = in_src_planversion_id
	AND ex.topworkplan_id = t.topworkplan_id
	AND ex.bottomwrkplan_id =b.bottomwrkplan_id;

	COMMIT;


	IF in_batchStatusFlag <> 0 AND in_batchStatusFlag IS NOT NULL THEN
		UPDATE maxdata.planworksheet
		SET Bat_Status_Flag =  in_batchStatusFlag
		WHERE planversion_id = t_planversion_id;
	END IF;
	COMMIT;
END;


-- Reset some columns that are used to save flags that are used for batch execute.
-- This flag was set when the row was inserted above.

n_sqlnum := 10000;

UPDATE maxdata.planversion
SET
--	batch_mode=0,
	copy_cl_hist_flg=0,
	copy_submitted_flg=0,
	copy_whatif_flg=0
WHERE planversion_id = t_planversion_id;
COMMIT;


-- truncate temporary tables
v_sql := 'TRUNCATE TABLE maxdata.tmp_expt_top';
EXECUTE IMMEDIATE v_sql;

v_sql := 'TRUNCATE TABLE maxdata.tmp_expt_bottom';
EXECUTE IMMEDIATE v_sql;

-- This code is to test the error handler
--n_sqlnum := 10009;
--v_sql := 'drop table xxxyyy';
--EXECUTE IMMEDIATE v_sql;

-- Delete the mmax_locks entry.

n_sqlnum := 11000;

DELETE FROM maxdata.mmax_locks
WHERE lock_id = t_planversion_id;

COMMIT;



EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := SUBSTR(v_sql,1,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := SUBSTR(v_sql,1,255);
		t_sql3 := SUBSTR(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
		COMMIT;

		out_errcode := SQLCODE;
		out_errmsg := v_sql;



		-- For the batch execute, do not delete planversion but only its worksheets.
		IF t_batch_execute = 1 THEN
			n_sqlnum := 12000;
			DECLARE CURSOR c_wrksht IS
			SELECT planworksheet_id
			FROM maxdata.planworksheet
			WHERE planversion_id = in_tar_planversion_id;
			BEGIN
				FOR c1 IN c_wrksht LOOP
					maxdata.p_del_plantable(
						94, -- for planworkshet
						c1.planworksheet_id,
						out_errcode2,
						out_errmsg2);
				END LOOP;
			END;
		ELSE
			n_sqlnum := 13000;

			IF t_planversion_id IS NOT NULL THEN
				maxdata.p_del_plantable(
					93, -- for planversion
					t_planversion_id,
					out_errcode2,
					out_errmsg2);
			END IF;
		END IF;

		COMMIT;

		RETURN;

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_COPY_PLANVERSION" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_COPY_PLANVERSION" TO "MAXUSER";
