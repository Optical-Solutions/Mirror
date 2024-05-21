--------------------------------------------------------
--  DDL for Procedure P_CREATE_BATCH_JOB
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_CREATE_BATCH_JOB" (
                in_plantable_lev		NUMBER, 		-- plan table level id eg 93 (planversion) or 94 (planworksheet)
	        in_plantable_id		NUMBER,        	-- Planversion or worksheet id.
	        in_batch_mode_flg 	NUMBER,        	--
	        in_batch_oper_cd	NUMBER,        	--
	        in_batch_init_flg		NUMBER,        	--
	        in_publish_flg		NUMBER,		--
	        in_balance_flg		NUMBER,        	--
	        in_new_plantable_id     NUMBER,        	-- When -1 then this procedure called after insert operation, when same id as in_plantable_id then this procedure called after update operation
	        in_future2           	NUMBER,        	-- (-1)
	        in_future3           	VARCHAR2,    	--
	        in_debug_flg         	NUMBER        	-- (0)
) AS

/*----------------------------------------------------------------------
Change History:

$Log: 2298_p_create_batch_job.sql,v $
Revision 1.8.8.1  2008/09/08 17:36:03  amkatr
FIXID S0531338: Replaced scheduled_start_dttm to sysdate.

Revision 1.8  2007/06/19 14:39:16  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2006/12/04 20:54:31  dirapa
Added Return statements for 6.1.2 so that this procedure doesn't do anything.
Triggers will do the actual job status related plugins

Revision 1.2  2006/11/30 20:47:48  dirapa
SO388648. Replaced the follwoing trigger code related to job status with this procedure.

tr_planversion_aft_u
tr_planversion_aft_i
tr_wksht_aft_i
tr_wksht_aft_u

Revision 1.1  2006/11/29 20:53:55  dirapa
Ported from UDB



Initial Entry.

Both after insert and update trigger code for batch job status on planversion and planworksheet was moved here.



----------------------------------------------------------------------------------*/

n_sqlnum 	        		NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_create_batch_job';
t_error_level      			VARCHAR2(6) 		:= 'info';
t_call            			VARCHAR2(1000);
v_sql              			VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);
t_int_null				NUMBER(10,0)		:= NULL;

t_batch_mode_flg		NUMBER			:=  NULL;
t_batch_oper_cd			NUMBER			:=  NULL;
t_batch_init_flg			NUMBER			:=  NULL;
t_publish_flg			NUMBER			:= NULL;
t_balance_flg			NUMBER			:= NULL;
t_seeding_flg			NUMBER			:= NULL;
t_batch_status			NUMBER;
t_batch_status_id		NUMBER;
t_seeding_id			NUMBER;
t_shifttime_offset		NUMBER;
t_name				VARCHAR2(100);
t_merch_level			NUMBER;
t_merch_direction		NUMBER;
t_loc_level				NUMBER;
t_loc_direction			NUMBER;
t_balance_kpi			NUMBER;
t_balance_dim			NUMBER;
t_copiedfrom_id			NUMBER;
t_create_userid			NUMBER;
t_copy_cl_hist_flg		NUMBER;
t_copy_submitted_flg 		NUMBER;
t_copy_whatif_flg		NUMBER;
t_trend_flag			NUMBER;
t_batch_job_status		NUMBER;

t_next_seq				NUMBER;
t_err_msg				VARCHAR2(255);
t_long_string 			VARCHAR2(4000);


BEGIN


--- In 6.1.2 this procedure is dummy. none of the below code will be executed. Triggers will plug in necessary job queue related info.

RETURN;


n_sqlnum  := 1000;
-- Log the parameters of the proc.

t_call := t_proc_name || ' ( ' ||
        COALESCE(in_plantable_lev,-123)  || ',' ||
        COALESCE(in_plantable_id,-123)  || ',' ||
        COALESCE(in_batch_mode_flg,-123)  || ',' ||
        COALESCE(in_batch_oper_cd,-123)  || ',' ||
        COALESCE(in_batch_init_flg,-123)  || ',' ||
        COALESCE(in_publish_flg,-123)  || ',' ||
        COALESCE(in_balance_flg,-123)  || ',' ||
        COALESCE(in_new_plantable_id,-123)  || ',' ||
        COALESCE(in_future2,-123)  || ',' ||
        COALESCE(in_future3, 'NULL') || ',' ||
        COALESCE(in_debug_flg,-123)  ||
        ' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql,  n_sqlnum  , t_int_null);
--COMMIT;

n_sqlnum  := 2000;

IF (in_plantable_lev IS NULL OR in_plantable_lev = -1 ) THEN
	RAISE_APPLICATION_ERROR(-20001,'Plantable level can not be null or -1');
END IF;

IF (in_plantable_id IS NULL OR in_plantable_id = -1 ) THEN
	RAISE_APPLICATION_ERROR(-20001,'Plantable id can not be null or -1');
END IF;

IF in_debug_flg > 0 THEN

	t_long_string :=  ' Procedure Parameters are :  ( ' ||
	       	' in_plantable_lev : ' || COALESCE(in_plantable_lev,-123)  || ',' ||
	       	' in_plantable_id : ' || COALESCE(in_plantable_id,-123)  || ',' ||
		' in_batch_mode_flg : ' || COALESCE(in_batch_mode_flg,-123)  || ',' ||
		' in_batch_oper_cd : ' || COALESCE(in_batch_oper_cd,-123)  || ',' ||
		' in_batch_init_flg : ' || COALESCE(in_batch_init_flg,-123)  || ',' ||
		' in_publish_flg : ' || COALESCE(in_publish_flg,-123)  || ',' ||
		' in_balance_flg : ' || COALESCE(in_balance_flg,-123)  || ',' ||
		' in_new_plantable_id : ' || COALESCE(in_new_plantable_id,-123)  || ',' ||
		' in_future2 : ' || COALESCE(in_future2,-123)  || ',' ||
		' in_future3 : ' || COALESCE(in_future3, 'NULL') || ',' ||
		' in_debug_flg : ' || COALESCE(in_debug_flg,-123)  ||
	' ) ';
	maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
END IF;

IF in_new_plantable_id > -1 THEN

	IF in_debug_flg > 0 THEN
			t_long_string := ' When Update happens';
			maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
	END IF;
	-- Below logic is for when a planversion or worksheet update happens

	IF in_plantable_lev = 93 THEN  -- PLANVERSION
		n_sqlnum  := 3000;

		IF in_debug_flg > 0 THEN
				t_long_string := ' When Update happens for Planversion';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
		END IF;

		SELECT batch_status, batch_mode, bat_init_flag, bat_oper_flag, publish_flg, balance,
				seeding_id,shifttime_offset, name, merch_level, merch_direction,
				loc_level,loc_direction,balance_kpi,balance_dim
		INTO
				t_batch_status, t_batch_mode_flg, t_batch_init_flg, t_batch_oper_cd, t_publish_flg, t_balance_flg,
				t_seeding_id,t_shifttime_offset, t_name, t_merch_level, t_merch_direction,
				t_loc_level, t_loc_direction,t_balance_kpi,t_balance_dim
		FROM maxdata.planversion
		WHERE planversion_id = in_plantable_id;

		IF in_debug_flg > 0 THEN
			t_long_string := ' Select Variables for Update of planversion are : ' ||
							' t_batch_status : ' || COALESCE(t_batch_status ,-123) ||
							' t_batch_mode_flg : ' || COALESCE(t_batch_mode_flg  ,-123) ||
							' t_batch_init_flg : ' || COALESCE(t_batch_init_flg ,-123) ||
							' t_batch_oper_cd : ' || COALESCE(t_batch_oper_cd  ,-123)||
							' t_publish_flg : ' || COALESCE(t_publish_flg  ,-123)||
							' t_balance_flg : ' || COALESCE(t_balance_flg  ,-123)||
							' t_seeding_id : ' || COALESCE(t_seeding_id ,-123) ||
							' t_shifttime_offset : ' || COALESCE(t_shifttime_offset ,-123) ||
							' t_name : ' || COALESCE(t_name ,'NULL') ||
							' t_merch_level : ' || COALESCE(t_merch_level ,-123) ||
							' t_merch_direction : ' || COALESCE(t_merch_direction ,-123) ||
							' t_loc_level : ' || COALESCE(t_loc_level ,-123) ||
							' t_loc_direction : ' || COALESCE(t_loc_direction ,-123) ||
							' t_balance_kpi : ' || COALESCE(t_balance_kpi ,-123) ||
							' t_balance_dim : ' || COALESCE(t_balance_dim  ,-123)
							;
			maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
		END IF;

		-- from update trigger of planversion

		IF (t_batch_status <> 0) THEN

			n_sqlnum  := 4000;

			SELECT batch_job_status
			INTO t_batch_job_status
			FROM maxdata.batch_status_lkup
			WHERE balance_stat_id = t_batch_status;

			IF in_debug_flg > 0 THEN
					t_long_string := ' Select Variables for  ' ||
									' t_batch_job_status : ' || COALESCE(t_batch_job_status ,'NULL') ;
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;
		END IF;

		-- For all the cases
		IF (COALESCE(t_batch_job_status,0) = 0 OR t_batch_job_status = 2) THEN
			n_sqlnum  :=5000;

			IF in_debug_flg > 0 THEN
				t_long_string := 'IF (COALESCE(t_batch_job_status,0) = 0 OR t_batch_job_status = 2) THEN';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;

			-- allow rebatch

			-- 4- time shift, 6- balance, 7- aggregate and 8- reforecast
			-- If bat_init_flag Changed except for balance schedule the batch job
			-- For balance it is always.

			-- This refers to balance flag on the version

			-- JOB TYPE CODE = 4, Timeshift
			IF (in_batch_oper_cd <> 1 AND t_batch_oper_cd = 1 AND t_shifttime_offset <> 0) THEN
				n_sqlnum  := 6000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'JOB TYPE CODE = 4, Timeshift';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;

				maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				-- Create a job record

				    n_sqlnum  :=  7000;

				    INSERT INTO maxdata.bpjq_job_queue (
					job_queue_id,
					job_nm,
					job_create_dttm,
					job_type_cd,
					scheduled_start_dttm,
					job_status_cd)
				    SELECT
					t_next_seq,
					job_nm,
					SYSDATE,
					job_type_cd,
					SYSDATE,
					job_status_cd
				    FROM maxdata.bpj1_job_queue_setup
				    WHERE job_type_cd = 4;

				    -- Create a param record

				    n_sqlnum  :=  8000;

				    INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				    VALUES
					(t_next_seq,
					1,
					in_plantable_id,
					NULL,
					NULL,
					'PLANVERSION_ID');
			END IF; -- IF (t_batch_oper_cd <> 1 AND t_batch_oper_cd = 1 AND t_shifttime_offset <> 0) THEN

			-- JOB TYPE CODE = 6, Balance

			IF ((in_balance_flg IS NULL OR in_balance_flg <> 1) AND t_balance_flg = 1) THEN

				n_sqlnum  :=  9000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'JOB TYPE CODE = 4, Timeshift';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;

				maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				    -- Create a job record

				    n_sqlnum  :=  10000;

				    INSERT INTO maxdata.bpjq_job_queue (
					job_queue_id,
					job_nm,
					job_create_dttm,
					job_type_cd,
					scheduled_start_dttm,
					job_status_cd)
				    SELECT
					t_next_seq,
					job_nm,
					SYSDATE,
					job_type_cd,
					SYSDATE,
					job_status_cd
				    FROM maxdata.bpj1_job_queue_setup
				    WHERE job_type_cd = 6;

				-- Create the param records

				    n_sqlnum  :=  11000;

				    INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				    VALUES
					(t_next_seq,
					1,
					in_plantable_id ,
					NULL,
					NULL,
					'PLANVERSION_ID');

				    n_sqlnum  :=  12000;
				    INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				    VALUES
					(t_next_seq,
					2,
					NULL,
					t_name,
					NULL,
					'PLANVERSION_NAME');

				    n_sqlnum  :=  13000;
				    INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				    VALUES
					(t_next_seq,
					3,
					t_merch_level,
					NULL,
					NULL,
					'MERCH_LEVEL');

				    n_sqlnum  :=  14000;
				    INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				    VALUES
					(t_next_seq,
					4,
					t_merch_direction,
					NULL,
					NULL,
					'MERCH_DIRECTION');

				    n_sqlnum  :=  15000;
				    INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				    VALUES
					(t_next_seq,
					5,
					t_loc_level,
					NULL,
					NULL,
					'LOC_LEVEL');

				    n_sqlnum  :=  16000;
				    INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				    VALUES
					(t_next_seq,
					6,
					t_loc_direction,
					NULL,
					NULL,
					'LOC_DIRECTION');

				    n_sqlnum  :=  17000;
				    INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				    VALUES
					(t_next_seq,
					7,
					t_balance_kpi,
					NULL,
					NULL,
					'BALANCE_KPI');

				    n_sqlnum  :=  18000;
				    INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				    VALUES
					(t_next_seq,
					8,
					t_balance_dim,
					NULL,
					NULL,
					'BALANCE_DIM');
			END IF; -- IF ((t_balance_flg IS NULL OR t_balance_flg <> 1) AND t_balance_flg = 1) THEN

			-- JOB TYPE CODE = 7, Aggregate

			IF (in_batch_init_flg <> 1 AND t_batch_init_flg = 1) THEN
				n_sqlnum  :=  19000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'JOB TYPE CODE = 7, Aggregate';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;

				maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				-- Create a job record

				n_sqlnum  :=  20000;
				INSERT INTO maxdata.bpjq_job_queue (
					job_queue_id,
					job_nm,
					job_create_dttm,
					job_type_cd,
					scheduled_start_dttm,
					job_status_cd)
				SELECT
					t_next_seq,
					job_nm,
					SYSDATE,
					job_type_cd,
					SYSDATE,
					job_status_cd
				FROM maxdata.bpj1_job_queue_setup
				WHERE job_type_cd = 7;

				-- Create a param record

				n_sqlnum  :=  21000;
				INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				VALUES
					(t_next_seq,
					1,
					in_plantable_id ,
					NULL,
					NULL,
					'PLANVERSION_ID');
			END IF;

			-- JOB TYPE CODE = 8, Reforecast

			IF (in_batch_init_flg <> 2 AND t_batch_init_flg = 2) THEN
				n_sqlnum  :=  22000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'JOB TYPE CODE = 8, Reforecast';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;

				maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				-- Create a job record

				n_sqlnum  :=  23000;
				INSERT INTO maxdata.bpjq_job_queue (
					job_queue_id,
					job_nm,
					job_create_dttm,
					job_type_cd,
					scheduled_start_dttm,
					job_status_cd)
				SELECT
					t_next_seq,
					job_nm,
					SYSDATE,
					job_type_cd,
					SYSDATE,
					job_status_cd
				FROM maxdata.bpj1_job_queue_setup
				WHERE job_type_cd = 8;

				-- Create a param record

				n_sqlnum  :=  24000;
				INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				VALUES
					(t_next_seq,
					1,
					in_plantable_id ,
					NULL,
					NULL,
					'PLANVERSION_ID');
			END IF;

			-- JOB TYPE CODE = 1
			IF (t_batch_oper_cd <> 3 AND t_batch_oper_cd = 3) THEN

				n_sqlnum  :=  25000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'JOB TYPE CODE = 1';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;
				maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				-- Create a job record

				n_sqlnum  :=  26000;
				INSERT INTO maxdata.bpjq_job_queue (
				    job_queue_id,
				    job_nm,
				    job_create_dttm,
				    job_type_cd,
				    scheduled_start_dttm,
				    job_status_cd)
				SELECT
				    t_next_seq,
				    job_nm,
				    SYSDATE,
				    job_type_cd,
				    SYSDATE,
				    job_status_cd
				FROM maxdata.bpj1_job_queue_setup
				WHERE job_type_cd = 1;

				-- Create a param record

				n_sqlnum  :=  27000;
				INSERT INTO maxdata.bpjp_job_parameter (
				    job_queue_id,
				    parameter_sequence_no,
				    numeric_parameter,
				    string_parameter,
				    datetime_parameter,
				    param_name)
				VALUES
				    (t_next_seq,
				    1,
				    in_plantable_id ,
				    NULL,
				    NULL,
				    'PLANVERSION_ID');
			END IF;



			-- JOB TYPE CODE = 9
			IF (in_batch_oper_cd <> 2 AND t_batch_oper_cd = 2) THEN

				n_sqlnum  :=  28000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'JOB TYPE CODE = 9';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;

				maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				-- Create a job record

				n_sqlnum  :=  29000;
				INSERT INTO maxdata.bpjq_job_queue (
				    job_queue_id,
				    job_nm,
				    job_create_dttm,
				    job_type_cd,
				    scheduled_start_dttm,
				    job_status_cd)
				SELECT
				    t_next_seq,
				    job_nm,
				    SYSDATE,
				    job_type_cd,
				    SYSDATE,
				    job_status_cd
				FROM maxdata.bpj1_job_queue_setup
				WHERE job_type_cd = 9;

				-- Create a param record

				n_sqlnum  :=  30000;
				INSERT INTO maxdata.bpjp_job_parameter (
				    job_queue_id,
				    parameter_sequence_no,
				    numeric_parameter,
				    string_parameter,
				    datetime_parameter,
				    param_name)
				VALUES
				    (t_next_seq,
				    1,
				    in_plantable_id ,
				    NULL,
				    NULL,
				    'PLANVERSION_ID');
			END IF; -- IF (in_batch_oper_cd <> 2 AND t_batch_oper_cd = 2) THEN

			-- JOB TYPE CODE = 11
			IF (in_batch_oper_cd <> 4 AND t_batch_oper_cd = 4) THEN

				n_sqlnum  :=  31000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'JOB TYPE CODE = 11';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;
				maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				-- Create a job record

				n_sqlnum  :=  32000;
				INSERT INTO maxdata.bpjq_job_queue (
				    job_queue_id,
				    job_nm,
				    job_create_dttm,
				    job_type_cd,
				    scheduled_start_dttm,
				    job_status_cd)
				SELECT
				    t_next_seq,
				    job_nm,
				    SYSDATE,
				    job_type_cd,
				    SYSDATE,
				    job_status_cd
				FROM maxdata.bpj1_job_queue_setup
				WHERE job_type_cd = 11;

				-- Create a param record

				n_sqlnum  :=  33000;
				INSERT INTO maxdata.bpjp_job_parameter (
				    job_queue_id,
				    parameter_sequence_no,
				    numeric_parameter,
				    string_parameter,
				    datetime_parameter,
				    param_name)
				VALUES
				    (t_next_seq,
				    1,
				    in_plantable_id ,
				    NULL,
				    NULL,
				    'PLANVERSION_ID');
			END IF; -- IF (in_batch_oper_cd <> 4 AND t_batch_oper_cd = 4) THEN

			-- JOB TYPE CODE = 13
			IF (in_batch_oper_cd <> 5 AND t_batch_oper_cd = 5) THEN

				n_sqlnum  :=  34000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'JOB TYPE CODE = 13';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;

				maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				-- Create a job record

				n_sqlnum  :=  35000;
				INSERT INTO maxdata.bpjq_job_queue (
				    job_queue_id,
				    job_nm,
				    job_create_dttm,
				    job_type_cd,
				    scheduled_start_dttm,
				    job_status_cd)
				SELECT
				    t_next_seq,
				    job_nm,
				    SYSDATE,
				    job_type_cd,
				    SYSDATE,
				    job_status_cd
				FROM maxdata.bpj1_job_queue_setup
				WHERE job_type_cd = 13;

				-- Create a param record

				n_sqlnum  :=  36000;
				INSERT INTO maxdata.bpjp_job_parameter (
				    job_queue_id,
				    parameter_sequence_no,
				    numeric_parameter,
				    string_parameter,
				    datetime_parameter,
				    param_name)
				VALUES
				    (t_next_seq,
				    1,
				    in_plantable_id ,
				    NULL,
				    NULL,
				    'PLANVERSION_ID');
			END IF; -- IF (in_batch_oper_cd <> 5 AND t_batch_oper_cd = 5) THEN

			-- JOB TYPE CODE = 20

			IF (in_publish_flg <> 1 AND t_publish_flg = 1) THEN

				n_sqlnum  :=  37000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'JOB TYPE CODE = 20';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;

				maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				-- Create a job record

				n_sqlnum  :=  38000;
				INSERT INTO maxdata.bpjq_job_queue (
				    job_queue_id,
				    job_nm,
				    job_create_dttm,
				    job_type_cd,
				    scheduled_start_dttm,
				    job_status_cd)
				SELECT
				    t_next_seq,
				    job_nm,
				    SYSDATE,
				    job_type_cd,
				    SYSDATE,
				    job_status_cd
				FROM maxdata.bpj1_job_queue_setup
				WHERE job_type_cd = 20;

				-- Create a param record

				n_sqlnum  :=  39000;
				INSERT INTO maxdata.bpjp_job_parameter (
				    job_queue_id,
				    parameter_sequence_no,
				    numeric_parameter,
				    string_parameter,
				    datetime_parameter,
				    param_name)
				VALUES
				    (t_next_seq,
				    1,
				    in_plantable_id ,
				    NULL,
				    NULL,
				    'PLANVERSION_ID');
			END IF; -- IF (in_publish_flg <> 1 AND t_publish_flg = 1) THEN
		END IF; -- IF (COALESCE(t_batch_job_status,0) = 0 OR t_batch_job_status = 2) THEN

	ELSIF in_plantable_lev = 94 THEN -- PLANWORKSHEET

		n_sqlnum  := 40000;

		IF in_debug_flg > 0 THEN
				t_long_string := ' When Update happens for Planworksheet';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
		END IF;

		SELECT  batch_mode,  publish_flg, batch_status_id , seeding_id
		INTO     t_batch_mode_flg,  t_publish_flg,  t_batch_status_id, t_seeding_id
		FROM maxdata.planworksheet
		WHERE planworksheet_id = in_plantable_id;

		IF in_debug_flg > 0 THEN
			t_long_string := ' Select Variables for Update Planworksheet are : ' ||
							' t_batch_mode_flg : ' || COALESCE(t_batch_mode_flg ,-123)  ||
							' t_publish_flg : ' || COALESCE(t_publish_flg  ,-123) ||
							' t_batch_status_id : ' || COALESCE(t_batch_status_id ,-123) ||
							' t_seeding_id : ' || COALESCE(t_seeding_id  ,-123)
							;
			maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
		END IF;

		-- from update trigger of planworksheet

		-- Make sure the trigger only creates records if the batch_mode flag is turned on
		IF in_batch_mode_flg = 0 AND t_batch_mode_flg = 1 THEN

			-- JOB TYPE CODE = 0
			IF t_batch_mode_flg = 1 AND t_batch_status_id = 1 OR (t_batch_mode_flg = 1 AND t_batch_status_id IS NULL AND t_seeding_id <> 0) THEN

				n_sqlnum  := 41000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'JOB TYPE CODE = 0';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;

				maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				-- Create a job record
				n_sqlnum  := 42000;
				INSERT INTO maxdata.bpjq_job_queue (
					job_queue_id,
					job_nm,
					job_create_dttm,
					job_type_cd,
					scheduled_start_dttm,
					job_status_cd)
				SELECT
					t_next_seq,
					job_nm,
					SYSDATE,
					job_type_cd,
					SYSDATE,
					job_status_cd
				FROM maxdata.bpj1_job_queue_setup
				WHERE job_type_cd = 0;

				-- Create a param record
				n_sqlnum  := 43000;
					INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				VALUES
					(t_next_seq,
					1,
					in_plantable_id,
					NULL,
					NULL,
					NULL);
			END IF; -- IF t_batch_mode_flg = 1 AND t_batch_status_id = 1 OR (t_batch_mode_flg = 1 AND t_batch_status_id IS NULL AND t_seeding_id <> 0) THEN

			IF t_publish_flg = 1  THEN
				n_sqlnum  := 44000;

				IF in_debug_flg > 0 THEN
					t_long_string := 'IF t_publish_flg = 1  THEN';
					maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
				END IF;

				    maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

				    -- Create a job record
				    n_sqlnum  := 6000;
				    INSERT INTO maxdata.bpjq_job_queue (
					job_queue_id,
					job_nm,
					job_create_dttm,
					job_type_cd,
					scheduled_start_dttm,
					job_status_cd)
				    SELECT
					t_next_seq,
					job_nm,
					SYSDATE,
					job_type_cd,
					SYSDATE,
					job_status_cd
				    FROM maxdata.bpj1_job_queue_setup
				    WHERE job_type_cd = 19;

				    -- Create a param record
				    n_sqlnum  := 45000;
				    INSERT INTO maxdata.bpjp_job_parameter (
					job_queue_id,
					parameter_sequence_no,
					numeric_parameter,
					string_parameter,
					datetime_parameter,
					param_name)
				    VALUES
					(t_next_seq,
					1,
					in_plantable_id,
					NULL,
					NULL,
					NULL);
			END IF; --  IF t_publish_flg = 1  THEN
		END IF; --IF in_batch_mode_flg = 0 AND t_batch_mode_flg = 1 THEN

	END IF; -- IF in_plantable_lev = 93 THEN

ELSE 	-- IF in_new_plantable_id > -1 THEN

	IF in_debug_flg > 0 THEN
			t_long_string := ' When Insert happens';
			maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
	END IF;

	n_sqlnum  := 46000;
	-- Below logic is for when a planversion or worksheet Insert happens

	IF in_plantable_lev = 93 THEN -- PLANVERSION

		n_sqlnum  := 47000;

		IF in_debug_flg > 0 THEN
			t_long_string := ' When Insert happens for Planversion';
			maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
		END IF;

		SELECT batch_status, batch_mode, bat_oper_flag, bat_init_flag,
				shifttime_offset, copiedfrom_id, publish_flg, name,
				create_userid, copy_cl_hist_flg, copy_submitted_flg, 	copy_whatif_flg,
				trend_flag , balance
		INTO
			t_batch_status, t_batch_mode_flg, t_batch_oper_cd, t_batch_init_flg,
			t_shifttime_offset , t_copiedfrom_id, t_publish_flg, t_name,
			t_create_userid, t_copy_cl_hist_flg, t_copy_submitted_flg, t_copy_whatif_flg,
			t_trend_flag, t_balance_flg

		FROM maxdata.planversion
		WHERE planversion_id = in_plantable_id;

		IF in_debug_flg > 0 THEN
			t_long_string := ' Select Variables for Insert Planversion are : ' ||
							' t_batch_status : ' || COALESCE(t_batch_status  ,-123) ||
							' t_batch_mode_flg : ' || COALESCE(t_batch_mode_flg ,-123) ||
							' t_batch_oper_cd : ' || COALESCE(t_batch_oper_cd ,-123) ||
							' t_batch_init_flg : ' || COALESCE(t_batch_init_flg ,-123) ||
							' t_shifttime_offset : ' || COALESCE(t_shifttime_offset ,-123) ||
							' t_copiedfrom_id : ' || COALESCE(t_copiedfrom_id ,-123) ||
							' t_publish_flg : ' || COALESCE(t_publish_flg ,-123) ||
							' t_name : ' || COALESCE(t_name,'NULL')  ||
							' t_create_userid : ' || COALESCE(t_create_userid ,-123) ||
							' t_copy_cl_hist_flg : ' || COALESCE(t_copy_cl_hist_flg ,-123) ||
							' t_copy_submitted_flg : ' || COALESCE(t_copy_submitted_flg ,-123) ||
							' t_copy_whatif_flg : ' || COALESCE(t_copy_whatif_flg  ,-123)||
							' t_trend_flag : ' || COALESCE(t_trend_flag ,-123) ||
							' t_balance_flg : ' || COALESCE(t_balance_flg ,-123)
							;
			maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
		END IF;

		-- Can't use "ELSIF" since multiple conditions may be true simultaneously

		-- JOB TYPE CODE = 1
		IF (t_batch_oper_cd = 3 AND (t_batch_status = 0 OR t_batch_status IS NULL)) THEN
		    n_sqlnum  := 48000;

			IF in_debug_flg > 0 THEN
				t_long_string := 'JOB TYPE CODE = 1';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;

		    maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

		    -- Create a job record
		    n_sqlnum  := 49000;
		    INSERT INTO maxdata.bpjq_job_queue (
			job_queue_id,
			job_nm,
			job_create_dttm,
			job_type_cd,
			scheduled_start_dttm,
			job_status_cd)
		    SELECT
			t_next_seq,
			job_nm,
			SYSDATE,
			job_type_cd,
			SYSDATE,
			job_status_cd
		    FROM maxdata.bpj1_job_queue_setup
		    WHERE job_type_cd = 1;

		    -- Create a param record
		    n_sqlnum  := 50000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			1,
			in_plantable_id	,
			NULL,
			NULL,
			NULL);
		END IF;

		-- JOB TYPE CODE = 4
		IF (t_batch_oper_cd = 1 AND (t_batch_status = 0 OR t_batch_status IS NULL) AND t_shifttime_offset <> 0) THEN
		    n_sqlnum  := 51000;

			IF in_debug_flg > 0 THEN
				t_long_string := 'JOB TYPE CODE = 4';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;

		    maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

		    -- Create a job record
		    n_sqlnum  := 52000;
		    INSERT INTO maxdata.bpjq_job_queue (
			job_queue_id,
			job_nm,
			job_create_dttm,
			job_type_cd,
			scheduled_start_dttm,
			job_status_cd)
		    SELECT
			t_next_seq,
			job_nm,
			SYSDATE,
			job_type_cd,
			SYSDATE,
			job_status_cd
		    FROM maxdata.bpj1_job_queue_setup
		    WHERE job_type_cd = 4;

		    -- Create a param record
		    n_sqlnum  := 53000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			1,
			in_plantable_id	,
			NULL,
			NULL,
			'PLANVERSION_ID');
		END IF;

		-- JOB TYPE CODE = 9
		IF (t_batch_oper_cd = 2 AND (t_batch_status = 0 OR t_batch_status IS NULL)) THEN
		    n_sqlnum  := 54000;

			IF in_debug_flg > 0 THEN
				t_long_string := 'JOB TYPE CODE = 9';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;

		    maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

		    -- Create a job record
		    n_sqlnum  := 55000;
		    INSERT INTO maxdata.bpjq_job_queue (
			job_queue_id,
			job_nm,
			job_create_dttm,
			job_type_cd,
			scheduled_start_dttm,
			job_status_cd)
		    SELECT
			t_next_seq,
			job_nm,
			SYSDATE,
			job_type_cd,
			SYSDATE,
			job_status_cd
		    FROM maxdata.bpj1_job_queue_setup
		    WHERE job_type_cd = 9;

		    -- Create a param record
		    n_sqlnum  := 56000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			1,
			in_plantable_id	,
			NULL,
			NULL,
			'PLANVERSION_ID');
		END IF;

		-- JOB TYPE CODE = 11
		IF (t_batch_oper_cd = 4 AND (t_batch_status = 0 OR t_batch_status IS NULL)) THEN
		    n_sqlnum  := 57000;

			IF in_debug_flg > 0 THEN
				t_long_string := 'JOB TYPE CODE = 11';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;

		    maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

		    -- Create a job record
		    n_sqlnum  := 58000;
		    INSERT INTO maxdata.bpjq_job_queue (
			job_queue_id,
			job_nm,
			job_create_dttm,
			job_type_cd,
			scheduled_start_dttm,
			job_status_cd)
		    SELECT
			t_next_seq,
			job_nm,
			SYSDATE,
			job_type_cd,
			SYSDATE,
			job_status_cd
		    FROM maxdata.bpj1_job_queue_setup
		    WHERE job_type_cd = 11;

		    -- Create a param record
		    n_sqlnum  := 59000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			1,
			in_plantable_id	,
			NULL,
			NULL,
			'PLANVERSION_ID');
		END IF;

		-- JOB TYPE CODE = 13
		IF (t_batch_oper_cd = 5 AND (t_batch_status = 0 OR t_batch_status IS NULL)) THEN
		    n_sqlnum  := 60000;

			IF in_debug_flg > 0 THEN
				t_long_string := 'JOB TYPE CODE = 13';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;

		    maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

		    -- Create a job record
		    n_sqlnum  := 61000;
		    INSERT INTO maxdata.bpjq_job_queue (
			job_queue_id,
			job_nm,
			job_create_dttm,
			job_type_cd,
			scheduled_start_dttm,
			job_status_cd)
		    SELECT
			t_next_seq,
			job_nm,
			SYSDATE,
			job_type_cd,
			SYSDATE,
			job_status_cd
		    FROM maxdata.bpj1_job_queue_setup
		    WHERE job_type_cd = 13;

		    -- Create a param record
		    n_sqlnum  := 62000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			1,
			in_plantable_id	,
			NULL,
			NULL,
			'PLANVERSION_ID');
		END IF;

		-- JOB TYPE CODE = 20
		IF (t_publish_flg = 1) THEN
		    n_sqlnum  := 63000;

			IF in_debug_flg > 0 THEN
				t_long_string := 'JOB TYPE CODE = 20';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;

		    maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

		    -- Create a job record
		    n_sqlnum  := 64000;
		    INSERT INTO maxdata.bpjq_job_queue (
			job_queue_id,
			job_nm,
			job_create_dttm,
			job_type_cd,
			scheduled_start_dttm,
			job_status_cd)
		    SELECT
			t_next_seq,
			job_nm,
			SYSDATE,
			job_type_cd,
			SYSDATE,
			job_status_cd
		    FROM maxdata.bpj1_job_queue_setup
		    WHERE job_type_cd = 20;

		    -- Create a param record
		    n_sqlnum  := 65000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			1,
			in_plantable_id	,
			NULL,
			NULL,
			'PLANVERSION_ID');
		END IF;

		-- JOB TYPE CODE = 23
		IF (t_batch_init_flg = 0 AND t_batch_oper_cd = 0 AND t_batch_mode_flg = 1 AND t_copiedfrom_id > 0) THEN
		    n_sqlnum  := 66000;

			IF in_debug_flg > 0 THEN
				t_long_string := 'JOB TYPE CODE = 23';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;

		    maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

		    -- Create a job record
		    n_sqlnum  := 67000;
		    INSERT INTO maxdata.bpjq_job_queue (
			job_queue_id,
			job_nm,
			job_create_dttm,
			job_type_cd,
			scheduled_start_dttm,
			job_status_cd)
		    SELECT
			t_next_seq,
			job_nm,
			SYSDATE,
			job_type_cd,
			SYSDATE,
			job_status_cd
		    FROM maxdata.bpj1_job_queue_setup
		    WHERE job_type_cd = 23;

		    -- Create the param records
		    n_sqlnum  := 68000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			1,
			t_copiedfrom_id,
			NULL,
			NULL,
			'SRC_PLANVERSION_ID');

		    n_sqlnum  := 69000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			2,
			in_plantable_id	,
			NULL,
			NULL,
			'TAR_PLANVERSION_ID');

		    n_sqlnum  := 70000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			3,
			NULL,
			t_name,
			NULL,
			'TAR_PLANVERS_NAM');

		    n_sqlnum  := 71000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			4,
			t_create_userid,
			NULL,
			NULL,
			'CREATE_USERID');

		    n_sqlnum  := 72000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			5,
			t_batch_mode_flg,
			NULL,
			NULL,
			'BATCH_MODE');

		    n_sqlnum  := 73000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			6,
			t_batch_status,
			NULL,
			NULL,
			'BATCHSTATUSFLAG');

		    n_sqlnum  := 74000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			7,
			t_copy_cl_hist_flg,
			NULL,
			NULL,
			'COPY_CL_HIST');

		    n_sqlnum  := 75000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			8,
			t_copy_submitted_flg,
			NULL,
			NULL,
			'COPY_SUBMITTED');

		    n_sqlnum  := 76000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			9,
			t_copy_whatif_flg,
			NULL,
			NULL,
			'COPY_WHATIF');

		    n_sqlnum  := 77000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			10,
			t_trend_flag,
			NULL,
			NULL,
			'TREND_FLAG');
		END IF;


	ELSIF in_plantable_lev = 94 THEN -- PLANWORKSHEET

		n_sqlnum  := 78000;

		IF in_debug_flg > 0 THEN
			t_long_string := ' When Insert happens for Planworksheet';
			maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
		END IF;

		SELECT batch_mode, batch_status_id,  seeding_id , publish_flg
		INTO
			t_batch_mode_flg, t_batch_status_id,t_seeding_id , t_publish_flg
		FROM maxdata.planworksheet
		WHERE planworksheet_id = in_plantable_id;

		IF in_debug_flg > 0 THEN
			t_long_string := ' Select Variables for Insert Planworksheet are : ' ||
							' t_batch_mode_flg : ' || COALESCE(t_batch_mode_flg ,-123) ||
							' t_batch_status_id : ' || COALESCE(t_batch_status_id ,-123) ||
							' t_seeding_id : ' || COALESCE(t_seeding_id ,-123) ||
							' t_publish_flg : ' || COALESCE(t_publish_flg  , -123)
							;
			maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
		END IF;

		-- JOB TYPE CODE = 0
		IF t_batch_mode_flg = 1 AND t_batch_status_id = 1 OR (t_batch_mode_flg = 1 AND t_batch_status_id IS NULL AND t_seeding_id <> 0) THEN
		    n_sqlnum  := 78000;

			IF in_debug_flg > 0 THEN
				t_long_string := 'JOB TYPE CODE = 0';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;

		    maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

		    -- Create a job record
		    n_sqlnum  := 79000;
		    INSERT INTO maxdata.bpjq_job_queue (
			job_queue_id,
			job_nm,
			job_create_dttm,
			job_type_cd,
			scheduled_start_dttm,
			job_status_cd)
		    SELECT
			t_next_seq,
			job_nm,
			SYSDATE,
			job_type_cd,
			SYSDATE,
			job_status_cd
		    FROM maxdata.bpj1_job_queue_setup
		    WHERE job_type_cd = 0;

		    -- Create a param record
		    n_sqlnum  := 80000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			1,
			in_plantable_id,
			NULL,
			NULL,
			NULL);
		END IF;

		IF t_publish_flg = 1  THEN
		    n_sqlnum  := 81000;

			IF in_debug_flg > 0 THEN
				t_long_string := 'IF t_publish_flg = 1  THEN';
				maxdata.p_log(t_proc_name, t_error_level,t_long_string, NULL,n_sqlnum);
			END IF;

		    maxapp.p_get_next_key(1,101,1,t_next_seq,t_err_msg);

		    -- Create a job record
		    n_sqlnum  := 82000;

		    INSERT INTO maxdata.bpjq_job_queue (
			job_queue_id,
			job_nm,
			job_create_dttm,
			job_type_cd,
			scheduled_start_dttm,
			job_status_cd)
		    SELECT
			t_next_seq,
			job_nm,
			SYSDATE,
			job_type_cd,
			SYSDATE,
			job_status_cd
		    FROM maxdata.bpj1_job_queue_setup
		    WHERE job_type_cd = 19;

		    -- Create a param record
		    n_sqlnum  := 83000;
		    INSERT INTO maxdata.bpjp_job_parameter (
			job_queue_id,
			parameter_sequence_no,
			numeric_parameter,
			string_parameter,
			datetime_parameter,
			param_name)
		    VALUES
			(t_next_seq,
			1,
			in_plantable_id,
			NULL,
			NULL,
			NULL);
		END IF; --t_publish_flg = 1  THEN

	END IF; --in_plantable_lev = 94 THEN

END IF; -- IF in_new_plantable_id > -1 THEN

COMMIT;

EXCEPTION
	WHEN CASE_NOT_FOUND THEN
		RAISE_APPLICATION_ERROR(-20001,' Not a valid object prefix code');

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

  GRANT EXECUTE ON "MAXDATA"."P_CREATE_BATCH_JOB" TO "MADMAX";
