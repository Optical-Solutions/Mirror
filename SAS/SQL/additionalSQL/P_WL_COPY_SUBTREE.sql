--------------------------------------------------------
--  DDL for Procedure P_WL_COPY_SUBTREE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_COPY_SUBTREE" (
        in_cube_id                      NUMBER,         -- required if source or target object is in WORKING tables;
                                                        -- else pass -1 if both of them are in PERMANENT tables.
        in_src_object_prefix_cd         VARCHAR2,       -- Table prefix of the source object.
        in_src_template_id              NUMBER,         -- of the source object.
        in_src_object_no                NUMBER,         -- of the source object. (-1 for NULL)
        in_tar_object_prefix_cd         VARCHAR2,       -- Table prefix of the target object.
        in_tar_template_id              NUMBER,         -- 0 'PMMODEL', NOT NULL 'PMACTIVE',(Only for Save As: -1 'WKACTIVE')
        in_tar_new_object_nm            VARCHAR2,       -- New unique name of the target object.
        in_last_post_time               DATE,           -- Required for Posting only. Else pass in NULL.
        in_max_user_id                  NUMBER,         -- Max User id
        in_max_group_id                 NUMBER,         -- Max Group id
        in_debug_flg                    NUMBER,         -- Internal. Only for debugging. App. passes -1.
        in_special_template_id          NUMBER,         -- Used ONLY by p_wl_copy_template
        in_tar_wk_task_no               NUMBER,      	-- Pass in the target worksheet_task_no for only WLKS(W)_kpi_set.
        						-- Pass 0 when copying to MODEL KPI_Set. Else pass in -1.
        in_future3                      NUMBER,         -- placeholder. Pass in -1.
        out_new_object_no       OUT     NUMBER          -- the newly created object.
) AS

/*
Change History

$Log: 2334_p_wl_copy_subtree.sql,v $
Revision 1.39  2007/06/19 14:39:04  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.35  2006/11/10 21:45:48  saghai
S0362566, S0376286 Performance Changes

Revision 1.34  2006/09/27 13:49:05  anchan
Moved SQL statement (harmless; for UDB sake)

Revision 1.33  2006/09/22 20:56:41  anchan
S0379869: specify target cube_id or -1

Revision 1.32  2006/09/11 13:37:32  anchan
S0366552: push out the changes to calling procs

Revision 1.31  2006/08/31 15:37:54  anchan
Moved initialize of varables

Revision 1.29  2006/08/30 16:15:17  anchan
Pass the cube_id

Revision 1.28  2006/08/29 20:05:47  anchan
S0362566: Added session_id column to allow for rewrite of performance-enhanced UDB version and straightforward porting

Revision 1.27  2006/08/14 20:22:43  anchan
S0344510: Replaced dynamic SQL statements with regular stmt.

Revision 1.26  2006/07/21 19:35:54  anchan
S0366552: Save changes made at or after last post time.

Revision 1.25  2006/07/21 19:23:26  anchan
S0344510: For finer granularity always, use the common variable t_new_object_no.

Revision 1.24  2006/06/21 17:08:00  saghai
S0363432 - Handling root_% columns in wlw1 and wltp tables

Revision 1.23  2006/05/19 17:13:17  anchan
No change--just a new timestamp for the script

Revision 1.22  2006/05/12 19:37:14  anchan
Added object_type_cd granularity to keep track of copy operation; also moved the code outside the main loop.

Revision 1.21  2006/04/14 16:39:33  makirk
Removed import_log delete code (redundant), added change history logging where needed, commented out param logging commit

Revision 1.20  2006/03/22 16:17:48  anchan
No change--just a new timestamp for the script

Revision 1.19  2006/03/21 14:25:52  anchan
Moved DELETE statements from p_wl_copy_subtree to p_wl_post_template as part of changes to fix
S0350517: "delete before insert of re-added rows".

Revision 1.18  2006/03/17 18:46:34  saghai
Changed function to procedure

Revision 1.17  2006/03/14 20:27:17  anchan
Removed COMMIT after POSTING.

Revision 1.16  2006/02/23 22:25:27  anchan
Cosmetic change--removed hash symbol from bug number.

Revision 1.15  2006/02/21 21:37:05  saghai
S0332904 - Added missing 'WORKING_TO_WKACTIVE' case

Revision 1.14  2006/02/16 22:26:02  saghai
PERFORMANCE-ENHANCED PACKAGE


V6.1
6.1.0-001 06/01/05 Sachin       Initial Entry

Description:

This is the core procedure used to copy/move any worksheet template object
from working to permanent tables and vice-versa.

NOTE: Do NOT issue any COMMITs from within this procedure.
The calling procedure should handle TRANSACTION control.
*/


n_sqlnum                        NUMBER(10,0);
t_proc_name                     VARCHAR2(32)            := 'p_wl_copy_subtree';
t_error_level                   VARCHAR2(6)             := 'info';
t_call                          VARCHAR2(1000);
v_sql                           VARCHAR2(1000)          := NULL;
t_sql2                          VARCHAR2(255);
t_sql3                          VARCHAR2(255);
t_future_param_int              NUMBER(10,0)            := -1;
t_int_null                      NUMBER(10,0)            := NULL;

t_next_obj_no_increment         NUMBER(10,0)            := 1;
t_int_negative_one              NUMBER(10,0)            := -1;

t_err_msg                       VARCHAR2(255);
t_comma_loc                     NUMBER(2);
t_comma                         CHAR(1) := ',';
t_table_copy_order              VARCHAR2(75);
t_table_prefix_cd               VARCHAR2(5);

t_object_type                   VARCHAR2(20)            := 'OBJECT_TYPE';
t_object_cd                     NUMBER(2);
t_root_object_cd                NUMBER(2);

t_where_clause                  VARCHAR2(1000);
t_upd_where_clause              VARCHAR2(2000);
t_cube_where                    VARCHAR2(100);
t_session_id                       NUMBER(10);
t_object_name                   VARCHAR2(30);
t_table_name                    VARCHAR2(50);
t_src_wk_perm                   VARCHAR2(1);
t_tar_wk_perm                   VARCHAR2(1);

t_column_list                   VARCHAR2(4000);
t_column_list_value             VARCHAR2(4000);
t_copy_type_nm                  VARCHAR2(30);

t_from_transfer_case            VARCHAR2(10) := NULL;
t_to_transfer_case              VARCHAR2(10) := NULL;
t_transfer_case                 VARCHAR2(20) := NULL;

t_tar_cube_id                   NUMBER(10) := 0;
t_tar_template_id               NUMBER(10) := 0;
t_new_object_no                 NUMBER(10) := -1;

t_usage_type_cd                 VARCHAR2(3);
t_predefined_flg                NUMBER(1);
t_next_display_seq_no           NUMBER(10);
t_max_user_id                   NUMBER(10)              := NULL;
t_max_group_id                  NUMBER(10)              := NULL;
t_tar_new_object_nm             VARCHAR2(100)           := NULL;
t_max_pane_no                   NUMBER(10)              := 0;
t_max_pane_node_no              NUMBER(10)              := 0;
t_max_dyn_level_no              NUMBER(10)              := 0;
t_column_value                  VARCHAR2(255);
t_new_wk_task_no		NUMBER(10)		:= NULL;

BEGIN

n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call:= t_proc_name || ' ( ' ||
        COALESCE(in_cube_id, -123) || ',''' ||
        in_src_object_prefix_cd|| ''',' ||
        COALESCE(in_src_template_id, -123) || ',' ||
        COALESCE(in_src_object_no, -123) || ',''' ||
        in_tar_object_prefix_cd || ''',' ||
        COALESCE(in_tar_template_id, -123) || ',''' ||
        in_tar_new_object_nm || ''',' ||
        TO_CHAR(in_last_post_time,'MM/DD/YYYY HH24:MI:SS') || ',' ||
        COALESCE(in_max_user_id, -123) || ',' ||
        COALESCE(in_max_group_id, -123) || ',' ||
        COALESCE(in_debug_flg, -1) || ',' ||
        COALESCE(in_special_template_id, -1) || ',' ||
        COALESCE(in_tar_wk_task_no, -1) || ',' ||
        COALESCE(in_future3, -1) || ',' ||
        'OUT out_new_object_no' ||
        ' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, t_int_null);
--COMMIT;

n_sqlnum := 3000;

IF COALESCE(in_src_template_id,-1) = -1 THEN
        RAISE_APPLICATION_ERROR(-20001,'Parameter Source Template Id cannot be NULL or -1.');
END IF;
IF in_tar_template_id IS NULL THEN
        RAISE_APPLICATION_ERROR(-20001,'Parameter Target Template Id cannot be NULL.');
END IF;

IF in_tar_template_id = -1 AND SUBSTR(in_tar_object_prefix_cd,1,4) <> 'WLWT' THEN
        RAISE_APPLICATION_ERROR(-20001,'Parameter Target Template Id cannot be -1 for '||in_tar_object_prefix_cd);
END IF;

-- Cube Id cannot be -1 if either Source or Target is in Working area.
IF (SUBSTR(in_src_object_prefix_cd,5,1) = 'W' AND COALESCE(in_cube_id,-1) = -1) OR
   (SUBSTR(in_tar_object_prefix_cd,5,1) = 'W' AND COALESCE(in_cube_id,-1) = -1) THEN
        RAISE_APPLICATION_ERROR(-20001,'Invalid Cube Id for Working Object.');
END IF;

IF in_debug_flg > 0 THEN
        v_sql := 'TRUNCATE TABLE maxdata.t_sc_log';
        EXECUTE IMMEDIATE v_sql;
END IF;

n_sqlnum := 6000;
-- Check source and target fifth character in table prefix code
IF SUBSTR(in_src_object_prefix_cd,5,1) = 'W' THEN
        t_src_wk_perm := 'W';
ELSE
        t_src_wk_perm := '';
END IF;


IF SUBSTR(in_tar_object_prefix_cd,5,1) = 'W' THEN
	t_tar_cube_id := in_cube_id;
        t_tar_wk_perm := 'W';
ELSE
	t_tar_cube_id := -1;
        t_tar_wk_perm := '';
END IF;

n_sqlnum := 7000;
t_session_id := -1; -- OR USERENV('SESSIONID'); UDB MUST use APPLICATION_ID() to obtain a SESSIONID.

n_sqlnum := 8000;
v_sql := 'TRUNCATE TABLE maxdata.sess_new_object_no'; --WHERE session_id='||TO_CHAR(t_session_id);
EXECUTE IMMEDIATE v_sql;

n_sqlnum := 9000;
-- Forcibly attach this function explicitly; otherwise this function only appears within Dynamic SQL in WLCL_column_list:
t_new_object_no := maxdata.f_wl_new_object_no(0,'DUMMY',0);
t_new_object_no := -1;

t_object_cd := maxdata.f_lookup_number(t_object_type,in_tar_object_prefix_cd);

-- Replacing one 'single quote' with two 'single quotes' so that dynamic sql can handle it.
t_tar_new_object_nm := REPLACE(in_tar_new_object_nm,'''','''''');

-- Special case ONLY when we are copying worksheet_tasks under a model template
-- in the p_wl_copy_template procedure
IF COALESCE(in_special_template_id,-1) <> -1 THEN
        n_sqlnum := 10100;
        t_tar_template_id := in_special_template_id;
ELSE
        n_sqlnum := 10200;
        t_tar_template_id := in_tar_template_id;
END IF;


-- Determine the order in which tables need to be copied for an object
n_sqlnum := 10000;
CASE  UPPER(SUBSTR(in_src_object_prefix_cd,1,4))
-- If Worksheet Template
WHEN 'WLWT' THEN
        n_sqlnum := 11000;
        t_table_copy_order := 'WLWT,WLDF,WLD1,WLTD,WLKS,WLKF,WLPL,WLW1,WLTP,WLLA,WLPN';
        t_object_name := 'worksheet_template';
        t_where_clause := '';

        SELECT  CASE WHEN t_tar_new_object_nm IS NULL
                THEN 'template_nm'
                ELSE ''''||t_tar_new_object_nm||''''
                END CASE
        INTO t_tar_new_object_nm
        FROM DUAL;

-- If Worksheet Task
WHEN 'WLW1' THEN
        n_sqlnum := 12000;
        t_table_copy_order := 'WLDF,WLD1,WLKS,WLKF,WLPL,WLW1,WLTP,WLLA,WLPN';
        t_object_name := 'worksheet_task';
        t_where_clause :=' AND worksheet_task_no = '||CAST(in_src_object_no AS VARCHAR2);

        SELECT  CASE WHEN t_tar_new_object_nm IS NULL
                THEN 'task_nm || ''[]'''
                ELSE ''''||t_tar_new_object_nm||''' || ''[]'''
                END CASE
        INTO t_tar_new_object_nm
        FROM DUAL;

        maxdata.p_wl_next_object_no ( t_tar_cube_id,t_tar_template_id,t_int_negative_one,t_object_cd,
                                        t_next_obj_no_increment,t_future_param_int,t_future_param_int,t_future_param_int,
                                        t_new_object_no);

        maxdata.p_wl_generate_object_no( t_session_id,in_cube_id,in_src_object_prefix_cd,in_src_template_id,in_src_object_no,in_tar_object_prefix_cd,
                                                t_tar_template_id,in_debug_flg,t_future_param_int,t_future_param_int,t_future_param_int);
-- If Pane Layout
WHEN 'WLPL' THEN
        n_sqlnum := 13000;
        t_table_copy_order := 'WLPL,WLPN';
        t_object_name := 'pane_layout';
        t_where_clause :=' AND pane_layout_no = '||CAST(in_src_object_no AS VARCHAR2);

        maxdata.p_wl_next_object_no (  t_tar_cube_id,t_tar_template_id,t_int_negative_one,t_object_cd,
                                        t_next_obj_no_increment,t_future_param_int,t_future_param_int,t_future_param_int,
                                        t_new_object_no);

-- If Dimension Layout
WHEN 'WLD1' THEN
        n_sqlnum := 14000;
        t_table_copy_order := 'WLD1';
        t_object_name := 'dimension_layout';
        t_where_clause :=' AND dimension_layout_no = '||CAST(in_src_object_no AS VARCHAR2);

        maxdata.p_wl_next_object_no (  t_tar_cube_id,t_tar_template_id,t_int_negative_one,t_object_cd,
                                        t_next_obj_no_increment,t_future_param_int,t_future_param_int,t_future_param_int,
                                        t_new_object_no);

-- If KPI Set
WHEN 'WLKS' THEN
        n_sqlnum := 15000;
        t_table_copy_order := 'WLDF,WLKS,WLKF';
        t_object_name := 'kpi_set';
        t_where_clause :=' AND kpi_set_no = '||CAST(in_src_object_no AS VARCHAR2);


        SELECT  CASE WHEN t_tar_new_object_nm IS NULL
                THEN 'kpi_set_nm || ''[]'''
                ELSE ''''||t_tar_new_object_nm||''' || ''[]'''
                END CASE
        INTO t_tar_new_object_nm
        FROM DUAL;

        maxdata.p_wl_next_object_no (  t_tar_cube_id,t_tar_template_id,t_int_negative_one,t_object_cd,
                                        t_next_obj_no_increment,t_future_param_int,t_future_param_int,t_future_param_int,
                                        t_new_object_no);

        maxdata.p_wl_generate_object_no( t_session_id,in_cube_id,in_src_object_prefix_cd,in_src_template_id,in_src_object_no,in_tar_object_prefix_cd,
                                                t_tar_template_id,in_debug_flg,t_future_param_int,t_future_param_int,t_future_param_int);
ELSE
        RAISE_APPLICATION_ERROR(-20001,'Invalid Table Prefix Code: '|| in_src_object_prefix_cd);
END CASE;


-- Get the transfer case based on input parameters
n_sqlnum := 20000;
maxdata.p_wl_transfer_case(     in_cube_id,
                                t_object_name,
                                in_src_object_prefix_cd,
                                in_src_template_id,
                                in_src_object_no,
                                in_tar_object_prefix_cd,
                                in_tar_template_id,   -- Should not be t_tar_template_id (For Handling Model Template Copy)
                                in_last_post_time,
                                t_transfer_case
                                );

n_sqlnum := 22000;
t_from_transfer_case := SUBSTR(t_transfer_case,1,INSTR(t_transfer_case, '_', 1, 1)-1);
t_to_transfer_case := SUBSTR(t_transfer_case,INSTR(t_transfer_case, '_', 1, 2)+1);

IF in_debug_flg > 0 THEN
        DBMS_OUTPUT.PUT_LINE ('Transfer Case = ' ||t_transfer_case);
END IF;


-- Set the where clause for Posting of Worksheet Template case; on UDB, specify down to microseconds:
n_sqlnum := 23000;
IF  t_transfer_case = 'WORKING_TO_PMACTIVE' THEN
        t_where_clause:=' AND create_dttm > TO_DATE('''||TO_CHAR(in_last_post_time,'MM/DD/YYYY HH24:MI:SS')||''',''MM/DD/YYYY HH24:MI:SS'')';
END IF;

-- Set the cube where clause for all cases
n_sqlnum := 24000;
IF t_transfer_case IN ('WORKING_TO_PMACTIVE','WORKING_TO_PMMODEL','WORKING_TO_WKACTIVE','WORKING_TO_WORKING') THEN
        t_cube_where := ' cube_id = '||CAST(in_cube_id AS VARCHAR2);

ELSIF t_transfer_case IN ('PMMODEL_TO_PMMODEL','PMACTIVE_TO_PMACTIVE','PMMODEL_TO_PMACTIVE','PMACTIVE_TO_PMMODEL') THEN
        t_cube_where := CAST(in_cube_id AS VARCHAR2)||' = -1';

ELSIF t_transfer_case IN ('PMACTIVE_TO_WORKING','PMMODEL_TO_WORKING') THEN
        t_cube_where := CAST(in_cube_id AS VARCHAR2)||' = '|| CAST(in_cube_id AS VARCHAR2);
END IF;



-- Setting max_user_id and max_group_id when copying to Model ONLY, else -1.
n_sqlnum := 25000;
IF t_to_transfer_case = 'PMMODEL' THEN
        -- Set in_max_user_id and in_max_group_id to NULL if they are -1.
        IF in_max_user_id = -1 THEN
                t_max_user_id := NULL;
        ELSE
                t_max_user_id := in_max_user_id;
        END IF;

        IF in_max_group_id = -1 THEN
                t_max_group_id := NULL;
        ELSE
                t_max_group_id := in_max_group_id;
        END IF;
ELSE
        t_max_group_id := NULL;
        t_max_user_id := NULL;
END IF;


n_sqlnum := 26000;
-- Set the target template id
IF  t_transfer_case IN ('PMACTIVE_TO_WORKING','WORKING_TO_PMACTIVE') THEN
        t_tar_template_id := in_src_template_id;

        -- Set object number to zero for STAGING/POSTING
        -- for WLOOW_object_operation. See n_sqlnum := 42100
        t_object_cd := 0;
ELSE
        t_tar_template_id := in_tar_template_id;
END IF;


n_sqlnum := 27000;
-- Template_id is always 0 for Models (except template)
IF SUBSTR(in_src_object_prefix_cd,1,4) <> 'WLWT' AND t_to_transfer_case = 'PMMODEL' THEN
        t_tar_template_id := 0;

        -- Special case ONLY when we are copying worksheet_tasks under a model template
        -- in the p_wl_copy_template procedure
        IF COALESCE(in_special_template_id,-1) <> -1 THEN
                n_sqlnum := 27200;
                t_tar_template_id := in_special_template_id;
        END IF;
END IF;

-- Set the display sequence no for working and active worksheet tasks
-- When copying to model worksheet task display sequence no is 0.

IF in_tar_object_prefix_cd IN ('WLW1W','WLW1') AND COALESCE(in_special_template_id,-1) = -1 THEN
        n_sqlnum := 28000;
        IF t_to_transfer_case = 'PMMODEL' THEN
                n_sqlnum := 28100;
                t_next_display_seq_no := 0;
        ELSE
                n_sqlnum := 28200;
                v_sql :=' SELECT COALESCE(MAX(display_sequence_no),0) + 1 '||
                        ' FROM maxdata.WLW1'||t_tar_wk_perm||'_worksheet_task'||
                        ' WHERE '||t_cube_where||
                        ' AND worksheet_template_id = '||CAST(t_tar_template_id AS VARCHAR2);

                EXECUTE IMMEDIATE v_sql
                INTO t_next_display_seq_no;

        END IF;
END IF;


-- Special Case:
-- Target worksheet_task_no supplied only when copying to a NON-Model Kpi Set
-- When copy a KPI set to a model task we want to store the task_no.
n_sqlnum := 28500;
IF UPPER(SUBSTR(in_src_object_prefix_cd,1,4)) = 'WLKS' AND COALESCE(in_tar_wk_task_no,-1) NOT IN (0,-1) THEN
	t_new_wk_task_no := in_tar_wk_task_no;
END IF;

n_sqlnum := 29000;
-- When copying to Model Template, copy ONLY WLWT record.
-- p_wl_copy_template will then copy all the Worksheet Task's individually
-- by making a call to p_wl_copy_subtree
IF SUBSTR(in_src_object_prefix_cd,1,4) = 'WLWT' AND t_to_transfer_case = 'PMMODEL' THEN
        t_table_copy_order := 'WLWT,WLTD';
END IF;

IF in_debug_flg > 0 THEN
        DBMS_OUTPUT.PUT_LINE ('Table Copy Order ='||t_table_copy_order);
END IF;


n_sqlnum := 32000;
-- Get next worksheet template id for COPY cases
IF UPPER(SUBSTR(in_tar_object_prefix_cd,1,4))='WLWT'
AND t_transfer_case NOT IN ('PMACTIVE_TO_WORKING','WORKING_TO_PMACTIVE') THEN
        maxdata.p_wl_next_object_no (-1,-1,-1,1,1,-1,-1,-1,t_tar_template_id);
END IF;

-- Make an entry in the copy status table before copying data
-- Triggers will check this entry to update '_no' columns
t_root_object_cd:= t_object_cd;

n_sqlnum := 33100;
DELETE FROM maxdata.WLOOW_object_operation
WHERE cube_id=t_tar_cube_id
AND worksheet_template_id=t_tar_template_id
AND object_type_cd=t_root_object_cd
AND object_no=t_new_object_no;

n_sqlnum := 33200;
INSERT INTO maxdata.WLOOW_object_operation (cube_id,worksheet_template_id,object_type_cd,object_no)
VALUES (t_tar_cube_id, t_tar_template_id,t_root_object_cd,t_new_object_no);
COMMIT;


-- Set the column lists and then insert into the tables based on transfer case
n_sqlnum := 40000;
LOOP
        -- Find the comma position.
        t_comma_loc := INSTR(t_table_copy_order, t_comma, 1, 1);

        IF t_comma_loc = 0 THEN
                t_table_prefix_cd := t_table_copy_order;
                t_table_copy_order := 'EXIT';
        ELSE
                t_table_prefix_cd := SUBSTR(t_table_copy_order,1,t_comma_loc - 1);
                t_table_copy_order := SUBSTR(t_table_copy_order,t_comma_loc+1);
        END IF;

        IF in_debug_flg > 0 THEN
                DBMS_OUTPUT.PUT_LINE ('Table = ' ||t_table_prefix_cd);
        END IF;

        -- Set usage_type_cd and predefinied_flg only when Copying to Model and
        -- when table being copied is same as object being copied
        n_sqlnum := 41000;
        IF t_to_transfer_case IN ('PMMODEL') AND t_table_prefix_cd = SUBSTR(in_src_object_prefix_cd,1,4) THEN
                t_usage_type_cd := '''M''';
                t_predefined_flg := 1;

                -- Special case ONLY when we are copying worksheet_tasks under a model template
                -- in the p_wl_copy_template procedure
                IF COALESCE(in_special_template_id,-1) <> -1 THEN
                        n_sqlnum := 41200;
                        t_usage_type_cd := '''A''';
                        t_predefined_flg := 0;
                END IF;
        ELSE
                t_usage_type_cd := '''A''';
                t_predefined_flg := 0;
        END IF;



        n_sqlnum := 44000;
        -- Get the table name of the table to be copied
        t_object_cd := maxdata.f_lookup_number(t_object_type,t_table_prefix_cd);
        t_table_name := maxdata.f_lookup_name(t_object_type, t_object_cd);
        t_table_name := SUBSTR(t_table_name,INSTR(t_table_name,'_') + 1);


        -- Now build the column and column value lists
        -- from the maxdata.WLCL_column_list table

        -- Set the column list to NULL for every loop:
        t_column_list := NULL;
        t_column_list_value := NULL;

        -- Add cube_id only when copying to working tables
        n_sqlnum := 45000;
        IF t_to_transfer_case = 'WORKING' THEN
                t_column_list := 'cube_id';
                t_column_list_value := ''|| CAST(in_cube_id AS VARCHAR2);
        END IF;

        -- Set the correct copy type
        n_sqlnum := 46000;
        IF t_transfer_case IN ('PMACTIVE_TO_WORKING','WORKING_TO_PMACTIVE')  THEN
                t_copy_type_nm := 'STAGEPOST';
        ELSE
                t_copy_type_nm := 'COPY';
        END IF;

        -- Build column list and column value list
        n_sqlnum := 47000;
        DECLARE
                CURSOR wlcd_cur IS
                SELECT * FROM maxdata.WLCL_column_list
                WHERE table_prefix_cd = t_table_prefix_cd
                AND (copy_type_nm IN t_copy_type_nm OR copy_type_nm = 'ALL')
                AND (root_object_prefix_cd = UPPER(SUBSTR(in_src_object_prefix_cd,1,4)) OR root_object_prefix_cd IS NULL);
        BEGIN

        FOR c_row IN wlcd_cur
        LOOP
                IF SUBSTR(c_row.column_value,1,1) = '%' THEN
                        CASE SUBSTR(c_row.column_value,2)
                        WHEN 't_tar_template_id' THEN t_column_value := CAST(t_tar_template_id AS VARCHAR2);
                        WHEN 't_usage_type_cd' THEN t_column_value :=  t_usage_type_cd;
                        WHEN 't_predefined_flg' THEN t_column_value :=  CAST(t_predefined_flg AS VARCHAR2);
                        WHEN 't_max_group_id' THEN t_column_value :=  COALESCE(CAST(t_max_group_id AS VARCHAR2),'NULL');
                        WHEN 't_max_user_id' THEN t_column_value :=  COALESCE(CAST(t_max_user_id AS VARCHAR2),'NULL');
                        WHEN 't_new_object_no' THEN t_column_value :=  CAST(t_new_object_no AS VARCHAR2);
                        WHEN 't_next_display_seq_no' THEN t_column_value :=  COALESCE(CAST(t_next_display_seq_no AS VARCHAR2),c_row.column_nm);
                        WHEN 't_tar_new_object_nm' THEN t_column_value := t_tar_new_object_nm;
                        WHEN 't_new_wk_task_no'	THEN t_column_value :=  COALESCE(CAST(t_new_wk_task_no AS VARCHAR2),'NULL');
                        WHEN 'SKIP_MODEL' THEN t_column_value := c_row.column_nm;
                        END CASE;
                ELSE
                	IF SUBSTR(c_row.column_value,1,1) = '(' THEN
                 		t_column_value := REPLACE(c_row.column_value,'%t_session_id',CAST(t_session_id AS VARCHAR2));
			ELSE
				t_column_value := COALESCE(c_row.column_value,c_row.column_nm);
		 	END IF;
                END IF;

		IF (t_to_transfer_case<>'PMMODEL')
		OR (c_row.column_value IS NULL)
		OR (c_row.column_value<>'%SKIP_MODEL') THEN

			IF (t_column_list IS NOT NULL) THEN
                		t_column_list := t_column_list||','||c_row.column_nm;
                		t_column_list_value := t_column_list_value||','||t_column_value;
			ELSE
                		t_column_list := c_row.column_nm;
                		t_column_list_value := t_column_value;
			END IF;

		END IF;

        END LOOP;
        END;

        -- Add create_dttm and update_dttm columns for STAGING case ONLY
        n_sqlnum := 49000;
        IF t_transfer_case IN ('PMACTIVE_TO_WORKING') THEN
                t_column_list := t_column_list ||',create_dttm, update_dttm';
                t_column_list_value := t_column_list_value ||',create_dttm, update_dttm';
        END IF;
        -- Finished building the column lists


        -- Now ready to insert new rows in the table

        IF t_transfer_case = 'WORKING_TO_PMACTIVE' AND t_table_prefix_cd = 'WLWT' THEN
                -- Should not insert any new worksheet template record while posting
                -- Dummy statement below
                n_sqlnum := 49999;
        ELSE
                n_sqlnum := 50000;
                IF in_debug_flg > 0 THEN
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+100,'INSERT INTO maxdata.'||t_table_prefix_cd||t_tar_wk_perm||'_'|| t_table_name);
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+200,'(' );
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+300,SUBSTR(t_column_list,1,254));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+400,SUBSTR(t_column_list,255,509));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+500,SUBSTR(t_column_list,511,766));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+600,SUBSTR(t_column_list,767,1022));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+700,SUBSTR(t_column_list,1023,1278));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+800,SUBSTR(t_column_list,1279,1534));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+800,SUBSTR(t_column_list,1535));
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+900,' ) SELECT ');
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+1000,SUBSTR(t_column_list_value,1,254));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1100,SUBSTR(t_column_list_value,255,509));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1200,SUBSTR(t_column_list_value,511,766));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1300,SUBSTR(t_column_list_value,767,1022));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1400,SUBSTR(t_column_list_value,1023,1278));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1500,SUBSTR(t_column_list_value,1279,1534));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1500,SUBSTR(t_column_list_value,1535));
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+1600,' FROM maxdata.'||t_table_prefix_cd||t_src_wk_perm||'_'|| t_table_name );
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+1700,' WHERE '||t_cube_where||' AND worksheet_template_id = '||in_src_template_id);
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+1800,SUBSTR(t_where_clause,1,254));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1900,SUBSTR(t_where_clause,255,510));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1900,SUBSTR(t_where_clause,511));
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+2000,'');
                        COMMIT;
                END IF;

                n_sqlnum := 50100;
                EXECUTE IMMEDIATE  'INSERT INTO maxdata.'||
                        t_table_prefix_cd||t_tar_wk_perm||'_'|| t_table_name ||
                        '(' || t_column_list ||
                        ') SELECT '||
                        t_column_list_value ||
                        ' FROM maxdata.'|| t_table_prefix_cd||t_src_wk_perm||'_'|| t_table_name ||
                        ' WHERE '||t_cube_where||
                        ' AND worksheet_template_id = '||CAST(in_src_template_id AS VARCHAR2) ||
                        t_where_clause;

        END IF;

        -- Set next_%%_no's on WLW1 and WLPL tables
        IF t_table_prefix_cd IN ('WLW1','WLPL') AND t_transfer_case NOT IN ('PMACTIVE_TO_WORKING','WORKING_TO_PMACTIVE') THEN

        -- Update next_no's for task-wide objects (WLPN, WLTP), since cannot be updated inside p_wl_generate_object_no.
        -- Applies to all COPY cases:
                IF t_table_prefix_cd = 'WLW1' THEN
                        n_sqlnum := 51200;
                        SELECT MAX(tar_object_no) INTO t_max_pane_no
                        FROM maxdata.sess_new_object_no
                        WHERE table_prefix_cd = 'WLTP'
                        AND (src_object_no > 0);

                        n_sqlnum := 51300;
                        SELECT MAX(tar_object_no) INTO t_max_dyn_level_no
                        FROM maxdata.sess_new_object_no
                        WHERE table_prefix_cd = 'WLLA'
                        AND (tar_object_no > 20000)
                        AND (src_object_no > 20000);

                        -- Update next_dimension_level_no only if over 20000
                        n_sqlnum := 51400;
			IF t_tar_wk_perm='W' THEN
                        	UPDATE maxdata.WLW1W_worksheet_task
                                SET   next_pane_no = t_max_pane_no,
                                      next_dimension_level_no =  COALESCE(t_max_dyn_level_no,next_dimension_level_no)
                                WHERE worksheet_template_id = t_tar_template_id
                                AND worksheet_task_no = t_new_object_no
				AND cube_id=in_cube_id;
			ELSE
                        	UPDATE maxdata.WLW1_worksheet_task
                                SET   next_pane_no = t_max_pane_no,
                                      next_dimension_level_no =  COALESCE(t_max_dyn_level_no,next_dimension_level_no)
                                WHERE worksheet_template_id = t_tar_template_id
                                AND worksheet_task_no = t_new_object_no;

			END IF;
                END IF;

                -- Update next_pane_node_no on WLPL table
                IF t_table_prefix_cd = 'WLPL' THEN
                        n_sqlnum := 51500;
                        SELECT MAX(tar_object_no) INTO t_max_pane_node_no
                        FROM maxdata.sess_new_object_no
                        WHERE table_prefix_cd = 'WLPN'
                        AND (src_object_no > 0);

                        n_sqlnum := 51600;
			IF t_tar_wk_perm='W' THEN
                        	UPDATE maxdata.WLPLw_pane_layout
                        	SET   next_pane_node_no = t_max_pane_node_no
                        	WHERE worksheet_template_id =t_tar_template_id
                        	AND worksheet_task_no = t_new_object_no
				AND cube_id=in_cube_id;
			ELSE
                        	UPDATE maxdata.WLPL_pane_layout
                        	SET   next_pane_node_no = t_max_pane_node_no
                        	WHERE worksheet_template_id =t_tar_template_id
                        	AND worksheet_task_no = t_new_object_no;
			END IF;
                END IF;
        END IF; -- IF t_transfer_case NOT IN ('PMACTIVE_TO_WORKING','WORKING_TO_PMACTIVE') THEN


        -- Set update where clause for Posting and
        -- Update changed records when POSTING Worksheet Template ONLY

        IF t_transfer_case = 'WORKING_TO_PMACTIVE' THEN
                n_sqlnum := 52000;

                CASE  UPPER(t_table_prefix_cd)
                -- If Worksheet Template
                WHEN 'WLWT' THEN
                        t_upd_where_clause :=   ' cube_id='||CAST(in_cube_id AS VARCHAR2)||
                                                ' AND worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)|| ')'||
                                                ' WHERE upd.worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND upd.worksheet_template_id IN ( SELECT worksheet_template_id';
                -- If KPI Field
                WHEN 'WLKF' THEN
                        t_upd_where_clause :=   ' cube_id='||CAST(in_cube_id AS VARCHAR2)||
                                                ' AND worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND kpi_set_no=upd.kpi_set_no'||
                                                ' AND '||t_table_name||'_no = upd.'||t_table_name||'_no '||
                                                ' AND kpi_dv_id=upd.kpi_dv_id)'||
                                                ' WHERE upd.worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND (upd.kpi_set_no,upd.'||t_table_name||'_no,upd.kpi_dv_id) IN '||
                                                                                '( SELECT kpi_set_no,'||t_table_name||'_no,kpi_dv_id';
                -- If Pane Node
                WHEN 'WLPN' THEN
                        t_upd_where_clause :=   ' cube_id='||CAST(in_cube_id AS VARCHAR2)||
                                                ' AND worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND pane_layout_no=upd.pane_layout_no'||
                                                ' AND '||t_table_name||'_no = upd.'||t_table_name||'_no )'||
                                                ' WHERE upd.worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND (upd.pane_layout_no,upd.'||t_table_name||'_no) IN '||
                                                                                '( SELECT pane_layout_no,'||t_table_name||'_no';
                -- If Task Pane
                WHEN 'WLTP' THEN
                        t_upd_where_clause :=   ' cube_id='||CAST(in_cube_id AS VARCHAR2)||
                                                ' AND worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND worksheet_task_no=upd.worksheet_task_no'||
                                                ' AND pane_no = upd.pane_no )'||
                                                ' WHERE upd.worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND (upd.worksheet_task_no,upd.pane_no) IN '||
                                                                                '( SELECT worksheet_task_no,pane_no';
                -- If Level Assignment
                WHEN 'WLLA' THEN
                        t_upd_where_clause :=   ' cube_id='||CAST(in_cube_id AS VARCHAR2)||
                                                ' AND worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND worksheet_task_no=upd.worksheet_task_no'||
                                                ' AND pane_no=upd.pane_no'||
                                                ' AND dimension_level_no=upd.dimension_level_no'||
                                                ' AND dimension_type_cd=upd.dimension_type_cd)'||
                                                ' WHERE upd.worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND (upd.worksheet_task_no,upd.pane_no,upd.dimension_level_no,upd.dimension_type_cd) IN '||
                                                                                '( SELECT worksheet_task_no,pane_no,dimension_level_no,dimension_type_cd';
                -- If Template Dataversion
                WHEN 'WLTD' THEN
                        t_upd_where_clause :=   ' cube_id='||CAST(in_cube_id AS VARCHAR2)||
                                                ' AND worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND kpi_dv_id=upd.kpi_dv_id ) '||
                                                ' WHERE upd.worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND (upd.kpi_dv_id) IN ( SELECT kpi_dv_id';

                ELSE
                        -- For WLDF,WLD1,WLKS,WLPL,WLW1

                        t_upd_where_clause :=   ' cube_id='||CAST(in_cube_id AS VARCHAR2)||
                                                ' AND worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND '||t_table_name||'_no = upd.'||t_table_name||'_no ) '||
                                                ' WHERE upd.worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                                ' AND upd.'||t_table_name||'_no IN ( SELECT '||t_table_name||'_no';
                END CASE;

		--Append the COMMON sub-where clause;on UDB, specify down to microseconds:
		t_upd_where_clause := t_upd_where_clause ||
                                       ' FROM maxdata.'||t_table_prefix_cd||t_src_wk_perm||'_'||t_table_name||
                                       ' WHERE cube_id='||CAST(in_cube_id AS VARCHAR2)||
                                       ' AND worksheet_template_id='||CAST(in_src_template_id AS VARCHAR2)||
                                       ' AND create_dttm < TO_DATE('''||TO_CHAR(in_last_post_time,'MM/DD/YYYY HH24:MI:SS')||''',''MM/DD/YYYY HH24:MI:SS'')'||
                                       ' AND update_dttm > TO_DATE('''||TO_CHAR(in_last_post_time,'MM/DD/YYYY HH24:MI:SS')||''',''MM/DD/YYYY HH24:MI:SS'') )';


                IF in_debug_flg > 0 THEN
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+100,'UPDATE maxdata.'||t_table_prefix_cd||t_tar_wk_perm||'_'|| t_table_name||' upd');
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+200,' SET ( ' );
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+300,SUBSTR(t_column_list,1,254));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+400,SUBSTR(t_column_list,255,510));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+500,SUBSTR(t_column_list,511,766));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+600,SUBSTR(t_column_list,767));
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+700,' ) = (SELECT ' );
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+800,SUBSTR(t_column_list_value,1,254));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+900,SUBSTR(t_column_list_value,255,510));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1000,SUBSTR(t_column_list_value,511,766));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1100,SUBSTR(t_column_list_value,767));
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+1200,' FROM maxdata.'||t_table_prefix_cd||t_src_wk_perm||'_'|| t_table_name||' t ' );
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+1300,' WHERE '|| SUBSTR(t_upd_where_clause,1,248));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1400,' WHERE '|| SUBSTR(t_upd_where_clause,249,503));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1500,' WHERE '|| SUBSTR(t_upd_where_clause,504,758));
                        --INSERT INTO maxdata.t_sc_log values (n_sqlnum+1600,' WHERE '|| SUBSTR(t_upd_where_clause,759));
                        INSERT INTO maxdata.t_sc_log values (n_sqlnum+1700,'');
                        --COMMIT;
                END IF;

                n_sqlnum := 55000;

                EXECUTE IMMEDIATE
                        ' UPDATE maxdata.' ||
                        t_table_prefix_cd||t_tar_wk_perm||'_'|| t_table_name ||' upd'||
                        ' SET ( ' || t_column_list ||
                        ' ) = ( SELECT '||
                                t_column_list_value ||
                                ' FROM maxdata.'||t_table_prefix_cd||t_src_wk_perm||'_'||t_table_name||' t '||
                                ' WHERE '|| t_upd_where_clause;
        END IF;


        IF t_table_copy_order = 'EXIT' THEN
                EXIT;
        END IF;

END LOOP;





-- Determine the new object no

IF t_transfer_case IN ('PMACTIVE_TO_WORKING','WORKING_TO_PMACTIVE') THEN
        n_sqlnum := 65000;
        out_new_object_no := -1;
ELSE
        n_sqlnum := 66000;
        CASE  UPPER(SUBSTR(in_src_object_prefix_cd,1,4))
        -- If Worksheet Template
        WHEN 'WLWT' THEN
                n_sqlnum := 66100;
                out_new_object_no := t_tar_template_id;

        ELSE
                n_sqlnum := 66200;
                out_new_object_no := t_new_object_no;

        END CASE;

END IF;

-- Delete copy status table entry after copying data
n_sqlnum := 68000;
DELETE FROM maxdata.WLOOW_object_operation
WHERE cube_id = t_tar_cube_id
AND worksheet_template_id = t_tar_template_id
AND object_type_cd = t_root_object_cd
AND object_no = t_new_object_no;

EXCEPTION
        WHEN CASE_NOT_FOUND THEN
                RAISE_APPLICATION_ERROR(-20001,' Not a valid table');

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

		-- Cleanup status entry after failure:
		DELETE FROM maxdata.WLOOW_object_operation
		WHERE cube_id = t_tar_cube_id
		AND worksheet_template_id = t_tar_template_id
		AND object_type_cd = t_root_object_cd
		AND object_no = t_new_object_no;
		COMMIT;

                RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/
