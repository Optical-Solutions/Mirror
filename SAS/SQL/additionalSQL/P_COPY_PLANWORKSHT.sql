--------------------------------------------------------
--  DDL for Procedure P_COPY_PLANWORKSHT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COPY_PLANWORKSHT" (
        in_cube_id                              NUMBER,         -- -1 if called from app or p_copy_planversion. NOT NULL if called by p_save_as_planworksht
        in_src_planworksheet_id                 NUMBER,
        in_tar_planworksheet_name               VARCHAR2,
        in_tar_planworksheet_desc               VARCHAR2,
        in_create_userid                        NUMBER,
        in_max_user_id                          NUMBER,
        in_max_group_id                         NUMBER,
        in_parent_id                            NUMBER,         -- planversion id if called to copy planversion. 0 otherwise.
        in_copy_plan_data                       NUMBER,
        in_copy_cl_hist                         NUMBER,         -- 0/1
        in_copy_submitted                       NUMBER,         -- 0/1
        in_set_whatif                           NUMBER,         -- 0/1
        in_future1                              NUMBER,         -- placeholder. Pass in -1.
        in_future2                              NUMBER,         -- placeholder
        in_future3                              VARCHAR2,       -- placeholder
        out_tar_planworksheet_id        OUT     NUMBER,
        out_errcode                     OUT     NUMBER,
        out_errmsg                      OUT     VARCHAR2
        )
AS
/*
$Log: 2352_p_copy_planworksht.sql,v $
Revision 1.16.6.1.2.2  2009/06/29 19:21:47  makirk
FIXID S0580161: Further changes for S0580161

Revision 1.16.6.1.2.1  2009/06/29 16:04:28  makirk
FIXID S0580161: Fix for S0580161

Revision 1.16.6.1  2008/11/26 20:39:53  makirk
Fix for S0506158

Revision 1.16  2007/10/05 18:56:21  amkatr
For Defect # S0467554 ,S0467551

Revision 1.15  2007/10/05 15:14:07  clapper
FIXID AUTOPUSH: SOS 1256274

Revision 1.14.2.1  2007/07/03 13:45:18  anchan
S0429890: exclude publish_flg column

Revision 1.14  2007/06/19 14:38:59  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.9  2006/04/17 19:39:29  saghai
Removed Delete from import_log

Revision 1.8  2006/03/17 14:39:30  saghai
S0331985. Added seperate logic for handling p_save_as_planworksht

Revision 1.7  2006/02/17 14:58:01  saghai
fixed in_copy_data

Revision 1.6  2006/02/01 17:48:42  saghai
case to handle in_copy_plan_data = 2

Revision 1.5  2006/01/06 15:51:45  saghai
Added check : Copy the worksheet template only if it is NOT NULL

Revision 1.4  2005/10/28 21:19:23  saghai
Added  in_set_whatif parameter

Revision 1.3  2005/10/18 18:50:36  healja
adding missing parent_id (planversion_id) when proc is called from p_copy_planversion

----------------------------------------------------------------------

-- Change History:
--V6.1.0
--6.1.0-001     07/18/05        Sachin  Changed mplan to 3-mplan tables.Added Copy of worksheet template
--6.1.0-001     07/15/05        DR      Changed logic for cluster history generation as per new cl_hist_status table definition.

--V5.6.2
-- 5.6.2-075    07/27/05        HELMI   MM018048 fixing typos in column names.
-- 5.6.2-072    06/25/05        helmi   MM018048 some cols have to be excluded from the copy.

--V5.6.1
-- 5.6.1-062    01/12/05        Sachin  Enh#2193,2200,2345: Support time-out, batch, cl hist copy, and whatif changes.

--V5.6
-- 5.6.0-029-37 10/18/04        DR      #17475 copy source invalidate column to target only when copy mplan records also requested i.e (i_mplan_data = 1)
-- 5.6.0-040    07/16/04        Sachin  #17120 Adding missing columns invalidate and num_master_change to insert of planworksheet
-- 5.6.0-029-26 07/01/04        Sachin  Enh #2202 , Defect 17038 for TD/BU cutoff flg
-- 5.6.0-026    03/08/04        DR      #16080, #16283  set whatif based on plandata and trend flag
-- 5.6.0-024    02/05/2004      Sachin  #16080. Setting whatif flag.
--V5.5
-- 5.5.0-all    17/11/2002      Helmi   Param to p_copy_pw_tmpl that the proc will supprt allocation as well .

--V5.4
-- 5.4.0-027    10/22/2002      Helmi   Adding the new col save_calc_flg to the planworksheet copy stmt(insert).
-- 5.4.0-018    9/26/02         Helmi   adding functionality to copy fp_exception records.
-- 5.4.0-000    08/07/03        DR      Added Merch_Refilter, Loc_Refilter columns for planworksheet insert statement.

--V5.3.4
--V5.3.4
-- 05/30/03     Sachin  Insert an entry into the cluster history status table for p_get_cl_hist_bat.
-- 01/28/03     Helmi   adding cols for seletcive reseeding.
-- 01/13/03     Sachin  Copied Locked_bv_flg and Reseed_flg fields for planworksheet
-- 12/17/02     Sachin  Adding commit and calling p_del_plantable at exception.
--                      It is to resolve concurrency bottleneck at seq id.

--V5.3.3
-- 6/13/02      helmi   modification to update whatif of the scr_pws and give 0 to variables in  copied one.
-- 5/24/02      helmi   adding input param in_copy_plan_data and putting a condition to copy mplan.

-- 5/13/02      DR      Added partial_merch_flag and partial_loc_flag.
-- 4/24/02      Rg      add reforecast_id
-- 4/9/02       Rg      If in_max_user_id or in_max_group_id is -1, then make it null during insert.
-- 4/8/02       Rg      added decode statement for trend flag and put the in_max_group_id into the insert statement.
-- 4/8/02       helmi   added new parameter in_max_group_id
-- 4/2/02       helmi   adding input param in_max_user_id and make it inside the insert stmt.
-- 3/11/02      Rg      bug 12510. submitted flag to be copied as original worksheet value when proc called from p_copy_planversion, --                         else 0.
-- 1/28/02      Rg      added plan_count column
-- 12/20/01     Joseph  Support trend_flag
-- 12/12/01     Joseph  Copy templates for dyn hier of v5.3.
-- 9/7/01       Rashmi  added new cols to planworksheet and mplan.
-- 08/13/01     Maurice Added 140 columns to mplan as part of built 64
-- 07/25/01     Joseph  Change 'status' of copied worksheet.
--                      Copy 'merch_alt_path'.
-- 07/18/01     Joseph  ORA only - Use p_get_next_key instead of insert trigger for mplan_id.
--                      SS/UDB already use p_get_next_key.
-- 06/11/01     Maurice added 4 cols to planworksheet and 1 col to mplan as part of build 34 manage members changes.
-- 05/17/01     Joseph  Check t_whatif code. Check planwork_stat_lkup.
*******************************************************************************************************
1. Change in P_copy_planworksht:
        A)  Last_update_date set to Sysdate and last_update_userid to in_create_userid
        B)  Added 6 additional columns when inserting into Planworksht table
                                                Sreeram 03/27/01
2.      A) Set WHATIF to be copied from the source WORKSHEET.  Otherwise, set it to 1.
        B) Define a new variable t_whatif to be copied from source planworksheet and then set it to 1.
                                                 Maurice Ndansi 04/11/2001
3. Changed in_parent_id <> 0  from in_parent_id is not null
*******************************************************************************************************/

n_sqlnum                        NUMBER(10,0);
t_proc_name                     VARCHAR2(32)            := 'p_copy_planworksht';
t_error_level                   VARCHAR2(6)             := 'info';
t_call                          VARCHAR2(1000);
v_sql                           VARCHAR2(4000)          := NULL;
v_sql2                          VARCHAR2(4000)          := NULL;
v_sql_extra                     VARCHAR2(4000)          := NULL;
t_sql2                          VARCHAR2(255);
t_sql3                          VARCHAR2(255);

t_planworksht                   VARCHAR2(255);
t_tar_planworksheet_name        VARCHAR2(255);
t_tar_planworksheet_desc        VARCHAR2(255);
t_tar_planworksheet_id          NUMBER;
t_planwk_status                 NUMBER;
t_seq_num                       NUMBER;
t_planversion_id                NUMBER;
t_cnt                           NUMBER;
t_whatif                        NUMBER(1);
t_submitted                     NUMBER(1);
t_inserted_flag                 NUMBER;
out_errcode2                    NUMBER;
out_errmsg2                     VARCHAR2(4000);
t_copied_from_id                NUMBER;
t_trend_flag                    NUMBER;
t_invalidate                    NUMBER;
t_loc_path_id                   NUMBER;
t_col_value                     VARCHAR2(100);
t_src_cl_tab_name               VARCHAR2(64);
t_tar_cl_tab_name               VARCHAR2(64);
t_future_param_int              NUMBER                  := -1;

t_list_mplan_table              VARCHAR2(100);
t_mplan_table                   VARCHAR2(30);
t_comma                         CHAR(1)                 := ',';
t_comma_loc                     NUMBER(2);
t_src_worksheet_template_id     NUMBER(10);
t_tar_worksheet_template_id     NUMBER(10);
t_src_object_prefix_cd          VARCHAR2(5);
t_counter                       NUMBER(1)               :=0;

BEGIN

n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
                COALESCE(in_cube_id, -1) || ',' ||
                COALESCE(in_src_planworksheet_id, -1) || ',' ||
                COALESCE(in_tar_planworksheet_name, 'NULL') || ',' ||
                COALESCE(in_tar_planworksheet_desc,'NULL') || ',' ||
                COALESCE(in_create_userid, -1) || ',' ||
                COALESCE(in_max_user_id, -1) || ',' ||
                COALESCE(in_max_group_id, -1) || ',' ||
                COALESCE(in_parent_id, -1) || ',' ||
                COALESCE(in_copy_plan_data, -1) || ',' ||
                COALESCE(in_copy_cl_hist, -1) || ',' ||
                COALESCE(in_copy_submitted, -1) || ',' ||
                COALESCE(in_set_whatif, -1) || ',' ||
                COALESCE(in_future1, -1) || ',' ||
                COALESCE(in_future2, -1) || ',' ||
                COALESCE(in_future3, 'NULL') || ',' ||
                ' OUT out_tar_planworksheet_id OUT out_errcode, OUT out_errmsg '||
                ' ) ';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
--COMMIT;

n_sqlnum := 1500;
-- Protect against single quotes being used in planworksheet names
-- Fix for S0506158
t_tar_planworksheet_name := REPLACE(in_tar_planworksheet_name,'''','');
-- Reopened for S0506158.  Reopened due to S0580161
t_tar_planworksheet_desc := REPLACE(in_tar_planworksheet_desc,'''','');

-- Initialize the error code/msg.
-- 0:success, 1:informational, 2:warning, 3:error

out_errcode     := 0;
out_errmsg      := '';
out_errcode2    := 0;
out_errmsg2     := '';
t_inserted_flag := 0;


n_sqlnum := 2000;
BEGIN
        SELECT planwork_stat_id,
               invalidate,
               loc_path_id,
               whatif,
               submitted,
               planversion_id,
               trend_flag,
               copiedfrom_id,
               worksheet_template_id
          INTO t_planwk_status,
               t_invalidate,
               t_loc_path_id,
               t_whatif,
               t_submitted,
               t_planversion_id,
               t_trend_flag,
               t_copied_from_id,
               t_src_worksheet_template_id
          FROM planworksheet
         WHERE planworksheet_id = in_src_planworksheet_id;

EXCEPTION
WHEN NO_DATA_FOUND THEN
        out_errcode := 3;
        out_errmsg := 'Specified planworksheet not found';
        ROLLBACK;
        RETURN;
END;

-- If procedure is called from p_copy_planvesrion we have to take parent id from the input param.
IF in_parent_id > 0 THEN
        t_planversion_id := in_parent_id;
END IF;

-- If the source plan status is NOT 'New', then use 'In-Progress' for target plan.
IF t_planwk_status <> 0 THEN
        t_planwk_status := 1;
END IF;

-- copy source invalidate column to target only when copy mplan records also requested i.e (i_mplan_data = 1)

IF in_copy_plan_data <> 1 THEN
        t_invalidate := 0;
END IF;

-- If this procedure was called to copy planversion,
-- then use the  parent id was passed in.
-- copy 'whatif' from the source planworksheet.
-- Otherwise, use the parent of the source plan.
-- set 'whatif' to 1
-- submitted shd be same as the source worksheet.

n_sqlnum := 4000;
IF in_set_whatif <> 1 THEN
        IF in_parent_id <= 0  THEN
                -- Whatif setting scenarios:
                -- If trend_flag = 1 then
                --      set whatif always as zero.
                -- elsif trend_flag = 0 then
                --       If plandata = 0 then  (copy dimension only)
                --              For target worksheet, set whatif 0 by setting the local variable
                --              For source worksheet,
                --                      if copied_from_id is null (that means, it is the top ancestor)
                --                      then set whatif of all descendants  to 1
                --                      else set whatif of all descendants as well as all ancestors to 1
                --       elsif plandata = 1 then (Make whatif worksheet with plan data)
                --              For source worksheet, no change
                --              For target worksheet, set whatif = 1
                --       else
                --              raise exception ('unexpected plandata');
                --       endif;
                -- else
                --      raise exception ('unexpected trend_flag' || trend_flag);
                -- endif;

                IF t_trend_flag = 1 THEN
                        t_whatif := 0;
                ELSIF t_trend_flag = 0 THEN
                        IF in_copy_plan_data = 0 THEN
                        BEGIN
                                t_whatif := 0;
                                t_planwk_status := 0;
                                t_submitted := 0;

                                IF t_copied_from_id IS NULL THEN
                                BEGIN
                                        n_sqlnum := 4800;

                                        UPDATE maxdata.planworksheet
                                           SET whatif = 1
                                         WHERE planworksheet_id = in_src_planworksheet_id
                                            OR copiedfrom_id    = in_src_planworksheet_id;
                                END;
                                ELSE
                                BEGIN
                                        n_sqlnum := 4900;

                                        UPDATE maxdata.planworksheet
                                           SET whatif = 1
                                         WHERE copiedfrom_id    = t_copied_from_id
                                            OR planworksheet_id = t_copied_from_id;

                                END;
                                END IF;
                        END;
                        ELSIF in_copy_plan_data in (1,2) THEN

                                t_whatif := 1;
                        ELSE
                        BEGIN
                                out_errcode := 3;
                                out_errmsg := 'Unexpected plan_data:' || in_copy_plan_data;
                                ROLLBACK;
                                RETURN;
                        END;
                        END IF;
                ELSE
                BEGIN
                        out_errcode := 3;
                        out_errmsg := 'Unexpected trend_flag:' || t_trend_flag;
                        ROLLBACK;
                        RETURN;
                END;
                END IF;

                t_submitted := 0;
        END IF;  --IF in_parent_id <= 0
ELSE
        t_whatif := 1;

END IF; --IF in_set_whatif <> 1 THEN

-- in_copy_plan_data is always 2 when called from p_save_as_planworksheet
-- Set whatif and submitted to the passed in parameter values
IF in_copy_plan_data in (2) THEN
        t_whatif    := in_set_whatif;
        t_submitted := in_copy_submitted;
END IF;


-- Check the sequence entry for planworksheet.

n_sqlnum := 5000;

SELECT seq_name
  INTO t_planworksht
  FROM maxapp.sequence
 WHERE level_type=94 AND entity_type=31;

IF UPPER(t_planworksht) <> UPPER('PLANWORKSHEET') THEN
        out_errcode := 3;
        out_errmsg := 'Planworksheet sequence entry level/type ids are wrong.';
        ROLLBACK;
        RETURN;
END IF;

-- Get the target worksheet template id.
IF in_cube_id = -1 THEN
        t_src_object_prefix_cd := 'WLWT';
ELSE
        t_src_object_prefix_cd := 'WLWTW';
END IF;

-- Copy the worksheet template only if it is NOT NULL
IF t_src_worksheet_template_id IS NOT NULL THEN
        n_sqlnum := 5500;
        maxdata.p_wl_copy_subtree (
                in_cube_id,
                t_src_object_prefix_cd,
                t_src_worksheet_template_id,
                -1,                             -- in_src_object_no
                'WLWT',
                -1,                             -- in_tar_template_id , 0 'PMMODEL', NOT NULL 'PMACTIVE',(Only for Save As: -1 'WKACTIVE')
                t_tar_planworksheet_name,
                NULL,                           -- in_last_post_time
                in_max_user_id,
                in_max_group_id,
                -1,                             -- in_debug_flg
                -1,                             -- in_future1
                -1,                             -- in_future2
                -1,                             -- in_future3
                t_tar_worksheet_template_id
        );
END IF;

-- Get the next id for planworksheet.
-- Do not use p_get_next_key because it commits the tranx.
-- We will commit at the end.

n_sqlnum := 6000;

maxapp.f_get_seq(94,31,t_tar_planworksheet_id);
COMMIT;

out_tar_planworksheet_id := t_tar_planworksheet_id;

-- Insert a new row into planworksheet, copying some fields from
-- the source plan.
-- last_update_date set to Sysdate, last_update_userid to in_create_userid
-- Modified the value from 1 to "Whatif" while inserting into Planworksheet......Sreeram 04/02/01

n_sqlnum := 7000;

v_sql  := NULL;
v_sql2 := NULL;

DECLARE CURSOR c_wrksht_cols IS
SELECT UPPER(column_name) column_name
  FROM user_tab_columns
 WHERE table_name = 'PLANWORKSHEET'
   AND UPPER(column_name) NOT IN ('BATCH_MODE','BAT_ERROR_MSG','BAT_STATUS_FLAG' ,'BATCH_STATUS_ID','PUBLISH_FLG')
 ORDER BY column_name;
BEGIN
FOR c1 IN c_wrksht_cols LOOP
        IF v_sql IS NULL  THEN
                v_sql :='INSERT INTO maxdata.planworksheet ( ';
        ELSE
                v_sql := v_sql ||',';
        END IF;
        v_sql := v_sql || c1.column_name;
        t_col_value :=  CASE c1.column_name
                        WHEN 'COPIEDFROM_ID' THEN 'COALESCE(copiedfrom_id, :in_src_planworksheet_id)'
                        WHEN 'CREATE_DATE' THEN 'SYSDATE'
                        WHEN 'CREATE_USERID' THEN ':in_create_userid'
                        WHEN 'DESCRIPTION' THEN ':t_tar_planworksheet_desc'
                        WHEN 'INVALIDATE' THEN ':t_invalidate'
                        WHEN 'LAST_UPDATE_DATE' THEN 'SYSDATE'
                        WHEN 'LAST_UPDATE_USERID' THEN ':in_create_userid'
                        WHEN 'MAX_GROUP_ID' THEN 'DECODE(:in_max_group_id,-1,NULL,:in_max_group_id)'
                        WHEN 'MAX_USER_ID' THEN 'DECODE(:in_max_user_id,-1,NULL,:in_max_user_id)'
                        WHEN 'NAME' THEN ':t_tar_planworksheet_name'
                        WHEN 'PLANVERSION_ID' THEN ':t_planversion_id'
                        WHEN 'PLANWORKSHEET_ID' THEN ':t_tar_planworksheet_id'
                        WHEN 'PLANWORK_STAT_ID' THEN ':t_planwk_status'
                        WHEN 'SAVE_CALC_FLG' THEN '0'
                        WHEN 'SUBMITTED' THEN 'DECODE(:in_copy_submitted,0,0,:t_submitted)'
                        WHEN 'SUBMIT_DATE' THEN 'DECODE(:in_copy_submitted,0,NULL,submit_date)'
                        WHEN 'SUBMIT_USERID' THEN 'DECODE(:in_copy_submitted,0,NULL,submit_userid)'
                        WHEN 'WHATIF' THEN ':t_whatif'
                        WHEN 'WORKSHEET_TEMPLATE_ID' THEN ':t_tar_worksheet_template_id'
                        ELSE c1.column_name
                        END;

        IF v_sql2 IS NULL  THEN
                v_sql2 := ' ) SELECT ';
        ELSE
                v_sql2 := v_sql2 ||',';
        END IF;
        v_sql2 := v_sql2 || t_col_value;
END LOOP;
END;

--WARNING: Do not CHANGE THE ORDER OF THE COLUMNS in the USING clause.
--They have a dependancy on the order of COLUMN_NAME's.

EXECUTE IMMEDIATE v_sql || COALESCE(v_sql2,' ') ||
                ' FROM maxdata.planworksheet' ||
                ' WHERE planworksheet_id = :in_src_planworksheet_id'
USING   in_src_planworksheet_id,
        in_create_userid,
        t_tar_planworksheet_desc,
        t_invalidate,
        in_create_userid,
        in_max_group_id,
        in_max_group_id,
        in_max_user_id,
        in_max_user_id,
        t_tar_planworksheet_name,
        t_planversion_id,
        t_tar_planworksheet_id,
        t_planwk_status,
        in_copy_submitted,
        t_submitted,
        in_copy_submitted,
        in_copy_submitted,
        t_whatif,
        t_tar_worksheet_template_id,
        in_src_planworksheet_id;


t_inserted_flag := 1;



-- copy templates belonging to this worksheet.

n_sqlnum := 8000;

maxdata.p_copy_pw_tmpl (-1, in_src_planworksheet_id, -1, t_tar_planworksheet_id, 'DIMSET');
COMMIT;

n_sqlnum := 8500;
IF in_parent_id > 0 THEN
BEGIN
        maxdata.p_copy_fav_assign (in_src_planworksheet_id, t_tar_planworksheet_id);
        COMMIT;
END;
END IF;


-- here we have to update the col with the new_top/buttom worksheet_ids in the tmp table
-- the tmp table is created in p_copy_planversion

n_sqlnum := 9000;

IF in_parent_id > 0 THEN

        UPDATE maxdata.tmp_expt_top
           SET new_top_ws_id  = t_tar_planworksheet_id
         WHERE topworkplan_id = in_src_planworksheet_id;

        UPDATE maxdata.tmp_expt_bottom
           SET new_bottom_ws_id = t_tar_planworksheet_id
         WHERE bottomwrkplan_id = in_src_planworksheet_id;
END IF;

-- Now, copy its child MPLANs.

n_sqlnum := 10000;

IF in_copy_plan_data IN (1,2) THEN

        t_list_mplan_table := 'MPLAN_WORKING,MPLAN_ATTRIB';

        IF in_copy_submitted= 1 THEN
                t_list_mplan_table := t_list_mplan_table || ',MPLAN_SUBMIT';
        END IF;

        n_sqlnum := 11000;
        LOOP
                t_counter := t_counter + 1;

                -- Find the comma position.
                t_comma_loc := INSTR(t_list_mplan_table, t_comma, 1, 1);

                IF t_comma_loc = 0 THEN
                        t_mplan_table := t_list_mplan_table;
                        t_list_mplan_table := 'EXIT';
                ELSE
                        t_mplan_table := SUBSTR(t_list_mplan_table,1,t_comma_loc - 1);
                        t_list_mplan_table := SUBSTR(t_list_mplan_table,t_comma_loc+1);
                END IF;

                n_sqlnum := 11000 + t_counter*100;
                -- Check if there is any mplans for this planworksheet.
                v_sql:= ' SELECT COUNT(*) ' ||
                        ' FROM maxdata.'||t_mplan_table||
                        ' WHERE workplan_id = '||in_src_planworksheet_id;

                EXECUTE IMMEDIATE v_sql
                INTO t_cnt;

                IF t_cnt <> 0 THEN
                        n_sqlnum := 12000 + t_counter*100;

                        -- Generate the column list from the data dictionary
                        v_sql := NULL;
                        v_sql_extra := NULL;

                        DECLARE CURSOR c_mplan_cols IS

                        SELECT column_name
                          FROM user_tab_columns
                         WHERE table_name = UPPER(t_mplan_table)
                           AND UPPER(column_name) NOT IN ( 'WORKPLAN_ID')
                         ORDER BY column_id;

                        BEGIN
                        FOR c1 IN c_mplan_cols LOOP
                                IF v_sql_extra IS NULL THEN
                                        IF LENGTH(v_sql) > 3969 THEN
                                                v_sql_extra := ',' || c1.column_name;
                                        ELSE
                                                IF v_sql IS NULL THEN
                                                        v_sql := c1.column_name;
                                                ELSE
                                                        v_sql := v_sql ||','|| c1.column_name;
                                                END IF;
                                        END IF;

                                ELSE
                                        v_sql_extra := v_sql_extra ||',' || c1.column_name;
                                END IF;

                        END LOOP;
                        END;

                        n_sqlnum := 13000 + t_counter*100;

                        EXECUTE IMMEDIATE       ' INSERT INTO maxdata.'||t_mplan_table||
                                                ' (' ||
                                                ' WORKPLAN_ID,'||
                                                v_sql ||
                                                COALESCE(v_sql_extra,' ') ||
                                                ' ) SELECT '||
                                                t_tar_planworksheet_id ||','||
                                                v_sql ||
                                                v_sql_extra ||
                                                ' FROM maxdata.'||t_mplan_table||
                                                ' WHERE workplan_id = '||
                                                in_src_planworksheet_id;

                END IF; --IF t_cnt <> 0 THEN

                IF t_list_mplan_table = 'EXIT' THEN
                        EXIT ;
                END IF;
        END LOOP;
END IF; --if in_copy_plan_data = 1 then

-- Insert an entry into the cluster history status table for p_get_cl_hist_bat.

n_sqlnum := 15000;

IF t_loc_path_id > 1000 AND in_copy_cl_hist = 1 THEN
BEGIN
        n_sqlnum := 15100;

        -- Insert cl_hist_status entries for the target worksheet as Source worksheet

        DECLARE CURSOR c_cl_status IS

        SELECT kpi_dv_id, table_nm, status
          FROM maxdata.cl_hist_status
         WHERE planworksheet_id = in_src_planworksheet_id;

        BEGIN

        FOR c1 in c_cl_status LOOP
        BEGIN
                n_sqlnum := 15200;
                maxdata.p_insert_cl_status(t_tar_planworksheet_id,c1.kpi_dv_id,in_src_planworksheet_id,t_future_param_int,t_future_param_int);

                -- Copy T_CL tables only when the status is 'OK'
                IF c1.status = 'OK' THEN
                BEGIN
                        n_sqlnum := 15300;

                        SELECT table_nm
                          INTO t_tar_cl_tab_name
                          FROM maxdata.cl_hist_status
                         WHERE planworksheet_id = t_tar_planworksheet_id
                           AND kpi_dv_id        = c1.kpi_dv_id;

                        n_sqlnum := 15500;

                        -- UDB create and insert records are two different statements

                        v_sql:= ' CREATE TABLE '|| t_tar_cl_tab_name ||
                                ' NOLOGGING PCTFREE 0 STORAGE (NEXT 10M) TABLESPACE mmax_cl_hist ' ||
                                ' AS SELECT * FROM '|| c1.table_nm;

                        n_sqlnum := 15600;
                        -- Ignore any error because p_get_cl_hist procedure
                        -- will check the existance of the table anyway
                        BEGIN
                                EXECUTE IMMEDIATE v_sql;
                        EXCEPTION
                                WHEN OTHERS THEN NULL;
                        END;
                END;
                END IF; -- IF c1.status = 'OK' THEN
        END;
        END LOOP; -- end of cl_hist_status loop
        END; -- DECLARE
END;
END IF; -- IF t_loc_path_id > 1000 AND in_copy_cl_hist = 1 THEN

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

                out_errcode := SQLCODE;
                out_errmsg := v_sql;

                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
                COMMIT;

                -- Delete planworksheet record if created.
                IF t_inserted_flag = 1 THEN
                        MAXDATA.p_del_plantable(
                                94, -- for planwrksht
                                t_tar_planworksheet_id,
                                out_errcode2,
                                out_errmsg2);
                END IF;


                IF in_parent_id <> 0 THEN
                        RAISE;
                ELSE
                        RETURN;
                END IF;

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_COPY_PLANWORKSHT" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_COPY_PLANWORKSHT" TO "MAXUSER";
