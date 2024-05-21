--------------------------------------------------------
--  DDL for Procedure P_WL_CLEANUP_JOB
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_CLEANUP_JOB" (
        in_future1                      NUMBER,         -- placeholder. Pass in -1.
        in_future2                      NUMBER,         -- placeholder. Pass in -1.
        in_future3                      NUMBER          -- placeholder. Pass in -1.
) AS

/*
$Log: 2348_p_wl_cleanup_job.sql,v $
Revision 1.14  2007/06/19 14:40:12  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.10.8.1  2007/06/05 15:32:48  vejang
Moved from 6121 to 612HF4

Revision 1.10.6.1  2007/05/09 16:18:24  anchan
S0423516: Moved truncate of t_cube*cluster tables to p_wl_truncate;

Revision 1.10  2006/09/21 16:54:24  makirk
Moved import_log cleanup from ins_import_log to p_wl_cleanup_job

Revision 1.9  2006/05/05 12:58:17  joscho
Make cluster batch and cube cleanup mutually exclusive.

Revision 1.8  2006/03/01 19:49:05  anchan
Added EXCEPTION block in each section, so that the procedure can continue after a failure in one section.

----------------------------------------------------------------------

Change History

V6.1
6.1.0-001 06/15/05 Sachin       Initial Entry

Description:

This procedure should run every night

--------------------------------------------------------------------------------*/

n_sqlnum                NUMBER(10,0);
t_proc_name             VARCHAR2(32)    := 'p_wl_cleanup_job';
t_error_level           VARCHAR2(6)     := 'info';
t_call                  VARCHAR2(1000);
v_sql                   VARCHAR2(1000)  := NULL;
t_sql2                  VARCHAR2(255);
t_sql3                  VARCHAR2(255);

t_plantable_level       NUMBER;
t_error_code            NUMBER;
t_error_msg             VARCHAR2(1000);
t_row_cnt               NUMBER;
t_cutoff_date           DATE;

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
        COALESCE(in_future1, -1) || ',' ||
        COALESCE(in_future2, -1) || ',' ||
        COALESCE(in_future3, -1) ||
        ' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
COMMIT;



n_sqlnum := 5000;

-- Delete 'Temporary Worksheets'
-- Trigger on planworksheet table ,tr_wksht_aft_d will mark WLWT record for deletion

-- Set planworksheet level
t_plantable_level := 94;

DECLARE CURSOR wksht_cur IS
SELECT planworksheet_id
FROM maxdata.planworksheet
WHERE temporary_flg = 1;
BEGIN
FOR c_row IN wksht_cur
LOOP
        n_sqlnum := 5100;

        maxdata.p_del_plantable (
                t_plantable_level,
                c_row.planworksheet_id,
                t_error_code,
                t_error_msg
                );

        IF t_error_code = 3 THEN
                -- What to do when error happens.
                NULL;
        END IF;

END LOOP;

--In  case of error, log the error message and continue with next section:
EXCEPTION
                 WHEN OTHERS THEN
                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM || ' (' || t_call ||
                                ', SQL#:' || n_sqlnum || ')';
                t_sql2 := substr(v_sql,1,255);
                t_sql3 := substr(v_sql,256,255);
                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
END;




-- Clean up Worksheet templates marked for deletion

n_sqlnum := 10000;

DECLARE CURSOR wlwt_cur IS
SELECT worksheet_template_id
FROM maxdata.WLWT_worksheet_template
WHERE deleted_flg = 1;
BEGIN
FOR c_row IN wlwt_cur
LOOP
        n_sqlnum := 10100;

        maxdata.p_wl_delete_template (
                -1,     -- in_cube_id
                c_row.worksheet_template_id,
                -1,     -- in_future1
                -1,     -- in_future2
                -1      -- in_future3
        );

END LOOP;
--In  case of error, log the error message and continue with next section:
EXCEPTION
                 WHEN OTHERS THEN
                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM || ' (' || t_call ||
                                ', SQL#:' || n_sqlnum || ')';
                t_sql2 := substr(v_sql,1,255);
                t_sql3 := substr(v_sql,256,255);
                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
END;

-- For concurrency reason, when users delete MODEL tasks
-- the delete proc just marks the record below as 'D'
n_sqlnum := 11000;

DECLARE CURSOR wlw1_cur IS
SELECT worksheet_task_no
FROM maxdata.WLW1_worksheet_task
WHERE worksheet_template_id=0
AND usage_type_cd='D';
BEGIN
FOR c_row IN wlw1_cur
LOOP
        n_sqlnum := 11100;

        maxdata.p_wl_delete_object (
                -1,     -- Pass -1 for PERMANENT object
                2,      -- Table prefix type of the object.
                1,      -- 1 for permanent , 0 for Working are the only valid values
                0,      -- template_id of the object.
                c_row.worksheet_task_no, -- Pass -1 for NULL
                -1,     -- Pass -1 for NULL
                -1,     -- Pass -1 for NULL
                NULL,   -- M,L,T or NULL
                -1,     -- Placeholder. Pass in -1.
                -1,     -- Placeholder. Pass in -1.
                -1      -- Placeholder. Pass in -1.
);
END LOOP;
--In  case of error, log the error message and continue with next section:
EXCEPTION
                 WHEN OTHERS THEN
                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM || ' (' || t_call ||
                                ', SQL#:' || n_sqlnum || ')';
                t_sql2 := substr(v_sql,1,255);
                t_sql3 := substr(v_sql,256,255);
                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
END;



-- Clean up the Working tables
-- Delete all records where the posted_dttm is older than 24 hours
n_sqlnum := 15000;

DECLARE CURSOR wlwtw_cur IS
SELECT cube_id,worksheet_template_id
FROM maxdata.WLWTW_worksheet_template
WHERE cube_id NOT IN(SELECT lock_id FROM maxdata.mmax_locks);
BEGIN
FOR c_row IN wlwtw_cur
LOOP
        n_sqlnum := 15100;

        maxdata.p_wl_delete_template (
                c_row.cube_id,
                c_row.worksheet_template_id,
                -1,     -- in_future1
                -1,     -- in_future2
                -1      -- in_future3
        );

END LOOP;
--In  case of error, log the error message and continue with next section:
EXCEPTION
                 WHEN OTHERS THEN
                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM || ' (' || t_call ||
                                ', SQL#:' || n_sqlnum || ')';
                t_sql2 := substr(v_sql,1,255);
                t_sql3 := substr(v_sql,256,255);
                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
END;

-- Clean up the import log
-- For each log_id, delete records older than 3 months and exceeding 100 rows:
n_sqlnum := 17000;
DELETE FROM maxdata.import_log
WHERE log_date<SYSDATE-93 -- older than 3 months ago
AND log_id IN
  (SELECT log_id FROM maxdata.import_log
   GROUP BY log_id
   HAVING COUNT(*)>100);

COMMIT;

BEGIN
n_sqlnum := 20000;
maxdata.p_wl_truncate_cube;
--In  case of error, log the error message and continue with next section:
EXCEPTION
        WHEN OTHERS THEN
        -- Log the error message
        t_error_level := 'error';
        v_sql := SQLERRM || ' (' || t_call ||
                       ', SQL#:' || n_sqlnum || ')';
        t_sql2 := substr(v_sql,1,255);
        t_sql3 := substr(v_sql,256,255);
        maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
END;

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
